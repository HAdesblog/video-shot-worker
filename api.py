"""Submit S3 objects for processing and read shot results."""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

import boto3
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, StringConstraints


class JobSubmission(BaseModel):
    job_id: UUID
    source_key: Annotated[str, StringConstraints(min_length=1, max_length=1024)]


class Shot(BaseModel):
    index: int
    start_us: int
    end_us: int
    frame_key: str


class JobStatus(BaseModel):
    job_id: UUID
    source_key: str
    status: Literal["pending", "queued", "completed"]
    completed_at: datetime | None = None
    frame_bucket: str
    shots: list[Shot]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.database_url = os.environ["DATABASE_URL"]
    app.state.queue_url = os.environ["QUEUE_URL"]
    app.state.input_bucket = os.environ["INPUT_BUCKET"]
    app.state.output_bucket = os.environ["OUTPUT_BUCKET"]
    app.state.sqs = boto3.client("sqs")
    app.state.s3 = boto3.client("s3", config=Config(s3={"addressing_style": "path"}))
    yield


app = FastAPI(title="Video Shot Worker", lifespan=lifespan)


@app.post("/jobs", response_model=JobStatus, status_code=status.HTTP_202_ACCEPTED)
def submit_job(job: JobSubmission, request: Request) -> JobStatus:
    state = request.app.state
    with psycopg.connect(state.database_url) as db:
        completed = db.execute(
            "SELECT source_key FROM completed_jobs WHERE job_id = %s", (job.job_id,),
        ).fetchone()
        if completed and completed[0] != job.source_key:
            raise HTTPException(409, "This job ID already belongs to another object")
        if not completed:
            db.execute(
                """
                INSERT INTO submitted_jobs (job_id, source_key)
                VALUES (%s, %s) ON CONFLICT (job_id) DO NOTHING
                """,
                (job.job_id, job.source_key),
            )
    # Reserve the ID durably before publishing. A retry must use the same source.
    if not completed:
        enqueue_job(job, request)
    return read_job(job.job_id, request)


def enqueue_job(job: JobSubmission, request: Request) -> None:
    state = request.app.state
    with psycopg.connect(state.database_url) as db:
        source_key, enqueued_at = db.execute(
            """
            SELECT source_key, enqueued_at FROM submitted_jobs
            WHERE job_id = %s FOR UPDATE
            """,
            (job.job_id,),
        ).fetchone()
        if source_key != job.source_key:
            raise HTTPException(409, "This job ID already belongs to another object")
        if enqueued_at is not None:
            return

        try:
            state.s3.head_object(Bucket=state.input_bucket, Key=source_key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                raise HTTPException(404, "Video was not found in the input bucket") from exc
            raise HTTPException(503, "Input storage is unavailable") from exc
        except BotoCoreError as exc:
            raise HTTPException(503, "Input storage is unavailable") from exc

        try:
            state.sqs.send_message(QueueUrl=state.queue_url, MessageBody=job.model_dump_json())
        except (BotoCoreError, ClientError) as exc:
            raise HTTPException(503, "Queue unavailable. Retry with the same job ID.") from exc

        db.execute(
            "UPDATE submitted_jobs SET enqueued_at = now() WHERE job_id = %s",
            (job.job_id,),
        )


@app.get("/jobs/{job_id}", response_model=JobStatus)
def read_job(job_id: UUID, request: Request) -> JobStatus:
    state = request.app.state
    with psycopg.connect(state.database_url) as db:
        job = db.execute(
            """
            SELECT coalesce(done.source_key, submitted.source_key),
                   submitted.enqueued_at, done.completed_at
            FROM submitted_jobs AS submitted
            FULL JOIN completed_jobs AS done USING (job_id)
            WHERE job_id = %s
            """,
            (job_id,),
        ).fetchone()
        if job is None:
            raise HTTPException(404, "Job not found")
        source_key, enqueued_at, completed_at = job
        shots = []
        if completed_at:
            shots = [
                Shot(index=index, start_us=start, end_us=end, frame_key=key)
                for index, start, end, key in db.execute(
                    """
                    SELECT shot_index, start_us, end_us, frame_key
                    FROM shots WHERE job_id = %s ORDER BY shot_index
                    """,
                    (job_id,),
                )
            ]

    job_status: Literal["pending", "queued", "completed"] = "pending"
    if completed_at:
        job_status = "completed"
    elif enqueued_at:
        job_status = "queued"
    return JobStatus(
        job_id=job_id, source_key=source_key, status=job_status,
        completed_at=completed_at, frame_bucket=state.output_bucket, shots=shots,
    )
