"""Consume SQS jobs and persist ordered shot metadata in PostgreSQL."""

import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID

import boto3
import psycopg
from botocore.config import Config

from media import extract_frame, shot_ranges

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self) -> None:
        self.database_url = os.environ["DATABASE_URL"]
        self.queue_url = os.environ["QUEUE_URL"]
        self.input_bucket = os.environ["INPUT_BUCKET"]
        self.output_bucket = os.environ["OUTPUT_BUCKET"]
        self.sqs = boto3.client("sqs")
        self.s3 = boto3.client(
            "s3", config=Config(s3={"addressing_style": "path"}),
        )

    def process(self, job_id: UUID, source_key: str) -> None:
        with psycopg.connect(self.database_url) as db:
            completed = db.execute(
                "SELECT 1 FROM completed_jobs WHERE job_id = %s", (job_id,),
            ).fetchone()
        if completed:
            logger.info("Job %s already completed", job_id)
            return

        rows = []
        with TemporaryDirectory() as tmp:
            workdir = Path(tmp)
            source = workdir / "source"
            self.s3.download_file(self.input_bucket, source_key, str(source))
            for index, (start, end) in enumerate(shot_ranges(source)):
                image = workdir / f"{index}.jpg"
                extract_frame(source, start, end, image)
                key = f"{job_id}/{index}.jpg"
                self.s3.upload_file(
                    str(image), self.output_bucket, key,
                    ExtraArgs={"ContentType": "image/jpeg"},
                )
                rows.append((job_id, index, start, end, key))

        with psycopg.connect(self.database_url) as db, db.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO completed_jobs (job_id, source_key)
                VALUES (%s, %s)
                ON CONFLICT (job_id) DO NOTHING
                RETURNING job_id
                """,
                (job_id, source_key),
            )
            if cursor.fetchone():
                cursor.executemany(
                    """
                    INSERT INTO shots (job_id, shot_index, start_us, end_us, frame_key)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    rows,
                )
        logger.info("Processed job %s with %s shots", job_id, len(rows))

    def run_once(self, wait_seconds: int = 20) -> None:
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
        )
        for message in response.get("Messages", []):
            try:
                payload = json.loads(message["Body"])
                job_id = UUID(payload["job_id"])
                source_key = payload["source_key"]
                if not isinstance(source_key, str) or not source_key:
                    raise ValueError("source_key must be a nonempty string")
                self.process(job_id, source_key)
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )
            except Exception:
                logger.exception("Message %s remains eligible for retry", message["MessageId"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = Worker()
    while True:
        worker.run_once()
