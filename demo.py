"""Generate a sample clip and replay one SQS job against the local stack."""

import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import psycopg

from media import run
from worker import Worker


def main() -> None:
    worker = Worker()
    for bucket in (worker.input_bucket, worker.output_bucket):
        worker.s3.create_bucket(Bucket=bucket)
    worker.queue_url = worker.sqs.create_queue(
        QueueName="shot-detection", Attributes={"VisibilityTimeout": "300"},
    )["QueueUrl"]

    job_id = uuid4()
    source_key = f"{job_id}.mp4"
    with TemporaryDirectory() as tmp:
        sample = Path(tmp) / "fixture.mp4"
        run(
            "ffmpeg", "-nostdin", "-v", "error",
            "-f", "lavfi", "-i", "color=black:s=160x120:r=25:d=1",
            "-f", "lavfi", "-i", "color=white:s=160x120:r=25:d=1",
            "-f", "lavfi", "-i", "color=black:s=160x120:r=25:d=1",
            "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(sample),
        )
        worker.s3.upload_file(str(sample), worker.input_bucket, source_key)

    body = json.dumps({"job_id": str(job_id), "source_key": source_key})
    for _ in range(2):
        worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)
        worker.run_once(wait_seconds=2)
        pending = worker.sqs.get_queue_attributes(
            QueueUrl=worker.queue_url,
            AttributeNames=["ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        assert pending["ApproximateNumberOfMessagesNotVisible"] == "0", pending
        with psycopg.connect(worker.database_url) as db:
            rows = db.execute(
                """
                SELECT shot_index, start_us, end_us, frame_key FROM shots
                WHERE job_id = %s ORDER BY shot_index
                """,
                (job_id,),
            ).fetchall()
        assert len(rows) == 3, rows
        for index, start, end, key in rows:
            assert abs(start - index * 1_000_000) <= 40_000, rows
            assert abs(end - (index + 1) * 1_000_000) <= 40_000, rows
            worker.s3.head_object(Bucket=worker.output_bucket, Key=key)

    print(f"{job_id}: three shots recorded after replay")
    for index, start, end, key in rows:
        print(f"Shot {index + 1}  {start / 1e6:.2f}–{end / 1e6:.2f}s  {key}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
