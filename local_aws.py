"""Provision the local S3/SQS emulator and optionally upload a sample clip."""

import argparse
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from botocore.exceptions import ClientError

from media import run
from worker import Worker


def prepare_local(worker: Worker, queue_name: str) -> str:
    if not os.environ.get("AWS_ENDPOINT_URL"):
        raise SystemExit("This command requires a local AWS emulator endpoint")
    for bucket in (worker.input_bucket, worker.output_bucket):
        try:
            worker.s3.create_bucket(Bucket=bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise
    return worker.sqs.create_queue(
        QueueName=queue_name, Attributes={"VisibilityTimeout": "300"},
    )["QueueUrl"]


def upload_sample(worker: Worker, source_key: str) -> None:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upload-sample", action="store_true")
    args = parser.parse_args()

    worker = Worker()
    prepare_local(worker, "shot-detection")
    if args.upload_sample:
        job_id = str(uuid4())
        source_key = f"samples/{job_id}.mp4"
        upload_sample(worker, source_key)
        print(json.dumps({"job_id": job_id, "source_key": source_key}))
