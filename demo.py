"""Manually exercise duplicate deliveries and the worker's commit boundaries."""

import json
import logging
from contextlib import contextmanager
from unittest.mock import patch
from uuid import UUID, uuid4

import psycopg

from local_aws import prepare_local, upload_sample
from worker import Worker


class SimulatedExit(SystemExit):
    """Leave the message unacknowledged just as a stopped worker would."""


def enqueue_sample(worker: Worker) -> tuple[UUID, str]:
    job_id = uuid4()
    source_key = f"samples/{job_id}.mp4"
    upload_sample(worker, source_key)
    body = json.dumps({"job_id": str(job_id), "source_key": source_key})
    worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)
    return job_id, body


def job_rows(worker: Worker, job_id: UUID) -> list[tuple]:
    with psycopg.connect(worker.database_url) as db:
        return db.execute(
            """
            SELECT shot_index, start_us, end_us, frame_key FROM shots
            WHERE job_id = %s ORDER BY shot_index
            """,
            (job_id,),
        ).fetchall()


def assert_complete(worker: Worker, job_id: UUID) -> list[tuple]:
    with psycopg.connect(worker.database_url) as db:
        completed = db.execute(
            "SELECT 1 FROM completed_jobs WHERE job_id = %s", (job_id,),
        ).fetchone()
    assert completed, f"{job_id} has no completion record"
    rows = job_rows(worker, job_id)
    assert [row[0] for row in rows] == [0, 1, 2], rows
    for index, start, end, key in rows:
        # The 25 fps fixture has cuts at exactly 1s and 2s.
        assert abs(start - index * 1_000_000) <= 40_000, rows
        assert abs(end - (index + 1) * 1_000_000) <= 40_000, rows
        worker.s3.head_object(Bucket=worker.output_bucket, Key=key)
    return rows


def assert_acknowledged(worker: Worker) -> None:
    names = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    counts = worker.sqs.get_queue_attributes(
        QueueUrl=worker.queue_url, AttributeNames=names,
    )["Attributes"]
    assert all(counts[name] == "0" for name in names), counts


@contextmanager
def received_delivery(worker: Worker):
    messages = worker.sqs.receive_message(
        QueueUrl=worker.queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2,
    )["Messages"]
    with patch.object(worker.sqs, "receive_message", return_value={"Messages": messages}):
        yield messages[0]["ReceiptHandle"]


def simulate_exit(worker: Worker, target: object, method: str) -> str:
    with received_delivery(worker) as receipt:
        with patch.object(target, method, side_effect=SimulatedExit(method)):
            try:
                worker.run_once()
            except SimulatedExit:
                return receipt
    raise AssertionError(f"Expected worker to exit at {method}")


def make_visible(worker: Worker, receipt: str) -> None:
    # Advance the test without sleeping for the visibility timeout.
    worker.sqs.change_message_visibility(
        QueueUrl=worker.queue_url, ReceiptHandle=receipt, VisibilityTimeout=0,
    )


def main() -> None:
    worker = Worker()
    worker.queue_url = prepare_local(worker, f"shot-demo-{uuid4()}")

    job, body = enqueue_sample(worker)
    worker.run_once(wait_seconds=2)
    expected = assert_complete(worker, job)
    assert_acknowledged(worker)
    worker.sqs.send_message(QueueUrl=worker.queue_url, MessageBody=body)
    worker.run_once(wait_seconds=2)
    assert assert_complete(worker, job) == expected
    assert_acknowledged(worker)
    print("PASS: duplicate deliveries leave one set of three shots")

    job, _ = enqueue_sample(worker)
    receipt = simulate_exit(worker, worker, "save_results")
    assert job_rows(worker, job) == []
    with psycopg.connect(worker.database_url) as db:
        assert db.execute(
            "SELECT 1 FROM completed_jobs WHERE job_id = %s", (job,),
        ).fetchone() is None
    make_visible(worker, receipt)
    worker.run_once(wait_seconds=2)
    assert_complete(worker, job)
    assert_acknowledged(worker)
    print("PASS: delivery retried after exit before PostgreSQL commit")

    job, _ = enqueue_sample(worker)
    receipt = simulate_exit(worker, worker.sqs, "delete_message")
    expected = assert_complete(worker, job)
    make_visible(worker, receipt)
    worker.run_once(wait_seconds=2)
    assert assert_complete(worker, job) == expected
    assert_acknowledged(worker)
    print("PASS: delivery acknowledged after exit following PostgreSQL commit")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
