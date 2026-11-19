# Video Shot Worker

A Python backend that takes an SQS job, downloads a video from S3, detects hard cuts with FFmpeg, and saves each shot's time range and preview image location in PostgreSQL.

## Submit a clip and fetch its shots

Start the local services with Docker Compose. Moto supplies the S3 and SQS APIs locally.

    docker compose run --rm -T worker python local_aws.py --upload-sample > /tmp/shot-job.json
    docker compose up -d api worker

The setup command uploads a generated MP4 and writes a job UUID and source key to /tmp/shot-job.json. Use the same JSON body to retry submission.

    curl --fail-with-body -sS \
      -H 'Content-Type: application/json' \
      --data-binary @/tmp/shot-job.json \
      http://localhost:8008/jobs

    job_id=$(python3 -c 'import json; print(json.load(open("/tmp/shot-job.json"))["job_id"])')
    curl --fail-with-body -sS "http://localhost:8008/jobs/$job_id"

Repeat the GET until the sample reaches completed. The response includes three shot time ranges in microseconds and the corresponding JPEG keys. Interactive API documentation is available at http://localhost:8008/docs.

| Route | Behavior |
| --- | --- |
| POST /jobs | Accepts a client-generated job UUID and an existing object key. Creates one logical job, even if the client retries the request. Reusing the UUID for a different key returns 409. |
| GET /jobs/{job_id} | Returns pending, queued, or completed status. Completed jobs include ordered shot rows. Workers can also process raw SQS messages with the same JSON contract. |

## Exercise failure recovery locally

    docker compose run --rm worker python demo.py

The fixture changes from black to white at 1 second and back to black at 2 seconds. The script checks for exactly three ordered shots with boundaries within one frame of the expected timestamps and a JPEG in S3 for each shot.

- Deliver the same job twice and compare the saved results.
- Simulate a worker exit before writing its PostgreSQL results. Make the unacknowledged SQS delivery visible again and verify the retry completes the job.
- Simulate a worker exit after PostgreSQL commits but before SQS acknowledgement. Redeliver the same message and verify the shot rows stay identical and the message is acknowledged.

## Delivery and persistence

The API reserves the job UUID in PostgreSQL before sending an SQS message. If the queue request fails or its response is lost, repeat the POST with the same UUID and key. The worker commits the completion record and all shot rows in one transaction, then acknowledges the message. The completion record is unique per UUID. Preview JPEGs use deterministic keys, so repeating work for the same job replaces the same objects. Keep the source object immutable for the life of a job.

To use AWS, provision the input and output buckets and the SQS queue. Apply schema.sql to PostgreSQL, including when upgrading an existing database from the worker-only version. Provide DATABASE_URL, QUEUE_URL, INPUT_BUCKET, and OUTPUT_BUCKET and use the standard AWS SDK credentials and region configuration. Start python worker.py and uvicorn api:app separately. Set a visibility timeout appropriate to your job duration and configure a dead-letter queue for repeated failures.

The API is a single-tenant demo that binds to localhost in Docker Compose. Add authentication and tenant-scoped object access before exposing it on a shared network.
