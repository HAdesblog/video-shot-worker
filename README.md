# Video Shot Worker

A small Python backend that takes a video job from SQS, detects hard cuts with FFmpeg, uploads a representative JPEG for each shot to S3, and stores ordered shot metadata in PostgreSQL.

## Run the local demo

From the repository root, with Docker and Docker Compose installed:

    docker compose run --rm worker python demo.py

The demo brings up PostgreSQL and Moto, a local S3/SQS emulator. It generates a three-second clip with hard cuts at one and two seconds, uploads the clip, and sends the same job twice. Its assertions check for exactly three ordered shots, boundaries within one frame of the expected timestamps, an S3 image for each shot, and acknowledgement after each delivery.

## How a job works

1. Upload a video to the input bucket and send an SQS message with a stable job UUID and the object's key.
2. The worker downloads the video, uses FFmpeg's scene score to detect cuts, and uploads one JPEG per shot at a deterministic key.
3. The worker commits the completion record and all shot rows in one PostgreSQL transaction, then deletes the SQS message. The job ID is unique, so a redelivered message cannot create a second set of rows.

Example message:

    {"job_id":"191d3a83-85ec-4c77-a835-9324bbe54287","source_key":"uploads/clip.mp4"}

Each shot stores its zero-based order, start and end times in microseconds, and frame object key. Treat the source object as immutable for a given job ID.

## Run the worker against AWS

Provision the input and frame buckets and an SQS queue in the desired AWS region. Apply [schema.sql](schema.sql) to PostgreSQL. Configure the AWS region and credentials through the standard SDK provider chain, set DATABASE_URL, QUEUE_URL, INPUT_BUCKET, and OUTPUT_BUCKET, and run python worker.py with FFmpeg and FFprobe installed. Omit AWS_ENDPOINT_URL when using AWS.

This demo focuses on short clips and hard cuts. For longer jobs, configure a suitable SQS visibility timeout or renew it during processing. Configure a dead-letter queue in AWS to isolate jobs that repeatedly fail.
