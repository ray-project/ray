#!/bin/bash
# ABOUTME: Downloads Anyscale job logs and S3 telemetry to a local directory.
# ABOUTME: Takes a job ID and S3 prefix as arguments, creates a directory named after the job ID.

set -euo pipefail

JOB_ID="${1:?Usage: download_job_output <job_id> <s3_prefix>}"
S3_PREFIX="${2:?Usage: download_job_output <job_id> <s3_prefix>}"
S3_BUCKET="${PROFILING_S3_BUCKET:-anyscale-staging-data-cld-kvedzwag2qa8i5bjxuevf5i7}"

echo "=== Downloading job logs ==="
anyscale logs job --id "$JOB_ID" --download --download-dir .

cd "$JOB_ID"

echo ""
echo "=== Downloading telemetry from S3 ==="
aws s3 cp "s3://${S3_BUCKET}/${S3_PREFIX}/" . --recursive

echo ""
echo "=== Done ==="
echo "Output in: $(pwd)"
ls -1
