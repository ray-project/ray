# ABOUTME: Uploads profiling and monitoring artifacts from shared storage to S3.
# ABOUTME: Matches files by glob patterns and uploads them under a job-specific S3 prefix.

import glob as globmod
import os


DEFAULT_S3_BUCKET = os.environ.get(
    "PROFILING_S3_BUCKET",
    "anyscale-staging-data-cld-kvedzwag2qa8i5bjxuevf5i7",
)

DEFAULT_UPLOAD_PATTERNS = [
    "gpu_usage*",
    "net_counters*",
    "nsys_*",
    "torch_profile_*",
    "driver_gc*",
    "pyspy_*",
    "perf_*",
    "result*.json",
    "object_store_state*",
    "actor_placement*",
    "plasma_stats*",
]


def upload(outdir, s3_prefix, s3_bucket=None, patterns=None):
    """Upload telemetry files from outdir to S3.

    Args:
        outdir: Local directory containing profiling/monitoring output.
        s3_prefix: S3 key prefix (e.g. "image-embedding-jsonl/<job_id>").
        s3_bucket: S3 bucket name. Defaults to PROFILING_S3_BUCKET env var.
        patterns: List of glob patterns to match. Defaults to DEFAULT_UPLOAD_PATTERNS.
    """
    import boto3

    if s3_bucket is None:
        s3_bucket = DEFAULT_S3_BUCKET
    if patterns is None:
        patterns = DEFAULT_UPLOAD_PATTERNS

    s3 = boto3.client("s3")
    total_uploaded = 0
    for pattern in patterns:
        files = globmod.glob(os.path.join(outdir, pattern))
        for filepath in files:
            key = f"{s3_prefix}/{os.path.basename(filepath)}"
            print(f"Uploading {filepath} -> s3://{s3_bucket}/{key}")
            s3.upload_file(filepath, s3_bucket, key)
            total_uploaded += 1
    print(f"Uploaded {total_uploaded} telemetry files to s3://{s3_bucket}/{s3_prefix}/")
