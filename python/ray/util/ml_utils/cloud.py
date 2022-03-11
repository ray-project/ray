import subprocess

from ray import logger

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)


def is_cloud_target(target: str):
    return any(target.startswith(prefix) for prefix in ALLOWED_REMOTE_PREFIXES)


def clear_bucket(bucket: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not clear bucket contents: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    try:
        if bucket.startswith(S3_PREFIX):
            subprocess.check_call(["aws", "s3", "rm", "--recursive", "--quiet", bucket])
        elif bucket.startswith(GS_PREFIX):
            subprocess.check_call(["gsutil", "-m", "rm", "-f", "-r", bucket])
        elif bucket.startswith(HDFS_PREFIX):
            subprocess.check_call(["hdfs", "dfs", "-rm", "-r", bucket])

    except Exception as e:
        logger.warning(f"Caught exception when clearing bucket `{bucket}`: {e}")


def download_from_bucket(bucket: str, local_path: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    if bucket.startswith(S3_PREFIX):
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", bucket, local_path]
        )
    elif bucket.startswith(GS_PREFIX):
        subprocess.check_call(["gsutil", "-m", "cp", "-r", bucket, local_path])
    elif bucket.startswith(HDFS_PREFIX):
        subprocess.check_call(["hdfs", "dfs", "-get", bucket, local_path])


def upload_to_bucket(bucket: str, local_path: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    if bucket.startswith(S3_PREFIX):
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", local_path, bucket]
        )
    elif bucket.startswith(GS_PREFIX):
        subprocess.check_call(["gsutil", "-m", "cp", "-r", local_path, bucket])
    elif bucket.startswith(HDFS_PREFIX):
        subprocess.check_call(["hdfs", "dfs", "-put", local_path, bucket])
