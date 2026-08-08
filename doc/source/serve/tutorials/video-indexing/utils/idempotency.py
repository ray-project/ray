"""Idempotency helpers: deterministic video id + an S3 done-marker.

At-least-once delivery means a task can be redelivered (retry, replica crash).
A deterministic video_id plus a done-marker lets the handler skip work that
already completed, so redelivery does not re-index or duplicate embeddings.
"""

import hashlib

import boto3
from botocore.exceptions import ClientError

from constants import S3_BUCKET, S3_DONE_PREFIX


def video_id_for(video_uri: str) -> str:
    """Deterministic id for a video URI (stable across redeliveries)."""
    return hashlib.sha1(video_uri.encode()).hexdigest()[:16]


def _done_key(video_id: str) -> str:
    return f"{S3_DONE_PREFIX}{video_id}.done"


def is_done(video_id: str) -> bool:
    """True if this video was already indexed (done-marker exists in S3)."""
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=_done_key(video_id))
        return True
    except ClientError as e:
        # Missing marker = not done. Let real errors (auth, network) propagate
        # rather than silently re-indexing.
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def mark_done(video_id: str) -> None:
    """Write the done-marker after a successful index."""
    s3 = boto3.client("s3")
    s3.put_object(Bucket=S3_BUCKET, Key=_done_key(video_id), Body=b"")
