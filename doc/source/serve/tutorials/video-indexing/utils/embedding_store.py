"""Write indexed video embeddings to S3 (the indexing output).

Per-video shards (one .npz of embeddings + one .json manifest per video) avoid
an append race when multiple consumer replicas index concurrently. Same video_id
overwrites the same keys, so re-indexing is idempotent. No query side in v1.
"""

import io
import json

import boto3
import numpy as np

from constants import S3_BUCKET, S3_EMBEDDINGS_PREFIX, S3_MANIFEST_PREFIX


def write_video_embeddings(
    video_id: str,
    video_uri: str,
    pooled: np.ndarray,
    frame_embeddings: np.ndarray,
    chunk_meta: list[dict],
) -> dict:
    """Store pooled + per-frame embeddings (.npz) and a manifest shard (.json)."""
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable is not set")
    s3 = boto3.client("s3")

    buf = io.BytesIO()
    np.savez_compressed(buf, pooled=pooled, frame_embeddings=frame_embeddings)
    embeddings_key = f"{S3_EMBEDDINGS_PREFIX}{video_id}.npz"
    s3.put_object(Bucket=S3_BUCKET, Key=embeddings_key, Body=buf.getvalue())

    manifest = {
        "video_id": video_id,
        "video_uri": video_uri,
        "num_chunks": len(chunk_meta),
        "num_frames": int(frame_embeddings.shape[0]),
        "embedding_dim": int(pooled.shape[0]),
        "chunks": chunk_meta,
    }
    manifest_key = f"{S3_MANIFEST_PREFIX}{video_id}.json"
    s3.put_object(
        Bucket=S3_BUCKET, Key=manifest_key, Body=json.dumps(manifest).encode()
    )

    return {"embeddings_key": embeddings_key, "manifest_key": manifest_key}
