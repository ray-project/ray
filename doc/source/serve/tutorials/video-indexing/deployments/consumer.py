"""VideoIndexConsumer deployment.

The @task_consumer that does the work: pull a task, download the video from S3,
chunk it with ffmpeg, encode each chunk on the GPU encoder, and store per-video
embeddings to S3. Autoscales on Redis queue depth.
"""

import tempfile
from pathlib import Path
from typing import Optional

import boto3
import numpy as np
from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
from ray.serve.task_consumer import task_consumer, task_handler

from constants import (
    DEFAULT_CHUNK_DURATION,
    DEFAULT_NUM_FRAMES,
    FFMPEG_THREADS,
    INDEX_QUEUE,
    REDIS_BROKER_URL,
    TASK_INDEX_VIDEO,
)
from deployments.processor import PROCESSOR_CONFIG
from utils.embedding_store import write_video_embeddings
from utils.idempotency import is_done, mark_done, video_id_for
from utils.s3 import parse_s3_uri
from utils.video import chunk_video


@serve.deployment(
    ray_actor_options={"num_cpus": FFMPEG_THREADS},
    max_ongoing_requests=5,
    # Scale on Redis queue depth: the policy polls the broker and combines
    # queue length with in-flight requests to pick the replica count.
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=40,
        # Higher target + long look-back + gentle downscale = smooth proportional
        # tracking rather than a 1<->max limit cycle; min_replicas=1 lets the pool
        # return to a single replica when the queue is empty.
        target_ongoing_requests=4,
        upscale_delay_s=15,
        downscale_delay_s=60,
        metrics_interval_s=2,
        look_back_period_s=45,
        downscaling_factor=0.4,
        policy=AutoscalingPolicy(
            policy_function="ray.serve.async_inference_autoscaling_policy:AsyncInferenceAutoscalingPolicy",
            policy_kwargs={
                "broker_url": REDIS_BROKER_URL,
                "queue_name": INDEX_QUEUE,
                "poll_interval_s": 2.0,
            },
        ),
    ),
)
@task_consumer(task_processor_config=PROCESSOR_CONFIG)
class VideoIndexConsumer:
    """
    Consumes index tasks: download the video from S3, chunk it with ffmpeg,
    encode each chunk on the GPU encoder, and store embeddings to S3.

    The handler is synchronous (required) and blocks on the GPU DeploymentHandle
    from a Celery worker thread (safe: no running asyncio loop there). It is
    idempotent on video_id so at-least-once redelivery does not re-index.
    """

    def __init__(self, encoder):
        self.encoder = encoder

    @task_handler(name=TASK_INDEX_VIDEO)  # MUST be synchronous
    def index_video(self, video_uri: str, video_id: Optional[str] = None):
        vid = video_id or video_id_for(video_uri)
        if is_done(vid):
            return {"video_id": vid, "status": "skipped_already_indexed"}

        bucket, key = parse_s3_uri(video_uri)
        # boto3 client per call: it is not safe to share across worker threads.
        s3 = boto3.client("s3")
        suffix = Path(key).suffix or ".mp4"
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        tmp.close()
        try:
            s3.download_file(bucket, key, tmp.name)
            chunks = chunk_video(
                tmp.name,
                chunk_duration=DEFAULT_CHUNK_DURATION,
                num_frames_per_chunk=DEFAULT_NUM_FRAMES,
                ffmpeg_threads=FFMPEG_THREADS,
                use_single_ffmpeg=True,
            )
            if not chunks:
                raise ValueError(f"No chunks extracted from {video_uri}")

            # Fire all chunk encodes concurrently (object-store refs), then block.
            refs = [self.encoder.remote(c.frames) for c in chunks]
            per_chunk = [r.result()["frame_embeddings"] for r in refs]

            frame_embeddings = np.concatenate(per_chunk, axis=0)
            pooled = frame_embeddings.mean(axis=0)
            pooled = pooled / (np.linalg.norm(pooled) + 1e-12)
            chunk_meta = [
                {"index": c.index, "start_time": c.start_time, "duration": c.duration}
                for c in chunks
            ]

            write_video_embeddings(vid, video_uri, pooled, frame_embeddings, chunk_meta)
            mark_done(vid)
            return {
                "video_id": vid,
                "status": "indexed",
                "num_chunks": len(chunks),
                "num_frames": int(frame_embeddings.shape[0]),
                "embedding_dim": int(pooled.shape[0]),
            }
        finally:
            Path(tmp.name).unlink(missing_ok=True)
