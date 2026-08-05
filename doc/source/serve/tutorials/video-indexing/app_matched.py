"""Ray Serve app for the IDENTICAL-setup benchmark vs SageMaker Async Inference.

To match SageMaker's monolithic endpoint exactly, this collapses our normal
disaggregated (consumer + separate GPU encoder) design into a SINGLE GPU
deployment that does the whole pipeline per request (download -> ffmpeg chunk ->
SigLIP encode -> write embeddings), on g4dn.xlarge (1 T4 each), autoscaling
1 -> 4 on Redis queue depth. That mirrors "one GPU box per task, scale 1->4 on
backlog" so the only variable vs SageMaker is the orchestration engine.

import path: app_matched:app
"""

import asyncio
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import boto3
import numpy as np
import torch
from fastapi import FastAPI
from pydantic import BaseModel
from ray import serve
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)
from transformers import AutoImageProcessor, AutoModel

from constants import (
    DEFAULT_CHUNK_DURATION,
    DEFAULT_NUM_FRAMES,
    FAILED_QUEUE,
    FFMPEG_THREADS,
    INDEX_QUEUE,
    MAX_RETRIES,
    MODEL_NAME,
    REDIS_BACKEND_URL,
    REDIS_BROKER_URL,
    TASK_INDEX_VIDEO,
    UNPROCESSABLE_QUEUE,
    VISIBILITY_TIMEOUT_S,
)
from utils.embedding_store import write_video_embeddings
from utils.idempotency import is_done, mark_done, video_id_for
from utils.s3 import parse_s3_uri
from utils.video import chunk_video, frames_to_pil_list

logger = logging.getLogger("ray.serve")

PROCESSOR_CONFIG = TaskProcessorConfig(
    queue_name=INDEX_QUEUE,
    adapter_config=CeleryAdapterConfig(
        broker_url=REDIS_BROKER_URL,
        backend_url=REDIS_BACKEND_URL,
        app_custom_config={
            "broker_pool_limit": 128,
            "task_publish_retry": False,
            "task_send_sent_event": False,
            "worker_send_task_events": False,
        },
        broker_transport_options={
            "visibility_timeout": VISIBILITY_TIMEOUT_S,
            "socket_keepalive": True,
            "health_check_interval": 30,
        },
    ),
    max_retries=MAX_RETRIES,
    failed_task_queue_name=FAILED_QUEUE,
    unprocessable_task_queue_name=UNPROCESSABLE_QUEUE,
)

fastapi_app = FastAPI(title="Matched Video Indexing (vs SageMaker)")


class IndexRequest(BaseModel):
    video_uri: str
    video_id: Optional[str] = None


@serve.deployment(
    max_ongoing_requests=128,
    ray_actor_options={"num_cpus": 1},
    # Spread ingress replicas one-per-node so each gets its own HTTP proxy; this
    # keeps enqueue-ack latency low under load (measured ~12.7 ms p50 vs ~19.4 ms
    # when the ingress is packed onto a single node).
    max_replicas_per_node=1,
    autoscaling_config=AutoscalingConfig(
        min_replicas=6,
        max_replicas=8,
        upscale_delay_s=0,
        downscale_delay_s=300,
        upscaling_factor=2.0,
    ),
)
@serve.ingress(fastapi_app)
class IndexingIngress:
    """Front-end: accept + enqueue (analogous to the SageMaker endpoint front)."""

    def __init__(self, indexer):
        self.adapter = instantiate_adapter_from_config(PROCESSOR_CONFIG)
        self.enqueue_pool = ThreadPoolExecutor(max_workers=64, thread_name_prefix="enqueue")
        logger.info("IndexingIngress ready")

    @fastapi_app.post("/index")
    async def index(self, req: IndexRequest):
        loop = asyncio.get_running_loop()

        def _enqueue():
            return self.adapter.enqueue_task_sync(
                task_name=TASK_INDEX_VIDEO,
                kwargs={"video_uri": req.video_uri, "video_id": req.video_id},
                ignore_result=True,
                retry=False,
            )

        result = await loop.run_in_executor(self.enqueue_pool, _enqueue)
        return {"task_id": result.id, "status": result.status}

    @fastapi_app.get("/healthz")
    def healthz(self):
        return {"status": "ok"}

    @fastapi_app.get("/replicas")
    def replicas(self):
        status = serve.status()
        out = {}
        app = status.applications.get("video-indexing")
        if app is not None:
            for name, dep in app.deployments.items():
                out[name] = {k.value: v for k, v in dep.replica_states.items()}
        return out

    @fastapi_app.post("/admin/flush")
    def admin_flush(self):
        import redis as _redis

        out = {}
        for label, url in [("broker", REDIS_BROKER_URL), ("backend", REDIS_BACKEND_URL)]:
            try:
                _redis.from_url(url).flushdb()
                out[label] = "flushed"
            except Exception as e:
                out[label] = repr(e)
        return out

    @fastapi_app.get("/queue")
    async def queue(self):
        import redis as _redis

        def _read():
            out = {"queue_name": INDEX_QUEUE}
            try:
                r = _redis.from_url(REDIS_BROKER_URL)
                out["queue_depth"] = r.llen(INDEX_QUEUE)
                out["dlq_depth"] = r.llen(FAILED_QUEUE)
            except Exception as e:
                out["redis_error"] = repr(e)
            return out

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.enqueue_pool, _read)


@serve.deployment(
    ray_actor_options={"num_gpus": 1, "num_cpus": 3},
    max_ongoing_requests=4,
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=4,
        target_ongoing_requests=2,
        upscale_delay_s=0,
        downscale_delay_s=5,
        metrics_interval_s=1,
        look_back_period_s=2,
        policy=AutoscalingPolicy(
            policy_function="ray.serve.async_inference_autoscaling_policy:AsyncInferenceAutoscalingPolicy",
            policy_kwargs={
                "broker_url": REDIS_BROKER_URL,
                "queue_name": INDEX_QUEUE,
                "poll_interval_s": 1.0,
            },
        ),
    ),
)
@task_consumer(task_processor_config=PROCESSOR_CONFIG)
class MatchedIndexer:
    """One GPU box does the whole pipeline per task (mirror of the SageMaker handler)."""

    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
        self.model = AutoModel.from_pretrained(MODEL_NAME).to(self.device)
        self.model.eval()

    def _encode(self, frames: np.ndarray) -> np.ndarray:
        pil_images = frames_to_pil_list(frames)
        inputs = self.processor(images=pil_images, return_tensors="pt").to(self.device)
        with torch.no_grad():
            with torch.amp.autocast(device_type=self.device, enabled=self.device == "cuda"):
                outputs = self.model.get_image_features(**inputs)
                emb = torch.nn.functional.normalize(outputs, p=2, dim=1)
        return emb.cpu().numpy().astype(np.float32)

    @task_handler(name=TASK_INDEX_VIDEO)  # MUST be synchronous
    def index_video(self, video_uri: str, video_id: Optional[str] = None):
        vid = video_id or video_id_for(video_uri)
        if is_done(vid):
            return {"video_id": vid, "status": "skipped_already_indexed"}

        bucket, key = parse_s3_uri(video_uri)
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
            per_chunk = [self._encode(c.frames) for c in chunks]
            frame_embeddings = np.concatenate(per_chunk, axis=0)
            pooled = frame_embeddings.mean(axis=0)
            pooled = pooled / (np.linalg.norm(pooled) + 1e-12)
            chunk_meta = [
                {"index": c.index, "start_time": c.start_time, "duration": c.duration}
                for c in chunks
            ]
            write_video_embeddings(vid, video_uri, pooled, frame_embeddings, chunk_meta)
            mark_done(vid)
            return {"video_id": vid, "status": "indexed", "num_frames": int(frame_embeddings.shape[0])}
        finally:
            Path(tmp.name).unlink(missing_ok=True)


indexer = MatchedIndexer.bind()
app = IndexingIngress.bind(indexer=indexer)
