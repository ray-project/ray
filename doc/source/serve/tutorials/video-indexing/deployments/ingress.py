"""IndexingIngress deployment.

The producer side of the service: accepts POST /index, enqueues the task onto
the broker, and returns a task id immediately. Also exposes observability routes
(/queue, /replicas, /status/{id}). It only enqueues; the consumer does the work.
"""

import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel
from ray import serve
from ray.serve.config import AutoscalingConfig
from ray.serve.task_consumer import instantiate_adapter_from_config

from constants import (
    FAILED_QUEUE,
    INDEX_QUEUE,
    REDIS_BACKEND_URL,
    REDIS_BROKER_URL,
    TASK_INDEX_VIDEO,
)
from deployments.processor import PROCESSOR_CONFIG

logger = logging.getLogger("ray.serve")

fastapi_app = FastAPI(title="Async Video Indexing")


class IndexRequest(BaseModel):
    video_uri: str  # s3://bucket/key
    video_id: Optional[str] = None


@serve.deployment(
    max_ongoing_requests=128,
    ray_actor_options={"num_cpus": 1},
    # Spread ingress replicas one-per-node so each gets its own HTTP proxy; this
    # keeps enqueue-ack latency low under load (one proxy per node fans out the
    # request handling instead of queuing behind a couple of busy proxies).
    max_replicas_per_node=1,
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=6,
        target_ongoing_requests=10,
        upscale_delay_s=10,
        downscale_delay_s=30,
    ),
)
@serve.ingress(fastapi_app)
class IndexingIngress:
    """Accepts index requests, enqueues them, and reports task status."""

    def __init__(self, consumer):
        # Producer-side adapter. instantiate_adapter_from_config calls
        # initialize() but never start_consumer(), so this deployment only
        # enqueues. `consumer` is bound only to include it in the app graph.
        self.adapter = instantiate_adapter_from_config(PROCESSOR_CONFIG)
        # Offload the blocking Celery enqueue off the event loop so the replica
        # keeps accepting and acking requests while each send is in flight.
        self.enqueue_pool = ThreadPoolExecutor(
            max_workers=64, thread_name_prefix="enqueue"
        )
        logger.info("IndexingIngress ready")

    @fastapi_app.post("/index")
    async def index(self, req: IndexRequest):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.enqueue_pool,
            functools.partial(
                self.adapter.enqueue_task_sync,
                task_name=TASK_INDEX_VIDEO,
                kwargs={"video_uri": req.video_uri, "video_id": req.video_id},
                # keep the result so GET /status/{task_id} can report completion
                ignore_result=False,
                retry=False,
            ),
        )
        return {"task_id": result.id, "status": result.status}

    @fastapi_app.get("/status/{task_id}")
    async def status(self, task_id: str):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.enqueue_pool,
            functools.partial(self.adapter.get_task_status_sync, task_id),
        )
        return {"task_id": task_id, "status": result.status, "result": result.result}

    @fastapi_app.get("/healthz")
    def healthz(self):
        return {"status": "ok"}

    @fastapi_app.get("/replicas")
    def replicas(self):
        # Per-deployment running replica counts, for observing autoscaling.
        status = serve.status()
        out = {}
        app = status.applications.get("video-indexing")
        if app is not None:
            for name, dep in app.deployments.items():
                out[name] = {k.value: v for k, v in dep.replica_states.items()}
        return out

    @fastapi_app.get("/proxies")
    def proxies(self):
        # HTTP proxy count + status per node (Serve runs one proxy per node).
        status = serve.status()
        pr = getattr(status, "proxies", {}) or {}
        out = {"count": len(pr)}
        try:
            out["states"] = {
                str(nid): str(getattr(p, "status", p)) for nid, p in pr.items()
            }
        except Exception as e:
            out["error"] = repr(e)
        return out

    @fastapi_app.post("/admin/flush")
    def admin_flush(self):
        # Clear the broker + backend Redis DBs (reset the queue between runs).
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
        # Queue depth (and dead-letter depth) for observing autoscaling.
        # ElastiCache is VPC-only, so this is read from inside the cluster.
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
