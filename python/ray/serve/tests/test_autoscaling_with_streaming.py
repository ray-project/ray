import asyncio
import os
import sys
import threading
import time

import aiohttp
import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentStatus
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_replicas_eq,
    tlog,
)
from ray.serve.handle import DeploymentHandle

APP_NAME = "autoscaling-with-streaming"
BACKEND_NAME = "Backend"
ROUTE_PREFIX = "/app"

N_INGRESS = 4
MIN_REPLICAS = 1
MAX_REPLICAS = 2
NUM_CHUNKS = 20
CHUNK_DELAY_S = 0.15

# (qps, duration (s)), total = 112 requests
LOAD_PROFILE = [
    (1.0, 6),
    (8.0, 12),
    (1.0, 10),
]

RAY_SERVE_ENV_OVERRIDES = {
    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
    "RAY_SERVE_ENABLE_HA_PROXY": "1",
    "RAY_SERVE_LOG_TO_STDERR": "0",
    "RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP": "0",
    "RAY_SERVE_USE_GRPC_BY_DEFAULT": "1",
}

THROUGHPUT_OPTIMIZED_ENV_VARS = {
    "RAY_SERVE_THROUGHPUT_OPTIMIZED": "1",
}


@ray.remote
class RequestCounter:
    """Tracks in-flight and finished request counts."""

    def __init__(self):
        self.inflight = 0
        self.finished = 0

    def on_start(self):
        self.inflight += 1

    def on_finish(self):
        self.inflight -= 1
        self.finished += 1

    def snapshot(self):
        return {"inflight": self.inflight, "finished": self.finished}


@serve.deployment(
    name=BACKEND_NAME,
    autoscaling_config={
        "min_replicas": MIN_REPLICAS,
        "max_replicas": MAX_REPLICAS,
        "target_ongoing_requests": 2,
        "upscale_delay_s": 2,
        "downscale_delay_s": 8,
        "metrics_interval_s": 1,
        "look_back_period_s": 5,
    },
    max_ongoing_requests=4,
    graceful_shutdown_timeout_s=1,
)
class Backend:
    def __init__(self, counter_handle):
        self._counter = counter_handle

    async def stream(self):
        await self._counter.on_start.remote()
        try:
            for i in range(NUM_CHUNKS):
                yield f"{i}\n".encode()
                await asyncio.sleep(CHUNK_DELAY_S)
        finally:
            await self._counter.on_finish.remote()

    async def __call__(self):
        await self._counter.on_start.remote()
        try:
            await asyncio.sleep(NUM_CHUNKS * CHUNK_DELAY_S)
            return {"ok": True}
        finally:
            await self._counter.on_finish.remote()


@serve.deployment(num_replicas=N_INGRESS, max_ongoing_requests=1000)
class Ingress:
    def __init__(self, backend: DeploymentHandle, stream: bool):
        self._stream = stream
        if self._stream:
            self._backend = backend.options(
                stream=True, method_name="stream", _by_reference=False
            )
        else:
            self._backend = backend

    async def __call__(self, request: Request):
        if self._stream:
            return StreamingResponse(self._backend.remote(), media_type="text/plain")
        return await self._backend.remote()


def _build_app(stream: bool):
    counter = RequestCounter.options(
        name="request_counter", lifetime="detached"
    ).remote()
    app = Ingress.bind(Backend.bind(counter), stream)
    return app, counter


async def _run_phase(session, url, stream, qps, duration_s, inflight, counters):
    """Run one load phase at the given QPS for duration_s seconds."""
    interval_s = 1.0 / qps
    deadline = time.monotonic() + duration_s

    async def one_request():
        counters["sent"] += 1
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=120)
            ) as resp:
                if stream:
                    async for _ in resp.content.iter_chunked(1024):
                        pass
                else:
                    await resp.read()
                counters["ok"] += 1
        except Exception:
            counters["errors"] += 1

    while time.monotonic() < deadline:
        task = asyncio.create_task(one_request())
        inflight.add(task)
        task.add_done_callback(inflight.discard)
        await asyncio.sleep(interval_s)


async def _send_load(url: str, stream: bool):
    """Replay the load phases and return final counters."""
    inflight: set = set()
    counters = {"sent": 0, "ok": 0, "errors": 0}

    async with aiohttp.ClientSession() as session:
        for qps, duration_s in LOAD_PROFILE:
            await _run_phase(session, url, stream, qps, duration_s, inflight, counters)

        await asyncio.sleep(20)

        # Wait for all in-flight requests to complete.
        if inflight:
            await asyncio.gather(*list(inflight), return_exceptions=True)

    return counters


def _send_load_in_thread(url: str, stream: bool):
    """Run the async load generator in a thread."""
    result = {}
    error = [None]

    def _run():
        try:
            result.update(asyncio.run(_send_load(url, stream)))
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t, result, error


@pytest.mark.parametrize(
    "ray_instance, stream",
    [
        (RAY_SERVE_ENV_OVERRIDES, True),
        (RAY_SERVE_ENV_OVERRIDES, False),
        (THROUGHPUT_OPTIMIZED_ENV_VARS, True),
        (THROUGHPUT_OPTIMIZED_ENV_VARS, False),
    ],
    ids=[
        "env_overrides_stream",
        "env_overrides_no_stream",
        "throughput_optimized_stream",
        "throughput_optimized_no_stream",
    ],
    indirect=["ray_instance"],
)
def test_autoscaling_with_streaming(ray_instance, stream):
    """deploy -> settle -> load -> assert 1->2 -> drain -> assert 2->1."""

    # 1) Deploy
    app, counter = _build_app(stream)
    serve.run(app, name=APP_NAME, route_prefix=ROUTE_PREFIX)
    tlog(
        f"Deployed app with configuration: "
        f"{stream=} "
        f"{' '.join(f'{k}={v}' for k, v in os.environ.items() if k.startswith('RAY_SERVE_'))}"
    )

    wait_for_condition(
        check_deployment_status,
        name=BACKEND_NAME,
        expected_status=DeploymentStatus.HEALTHY,
        app_name=APP_NAME,
        timeout=30,
    )
    wait_for_condition(
        check_num_replicas_eq,
        name=BACKEND_NAME,
        target=MIN_REPLICAS,
        app_name=APP_NAME,
        timeout=30,
    )
    tlog("Deployment healthy with 1 replica.")

    # 2) Settle
    tlog("Waiting 10 s for the system to settle.")
    time.sleep(10)

    # 3) Send load
    url = f"http://localhost:8000{ROUTE_PREFIX}"
    load_thread, load_counters, load_error = _send_load_in_thread(url, stream)
    tlog("Load generation started.")

    # 4) Assert replicas scale from 1 -> 2 during load
    wait_for_condition(
        check_num_replicas_eq,
        name=BACKEND_NAME,
        target=MAX_REPLICAS,
        app_name=APP_NAME,
        timeout=60,
        retry_interval_ms=1000,
    )
    tlog("Replicas scaled up to 2.")

    # 5) Wait for load to finish; assert all requests reported 'ok'
    load_thread.join(timeout=180)
    assert not load_thread.is_alive(), "Load generation thread did not finish in time"
    assert load_error[0] is None, f"Load generation failed: {load_error[0]}"

    tlog(f"Load finished. counters={load_counters}")

    assert load_counters["ok"] == load_counters["sent"], (
        f"Expected all {load_counters['sent']} requests to succeed, "
        f"but ok={load_counters['ok']}, errors={load_counters['errors']}"
    )
    tlog(f"All {load_counters['ok']} requests reported ok.")

    # 6) Assert replicas scale from 2 -> 1 after drain
    wait_for_condition(
        check_num_replicas_eq,
        name=BACKEND_NAME,
        target=MIN_REPLICAS,
        app_name=APP_NAME,
        timeout=60,
    )
    tlog("Replicas scaled back down to 1. Test passed.")

    # Cleanup
    serve.delete(APP_NAME)
    ray.kill(ray.get_actor("request_counter"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
