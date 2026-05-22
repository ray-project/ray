"""Nightly integration test: TTFTAutoscalingPolicy against real vLLM metrics.

What this exercises end-to-end:

1. Deploys a Qwen-0.5B vLLM Serve app with ``log_engine_metrics=True`` so the
   ``RayPrometheusStatLogger`` registers ``vllm:time_to_first_token_seconds``
   (and the rest of the standard vLLM histograms) into Ray's metrics registry.
2. Starts a Prometheus subprocess scraping every Ray node's metrics endpoint
   discovered via ``ray.nodes()``. Waits for at least one successful scrape so
   the histogram_quantile query can resolve to a real number.
3. Sends sustained concurrent OpenAI completion traffic to push p99 TTFT
   above the configured target.
4. Polls ``serve.status()`` until the deployment scales from 1 -> 2 replicas
   (or fails on timeout).

This test assumes the ``prometheus`` binary is on PATH in the test image
(llm-cu128). It does not download or vendor Prometheus.

Cost: ~3-6 minutes wall time. Belongs in the nightly llm_serve_vllm_integration
group, not standard CI.
"""
import asyncio
import contextlib
import logging
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import List

import pytest
import requests
from openai import AsyncOpenAI

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.config import AutoscalingPolicy
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.ttft_autoscaling_policy import TTFTAutoscalingPolicy

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MODEL_SOURCE = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"

# Lower than realistic so even modestly loaded p99 crosses it -- the test
# verifies the autoscaling *mechanism*, not vLLM latency characterization.
TARGET_P99_TTFT_S = 0.1

PROMETHEUS_PORT = 9090
PROMETHEUS_QUERY_URL = f"http://127.0.0.1:{PROMETHEUS_PORT}/api/v1/query"
PROMETHEUS_SCRAPE_INTERVAL_S = 5

CONCURRENT_STREAMS = 32
TRAFFIC_DURATION_S = 90  # > rate window (1m) + a margin for scrape + decide


# ---------------------------------------------------------------------------
# Prometheus fixture
# ---------------------------------------------------------------------------


def _ray_metrics_targets() -> List[str]:
    """Build a Prometheus static_configs target list from live Ray nodes.

    Each Ray node exposes /metrics on ``MetricsExportPort``. Without scraping
    all nodes the histogram_quantile would silently miss the replica's data.
    """
    targets = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        host = node["NodeManagerAddress"]
        port = node["MetricsExportPort"]
        targets.append(f"{host}:{port}")
    if not targets:
        raise RuntimeError("No alive Ray nodes found to scrape.")
    return targets


@pytest.fixture(scope="module")
def prometheus_subprocess():
    """Start prometheus pointed at every Ray node's metrics endpoint.

    Assumes ``prometheus`` is on PATH (provided by the test image).
    """
    if shutil.which("prometheus") is None:
        pytest.skip("prometheus binary not on PATH; skipping autoscaling test")

    # Ray must already be up so we can discover nodes.
    if not ray.is_initialized():
        ray.init(address="auto", ignore_reinit_error=True)
    targets = _ray_metrics_targets()
    logger.info(f"Prometheus will scrape: {targets}")

    workdir = Path(tempfile.mkdtemp(prefix="prom-ttft-"))
    config_path = workdir / "prometheus.yml"
    targets_yaml = "\n".join(f"          - {t!r}" for t in targets)
    config_path.write_text(
        textwrap.dedent(
            f"""\
            global:
              scrape_interval: {PROMETHEUS_SCRAPE_INTERVAL_S}s
              evaluation_interval: {PROMETHEUS_SCRAPE_INTERVAL_S}s
            scrape_configs:
              - job_name: ray
                static_configs:
                  - targets:
{targets_yaml}
            """
        )
    )

    proc = subprocess.Popen(
        [
            "prometheus",
            f"--config.file={config_path}",
            f"--web.listen-address=:{PROMETHEUS_PORT}",
            f"--storage.tsdb.path={workdir / 'data'}",
            # Tighter retention so the on-disk WAL doesn't grow during the run.
            "--storage.tsdb.retention.time=15m",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for Prometheus's HTTP API to be reachable.
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            r = requests.get(f"http://127.0.0.1:{PROMETHEUS_PORT}/-/ready", timeout=1)
            if r.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(0.5)
    else:
        proc.terminate()
        raise TimeoutError("Prometheus did not become ready within 30s")
    logger.info("Prometheus is ready.")

    try:
        yield
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=10)
        if proc.poll() is None:
            proc.kill()
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Serve deployment fixture
# ---------------------------------------------------------------------------


def _build_llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=RAY_MODEL_ID,
            model_source=MODEL_SOURCE,
        ),
        log_engine_metrics=True,
        deployment_config={
            "autoscaling_config": {
                "min_replicas": 1,
                "max_replicas": 2,
                "upscale_delay_s": 5.0,
                # High enough we never see a downscale race during the test.
                "downscale_delay_s": 600.0,
                "metrics_interval_s": 0.5,
                "policy": AutoscalingPolicy(
                    policy_function=TTFTAutoscalingPolicy,
                    policy_kwargs={
                        "prometheus_query_url": PROMETHEUS_QUERY_URL,
                        "target_p99_ttft_s": TARGET_P99_TTFT_S,
                        "fetch_interval_s": 2.0,
                        "cache_ttl_s": 30.0,
                        "query_timeout_s": 3.0,
                    },
                ),
            },
        },
        engine_kwargs=dict(tensor_parallel_size=1, pipeline_parallel_size=1),
        runtime_env=None,
    )


@pytest.fixture(scope="module")
def llm_app(prometheus_subprocess):
    llm_config = _build_llm_config()
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    try:
        _wait_for_server_ready("http://127.0.0.1:8000", RAY_MODEL_ID, timeout=300)
        yield "http://127.0.0.1:8000"
    finally:
        serve.shutdown()


def _wait_for_server_ready(url: str, model_id: str, timeout: int) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.post(
                f"{url}/v1/completions",
                json={
                    "model": model_id,
                    "prompt": "test",
                    "max_tokens": 1,
                    "temperature": 0,
                },
                timeout=10,
            )
            if r.status_code == 200:
                logger.info(f"Server at {url} is ready.")
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise TimeoutError(f"LLM server at {url} not ready within {timeout}s")


# ---------------------------------------------------------------------------
# Traffic generator
# ---------------------------------------------------------------------------


async def _drive_concurrent_traffic(url: str, duration_s: float) -> None:
    client = AsyncOpenAI(base_url=f"{url}/v1", api_key="fake-key")
    stop_at = time.monotonic() + duration_s

    async def one_request() -> None:
        # We don't care about the output -- only that the request hits the
        # engine and contributes a TTFT sample.
        try:
            await client.completions.create(
                model=RAY_MODEL_ID,
                prompt="Write a single sentence about Paris.",
                max_tokens=8,
                temperature=0.0,
            )
        except Exception as e:
            logger.warning(f"Traffic request failed: {e}")

    async def worker() -> None:
        while time.monotonic() < stop_at:
            await one_request()

    await asyncio.gather(*[worker() for _ in range(CONCURRENT_STREAMS)])


def _wait_for_metric_in_prometheus(query: str, timeout: int = 120) -> None:
    """Block until ``query`` resolves to a non-empty vector result.

    Until vLLM has accumulated samples AND Prometheus has scraped them at
    least once, ``histogram_quantile(...)`` returns an empty vector -- the
    policy then sees ``no_metrics`` and won't scale, so we can't observe
    scale-up until this condition is met. Polling here removes that race.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{PROMETHEUS_QUERY_URL}",
                params={"query": query},
                timeout=5,
            )
            payload = r.json()
            if payload.get("status") == "success" and payload.get("data", {}).get(
                "result"
            ):
                logger.info(f"Prometheus has data for {query!r}.")
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(2)
    raise TimeoutError(
        f"Prometheus never returned data for query {query!r} within {timeout}s"
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def _running_replicas(name: str) -> int:
    status = serve.status()
    app = status.applications.get(SERVE_DEFAULT_APP_NAME)
    if app is None:
        return 0
    dep = app.deployments.get(name)
    if dep is None:
        return 0
    # Replica-states map values to counts. RUNNING is the key we care about.
    return dep.replica_states.get("RUNNING", 0)


def test_ttft_policy_scales_up_under_real_vllm_traffic(llm_app):
    """vLLM samples -> Ray metrics -> Prometheus scrape -> policy
    fetch -> scale-up decision -> Serve increases replica count."""

    status = serve.status()
    app = status.applications[SERVE_DEFAULT_APP_NAME]
    server_deployments = [
        name for name in app.deployments if name.lower().startswith("llmserver")
    ]
    assert (
        len(server_deployments) == 1
    ), f"Expected exactly one LLMServer deployment, found: {list(app.deployments)}"
    deployment_name = server_deployments[0]
    logger.info(f"Watching deployment {deployment_name!r}")

    assert _running_replicas(deployment_name) == 1

    # Drive traffic in the background; meanwhile wait for the histogram to
    # populate, then poll for scale-up.
    async def run_test() -> None:
        traffic_task = asyncio.create_task(
            _drive_concurrent_traffic(llm_app, TRAFFIC_DURATION_S)
        )

        # Wait until Prometheus actually has TTFT samples to query.
        _wait_for_metric_in_prometheus(TTFTAutoscalingPolicy.P99_TTFT_QUERY)

        # Now wait for scale-up. Budget: upscale_delay (5s) + fetch_interval
        # (2s) + scrape_interval (5s) + slack. 60s is comfortable.
        wait_for_condition(
            lambda: _running_replicas(deployment_name) >= 2,
            timeout=60,
            retry_interval_ms=1000,
        )
        logger.info(
            f"Observed scale-up to {_running_replicas(deployment_name)} replicas"
        )

        # Let the traffic task drain or cancel it -- we have what we needed.
        traffic_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await traffic_task

    asyncio.run(run_test())


if __name__ == "__main__":
    sys.exit(pytest.main(["-xvs", __file__]))
