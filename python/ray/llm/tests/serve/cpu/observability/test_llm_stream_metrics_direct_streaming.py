"""Full LLM HTTP path and real Prometheus sink, with a deterministic CPU engine.

C tests own scanner edge cases; HAProxy tests own stalled RPCs. This suite
owns flag OFF/ON behavior, engine identity correlation, and timing boundaries.
"""

import asyncio
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import openai
import pytest

import ray
from ray import serve
from ray._common.test_utils import fetch_prometheus_metrics, wait_for_condition
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.metrics.llm_metrics import (
    ITL_BUCKETS,
    TTFT_BUCKETS,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.serve._private import constants as serve_constants
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    ServerConfig,
    stream_metrics_socket_path,
)
from ray.serve._private.haproxy_metrics import HAProxyMetricsCollector
from ray.serve.config import HTTPOptions


def _metric_engine_cls():
    class _MetricEngine(MockVLLMEngine):
        """Use vLLM's actual Ray wrapper to check cross-worker correlation."""

        def __init__(self, llm_config):
            super().__init__(llm_config)
            from vllm.v1.metrics.ray_wrappers import (
                RayGaugeWrapper,
                RayHistogramWrapper,
            )

            self.ttft = RayHistogramWrapper(
                "vllm:time_to_first_token_seconds",
                labelnames=["model_name", "engine"],
                buckets=TTFT_BUCKETS,
            ).labels(llm_config.model_id, "0")
            self.itl = RayHistogramWrapper(
                "vllm:inter_token_latency_seconds",
                labelnames=["model_name", "engine"],
                buckets=ITL_BUCKETS,
            ).labels(llm_config.model_id, "0")
            self.decode_elapsed = RayGaugeWrapper(
                "stream_test_decode_elapsed_seconds",
                labelnames=["model_name", "engine"],
            ).labels(llm_config.model_id, "0")
            self.decode_elapsed_sum = 0.0

        async def _measure(self, stream, streaming, delay_end=True):
            previous = None
            started = time.perf_counter()
            chunk_index = 0
            async for chunk in stream:
                if streaming and chunk != "data: [DONE]\n\n":
                    await asyncio.sleep(0.09)
                    now = time.perf_counter()
                    if previous is None:
                        self.ttft.observe(now - started)
                    else:
                        self.itl.observe(now - previous)
                        self.decode_elapsed_sum += now - previous
                        self.decode_elapsed.set(self.decode_elapsed_sum)
                    previous = now
                    # Model a vLLM stream whose yields contain 3,4,2,1 tokens.
                    # Native metadata preserves N without tokenizing its text.
                    event = json.loads(chunk.removeprefix("data: ").strip())
                    if os.environ["RAY_SERVE_ENABLE_LLM_STREAMING_METRICS"] == "1":
                        event["choices"][0]["ray_token_count"] = [3, 4, 2, 1][
                            chunk_index
                        ]
                    chunk_index += 1
                    # Compact, like vLLM's pydantic serializer that HAProxy scans.
                    chunk = "data: " + json.dumps(event, separators=(",", ":")) + "\n\n"
                if isinstance(chunk, str):
                    chunk = chunk.replace("test_0", "test_0" + "x" * 5000)
                yield chunk
            if streaming and delay_end:
                await asyncio.sleep(0.4)

        def _generate_chat_response(self, request, prompt_text, max_tokens):
            return self._measure(
                super()._generate_chat_response(request, prompt_text, max_tokens),
                request.stream,
                delay_end=prompt_text != "completed-before-reload",
            )

        def _generate_completion_response(self, request, prompt_text, max_tokens):
            return self._measure(
                super()._generate_completion_response(request, prompt_text, max_tokens),
                request.stream,
                delay_end=prompt_text != "completed-before-reload",
            )

    return _MetricEngine


def _metric_deployments():
    engine_cls = _metric_engine_cls()

    class MetricServer(LLMServer):
        async def __init__(self, llm_config):
            await super().__init__(llm_config, engine_cls=engine_cls)

    class DelayedRouter(LLMRouter):
        async def _pick_replica(self, *args, **kwargs):
            await asyncio.sleep(0.2)
            return await super()._pick_replica(*args, **kwargs)

    return MetricServer, DelayedRouter


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module", params=[False, True], ids=["default-off", "opt-in"])
def metrics_cluster(request):
    metrics_port, http_port = _free_port(), _free_port()
    with (
        patch.dict(
            os.environ,
            {
                "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                "RAY_SERVE_ENABLE_HA_PROXY": "0",
                "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "0",
                "RAY_SERVE_ENABLE_LLM_STREAMING_METRICS": str(int(request.param)),
                "RAY_WORKER_TIMEOUT_S": "15",
            },
        ),
        patch.object(serve_constants, "RAY_SERVE_ENABLE_DIRECT_INGRESS", True),
    ):
        ray.init(
            address="local",
            num_cpus=4,
            include_dashboard=False,
            runtime_env={},
            object_store_memory=100 * 1024 * 1024,
            _metrics_export_port=metrics_port,
            _system_config={"metrics_report_interval_ms": 100},
        )
        serve.start(http_options={"host": "127.0.0.1", "port": http_port})
        yield f"127.0.0.1:{metrics_port}", request.param
        serve.shutdown()
        ray.shutdown()


@pytest.mark.asyncio
async def test_direct_stream_metrics(metrics_cluster, mock_llm_config, tmp_path):
    """Real HAProxy + LLMRouter + LLMServer HTTP path to Ray's exporter, no GPU."""
    metrics_address, enabled = metrics_cluster
    model_id = "metric-full-path"
    mock_llm_config.model_loading_config.model_id = model_id
    server_cls, router_cls = _metric_deployments()
    server = serve.deployment(serve.ingress()(server_cls)).bind(mock_llm_config)
    handle = serve.run(server, name="engine")
    serve.run(
        serve.deployment(router_cls, ray_actor_options={"num_cpus": 0}).bind(
            server=handle
        ),
        name="router",
        route_prefix=None,
    )
    controller = serve.context._get_global_client()._controller
    replicas = ray.get(controller._all_running_replicas.remote())
    engine = next(
        items[0]
        for deployment, items in replicas.items()
        if deployment.name == "MetricServer"
    )
    router = next(
        items[0]
        for deployment, items in replicas.items()
        if deployment.name == "DelayedRouter"
    )
    assert engine.backend_http_port and router.backend_http_port
    port = _free_port()
    # Appending a suffix to a valid 95-byte metrics path would exceed HAProxy's
    # 97-byte Unix socket limit. Exercise the bounded observer path end to end.
    metrics_path = tmp_path / ("m" * (94 - len(str(tmp_path).encode())))
    cfg = HAProxyConfig(
        http_options=HTTPOptions(host="127.0.0.1", port=port),
        nbthread=1,
        socket_path=str(tmp_path / "admin.sock"),
        server_state_base=str(tmp_path),
        server_state_file=str(tmp_path / "server-state"),
        metrics_socket_path=str(metrics_path),
        stats_port=_free_port(),
        metrics_port=_free_port(),
        metrics_enabled=True,
        stream_metrics_enabled=enabled,
        stream_metrics_agent_address=(
            f"127.0.0.1:{ray._private.worker._global_node.metrics_agent_port}"
        ),
        has_received_routes=True,
        has_received_servers=True,
        health_check_inter="100ms",
        log_target="/dev/null",
    )
    backend = BackendConfig(
        name="llm",
        path_prefix="/",
        app_name="llm",
        servers=[
            ServerConfig(
                name="engine",
                host="127.0.0.1",
                port=engine.backend_http_port,
                replica_id=engine.replica_id.to_full_id_str(),
            )
        ],
        ingress_request_router_servers=[
            ServerConfig(name="router", host="127.0.0.1", port=router.backend_http_port)
        ],
    )
    proxy = HAProxyApi(
        cfg=cfg,
        backend_configs={"llm": backend},
        config_file_path=str(tmp_path / "haproxy.cfg"),
    )
    collector = HAProxyMetricsCollector(proxy, node_id="test")
    await collector.bind_and_attach(cfg.metrics_socket_path)
    client = openai.Client(base_url=f"http://127.0.0.1:{port}/v1", api_key="test")

    def generate(api, *, prompt="hello", started=None):
        kwargs = dict(model=model_id, max_tokens=4, stream=True)
        if api == "chat":
            response = client.chat.completions.with_streaming_response.create(
                messages=[{"role": "user", "content": "hello"}], **kwargs
            )
        else:
            response = client.completions.with_streaming_response.create(
                prompt=prompt, **kwargs
            )
        # Drain HTTP through EOF: the usual SDK iterator stops at [DONE], which
        # would close the connection before our intentionally delayed HTTP end.
        with response as stream:
            assert "x-ray-llm-metric-tags" not in stream.headers
            output = []
            for line in stream.iter_lines():
                if not line.startswith("data: ") or line == "data: [DONE]":
                    continue
                if started is not None:
                    started.set()
                choice = json.loads(line[6:])["choices"][0]
                output.append(
                    choice["delta"].get("content", "")
                    if api == "chat"
                    else choice["text"]
                )
            return "".join(output)

    try:
        await proxy.start()
        expected = "test_0" + "x" * 5000 + " test_1 test_2 test_3"
        with ThreadPoolExecutor(max_workers=2) as pool:
            outputs = await asyncio.to_thread(
                lambda: list(pool.map(generate, ["chat", "completions"]))
            )
        assert outputs == [expected] * 2

        await asyncio.to_thread(
            client.completions.create, model=model_id, prompt="hello", max_tokens=4
        )
        with pytest.raises(openai.NotFoundError):
            await asyncio.to_thread(
                client.completions.create,
                model="missing-model",
                prompt="hello",
                stream=True,
            )

        def check_metrics():
            samples = fetch_prometheus_metrics([metrics_address])
            if not enabled:
                assert not any(name.startswith("ray_serve_llm_") for name in samples)
            # General ingress logging counts every HTTP request once,
            # independently of the per-output streaming observations.
            requests = [
                sample
                for sample in samples.get("ray_serve_num_http_requests_total", [])
                if sample.labels.get("application") == "llm"
                and sample.labels.get("method") == "POST"
            ]
            assert sum(sample.value for sample in requests) == 4
            observed = {}
            assert all(
                s.value == 0
                for s in samples.get(
                    "ray_serve_llm_stream_metrics_collection_errors_total", []
                )
            )
            for prefix in ("ray_serve_llm", "ray_vllm"):
                expected_counts = [
                    ("time_to_first_token", 2),
                    ("inter_token_latency", 14 if prefix == "ray_serve_llm" else 6),
                ]
                if prefix == "ray_serve_llm":
                    expected_counts += [("router_overhead", 2), ("request_duration", 2)]
                for metric, count in expected_counts:
                    name = f"{prefix}_{metric}_seconds"
                    matching = [
                        s
                        for s in samples.get(f"{name}_count", [])
                        if s.labels.get("model_name") == model_id
                    ]
                    if prefix == "ray_serve_llm" and not enabled:
                        assert not matching
                        continue
                    assert len(matching) == 1, (name, samples.keys())
                    assert matching[0].value == count
                    assert "request_id" not in matching[0].labels
                    observed[name] = matching[0]
            if not enabled:
                return True
            for metric in ("time_to_first_token", "inter_token_latency"):
                e2e = observed[f"ray_serve_llm_{metric}_seconds"]
                engine = observed[f"ray_vllm_{metric}_seconds"]
                assert e2e.labels["engine"] == engine.labels["engine"] == "0"
                assert e2e.labels["ReplicaId"] == engine.labels["ReplicaId"]
                assert e2e.labels["engine_worker_id"] == engine.labels["WorkerId"]
                assert e2e.labels["WorkerId"] != engine.labels["WorkerId"]
            framework_sum = sum(
                s.value
                for s in samples["ray_serve_llm_inter_token_latency_seconds_sum"]
                if s.labels.get("model_name") == model_id
            )
            # Ray's native histogram export approximates sums from bucket
            # midpoints. Use an exact clock-based gauge as the timing reference.
            engine_sum = sum(
                s.value
                for s in samples["ray_stream_test_decode_elapsed_seconds"]
                if s.labels.get("model_name") == model_id
            )
            assert framework_sum == pytest.approx(engine_sum, abs=0.06)
            # The transport arrival interval is normalized by 4,2,1 tokens.
            buckets = samples["ray_serve_llm_inter_token_latency_seconds_bucket"]
            assert (
                sum(
                    s.value
                    for s in buckets
                    if s.labels.get("model_name") == model_id
                    and s.labels["le"] == "0.075"
                )
                == 12
            )
            for metric in ("router_overhead", "request_duration"):
                timing = observed[f"ray_serve_llm_{metric}_seconds"]
                assert timing.labels["ReplicaId"] == engine.labels["ReplicaId"]
                assert timing.labels["engine_worker_id"] == engine.labels["WorkerId"]
            # The real router sleeps 200ms before selection. That delay must
            # contribute to TTFT, while transport fragmentation adds no ITLs.
            first_bucket = [
                s
                for s in samples["ray_serve_llm_time_to_first_token_seconds_bucket"]
                if s.labels.get("model_name") == model_id
                and float(s.labels["le"]) == 0.1
            ]
            assert [s.value for s in first_bucket] == [0]
            # Router overhead includes the injected 200ms decision delay, while
            # request duration also includes 400ms after the SSE [DONE] event.
            for metric, boundary in (
                ("router_overhead", 0.1),
                ("request_duration", 0.5),
            ):
                buckets = [
                    s.value
                    for s in samples[f"ray_serve_llm_{metric}_seconds_bucket"]
                    if s.labels.get("model_name") == model_id
                    and float(s.labels["le"]) == boundary
                ]
                assert buckets == [0]
            router_upper = [
                s.value
                for s in samples["ray_serve_llm_router_overhead_seconds_bucket"]
                if s.labels.get("model_name") == model_id
                and float(s.labels["le"]) == 0.5
            ]
            assert router_upper == [2]
            return True

        await asyncio.to_thread(
            wait_for_condition, check_metrics, timeout=60, retry_interval_ms=200
        )
        if enabled:
            # Start a fresh generation. Complete a stream and begin another
            # before its first periodic export, then retire it immediately.
            await proxy.reload()
            client.close()
            client = openai.Client(
                base_url=f"http://127.0.0.1:{port}/v1", api_key="test"
            )
            retiring = proxy._proc
            generation = proxy._stream_metrics_generation
            await asyncio.to_thread(
                generate, "completions", prompt="completed-before-reload"
            )
            started = threading.Event()
            in_flight = asyncio.create_task(
                asyncio.to_thread(generate, "completions", started=started)
            )
            assert await asyncio.to_thread(started.wait, 5)
            before = fetch_prometheus_metrics([metrics_address])
            assert not any(
                generation in sample.labels.get("Component", "")
                for sample in before.get(
                    "ray_serve_llm_request_duration_seconds_count", []
                )
            ), "Both streams must begin before this generation's periodic export"
            await proxy.reload()
            assert (
                retiring.returncode is None
            ), "The in-flight response must keep retirement alive"
            assert await in_flight == expected
            client.close()
            client = openai.Client(
                base_url=f"http://127.0.0.1:{port}/v1", api_key="test"
            )
            assert await asyncio.to_thread(generate, "completions") == expected
            await asyncio.wait_for(retiring.wait(), timeout=5)
            assert retiring.returncode == 0

            def check_retired_metrics():
                samples = fetch_prometheus_metrics([metrics_address])
                for metric, count in [
                    ("time_to_first_token", 2),
                    ("inter_token_latency", 14),
                    ("request_duration", 2),
                    ("router_overhead", 2),
                ]:
                    matching = [
                        sample
                        for sample in samples.get(
                            f"ray_serve_llm_{metric}_seconds_count", []
                        )
                        if generation in sample.labels.get("Component", "")
                    ]
                    assert sum(sample.value for sample in matching) == count
                assert not any(
                    sample.value
                    for sample in samples.get(
                        "ray_serve_llm_stream_metrics_collection_errors_total", []
                    )
                )
                return True

            await asyncio.to_thread(
                wait_for_condition,
                check_retired_metrics,
                timeout=4,
                retry_interval_ms=100,
            )
            assert not Path(
                stream_metrics_socket_path(cfg.metrics_socket_path, generation)
            ).exists()

            def check_generation_expired():
                samples = fetch_prometheus_metrics([metrics_address])
                matching = samples.get(
                    "ray_serve_llm_request_duration_seconds_count", []
                )
                assert not any(
                    generation in sample.labels.get("Component", "")
                    for sample in matching
                )
                current = [
                    sample
                    for sample in matching
                    if proxy._stream_metrics_generation
                    in sample.labels.get("Component", "")
                ]
                assert len(current) == 1 and current[0].value == 1
                return True

            # The new process continues reporting under the same ProxyActor
            # WorkerId. Its reports must not keep the retired generation alive.
            await asyncio.to_thread(
                wait_for_condition,
                check_generation_expired,
                timeout=25,
                retry_interval_ms=200,
            )
    finally:
        client.close()
        await proxy.stop()
        collector.close()
        serve.delete("router")
        serve.delete("engine")
