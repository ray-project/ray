import asyncio
import base64
import json
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock, call, patch

import openai
import pytest

import ray
from ray import serve
from ray._common.test_utils import fetch_prometheus_metrics, wait_for_condition
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.observability.metrics import llm_metrics
from ray.llm._internal.serve.observability.metrics.llm_metrics import (
    ITL_BUCKETS,
    LLM_METRICS_HEADER,
    TTFT_BUCKETS,
    HAProxyLLMStreamMetrics,
    LLMStreamMetricsMetadataMiddleware,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.serve._private import constants as serve_constants, haproxy_metrics
from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    ServerConfig,
)
from ray.serve._private.haproxy_metrics import HAProxyMetricsCollector
from ray.serve.config import HTTPOptions

TAGS = {"model_name": "model", "ReplicaId": "r", "engine_worker_id": "w"}


def _sse(choices):
    return f"data: {json.dumps({'choices': choices})}\n\n".encode()


def _packet(
    frame, body, now, *, stream="s", part=0, parts=1, started=0.0, router_overhead=0.2
):
    tags = base64.b64encode(json.dumps(TAGS).encode()).decode()
    size = len(body) if body is not None else -1
    encoded = body.hex() if body is not None else str(int(router_overhead * 1_000_000))
    return (
        f"ray_llm_stream|{stream}|{frame}|{part}|{parts}|{int(started * 1_000_000)}|{int(now * 1_000_000)}|"
        f"{tags}|{size}|{encoded}\n"
    ).encode()


@pytest.fixture
def recorder():
    recorder = HAProxyLLMStreamMetrics()
    recorder.metrics.time_to_first_token.observe = MagicMock()
    recorder.metrics.inter_token_latency.observe = MagicMock()
    recorder.metrics.router_overhead.observe = MagicMock()
    recorder.metrics.request_duration.observe = MagicMock()
    recorder.metrics.collection_errors.inc = MagicMock()
    yield recorder
    recorder.close()


@pytest.mark.parametrize("api", ["chat", "completions"])
def test_stream_metrics_time_output_per_choice(recorder, api):
    """Fragmented transport, batched SSE, control chunks and choices preserve timing."""

    def token(index, text):
        return (
            {"index": index, "delta": {"content": text}}
            if api == "chat"
            else {"index": index, "text": text}
        )

    bodies = [
        (1.0, _sse([{"index": 0, "delta": {"role": "assistant"}}])),
        (2.0, _sse([token(0, "a")])[:10]),
        (3.0, _sse([token(0, "a")])[10:]),
        (5.0, _sse([token(0, "b"), token(1, "x")]) + _sse([token(0, "c")])),
        (6.0, _sse([token(1, "y")]).replace(b"\n", b"\r\n")),
        (8.0, _sse([{"index": 0, "delta": {}, "finish_reason": "stop"}])),
        (9.0, b'data: {"choices": [], "usage": {"completion_tokens": 5}}\n\n'),
        (10.0, b'data: {"error": {"message": "failed"}}\n\ndata: [DONE]\n\n'),
    ]
    for frame, (timestamp, body) in enumerate(bodies):
        recorder.submit(_packet(frame, body, timestamp))
    recorder.submit(_packet(len(bodies), None, 11.0))
    recorder.close()
    assert recorder.metrics.time_to_first_token.observe.call_args_list == [
        call(3.0, tags=TAGS),
        call(5.0, tags=TAGS),
    ]
    assert recorder.metrics.inter_token_latency.observe.call_args_list == [
        call(2.0, tags=TAGS),
        call(0.0, tags=TAGS),
        call(1.0, tags=TAGS),
    ]
    # Request-level timing counts once even when there are multiple choices.
    recorder.metrics.router_overhead.observe.assert_called_once_with(0.2, tags=TAGS)
    recorder.metrics.request_duration.observe.assert_called_once_with(11.0, tags=TAGS)
    assert not recorder.streams
    recorder.metrics.collection_errors.inc.assert_not_called()


@pytest.mark.parametrize("completion_frame", [0, 2], ids=["empty", "lost-payloads"])
def test_request_timing_does_not_require_token_observations(recorder, completion_frame):
    recorder.submit(
        _packet(completion_frame, None, 5.0, started=2.0, router_overhead=0.25)
    )
    recorder.close()
    recorder.metrics.request_duration.observe.assert_called_once_with(3.0, tags=TAGS)
    recorder.metrics.router_overhead.observe.assert_called_once_with(0.25, tags=TAGS)
    recorder.metrics.time_to_first_token.observe.assert_not_called()
    recorder.metrics.inter_token_latency.observe.assert_not_called()
    assert not recorder.streams


@pytest.mark.asyncio
@pytest.mark.parametrize("slow_operation", ["decode", "emit"])
async def test_slow_collection_does_not_block_capture_or_change_timestamps(
    monkeypatch, recorder, slow_operation
):
    entered, release = threading.Event(), threading.Event()
    sender_thread = threading.get_ident()
    ttft = recorder.metrics.time_to_first_token.observe

    def slow(operation):
        def run(*args, **kwargs):
            assert threading.get_ident() != sender_thread
            entered.set()
            assert release.wait(timeout=5)
            return operation(*args, **kwargs)

        return run

    if slow_operation == "decode":
        monkeypatch.setattr(llm_metrics.json, "loads", slow(json.loads))
    else:
        recorder.metrics.time_to_first_token.observe = slow(ttft)
    try:
        recorder.submit(_packet(0, _sse([{"text": "first"}]), 1.0))
        assert await asyncio.to_thread(entered.wait, 2)
        recorder.submit(_packet(1, _sse([{"text": "second"}]), 2.0))
        recorder.submit(_packet(2, None, 3.0))
        ttft.assert_not_called()
        recorder.metrics.inter_token_latency.observe.assert_not_called()
        recorder.metrics.request_duration.observe.assert_not_called()
        recorder.metrics.router_overhead.observe.assert_not_called()
    finally:
        release.set()
        await asyncio.to_thread(recorder.close)
    ttft.assert_called_once_with(1.0, tags=TAGS)
    recorder.metrics.inter_token_latency.observe.assert_called_once_with(1.0, tags=TAGS)
    recorder.metrics.request_duration.observe.assert_called_once_with(3.0, tags=TAGS)
    recorder.metrics.router_overhead.observe.assert_called_once_with(0.2, tags=TAGS)
    assert not recorder._thread.is_alive()


@pytest.mark.parametrize("failure", ["gap", "truncated", "emission", "buffer"])
def test_collection_failure_isolates_stream_and_recovers(
    recorder, monkeypatch, failure
):
    if failure == "emission":
        recorder.metrics.inter_token_latency.observe.side_effect = RuntimeError(
            "unavailable"
        )
    if failure == "buffer":
        monkeypatch.setattr(llm_metrics, "_MAX_METRIC_CHUNK_BYTES", 256)
    recorder.submit(_packet(0, _sse([{"text": "first"}]), 1.0))
    second = _packet(2 if failure == "gap" else 1, _sse([{"text": "second"}]), 2.0)
    if failure == "truncated":
        second = second[:-5] + b"\n"
    if failure == "buffer":
        second = _packet(1, b"x" * 300, 2.0)
    recorder.submit(second)
    recorder.submit(
        _packet(3 if failure == "gap" else 2, _sse([{"text": "lost"}]), 3.0)
    )
    recorder.submit(_packet(4 if failure == "gap" else 3, None, 4.0))
    recorder.submit(
        _packet(
            0,
            _sse(
                [
                    {
                        "delta": {
                            "reasoning_content": "think",
                            "tool_calls": [{"index": 0}],
                        }
                    }
                ]
            ),
            5.0,
            stream="healthy",
        )
    )
    recorder.close()
    assert recorder.metrics.time_to_first_token.observe.call_count == 2
    assert recorder.metrics.inter_token_latency.observe.call_count == (
        1 if failure == "emission" else 0
    )
    recorder.metrics.collection_errors.inc.assert_called_once_with()
    # A lost/invalid token event does not invalidate the captured start/end clocks.
    recorder.metrics.request_duration.observe.assert_called_once_with(4.0, tags=TAGS)
    recorder.metrics.router_overhead.observe.assert_called_once_with(0.2, tags=TAGS)


@pytest.mark.asyncio
async def test_collection_queue_is_bounded_and_loss_does_not_bridge_intervals(
    monkeypatch,
):
    monkeypatch.setattr(llm_metrics, "_MAX_PENDING_METRIC_CHUNKS", 1)
    recorder = HAProxyLLMStreamMetrics()
    entered, release = threading.Event(), threading.Event()
    ttft, itl = MagicMock(), MagicMock()

    def record(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=5)
        ttft(*args, **kwargs)

    recorder.metrics.time_to_first_token.observe = record
    recorder.metrics.inter_token_latency.observe = itl
    recorder.metrics.collection_errors.inc = MagicMock()
    try:
        recorder.submit(_packet(0, _sse([{"text": "first"}]), 1.0))
        assert await asyncio.to_thread(entered.wait, 2)
        recorder.submit(_packet(1, _sse([{"text": "second"}]), 2.0))
        recorder.submit(_packet(2, _sse([{"text": "dropped"}]), 3.0))
        assert recorder.queue.qsize() == 1
        release.set()
        await asyncio.to_thread(recorder.queue.join)
        recorder.submit(_packet(3, _sse([{"text": "gap"}]), 4.0))
        await asyncio.to_thread(recorder.queue.join)
        recorder.submit(_packet(0, _sse([{"text": "healthy"}]), 5.0, stream="healthy"))
    finally:
        release.set()
        await asyncio.to_thread(recorder.close)
    assert ttft.call_count == 2
    itl.assert_called_once_with(1.0, tags=TAGS)
    assert recorder.metrics.collection_errors.inc.call_count == 2


@pytest.mark.parametrize("limit", ["capacity", "idle"])
def test_incomplete_stream_state_is_bounded_and_expires(monkeypatch, recorder, limit):
    if limit == "capacity":
        monkeypatch.setattr(llm_metrics, "_MAX_ACTIVE_METRIC_STREAMS", 1)
    recorder.submit(_packet(0, _sse([{"text": "first"}]), 1.0))
    recorder.queue.join()
    if limit == "idle":
        recorder.streams[b"s"].touched_at = time.monotonic() - 301
    recorder.submit(_packet(0, _sse([{"text": "second"}]), 2.0, stream="new"))
    recorder.queue.join()
    assert list(recorder.streams) == [b"new"]
    recorder.metrics.collection_errors.inc.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, content_type",
    [
        (200, b"text/event-stream"),
        (200, b"application/json"),
        (404, b"text/event-stream"),
    ],
)
async def test_native_middleware_only_adds_identity_to_successful_streams(
    monkeypatch, status, content_type
):
    monkeypatch.setattr(llm_metrics, "get_llm_metric_tags", lambda _: TAGS)
    messages = [
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", content_type)],
        },
        {"type": "http.response.body", "body": _sse([{"text": "token"}])},
    ]

    async def app(scope, receive, send):
        for message in messages:
            await send(message)

    send = AsyncMock()
    middleware = LLMStreamMetricsMetadataMiddleware(app, model_name="model")
    await middleware({"type": "http", "path": "/v1/completions"}, AsyncMock(), send)
    headers = dict(send.call_args_list[0].args[0]["headers"])
    if status == 200 and content_type == b"text/event-stream":
        assert json.loads(base64.b64decode(headers[LLM_METRICS_HEADER])) == TAGS
    else:
        assert LLM_METRICS_HEADER not in headers
    assert send.call_args_list[1].args[0] is messages[1]
    assert LLM_METRICS_HEADER not in dict(messages[0]["headers"])


def _metric_engine_cls():
    class _MetricEngine(MockVLLMEngine):
        """Use vLLM's actual Ray wrapper to check cross-worker correlation."""

        def __init__(self, llm_config):
            super().__init__(llm_config)
            from vllm.v1.metrics.ray_wrappers import RayHistogramWrapper

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

        async def _measure(self, stream, streaming):
            previous = None
            started = time.perf_counter()
            async for chunk in stream:
                if streaming and chunk != "data: [DONE]\n\n":
                    now = time.perf_counter()
                    if previous is None:
                        self.ttft.observe(now - started)
                    else:
                        self.itl.observe(now - previous)
                    previous = now
                if isinstance(chunk, str):
                    chunk = chunk.replace("test_0", "test_0" + "x" * 5000)
                yield chunk
            if streaming:
                # Delay closing the response after [DONE] to distinguish response
                # completion from first/last token and the SSE terminal marker.
                await asyncio.sleep(0.4)

        def _generate_chat_response(self, request, prompt_text, max_tokens):
            return self._measure(
                super()._generate_chat_response(request, prompt_text, max_tokens),
                request.stream,
            )

        def _generate_completion_response(self, request, prompt_text, max_tokens):
            return self._measure(
                super()._generate_completion_response(request, prompt_text, max_tokens),
                request.stream,
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
    import os

    metrics_port, http_port = _free_port(), _free_port()
    with (
        patch.dict(
            os.environ,
            {
                "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                "RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S": "0",
                "RAY_SERVE_ENABLE_LLM_STREAMING_METRICS": str(int(request.param)),
            },
        ),
        patch.object(serve_constants, "RAY_SERVE_ENABLE_DIRECT_INGRESS", True),
        patch.object(
            haproxy_metrics, "RAY_SERVE_ENABLE_LLM_STREAMING_METRICS", request.param
        ),
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
async def test_full_path_metrics_through_haproxy_router_and_native_replica(
    metrics_cluster, mock_llm_config, tmp_path
):
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
    cfg = HAProxyConfig(
        http_options=HTTPOptions(host="127.0.0.1", port=port),
        nbthread=1,
        socket_path=str(tmp_path / "admin.sock"),
        metrics_socket_path=str(tmp_path / "metrics.sock"),
        stats_port=_free_port(),
        metrics_port=_free_port(),
        metrics_enabled=True,
        llm_streaming_metrics_enabled=enabled,
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

    def generate(api):
        kwargs = dict(model=model_id, max_tokens=4, stream=True)
        if api == "chat":
            response = client.chat.completions.with_streaming_response.create(
                messages=[{"role": "user", "content": "hello"}], **kwargs
            )
        else:
            response = client.completions.with_streaming_response.create(
                prompt="hello", **kwargs
            )
        # Drain HTTP through EOF: the usual SDK iterator stops at [DONE], which
        # would close the connection before our intentionally delayed HTTP end.
        with response as stream:
            assert "x-ray-llm-metric-tags" not in stream.headers
            output = []
            for line in stream.iter_lines():
                if not line.startswith("data: ") or line == "data: [DONE]":
                    continue
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

        if enabled:
            # Block the actual collector while a third stream crosses HAProxy.
            # The full response must finish before histogram recording is released.
            recorder = collector._llm_stream_metrics
            entered, release = threading.Event(), threading.Event()
            observe = recorder.metrics.time_to_first_token.observe

            def slow_observe(*args, **kwargs):
                entered.set()
                assert release.wait(timeout=5)
                observe(*args, **kwargs)

            recorder.metrics.time_to_first_token.observe = slow_observe
            response_task = asyncio.create_task(asyncio.to_thread(generate, "chat"))
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                assert await asyncio.wait_for(response_task, timeout=2) == expected
                assert not release.is_set()
            finally:
                release.set()
                await asyncio.to_thread(recorder.queue.join)
                recorder.metrics.time_to_first_token.observe = observe
        else:
            assert collector._llm_stream_metrics is None
            assert await asyncio.to_thread(generate, "chat") == expected
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
            # Moving the metrics logger to global must still count each HTTP
            # request once, independently of the per-output observations.
            requests = [
                sample
                for sample in samples.get("ray_serve_num_http_requests_total", [])
                if sample.labels.get("application") == "llm"
                and sample.labels.get("method") == "POST"
            ]
            assert sum(sample.value for sample in requests) == 5
            observed = {}
            for prefix in ("ray_serve_llm", "ray_vllm"):
                expected_counts = [
                    ("time_to_first_token", 3),
                    ("inter_token_latency", 9),
                ]
                if prefix == "ray_serve_llm":
                    expected_counts += [("router_overhead", 3), ("request_duration", 3)]
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
                    assert len(matching) == 1
                    assert matching[0].value == count
                    assert "request_id" not in matching[0].labels
                    observed[name] = matching[0]
            if not enabled:
                assert collector._llm_stream_metrics is None
                return True
            for metric in ("time_to_first_token", "inter_token_latency"):
                e2e = observed[f"ray_serve_llm_{metric}_seconds"]
                engine = observed[f"ray_vllm_{metric}_seconds"]
                assert e2e.labels["ReplicaId"] == engine.labels["ReplicaId"]
                assert e2e.labels["engine_worker_id"] == engine.labels["WorkerId"]
                assert e2e.labels["WorkerId"] != engine.labels["WorkerId"]
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
            assert router_upper == [3]
            return True

        await asyncio.to_thread(
            wait_for_condition, check_metrics, timeout=60, retry_interval_ms=200
        )
        if enabled:
            await asyncio.to_thread(collector._llm_stream_metrics.queue.join)
            assert not collector._llm_stream_metrics.streams
    finally:
        client.close()
        await proxy.stop()
        collector.close()
        serve.delete("router")
        serve.delete("engine")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
