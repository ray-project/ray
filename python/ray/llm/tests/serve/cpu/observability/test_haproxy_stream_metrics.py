"""Native HTTP filtering and export transport; scanning has standalone C tests."""

import contextlib
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
import pytest
import requests

import ray.serve._private.haproxy as haproxy
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.serve._private import haproxy_stream_metrics
from ray.serve._private.haproxy import (
    get_haproxy_binary,
    stream_metrics_socket_path,
)
from ray.serve._private.haproxy_stream_metrics import (
    render_stream_metrics_export_config,
)

TAGS = json.dumps(
    {
        "model_name": "test",
        "ReplicaId": "replica",
        "engine_worker_id": "engine",
        "engine": "0",
    },
    separators=(",", ":"),
)
# Compact, like vLLM's pydantic serializer that HAProxy scans.
WIRES = [
    (
        "data: "
        + json.dumps(
            {"choices": [{"index": 0, "text": "x", "ray_token_count": 1}]},
            separators=(",", ":"),
        )
        + "\r\n\r\n"
    ).encode()
    for _ in range(64)
]


class _Backend(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("x-ray-llm-metric-tags", TAGS)
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        for wire in WIRES:
            self.wfile.write(f"{len(wire):x}\r\n".encode() + wire + b"\r\n")
            self.wfile.flush()
            time.sleep(0.002)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextlib.contextmanager
def _observer(tmp_path: Path, servicer, nbthread: int = 1, env=None):
    """Run HAProxy with the observer between an SSE backend and a gRPC sink.

    Yields (url, process, log path, rendered Lua path).
    """
    backend = ThreadingHTTPServer(("127.0.0.1", 0), _Backend)
    threading.Thread(target=backend.serve_forever, daemon=True).start()
    sink = grpc.server(ThreadPoolExecutor(max_workers=max(2, nbthread)))
    reporter_pb2_grpc.add_ReporterServiceServicer_to_server(servicer, sink)
    port = sink.add_insecure_port("127.0.0.1:0")
    sink.start()
    socket_path = stream_metrics_socket_path(str(tmp_path / "metrics"))
    source = (Path(haproxy.__file__).parent / "stream_metrics.lua").read_text()
    source = source.replace(
        "@STREAM_METRICS_EXPORT_CONFIG@",
        render_stream_metrics_export_config(socket_path),
    )
    source += '\nassert(native_metrics,"native HTX observer did not load")\n'
    lua = tmp_path / "metrics.lua"
    lua.write_text(source)
    http_port = _free_port()
    cfg = tmp_path / "haproxy.cfg"
    cfg.write_text(
        f"""global
    lua-load-per-thread {lua}
    nbthread {nbthread}
defaults
    mode http
    timeout connect 2s
    timeout client 5s
    timeout server 5s
frontend stream
    bind 127.0.0.1:{http_port}
    filter ray_stream_metrics
    http-request set-var(txn.via_ingress_request_router) bool(true)
    http-request set-var(txn.ingress_request_router_latency_us) int(20000)
    default_backend engine
backend engine
    server engine 127.0.0.1:{backend.server_port}
frontend ray_stream_metrics_export
    bind {socket_path} mode 600 ray-metrics-export
    timeout client 2s
    http-request use-service lua.stream_metrics_status if {{ path /status }}
    default_backend ray_stream_metrics_agent
backend ray_stream_metrics_agent
    option abortonclose
    timeout connect 1s
    timeout server 1s
    server agent 127.0.0.1:{port} proto h2
"""
    )
    log_path = tmp_path / "haproxy.log"
    with log_path.open("w") as log:
        proc = subprocess.Popen(
            [get_haproxy_binary(), "-db", "-f", str(cfg)],
            stdout=log,
            stderr=log,
            env=env,
        )
        try:
            time.sleep(0.2)
            assert proc.poll() is None, log_path.read_text()
            yield f"http://127.0.0.1:{http_port}", proc, log_path, lua
        finally:
            proc.terminate()
            proc.wait(timeout=10)
            sink.stop(0).wait()
            backend.shutdown()
            backend.server_close()


def _request(url: str) -> None:
    response = requests.get(url, timeout=2)
    response.raise_for_status()
    assert "x-ray-llm-metric-tags" not in response.headers
    assert response.content == b"".join(WIRES)


@pytest.mark.parametrize("nbthread", [1, 4])
@pytest.mark.parametrize(
    "retire", [False, True], ids=["recovery", "bounded-retirement"]
)
def test_stalled_export(tmp_path, nbthread, retire):
    """A blocked reporter cannot block the body filter or lose observations."""
    received = queue.Queue()
    blocked, unblock = threading.Event(), threading.Event()

    class Sink(reporter_pb2_grpc.ReporterServiceServicer):
        def ReportOCMetrics(self, request, context):
            blocked.set()
            assert unblock.wait(5)
            received.put(request)
            return reporter_pb2.ReportOCMetricsReply()

    try:
        with _observer(tmp_path, Sink(), nbthread) as (url, proc, log_path, _):
            _request(url)
            assert blocked.wait(3), log_path.read_text()
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda _: _request(url), range(4)))
            assert not unblock.is_set(), "Responses must finish before releasing export"
            if retire:
                proc.send_signal(signal.SIGUSR1)
                proc.wait(timeout=12)
                assert proc.returncode == 0
                return
            unblock.set()
            latest = {}
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                report = received.get(timeout=3)
                for metric in report.metrics:
                    assert "collection_errors" not in metric.metric_descriptor.name
                    for series in metric.timeseries:
                        component = series.label_values[0].value
                        latest[
                            (metric.metric_descriptor.name, component)
                        ] = series.points[0].distribution_value.count
                totals = {
                    name: sum(
                        value for (metric, _), value in latest.items() if metric == name
                    )
                    for name in [
                        "serve_llm_time_to_first_token_seconds",
                        "serve_llm_inter_token_latency_seconds",
                        "serve_llm_request_duration_seconds",
                        "serve_llm_router_overhead_seconds",
                    ]
                }
                if totals == {
                    "serve_llm_time_to_first_token_seconds": 5,
                    "serve_llm_inter_token_latency_seconds": 315,
                    "serve_llm_request_duration_seconds": 5,
                    "serve_llm_router_overhead_seconds": 5,
                }:
                    break
            else:
                pytest.fail(str(totals))
    finally:
        unblock.set()


@pytest.mark.parametrize("source", ["env", "file"])
def test_export_authorization(tmp_path, monkeypatch, source):
    """The exporter reads Ray's token from its existing source on every export;
    the token never appears in rendered configuration."""
    token = "secret-token-value"
    authorizations = queue.Queue()

    class Sink(reporter_pb2_grpc.ReporterServiceServicer):
        def ReportOCMetrics(self, request, context):
            authorizations.put(dict(context.invocation_metadata()).get("authorization"))
            return reporter_pb2.ReportOCMetricsReply()

    env = {k: v for k, v in os.environ.items() if not k.startswith("RAY_AUTH_TOKEN")}
    if source == "env":
        env["RAY_AUTH_TOKEN"] = token
        resolved = ""
    else:
        token_file = tmp_path / "token"
        token_file.write_text(token + "\n")
        resolved = str(token_file)
    monkeypatch.setattr(
        haproxy_stream_metrics, "stream_metrics_token_source", lambda: resolved
    )
    with _observer(tmp_path, Sink(), env=env) as (url, _, log_path, lua):
        assert token not in lua.read_text()
        assert token not in (tmp_path / "haproxy.cfg").read_text()
        _request(url)
        assert authorizations.get(timeout=5) == f"Bearer {token}", log_path.read_text()
        if source == "file":
            # Rotated file tokens apply to later exports.
            (tmp_path / "token").write_text("rotated\n")
            deadline = time.monotonic() + 5
            while authorizations.get(timeout=5) != "Bearer rotated":
                assert time.monotonic() < deadline


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
