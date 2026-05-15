"""Unit tests for `HAProxyMetricsCollector`.

The collector is the parser + recorder that sits behind the Unix dgram
socket HAProxy writes RFC 5424 log lines to. These tests drive the pure
parsing and record paths directly (no socket, no HAProxy, no asyncio).
End-to-end coverage of the whole pipeline -- HAProxy emits a log line,
asyncio reader picks it up, metric increments visible on /metrics --
lives in the integration suite alongside `test_haproxy_api.py`.
"""

import asyncio
import os
import socket as stdlib_socket
import tempfile
from typing import Optional

import pytest

from ray.serve._private.haproxy_metrics import (
    HAProxyMetricsCollector,
    ParsedMetrics,
    _DatagramHandler,
)

# Sample syslog line shaped the way HAProxy emits when rendered through
# the rfc5424 log target. The PRI / timestamp / app-name fields are
# intentionally noisy because the parser must locate the SD section by
# anchor, not by position.
_SAMPLE_PREFIX = "<134>1 2026-05-15T12:34:56.789Z host haproxy 12345 - "


def _line(sd_body: str) -> bytes:
    """Build a fake RFC 5424 datagram with the given SD body."""
    return f"{_SAMPLE_PREFIX}[serve@1 {sd_body}] - normal log message".encode()


# ---------------------------------------------------------------------------
# parse_line: pure parser tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sd_body, expected",
    [
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-1" '
            'router_latency_us="1234" body_truncated_full_length="-" '
            'via_router="1" failed="-"',
            ParsedMetrics(
                app="llm",
                intended_server="replica-1",
                actual_server="replica-1",
                router_latency_us=1234,
                body_truncated_full_length=None,
                via_router=True,
                failed=None,
            ),
            id="success-path-matched-pin",
        ),
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-2" '
            'router_latency_us="9000" body_truncated_full_length="-" '
            'via_router="1" failed="-"',
            ParsedMetrics(
                app="llm",
                intended_server="replica-1",
                actual_server="replica-2",
                router_latency_us=9000,
                body_truncated_full_length=None,
                via_router=True,
                failed=None,
            ),
            id="mismatch-redispatch",
        ),
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-1" '
            'router_latency_us="2000" body_truncated_full_length="500000" '
            'via_router="1" failed="-"',
            ParsedMetrics(
                app="llm",
                intended_server="replica-1",
                actual_server="replica-1",
                router_latency_us=2000,
                body_truncated_full_length=500000,
                via_router=True,
                failed=None,
            ),
            id="truncated-body",
        ),
        pytest.param(
            'app="llm" intended="-" actual="<NOSRV>" '
            'router_latency_us="-" body_truncated_full_length="-" '
            'via_router="-" failed="router_unreachable"',
            ParsedMetrics(
                app="llm",
                intended_server=None,
                actual_server="<NOSRV>",
                router_latency_us=None,
                body_truncated_full_length=None,
                via_router=False,
                failed="router_unreachable",
            ),
            id="failure-router-unreachable",
        ),
        pytest.param(
            'app="-" intended="-" actual="<NOSRV>" router_latency_us="-" '
            'body_truncated_full_length="-" via_router="-" failed="-"',
            ParsedMetrics(
                app=None,
                intended_server=None,
                actual_server="<NOSRV>",
                router_latency_us=None,
                body_truncated_full_length=None,
                via_router=False,
                failed=None,
            ),
            id="all-unset-not-routed",
        ),
    ],
)
def test_parse_line_extracts_expected_fields(
    sd_body: str, expected: ParsedMetrics
) -> None:
    parsed = HAProxyMetricsCollector.parse_line(_line(sd_body))
    assert parsed == expected


@pytest.mark.parametrize(
    "raw, why",
    [
        pytest.param(b"", "empty"),
        pytest.param(b"not a syslog line", "no-sd-section"),
        pytest.param(
            f'{_SAMPLE_PREFIX}[other@1 foo="bar"] msg'.encode(),
            "different-sd-id",
            id="wrong-sd-id",
        ),
        # Garbage bytes mid-line — the parser should still return None or a
        # best-effort ParsedMetrics rather than crashing.
        pytest.param(b"\xff\xfe\x00", "binary-garbage"),
    ],
)
def test_parse_line_returns_none_when_sd_missing(raw: bytes, why: str) -> None:
    assert HAProxyMetricsCollector.parse_line(raw) is None, why


def test_parse_line_handles_unknown_int_value() -> None:
    # router_latency_us isn't a number — parser should map it to None
    # rather than raise.
    line = _line(
        'app="llm" intended="r" actual="r" router_latency_us="notanint" '
        'body_truncated_full_length="-" via_router="1" failed="-"'
    )
    parsed = HAProxyMetricsCollector.parse_line(line)
    assert parsed is not None
    assert parsed.router_latency_us is None


# ---------------------------------------------------------------------------
# record: each metric path, driven from a constructed ParsedMetrics
# ---------------------------------------------------------------------------


class _RecordingMetric:
    """Stub Counter / Histogram that captures calls in-memory.

    Lets us assert against record() without depending on ray.util.metrics's
    Prometheus export wiring. Each call records (kind, tags, value).
    """

    def __init__(self) -> None:
        self.calls: list = []

    def inc(self, value: float = 1.0, tags: Optional[dict] = None) -> None:
        self.calls.append(("inc", dict(tags or {}), value))

    def observe(self, value: float, tags: Optional[dict] = None) -> None:
        self.calls.append(("observe", dict(tags or {}), value))


@pytest.fixture
def collector() -> HAProxyMetricsCollector:
    """Build a collector with real metric constructors but stubbed inc/observe.

    The collector's metric attributes are replaced post-init with
    `_RecordingMetric` so tests can assert against captured calls.
    """
    c = HAProxyMetricsCollector()
    c.truncated_bodies_counter = _RecordingMetric()
    c.latency_histogram = _RecordingMetric()
    c.replica_mismatches_counter = _RecordingMetric()
    c.failures_counter = _RecordingMetric()
    return c


def _ok(
    *,
    app: str = "llm",
    intended: str = "replica-A",
    actual: str = "replica-A",
    latency_us: Optional[int] = 1500,
    truncated: Optional[int] = None,
) -> ParsedMetrics:
    return ParsedMetrics(
        app=app,
        intended_server=intended,
        actual_server=actual,
        router_latency_us=latency_us,
        body_truncated_full_length=truncated,
        via_router=True,
        failed=None,
    )


def test_record_success_path_observes_latency(collector) -> None:
    collector.record(_ok(latency_us=2500))
    assert collector.latency_histogram.calls == [
        ("observe", {"application": "llm"}, 2.5)  # us → ms conversion
    ]
    assert collector.truncated_bodies_counter.calls == []
    assert collector.replica_mismatches_counter.calls == []
    assert collector.failures_counter.calls == []


def test_record_truncation_increments_counter(collector) -> None:
    collector.record(_ok(truncated=500_000))
    assert collector.truncated_bodies_counter.calls == [
        ("inc", {"application": "llm"}, 1.0)
    ]


def test_record_mismatch_when_intended_and_actual_differ(collector) -> None:
    collector.record(_ok(intended="replica-A", actual="replica-B"))
    assert collector.replica_mismatches_counter.calls == [
        ("inc", {"application": "llm"}, 1.0)
    ]


@pytest.mark.parametrize(
    "intended, actual, why",
    [
        ("replica-A", "replica-A", "matched-pin"),
        ("replica-A", "<NOSRV>", "no-server-reached"),
        ("replica-A", None, "actual-missing"),
        (None, "replica-A", "intended-missing"),
    ],
)
def test_record_does_not_count_mismatch(collector, intended, actual, why) -> None:
    collector.record(_ok(intended=intended, actual=actual))
    assert collector.replica_mismatches_counter.calls == [], why


@pytest.mark.parametrize(
    "reason",
    [
        "router_unreachable",
        "router_non_200",
        "unparseable_replica_id",
        "unknown_replica_id",
    ],
)
def test_record_failure_increments_failures_counter_with_reason(
    collector, reason
) -> None:
    parsed = ParsedMetrics(
        app="llm",
        intended_server=None,
        actual_server="<NOSRV>",
        router_latency_us=None,
        body_truncated_full_length=None,
        via_router=False,
        failed=reason,
    )
    collector.record(parsed)
    assert collector.failures_counter.calls == [
        ("inc", {"application": "llm", "reason": reason}, 1.0)
    ]
    # Failed routes never set via_router; success-path metrics stay quiet.
    assert collector.latency_histogram.calls == []
    assert collector.truncated_bodies_counter.calls == []
    assert collector.replica_mismatches_counter.calls == []


def test_record_skips_when_not_via_router_and_not_failed(collector) -> None:
    """Request didn't hit the router path at all (no router-bearing app, or
    router state not yet pushed). Nothing should be recorded."""
    parsed = ParsedMetrics(
        app=None,
        intended_server=None,
        actual_server="<NOSRV>",
        router_latency_us=None,
        body_truncated_full_length=None,
        via_router=False,
        failed=None,
    )
    collector.record(parsed)
    assert collector.failures_counter.calls == []
    assert collector.latency_histogram.calls == []
    assert collector.truncated_bodies_counter.calls == []
    assert collector.replica_mismatches_counter.calls == []


def test_record_uses_unknown_app_tag_when_app_missing(collector) -> None:
    """`application` is a required tag; missing app → 'unknown' rather than
    dropping the observation. Misconfigured frontends should still surface."""
    parsed = ParsedMetrics(
        app=None,
        intended_server="replica-A",
        actual_server="replica-B",
        router_latency_us=1000,
        body_truncated_full_length=None,
        via_router=True,
        failed=None,
    )
    collector.record(parsed)
    assert collector.replica_mismatches_counter.calls == [
        ("inc", {"application": "unknown"}, 1.0)
    ]


# ---------------------------------------------------------------------------
# DatagramHandler: end-to-end through the asyncio protocol layer
# ---------------------------------------------------------------------------


def test_datagram_handler_dispatches_to_record(collector) -> None:
    """Handler should parse the bytes and call record(); a malformed line
    should not raise out of datagram_received()."""
    handler = _DatagramHandler(collector)
    handler.datagram_received(
        _line(
            'app="llm" intended="X" actual="X" router_latency_us="100" '
            'body_truncated_full_length="-" via_router="1" failed="-"'
        ),
        ("addr", 0),
    )
    assert collector.latency_histogram.calls == [
        ("observe", {"application": "llm"}, 0.1)
    ]


def test_datagram_handler_swallows_malformed_lines(collector) -> None:
    handler = _DatagramHandler(collector)
    # Should not raise.
    handler.datagram_received(b"\xff junk \x00", ("addr", 0))
    assert collector.latency_histogram.calls == []


# ---------------------------------------------------------------------------
# bind_and_attach / close: real socket round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bind_and_attach_receives_datagram_then_close_unlinks(
    tmp_path,
) -> None:
    """End-to-end on the asyncio path: bind a dgram socket, send a real
    syslog line to it from another socket, assert the metric was recorded,
    then close and verify the socket file is gone."""
    collector = HAProxyMetricsCollector()
    # Replace the metric objects so we can assert on them without depending
    # on Ray's Prometheus registry.
    collector.latency_histogram = _RecordingMetric()
    collector.truncated_bodies_counter = _RecordingMetric()
    collector.replica_mismatches_counter = _RecordingMetric()
    collector.failures_counter = _RecordingMetric()

    sock_path = str(tmp_path / "metrics.sock")
    try:
        await collector.bind_and_attach(sock_path, loop=asyncio.get_event_loop())
        assert os.path.exists(sock_path)

        # Send one valid syslog line from a fresh client socket.
        client = stdlib_socket.socket(stdlib_socket.AF_UNIX, stdlib_socket.SOCK_DGRAM)
        try:
            line = _line(
                'app="llm" intended="r" actual="r" router_latency_us="500" '
                'body_truncated_full_length="-" via_router="1" failed="-"'
            )
            client.sendto(line, sock_path)
        finally:
            client.close()

        # Give asyncio a tick to drain.
        for _ in range(50):
            if collector.latency_histogram.calls:
                break
            await asyncio.sleep(0.01)

        assert collector.latency_histogram.calls == [
            ("observe", {"application": "llm"}, 0.5)
        ]
    finally:
        collector.close()

    # close() should both close the transport and unlink the socket file.
    assert not os.path.exists(sock_path)


@pytest.mark.asyncio
async def test_close_is_idempotent_and_safe_without_bind() -> None:
    """close() should never raise -- pre-bind, post-bind, or called twice."""
    collector = HAProxyMetricsCollector()
    # never bound
    collector.close()
    collector.close()


@pytest.mark.asyncio
async def test_bind_replaces_existing_socket_file(tmp_path) -> None:
    """Stale socket file from a crashed predecessor should not block bind."""
    sock_path = tmp_path / "metrics.sock"
    sock_path.write_bytes(b"")  # touch a stale file
    assert sock_path.exists()

    collector = HAProxyMetricsCollector()
    try:
        await collector.bind_and_attach(str(sock_path), loop=asyncio.get_event_loop())
        assert sock_path.exists()
    finally:
        collector.close()


# ---------------------------------------------------------------------------
# Config rendering: metrics-enabled vs metrics-disabled
# ---------------------------------------------------------------------------


def _render_with_metrics(enabled: bool) -> str:
    """Render the HAProxy config with metrics on or off; return the text.

    Imports inside the function so the module-level test discovery doesn't
    drag in HAProxy template rendering for tests that don't need it.
    """

    from ray.serve._private.haproxy import (
        BackendConfig,
        HAProxyApi,
        HAProxyConfig,
        ServerConfig,
    )
    from ray.serve.config import HTTPOptions

    with tempfile.TemporaryDirectory() as td:
        cfg = HAProxyConfig(
            http_options=HTTPOptions(host="127.0.0.1", port=8000),
            socket_path=os.path.join(td, "admin.sock"),
            metrics_enabled=enabled,
            metrics_socket_path=os.path.join(td, "metrics.sock"),
            has_received_routes=True,
            has_received_servers=True,
        )
        backend = BackendConfig(
            name="llm",
            path_prefix="/",
            app_name="llm",
            servers=[
                ServerConfig(
                    name="A", host="127.0.0.1", port=9001, replica_id="actor-A"
                ),
            ],
            ingress_request_router_servers=[
                ServerConfig(name="router", host="127.0.0.1", port=9100),
            ],
        )
        api = HAProxyApi(
            cfg=cfg,
            backend_configs={"llm": backend},
            config_file_path=os.path.join(td, "haproxy.cfg"),
        )
        api._generate_config_file_internal()
        with open(os.path.join(td, "haproxy.cfg")) as f:
            return f.read()


def test_rendered_config_contains_metrics_directives_when_enabled() -> None:
    rendered = _render_with_metrics(enabled=True)
    assert "log-format-sd" in rendered
    assert "[serve@1" in rendered
    assert "format rfc5424" in rendered
    assert "router_latency_us" in rendered


def test_rendered_config_omits_metrics_directives_when_disabled() -> None:
    rendered = _render_with_metrics(enabled=False)
    assert "log-format-sd" not in rendered
    assert "[serve@1" not in rendered
    assert "rfc5424" not in rendered


def _render_lua_with_metrics(enabled: bool) -> str:
    """Render the ingress-request-router Lua and return its text."""

    from ray.serve._private.haproxy import (
        BackendConfig,
        HAProxyApi,
        HAProxyConfig,
        ServerConfig,
    )
    from ray.serve.config import HTTPOptions

    with tempfile.TemporaryDirectory() as td:
        cfg = HAProxyConfig(
            http_options=HTTPOptions(host="127.0.0.1", port=8000),
            socket_path=os.path.join(td, "admin.sock"),
            metrics_enabled=enabled,
            metrics_socket_path=os.path.join(td, "metrics.sock"),
            has_received_routes=True,
            has_received_servers=True,
        )
        backend = BackendConfig(
            name="llm",
            path_prefix="/",
            app_name="llm",
            servers=[
                ServerConfig(
                    name="A", host="127.0.0.1", port=9001, replica_id="actor-A"
                ),
            ],
            ingress_request_router_servers=[
                ServerConfig(name="router", host="127.0.0.1", port=9100),
            ],
        )
        api = HAProxyApi(
            cfg=cfg,
            backend_configs={"llm": backend},
            config_file_path=os.path.join(td, "haproxy.cfg"),
        )
        lua_path = api._write_ingress_request_router_lua([backend])
        assert lua_path is not None
        with open(lua_path) as f:
            return f.read()


def test_rendered_lua_has_timing_calls_when_metrics_enabled() -> None:
    lua = _render_lua_with_metrics(enabled=True)
    assert "core.now()" in lua
    assert "ingress_request_router_latency_us" in lua
    assert "ingress_request_router_truncated_full_length" in lua


def test_rendered_lua_has_no_timing_calls_when_metrics_disabled() -> None:
    lua = _render_lua_with_metrics(enabled=False)
    # When metrics are off, the substitutions become empty strings -- the
    # Lua should have no `core.now()` calls and no metric-only set_var
    # references at all.
    assert "core.now()" not in lua
    assert "ingress_request_router_latency_us" not in lua
    assert "ingress_request_router_truncated_full_length" not in lua


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
