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


class _FakeHAProxyApi:
    """Minimal HAProxyApi stand-in. Required by the collector constructor; the
    push-metric tests pass it as an inert dummy, while the node-metrics tests
    drive its backend_configs / stats."""

    def __init__(
        self, backend_configs: Optional[dict] = None, stats: Optional[dict] = None
    ):
        self.backend_configs = backend_configs or {}
        self._stats = stats or {}

    async def get_all_stats(self) -> dict:
        return self._stats

    def count_haproxy_processes(self) -> int:
        return 0

    async def compute_target_mismatch(self) -> int:
        # Inert: the real symmetric-difference logic is covered by
        # test_compute_target_mismatch against a real HAProxyApi.
        return 0


# ---------------------------------------------------------------------------
# parse_line: pure parser tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "sd_body, expected",
    [
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-1" '
            'router_latency_us="1234" body_truncated_full_length="" '
            'via_router="1" failed=""',
            ParsedMetrics(
                app="llm",
                ingress_request_intended_server="replica-1",
                ingress_request_actual_server="replica-1",
                ingress_request_router_latency_us=1234,
                ingress_request_body_truncated_full_length=None,
                ingress_request_via_router=True,
                ingress_request_failed=None,
            ),
            id="success-path-matched-pin",
        ),
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-2" '
            'router_latency_us="9000" body_truncated_full_length="" '
            'via_router="1" failed=""',
            ParsedMetrics(
                app="llm",
                ingress_request_intended_server="replica-1",
                ingress_request_actual_server="replica-2",
                ingress_request_router_latency_us=9000,
                ingress_request_body_truncated_full_length=None,
                ingress_request_via_router=True,
                ingress_request_failed=None,
            ),
            id="mismatch-redispatch",
        ),
        pytest.param(
            'app="llm" intended="replica-1" actual="replica-1" '
            'router_latency_us="2000" body_truncated_full_length="500000" '
            'via_router="1" failed=""',
            ParsedMetrics(
                app="llm",
                ingress_request_intended_server="replica-1",
                ingress_request_actual_server="replica-1",
                ingress_request_router_latency_us=2000,
                ingress_request_body_truncated_full_length=500000,
                ingress_request_via_router=True,
                ingress_request_failed=None,
            ),
            id="truncated-body",
        ),
        pytest.param(
            'app="llm" intended="" actual="<NOSRV>" '
            'router_latency_us="" body_truncated_full_length="" '
            'via_router="" failed="router_unreachable"',
            ParsedMetrics(
                app="llm",
                ingress_request_intended_server=None,
                ingress_request_actual_server="<NOSRV>",
                ingress_request_router_latency_us=None,
                ingress_request_body_truncated_full_length=None,
                ingress_request_via_router=False,
                ingress_request_failed="router_unreachable",
            ),
            id="failure-router-unreachable",
        ),
        pytest.param(
            'app="" intended="" actual="<NOSRV>" router_latency_us="" '
            'body_truncated_full_length="" via_router="" failed=""',
            ParsedMetrics(
                app=None,
                ingress_request_intended_server=None,
                ingress_request_actual_server="<NOSRV>",
                ingress_request_router_latency_us=None,
                ingress_request_body_truncated_full_length=None,
                ingress_request_via_router=False,
                ingress_request_failed=None,
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
        'body_truncated_full_length="" via_router="1" failed=""'
    )
    parsed = HAProxyMetricsCollector.parse_line(line)
    assert parsed is not None
    assert parsed.ingress_request_router_latency_us is None


def test_parse_line_extracts_general_http_fields() -> None:
    """A real HAProxy line carries the general request fields with %ST/%Ta
    rendered unquoted (bare), alongside the quoted var-based fields."""
    line = _line(
        'app="llm" route="/llm" method="POST" status=200 latency_ms=42 '
        'deployment="LLMDeployment" term_state=--'
    )
    parsed = HAProxyMetricsCollector.parse_line(line)
    assert parsed.app == "llm"
    assert parsed.route == "/llm"
    assert parsed.method == "POST"
    assert parsed.status_code == "200"
    assert parsed.latency_ms == 42
    assert parsed.deployment == "LLMDeployment"
    # %ts renders unquoted, like %ST/%Ta.
    assert parsed.termination_state == "--"
    # Router fields are absent on a non-router line.
    assert parsed.ingress_request_via_router is False
    assert parsed.ingress_request_router_latency_us is None


def test_parse_line_unescapes_plus_e_escaping() -> None:
    """HAProxy's `+E` log-format flag escapes `"`, `\\` and `]` inside a quoted
    value. The parser undoes that so the metric tag holds the original name --
    and a value containing `]` must not truncate the SD section at the wrong
    bracket."""
    # deployment name `a]b"c\d` -> on the wire: a\]b\"c\\d
    line = _line(
        'app="app" route="/" method="GET" status=200 latency_ms=1 '
        'deployment="a\\]b\\"c\\\\d" term_state=--'
    )
    parsed = HAProxyMetricsCollector.parse_line(line)
    assert parsed is not None
    # Section wasn't truncated at the escaped `]`; status still parsed.
    assert parsed.status_code == "200"
    assert parsed.deployment == 'a]b"c\\d'


def test_parse_line_general_fields_empty_for_system_endpoints() -> None:
    """A 404 / system-endpoint line has empty app & deployment (-> None) but a
    real status; the route may be empty (404) or the system path."""
    line = _line('app="" route="" method="GET" status=404 latency_ms=3 deployment=""')
    parsed = HAProxyMetricsCollector.parse_line(line)
    assert parsed.app is None
    assert parsed.route is None
    assert parsed.status_code == "404"
    assert parsed.latency_ms == 3
    assert parsed.deployment is None


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


class _RecordingIngressMetrics:
    """Stub for RequestIngressMetrics that captures record_request kwargs."""

    def __init__(self) -> None:
        self.calls: list = []

    def record_request(self, **kwargs) -> None:
        self.calls.append(kwargs)


@pytest.fixture
def collector() -> HAProxyMetricsCollector:
    """Build a collector with real metric constructors but stubbed inc/observe.

    The collector's metric attributes are replaced post-init with
    `_RecordingMetric` so tests can assert against captured calls.
    """
    c = HAProxyMetricsCollector(haproxy_api=_FakeHAProxyApi(), node_id="test-node")
    c.truncated_bodies_counter = _RecordingMetric()
    c.latency_histogram = _RecordingMetric()
    c.replica_mismatches_counter = _RecordingMetric()
    c.failures_counter = _RecordingMetric()
    c.requests_counter = _RecordingMetric()
    c.request_ingress_metrics = _RecordingIngressMetrics()
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
        ingress_request_intended_server=intended,
        ingress_request_actual_server=actual,
        ingress_request_router_latency_us=latency_us,
        ingress_request_body_truncated_full_length=truncated,
        ingress_request_via_router=True,
        ingress_request_failed=None,
    )


def test_record_success_path_observes_latency(collector) -> None:
    collector.record(_ok(latency_us=2500))
    assert collector.latency_histogram.calls == [
        (
            "observe",
            {"application": "llm", "outcome": "success"},
            2.5,
        )  # us → ms conversion
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
        ingress_request_intended_server=None,
        ingress_request_actual_server="<NOSRV>",
        ingress_request_router_latency_us=None,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=False,
        ingress_request_failed=reason,
    )
    collector.record(parsed)
    assert collector.failures_counter.calls == [
        ("inc", {"application": "llm", "reason": reason}, 1.0)
    ]
    # router_latency_us=None means the Lua timer wasn't set (e.g. metrics
    # disabled in the rendered Lua, or earliest-stage failure). The
    # histogram should stay quiet; failures with a real latency value are
    # covered by the test below.
    assert collector.latency_histogram.calls == []
    assert collector.truncated_bodies_counter.calls == []
    assert collector.replica_mismatches_counter.calls == []


@pytest.mark.parametrize(
    "reason",
    [
        "router_unreachable",
        "router_non_200",
        "unparseable_replica_id",
        "unknown_replica_id",
    ],
)
def test_record_failure_with_latency_observes_outcome_failure(
    collector, reason
) -> None:
    """The Lua action now wraps the routing call with the timer, so failure
    paths carry a real latency_us. record() must tag the observation with
    `outcome="failure"` so success vs failure latency can be split in PromQL."""
    parsed = ParsedMetrics(
        app="llm",
        ingress_request_intended_server=None,
        ingress_request_actual_server="<NOSRV>",
        ingress_request_router_latency_us=4200,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=False,
        ingress_request_failed=reason,
    )
    collector.record(parsed)
    assert collector.latency_histogram.calls == [
        ("observe", {"application": "llm", "outcome": "failure"}, 4.2)
    ]
    # Failure path still bumps the failure + requests counters.
    assert collector.failures_counter.calls == [
        ("inc", {"application": "llm", "reason": reason}, 1.0)
    ]
    assert collector.requests_counter.calls == [("inc", {"application": "llm"}, 1.0)]


def test_record_skips_when_not_via_router_and_not_failed(collector) -> None:
    """Request didn't hit the router path at all (no router-bearing app, or
    router state not yet pushed). Nothing should be recorded."""
    parsed = ParsedMetrics(
        app=None,
        ingress_request_intended_server=None,
        ingress_request_actual_server="<NOSRV>",
        ingress_request_router_latency_us=None,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=False,
        ingress_request_failed=None,
    )
    collector.record(parsed)
    assert collector.failures_counter.calls == []
    assert collector.latency_histogram.calls == []
    assert collector.truncated_bodies_counter.calls == []
    assert collector.replica_mismatches_counter.calls == []
    assert collector.requests_counter.calls == []


def test_record_success_increments_requests_counter(collector) -> None:
    """Every successful router consultation bumps the requests counter
    with just the `application` tag."""
    collector.record(_ok())
    assert collector.requests_counter.calls == [("inc", {"application": "llm"}, 1.0)]


@pytest.mark.parametrize(
    "reason",
    [
        "router_unreachable",
        "router_non_200",
        "unparseable_replica_id",
        "unknown_replica_id",
    ],
)
def test_record_failure_increments_requests_counter(collector, reason) -> None:
    """Failed consultations are still consultations: requests_counter must
    bump on every failure reason so failure_total / requests_total yields
    the failure ratio."""
    parsed = ParsedMetrics(
        app="llm",
        ingress_request_intended_server=None,
        ingress_request_actual_server="<NOSRV>",
        ingress_request_router_latency_us=None,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=False,
        ingress_request_failed=reason,
    )
    collector.record(parsed)
    assert collector.requests_counter.calls == [("inc", {"application": "llm"}, 1.0)]


def test_record_requests_counter_uses_unknown_app_tag_when_app_missing(
    collector,
) -> None:
    """Missing `app` is reported as "unknown" instead of dropping the
    observation, matching the rest of the record() paths."""
    parsed = ParsedMetrics(
        app=None,
        ingress_request_intended_server="replica-A",
        ingress_request_actual_server="replica-A",
        ingress_request_router_latency_us=1000,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=True,
        ingress_request_failed=None,
    )
    collector.record(parsed)
    assert collector.requests_counter.calls == [
        ("inc", {"application": "unknown"}, 1.0)
    ]


def test_record_uses_unknown_app_tag_when_app_missing(collector) -> None:
    """`application` is a required tag; missing app → 'unknown' rather than
    dropping the observation. Misconfigured frontends should still surface."""
    parsed = ParsedMetrics(
        app=None,
        ingress_request_intended_server="replica-A",
        ingress_request_actual_server="replica-B",
        ingress_request_router_latency_us=1000,
        ingress_request_body_truncated_full_length=None,
        ingress_request_via_router=True,
        ingress_request_failed=None,
    )
    collector.record(parsed)
    assert collector.replica_mismatches_counter.calls == [
        ("inc", {"application": "unknown"}, 1.0)
    ]


# ---------------------------------------------------------------------------
# record: RequestIngressMetrics (serve_num_http_*) path
# ---------------------------------------------------------------------------


def _http(
    *,
    app: Optional[str] = "llm",
    route: Optional[str] = "/llm",
    method: Optional[str] = "GET",
    status: Optional[str] = "200",
    latency_ms: Optional[int] = 42,
    deployment: Optional[str] = "D",
    via_router: bool = False,
    termination_state: Optional[str] = None,
) -> ParsedMetrics:
    return ParsedMetrics(
        app=app,
        ingress_request_via_router=via_router,
        route=route,
        method=method,
        status_code=status,
        latency_ms=latency_ms,
        deployment=deployment,
        termination_state=termination_state,
    )


def test_record_emits_http_ingress_metrics(collector) -> None:
    collector.record(_http())
    assert collector.request_ingress_metrics.calls == [
        {
            "route": "/llm",
            "method": "GET",
            "application": "llm",
            "status_code": "200",
            "latency_ms": 42.0,
            "is_error": False,
            "deployment_name": "D",
        }
    ]


def test_record_http_ingress_marks_4xx_5xx_as_error(collector) -> None:
    collector.record(_http(status="503"))
    assert collector.request_ingress_metrics.calls[0]["is_error"] is True
    collector.record(_http(status="404"))
    assert collector.request_ingress_metrics.calls[1]["is_error"] is True


def test_record_http_ingress_empty_app_for_system_endpoints(collector) -> None:
    """404 / health / routes carry no app context (parsed as None); they are
    still recorded, with empty application/route/deployment -- matching the
    proxy, which records them with empty tags."""
    collector.record(_http(app=None, route="", deployment=None, status="404"))
    call = collector.request_ingress_metrics.calls[0]
    assert call["application"] == ""
    assert call["route"] == ""
    assert call["deployment_name"] == ""
    assert call["is_error"] is True


def test_record_http_ingress_client_abort_recorded_as_499(collector) -> None:
    """A client abort (HAProxy term_state starting with "C") is recorded as
    status 499, matching the Python proxy's client-disconnect convention -- even
    though HAProxy logged its own status (here 400)."""
    collector.record(_http(status="400", termination_state="CH"))
    call = collector.request_ingress_metrics.calls[0]
    assert call["status_code"] == "499"
    assert call["is_error"] is True


def test_record_http_ingress_non_client_term_state_keeps_status(collector) -> None:
    """A genuine 400 (no client abort) keeps its status; only leading-"C"
    termination states are remapped to 499."""
    collector.record(_http(status="400", termination_state="--"))
    assert collector.request_ingress_metrics.calls[0]["status_code"] == "400"
    # Server-side termination ("S...") is also left untouched.
    collector.record(_http(status="502", termination_state="SH"))
    assert collector.request_ingress_metrics.calls[1]["status_code"] == "502"


def test_record_http_ingress_skipped_without_status(collector) -> None:
    """A router-only line (no status) is not a per-request HTTP observation."""
    collector.record(_ok())  # status_code defaults to None
    assert collector.request_ingress_metrics.calls == []


def test_record_http_ingress_recorded_even_via_router(collector) -> None:
    """A router request is still an HTTP request: both the ingress metric and
    the router requests counter fire."""
    collector.record(_http(via_router=True))
    assert len(collector.request_ingress_metrics.calls) == 1
    assert collector.requests_counter.calls == [("inc", {"application": "llm"}, 1.0)]


def test_record_http_ingress_defaults_missing_latency_to_zero(collector) -> None:
    collector.record(_http(latency_ms=None))
    assert collector.request_ingress_metrics.calls[0]["latency_ms"] == 0.0


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
            'body_truncated_full_length="" via_router="1" failed=""'
        ),
        ("addr", 0),
    )
    assert collector.latency_histogram.calls == [
        ("observe", {"application": "llm", "outcome": "success"}, 0.1)
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
    collector = HAProxyMetricsCollector(
        haproxy_api=_FakeHAProxyApi(), node_id="test-node"
    )
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
                'body_truncated_full_length="" via_router="1" failed=""'
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
            ("observe", {"application": "llm", "outcome": "success"}, 0.5)
        ]
    finally:
        collector.close()

    # close() should both close the transport and unlink the socket file.
    assert not os.path.exists(sock_path)


@pytest.mark.asyncio
async def test_close_is_idempotent_and_safe_without_bind() -> None:
    """close() should never raise -- pre-bind, post-bind, or called twice."""
    collector = HAProxyMetricsCollector(
        haproxy_api=_FakeHAProxyApi(), node_id="test-node"
    )
    # never bound
    collector.close()
    collector.close()


@pytest.mark.asyncio
async def test_bind_replaces_existing_socket_file(tmp_path) -> None:
    """Stale socket file from a crashed predecessor should not block bind."""
    sock_path = tmp_path / "metrics.sock"
    sock_path.write_bytes(b"")  # touch a stale file
    assert sock_path.exists()

    collector = HAProxyMetricsCollector(
        haproxy_api=_FakeHAProxyApi(), node_id="test-node"
    )
    try:
        await collector.bind_and_attach(str(sock_path), loop=asyncio.get_event_loop())
        assert sock_path.exists()
    finally:
        collector.close()


# ---------------------------------------------------------------------------
# Node-level poll metrics: target mismatch
# ---------------------------------------------------------------------------


def _backend(name: str, server_names, fallback: Optional[str] = None):
    from ray.serve._private.haproxy import BackendConfig, ServerConfig

    return BackendConfig(
        name=name,
        path_prefix="/",
        servers=[
            ServerConfig(name=s, host="127.0.0.1", port=9000 + i)
            for i, s in enumerate(server_names)
        ],
        fallback_server=(
            ServerConfig(name=fallback, host="127.0.0.1", port=8999)
            if fallback is not None
            else None
        ),
    )


@pytest.mark.parametrize(
    "broadcasted, reported, expected_mismatch",
    [
        # Fully converged: every broadcasted server is reported, nothing extra.
        ({"s1", "s2"}, {"s1", "s2"}, 0),
        # A broadcasted server hasn't been applied to HAProxy yet.
        ({"s1", "s2", "s3"}, {"s1", "s2"}, 1),
        # HAProxy still reports a stale server we no longer broadcast.
        ({"s1", "s2"}, {"s1", "s2", "stale"}, 1),
        # Divergence in both directions counts each side.
        ({"s1", "s2"}, {"s1", "stale"}, 2),
    ],
)
def test_compute_target_mismatch(broadcasted, reported, expected_mismatch) -> None:
    # compute_target_mismatch lives on HAProxyApi (it reads backend_configs and
    # get_all_stats), so exercise the real method with a stubbed stats source.
    from ray.serve._private.haproxy import HAProxyApi, HAProxyConfig

    with tempfile.TemporaryDirectory() as td:
        api = HAProxyApi(
            cfg=HAProxyConfig(socket_path=os.path.join(td, "admin.sock")),
            backend_configs={"http-app": _backend("http-app", broadcasted)},
            config_file_path=os.path.join(td, "haproxy.cfg"),
        )

        async def fake_get_all_stats():
            return {"http-app": {name: object() for name in reported}}

        api.get_all_stats = fake_get_all_stats
        assert asyncio.run(api.compute_target_mismatch()) == expected_mismatch


def test_compute_target_mismatch_treats_fallback_server_as_expected() -> None:
    """The generated config renders the fallback server as a real backup
    `server` line, so HAProxy reports it in stats. It must count as expected,
    or the gauge would never converge to zero for backends with a fallback."""
    from ray.serve._private.haproxy import HAProxyApi, HAProxyConfig

    with tempfile.TemporaryDirectory() as td:
        api = HAProxyApi(
            cfg=HAProxyConfig(socket_path=os.path.join(td, "admin.sock")),
            backend_configs={
                "http-app": _backend("http-app", {"s1", "s2"}, fallback="fb")
            },
            config_file_path=os.path.join(td, "haproxy.cfg"),
        )

        async def fake_get_all_stats():
            # HAProxy reports the two servers plus the fallback backup server.
            return {"http-app": {"s1": object(), "s2": object(), "fb": object()}}

        api.get_all_stats = fake_get_all_stats
        assert asyncio.run(api.compute_target_mismatch()) == 0


def test_count_ongoing_http_requests_sums_http_backend_scur() -> None:
    """num_ongoing is sampled from each HTTP backend's aggregate `scur`,
    including the `-via-ingress-request-router` backend; gRPC and internal
    backends are excluded."""
    from ray.serve._private.common import RequestProtocol
    from ray.serve._private.haproxy import BackendConfig, HAProxyApi, HAProxyConfig

    # `show stat` CSV: an HTTP app backend, its via-router variant, a gRPC app
    # backend, and internal rows (frontend / stats). scur is what's summed.
    show_stat = (
        "# pxname,svname,scur,qcur,status\n"
        "http_frontend,FRONTEND,9,0,OPEN\n"
        "http-app,s1,3,0,UP\n"
        "http-app,s2,2,0,UP\n"
        "http-app,BACKEND,5,0,UP\n"
        "http-app-via-ingress-request-router,r1,2,0,UP\n"
        "http-app-via-ingress-request-router,BACKEND,2,0,UP\n"
        "grpc-app,g1,3,0,UP\n"
        "grpc-app,BACKEND,3,0,UP\n"
        "stats,BACKEND,1,0,UP\n"
    )
    with tempfile.TemporaryDirectory() as td:
        api = HAProxyApi(
            cfg=HAProxyConfig(socket_path=os.path.join(td, "admin.sock")),
            backend_configs={
                "http-app": BackendConfig(
                    name="http-app", path_prefix="/", protocol=RequestProtocol.HTTP
                ),
                "grpc-app": BackendConfig(
                    name="grpc-app", path_prefix="/", protocol=RequestProtocol.GRPC
                ),
            },
            config_file_path=os.path.join(td, "haproxy.cfg"),
        )

        async def fake_send(command):
            assert command == "show stat"
            return show_stat

        api._send_socket_command = fake_send
        # http-app BACKEND (5) + via-router BACKEND (2); gRPC (3) and the stats
        # listener are excluded.
        assert asyncio.run(api.count_ongoing_http_requests()) == 7


@pytest.mark.asyncio
async def test_start_polls_and_binds_dgram_reader(tmp_path) -> None:
    """start() begins node polling and always binds the per-request dgram
    reader, creating the socket's parent directory and returning its bind
    task."""
    api = _FakeHAProxyApi(backend_configs={}, stats={})
    loop = asyncio.get_event_loop()

    sock_path = tmp_path / "subdir" / "metrics.sock"
    collector = HAProxyMetricsCollector(haproxy_api=api, node_id="test-node")
    attach_task = collector.start(
        loop,
        poll_interval_s=10.0,
        metrics_socket_path=str(sock_path),
    )
    try:
        assert attach_task is not None
        await attach_task  # bind completes; makedirs created the parent dir
        assert sock_path.exists()
        assert collector._node_metrics_task is not None
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
            # `metrics_enabled` gates the per-request SD log line + socket;
            # `ingress_request_router_metrics_enabled` gates the router-specific
            # fields appended to it. These tests toggle both together (all
            # metrics on vs all off).
            metrics_enabled=enabled,
            ingress_request_router_metrics_enabled=enabled,
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
    # Match the directive, not the bare token: an explanatory comment in the
    # global block mentions "rfc5424" unconditionally; the `format rfc5424` log
    # target is what's actually gated on metrics being enabled.
    assert "format rfc5424" not in rendered


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
            ingress_request_router_metrics_enabled=enabled,
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
