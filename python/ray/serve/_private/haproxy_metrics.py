"""Metrics collection for the HAProxy ingress request router data path.

HAProxy is configured to emit one RFC 5424 syslog line per request to a
dedicated Unix dgram socket. Existing rfc3164 log targets are unaffected.
`HAProxyMetricsCollector` owns the parsing and the `ray.util.metrics`
objects; `_DatagramHandler` is the asyncio glue that hands datagrams in.
"""

import asyncio
import logging
import os
import re
import socket
from dataclasses import dataclass
from typing import Optional, cast

from ray.serve._private.common import RequestProtocol
from ray.serve._private.haproxy import HAProxyApi
from ray.serve._private.request_ingress_metrics import RequestIngressMetrics
from ray.util import metrics

logger = logging.getLogger(__name__)

# SD-ID we publish under. The leading bracket + this string is what the
# parser anchors on; only lines containing this section are processed.
_SD_ID = "serve@1"

# RFC 5424 SD element looks like `[serve@1 key="value" key="value"]`. Capture
# the body between the SD-ID and the closing `]`. A `]` inside a value is
# escaped as `\]`, so the body matches escaped pairs and stops at the first
# unescaped `]`.
_SD_SECTION_RE = re.compile(r"\[" + re.escape(_SD_ID) + r"(?P<body>(?:[^\]\\]|\\.)*)\]")

# Capture `key=value` pairs where the value is either RFC 5424 quoted (may
# contain spaces; backslash escapes allowed) or a bare token. HAProxy quotes
# sample-fetch values (`%[var(...)]`) and string aliases like `%HM`, but renders
# numeric aliases (`%ST`, `%Ta`) unquoted, so the parser must accept both.
_KV_RE = re.compile(r'(\w+)=(?:"((?:[^"\\]|\\.)*)"|(\S+))')

# HAProxy renders unset txn vars as an empty string in log-format. We map
# that to None so callers don't have to distinguish "unset" from "empty".
_UNSET = ""

# HAProxy escapes some characters in a quoted log value with a leading
# backslash. Drop the backslash so the tag holds the original name.
# Example: `a\"b` -> `a"b`.
_UNESCAPE_RE = re.compile(r"\\(.)")


def _unescape(value: str) -> str:
    return _UNESCAPE_RE.sub(r"\1", value)


@dataclass
class ParsedMetrics:
    """One per-request observation, parsed from the SD section.

    The first group is the general per-request ingress data present on every
    HTTP request through the frontend; it feeds the `serve_num_http_*` /
    `serve_http_request_latency_ms` families. The `ingress_request_*` fields
    are router-specific and only populated when ingress-request-router metrics
    are enabled and the request went through (or attempted) the router.
    """

    app: Optional[str] = None
    ingress_request_intended_server: Optional[str] = None
    ingress_request_actual_server: Optional[str] = None
    ingress_request_router_latency_us: Optional[int] = None
    ingress_request_body_truncated_full_length: Optional[int] = None
    ingress_request_via_router: bool = False
    ingress_request_failed: Optional[str] = None
    route: Optional[str] = None
    method: Optional[str] = None
    status_code: Optional[str] = None
    latency_ms: Optional[int] = None
    deployment: Optional[str] = None
    # HAProxy 2-char session termination state (%ts). A leading "C" means the
    # client aborted the connection; the recorder maps that to status 499.
    termination_state: Optional[str] = None


class HAProxyMetricsCollector:
    """Owns every `serve_haproxy_*` metric for one proxy node.

    Two families of metrics live here:

    - Per-request, push-based ingress-request-router metrics (Counters /
      Histogram), fed by HAProxy datagrams. `parse_line` and `record` are
      exposed for unit tests that want to drive these without binding
      anything; `bind_and_attach` wires an `AF_UNIX` dgram socket to the
      loop and `close` tears it down.
    - Node-level, poll-based gauges (process count and broadcasted-vs-reported
      target mismatch), sampled from an `HAProxyApi` on a periodic loop started
      by `start_node_metrics_polling`.

    The node-level gauges are always emitted; the datagram reader is only
    bound when ingress-request-router metrics are enabled.
    """

    # Sub-millisecond to 1s, biased toward the expected sub-10ms range for
    # a healthy local router consultation.
    _LATENCY_BUCKETS_MS = [
        0.5,
        1.0,
        2.0,
        5.0,
        10.0,
        25.0,
        50.0,
        100.0,
        250.0,
        500.0,
        1000.0,
    ]

    def __init__(
        self,
        haproxy_api: HAProxyApi,
        node_id: str,
        node_ip_address: str = "",
    ) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._socket_path: Optional[str] = None

        # Source for the node-level poll loop (process count + target mismatch).
        self._haproxy_api = haproxy_api
        self._node_id = node_id
        self._node_metrics_task: Optional[asyncio.Task] = None

        # Per-request HTTP ingress metrics (serve_num_http_requests, latency,
        # errors). In HAProxy mode these are emitted here from HAProxy log
        # datagrams rather than by a Python proxy, so requests HAProxy terminates
        # itself (e.g. 404, /-/routes, health checks) are still counted. HTTP
        # only -- HAProxy does not expose gRPC status (it lives in HTTP/2
        # trailers), so gRPC ingress metrics stay on the replica. The
        # ongoing-requests gauge is not driven here: it can't be derived from
        # per-request log lines.
        self.request_ingress_metrics = RequestIngressMetrics(
            RequestProtocol.HTTP,
            source="proxy",
            node_id=node_id,
            node_ip_address=node_ip_address,
        )

        self.truncated_bodies_counter = metrics.Counter(
            "serve_haproxy_ingress_router_truncations",
            description=(
                "Count of requests whose body was truncated by HAProxy "
                "(exceeded tune.bufsize) before being forwarded to the "
                "ingress request router."
            ),
            tag_keys=("application",),
        )
        self.latency_histogram = metrics.Histogram(
            "serve_haproxy_ingress_router_latency_ms",
            description=(
                "Wall-clock time (in milliseconds) HAProxy spent to resolve "
                "the request to a server via the ingress request router. "
                "Only includes successful routing attempts."
            ),
            boundaries=self._LATENCY_BUCKETS_MS,
            tag_keys=("application", "outcome"),
        )
        self.replica_mismatches_counter = metrics.Counter(
            "serve_haproxy_ingress_router_server_mismatch",
            description=(
                "Count of requests where HAProxy ultimately routed to a "
                "different replica than the one the ingress request router "
                "returned (typically because the named replica was DOWN and "
                "option redispatch picked another)."
            ),
            tag_keys=("application",),
        )
        self.failures_counter = metrics.Counter(
            "serve_haproxy_ingress_router_failures",
            description=(
                "Count of ingress-request-router consultations that failed "
                "to pin a replica, broken down by reason. Possible reasons: "
                "'router_unreachable' (socket connect/send/recv failed), "
                "'router_non_200' (router returned a non-200 status), "
                "'unparseable_replica_id' (router 200 but response body "
                "did not contain a string replica_id), "
                "'unknown_replica_id' (router returned a replica_id not "
                "present in the current replica map). All reasons return 503 "
                "to the client except 'unknown_replica_id', which routes to the "
                "fallback proxy when one is available (otherwise 503)."
            ),
            tag_keys=("application", "reason"),
        )
        self.requests_counter = metrics.Counter(
            "serve_haproxy_ingress_router_requests",
            description=(
                "The number of requests that have been processed by "
                "the ingress request router. This includes both successful "
                "and failed requests."
            ),
            tag_keys=("application",),
        )

        # Node-level gauges, sampled by _report_node_metrics_forever.
        self.process_count_gauge = metrics.Gauge(
            "serve_haproxy_process_count",
            description=(
                "Number of HAProxy processes running on the node for this proxy, "
                "spanning the live worker, draining workers from prior reloads, "
                "and any leaked/orphaned workers. A value persistently above 1 "
                "indicates HAProxy processes are not being reaped."
            ),
            tag_keys=("node_id",),
        )
        self.target_mismatch_gauge = metrics.Gauge(
            "serve_haproxy_target_mismatch",
            description=(
                "Number of targets that differ between the controller's "
                "broadcasted target set and the targets HAProxy actually reports "
                "in its stats on this node (symmetric set difference). A non-zero "
                "value means the HAProxy config has not yet converged to the "
                "broadcasted targets."
            ),
            tag_keys=("node_id",),
        )
        self.process_count_gauge.set_default_tags({"node_id": node_id})
        self.target_mismatch_gauge.set_default_tags({"node_id": node_id})

    @staticmethod
    def parse_line(line: bytes) -> Optional[ParsedMetrics]:
        """Extract metric fields from one RFC 5424 log datagram.

        Returns `None` when the SD section is absent or unparseable. The
        rest of the syslog line (priority, timestamp, message) is ignored.
        """
        try:
            text = line.decode("utf-8", errors="replace")
        except Exception:
            return None

        match = _SD_SECTION_RE.search(text)
        if not match:
            return None

        kv: dict = {}
        for key, quoted, bare in _KV_RE.findall(match.group("body")):
            # Bare tokens (numeric aliases) are used as-is; quoted values are
            # unescaped so the tag holds the original string. An empty quoted
            # value ("") becomes None.
            value = bare if bare else _unescape(quoted)
            kv[key] = value if value != _UNSET else None

        def as_int(key: str) -> Optional[int]:
            raw = kv.get(key)
            if raw is None:
                return None
            try:
                return int(raw)
            except ValueError:
                return None

        return ParsedMetrics(
            app=kv.get("app"),
            ingress_request_intended_server=kv.get("intended"),
            ingress_request_actual_server=kv.get("actual"),
            ingress_request_router_latency_us=as_int("router_latency_us"),
            ingress_request_body_truncated_full_length=as_int(
                "body_truncated_full_length"
            ),
            # HAProxy renders booleans as "1"/"0"; absence as "" -> False.
            ingress_request_via_router=kv.get("via_router") == "1",
            ingress_request_failed=kv.get("failed"),
            route=kv.get("route"),
            method=kv.get("method"),
            status_code=kv.get("status"),
            latency_ms=as_int("latency_ms"),
            deployment=kv.get("deployment"),
            termination_state=kv.get("term_state"),
        )

    def _record_ingress_request(self, parsed: ParsedMetrics) -> None:
        """Emit the per-request RequestIngressMetrics for one observation.

        Mirrors what the Python proxy records per request (the
        `serve_num_http_*` and `serve_http_request_latency_ms` families) --
        for every request, including ones HAProxy terminates itself (404,
        `/-/routes`, health checks), matching the proxy's tags (`application`
        and `route` are empty for those). Skips lines with no status, which
        aren't real request observations.
        """
        if parsed.status_code is None:
            return

        # A client abort (HAProxy termination state with a leading "C") is
        # recorded as 499, matching the Python proxy's client-disconnect
        # convention.
        status_code = parsed.status_code
        if parsed.termination_state and parsed.termination_state.startswith("C"):
            status_code = "499"

        try:
            is_error = int(status_code) >= 400
        except ValueError:
            # Non-numeric status (shouldn't happen for HTTP); treat as non-error.
            is_error = False

        self.request_ingress_metrics.record_request(
            route=parsed.route or "",
            method=parsed.method or "",
            application=parsed.app or "",
            status_code=status_code,
            # %Ta is integer-ms resolution; sub-ms requests round to 0.
            latency_ms=float(parsed.latency_ms or 0),
            is_error=is_error,
            deployment_name=parsed.deployment or "",
        )

    def record(self, parsed: ParsedMetrics) -> None:
        """Update metrics from one parsed observation.

        First records the general per-request ingress metrics (every HTTP
        request). Then records the router-specific metrics, which only apply to
        requests that went through (or attempted) the ingress request router:
        - `ingress_request_failed` set: the Lua action set `txn.ingress_request_router_failed`
          and returned early. Bump the failures counter with the reason; no
          replica was pinned, so other router metrics don't apply.
        - `ingress_request_via_router` true: the Lua action successfully pinned a replica.
          Record latency, truncation, and replica-mismatch as applicable.
        - Neither: the request didn't go through the router path at all
          (no router-bearing app matched, or router state not yet pushed).
          No router metrics to record.
        """
        # General per-request ingress metrics, independent of the router path.
        self._record_ingress_request(parsed)

        # `application` tag is required by the metric definitions; default
        # to "unknown" rather than dropping the observation, so misconfigured
        # frontends still show up in the data.
        app_tag = parsed.app or "unknown"
        tags = {"application": app_tag}

        if parsed.ingress_request_via_router and not parsed.ingress_request_failed:
            self.requests_counter.inc(tags=tags)

            if parsed.ingress_request_body_truncated_full_length is not None:
                self.truncated_bodies_counter.inc(tags=tags)

            # Only count mismatch when we have both sides AND the request actually
            # reached a server (ingress_request_actual_server is not None / "<NOSRV>"). If the
            # router pinned a replica but the request was rejected upstream of
            # server selection (e.g. queued and aborted), HAProxy logs "<NOSRV>"
            # for %s — we treat that as "not a mismatch, not a match".
            if (
                parsed.ingress_request_intended_server
                and parsed.ingress_request_actual_server
                and parsed.ingress_request_actual_server != "<NOSRV>"
                and parsed.ingress_request_intended_server
                != parsed.ingress_request_actual_server
            ):
                self.replica_mismatches_counter.inc(tags=tags)
        elif parsed.ingress_request_failed:
            self.requests_counter.inc(tags=tags)
            self.failures_counter.inc(
                tags={**tags, "reason": parsed.ingress_request_failed}
            )
        else:
            return

        if parsed.ingress_request_router_latency_us is not None:
            self.latency_histogram.observe(
                parsed.ingress_request_router_latency_us / 1_000.0,
                tags={
                    **tags,
                    "outcome": "failure"
                    if parsed.ingress_request_failed
                    else "success",
                },
            )

    async def _report_node_metrics_forever(self, interval_s: float) -> None:
        """Background task to emit the node-level HAProxy observability gauges."""
        consecutive_errors = 0
        while True:
            try:
                await asyncio.sleep(interval_s)
                # count_haproxy_processes does blocking /proc IO that scales
                # with the node's process count; run it in a thread so the
                # actor's event loop (health checks, reloads) isn't stalled.
                loop = asyncio.get_running_loop()
                count = await loop.run_in_executor(
                    None, self._haproxy_api.count_haproxy_processes
                )
                self.process_count_gauge.set(count)
                self.target_mismatch_gauge.set(
                    await self._haproxy_api.compute_target_mismatch()
                )
                # num_ongoing_requests can't be derived from the per-request log
                # lines, so it's sampled here from HAProxy's backend `scur`.
                self.request_ingress_metrics.set_num_ongoing_requests(
                    await self._haproxy_api.count_ongoing_http_requests()
                )
                consecutive_errors = 0
            except Exception:
                logger.exception("Unexpected error reporting HAProxy node metrics.")

                # Exponential backoff starting at 1s and capping at 10s.
                backoff_time_s = min(10, 2**consecutive_errors)
                consecutive_errors += 1
                await asyncio.sleep(backoff_time_s)

    def start_node_metrics_polling(
        self,
        loop: asyncio.AbstractEventLoop,
        interval_s: float,
    ) -> None:
        """Start the periodic loop that emits the node-level gauges."""
        if self._node_metrics_task is None:
            self._node_metrics_task = loop.create_task(
                self._report_node_metrics_forever(interval_s)
            )

    def start(
        self,
        loop: asyncio.AbstractEventLoop,
        *,
        poll_interval_s: float,
        metrics_socket_path: Optional[str] = None,
    ) -> asyncio.Task:
        """Start all metric collection for this proxy node.

        Always starts the node-level gauge poll loop. Also always creates the
        dgram socket directory and binds the per-request reader at `metrics_socket_path`
        (where HAProxy writes one RFC 5424 line per request), returning the
        bind task so the caller can await it (e.g. to surface bind failures at
        actor-readiness time).
        """
        self.start_node_metrics_polling(loop, poll_interval_s)
        os.makedirs(os.path.dirname(cast(str, metrics_socket_path)), exist_ok=True)
        return loop.create_task(
            self.bind_and_attach(cast(str, metrics_socket_path), loop=loop)
        )

    async def bind_and_attach(
        self,
        socket_path: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        """Bind a Unix dgram socket at `socket_path` and register the
        asyncio reader on `loop`.

        Many HAProxy frontends can write to the same socket; dgram
        delivery preserves message boundaries so the reader gets one
        observation per `recvfrom`.

        Idempotent: if a transport is already attached, it is closed
        first. On failure, the collector is left in an unbound state and
        `close()` is still safe to call.
        """
        if self._transport is not None:
            self.close()

        if loop is None:
            loop = asyncio.get_event_loop()

        try:
            os.unlink(socket_path)
        except FileNotFoundError:
            pass

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            sock.bind(socket_path)
            # Match the existing admin socket pattern (mode 666) so HAProxy
            # processes running as a different user can still send to it.
            os.chmod(socket_path, 0o666)
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _DatagramHandler(self), sock=sock
            )
        except Exception:
            sock.close()
            raise

        self._socket_path = socket_path
        self._transport = transport

    def close(self) -> None:
        """Tear down the node-metrics loop, the dgram transport, and the
        socket file.

        Safe to call multiple times; safe to call without ever having
        bound or started polling. The metric objects survive close — they
        are owned by Ray's metric registry, not this instance.
        """
        if self._node_metrics_task is not None:
            self._node_metrics_task.cancel()
            self._node_metrics_task = None
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self._socket_path is not None:
            try:
                os.unlink(self._socket_path)
            except FileNotFoundError:
                pass
            self._socket_path = None


class _DatagramHandler(asyncio.DatagramProtocol):
    def __init__(self, collector: HAProxyMetricsCollector) -> None:
        self._collector = collector

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: D401
        try:
            parsed = self._collector.parse_line(data)
            if parsed is not None:
                self._collector.record(parsed)
        except Exception:
            # A malformed datagram must never crash the proxy actor. Log
            # once per occurrence at debug to keep busy frontends quiet.
            logger.debug("Failed to handle HAProxy metrics datagram", exc_info=True)
