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
from typing import Optional

from ray.util import metrics

logger = logging.getLogger(__name__)

# SD-ID we publish under. The leading bracket + this string is what the
# parser anchors on; only lines containing this section are processed.
_SD_ID = "serve@1"

# RFC 5424 SD element looks like `[serve@1 key="value" key="value"]`.
# We extract the inside of the brackets after the SD-ID, then walk the
# `key="value"` pairs.
_SD_SECTION_RE = re.compile(
    r"\[" + re.escape(_SD_ID) + r"(?P<body>[^\]]*)\]"
)

# Quoted values may contain spaces; backslash escapes are allowed per RFC 5424.
# Capture the key="value" pairs.
_KV_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')

# HAProxy emits the literal dash `-` for unset txn vars. We map it to None
# so callers don't have to handle the sentinel themselves.
_UNSET = "-"


@dataclass
class ParsedMetrics:
    """One per-request observation, parsed from the SD section."""

    app: Optional[str]
    intended_replica: Optional[str]
    actual_server: Optional[str]
    router_latency_us: Optional[int]
    body_truncated_full_length: Optional[int]
    via_router: bool
    failed: Optional[str]


class HAProxyMetricsCollector:
    """Owns the three ingress-request-router Counter / Histogram objects
    and the dgram socket that feeds them.

    Exposes `parse_line` and `record` for unit tests that want to
    drive metrics without binding anything. `bind_and_attach` wires an
    `AF_UNIX` dgram socket to the loop; `close` tears it down.
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

    def __init__(self) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._socket_path: Optional[str] = None

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
                "Wall-clock time (in milliseconds) HAProxy spent calling "
                "the ingress request router."
            ),
            boundaries=self._LATENCY_BUCKETS_S,
            tag_keys=("application",),
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
        for key, value in _KV_RE.findall(match.group("body")):
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
            intended_replica=kv.get("intended"),
            actual_server=kv.get("actual"),
            router_latency_us=as_int("router_latency_us"),
            body_truncated_full_length=as_int("body_truncated_full_length"),
            # HAProxy renders booleans as "1"/"0"; absence as "-" -> None.
            via_router=kv.get("via_router") == "1",
            failed=kv.get("failed"),
        )

    def record(self, parsed: ParsedMetrics) -> None:
        """Update metrics from one parsed observation.

        Only requests that actually went through the router path
        (`via_router` true) contribute. Failed routes (router unreachable,
        bad response, unknown replica) never reached a backend, so the
        mismatch counter would be misleading; we keep the latency histogram
        observation regardless because timing is still meaningful.
        """
        if not parsed.via_router:
            return

        # `application` tag is required by the metric definition; default
        # to "unknown" rather than dropping the observation, so misconfigured
        # frontends still show up in the data.
        tags = {"application": parsed.app or "unknown"}

        if parsed.router_latency_us is not None:
            self.latency_histogram.observe(
                parsed.router_latency_us / 1_000.0, tags=tags
            )

        if parsed.body_truncated_full_length is not None:
            self.truncated_bodies_counter.inc(tags=tags)

        # Only count mismatch when we have both sides AND the request actually
        # reached a server (actual_server is not None / "<NOSRV>"). If the
        # router pinned a replica but the request was rejected upstream of
        # server selection (e.g. queued and aborted), HAProxy logs "<NOSRV>"
        # for %s — we treat that as "not a mismatch, not a match".
        if (
            not parsed.failed
            and parsed.intended_replica
            and parsed.actual_server
            and parsed.actual_server != "<NOSRV>"
            and parsed.intended_replica != parsed.actual_server
        ):
            self.replica_mismatches_counter.inc(tags=tags)

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
        """Tear down the dgram transport and remove the socket file.

        Safe to call multiple times; safe to call without ever having
        bound. The metric Counter / Histogram objects survive close —
        they are owned by Ray's metric registry, not this instance.
        """
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
