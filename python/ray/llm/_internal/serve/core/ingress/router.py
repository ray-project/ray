import asyncio
import atexit
import json
import os
import time
from types import SimpleNamespace
from typing import List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"

# ---------------------------------------------------------------------------
# Benchmark-only diagnostics for the direct-streaming ingress router.
#
# This deployment is pinned to num_replicas=1 (see
# _build_openai_ingress_request_router), so EVERY request in the deployment
# funnels its routing decision through this one actor's event loop. At high
# client concurrency that makes this replica a serialization point that no
# instrumentation inside request_router.py can observe -- the queue-based
# diagnostics there are on the `_reserve=True` path, which direct streaming
# never takes.
#
# `inflight` is the count of /internal/route calls concurrently inside the
# handler when this one entered. If it climbs with client concurrency while
# pick_replica_ms stays flat, requests are waiting on THIS replica rather than
# on replica selection, and widening this deployment (or removing the hop) is
# the lever -- not anything in the routing-task pool.
_PD_TRACE_ENABLED = bool(os.environ.get("RAY_PD_TRACE"))
_pd_inflight = 0

# Samples are BUFFERED, not logged per request. The earlier version called
# logger.info() here on every request, inside the single pinned LLMRouter
# actor's event loop -- the same loop whose scheduling delay this trace exists
# to measure, and with RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE defaulting to 1
# (one synchronous flush per line). That put a disk write inside the measured
# pick_replica window: the probe perturbed the quantity it reports, and the
# native side of the comparison carries no equivalent cost, so the measured
# Ray-vs-native gap was inflated by an unknown amount. A list append is ~100ns
# and cannot flush; the drain task below emits aggregates off the hot path.
_pd_samples: List[Tuple[float, float, float, int, int]] = []
_PD_DRAIN_INTERVAL_S = 5.0
_pd_drain_task = None
# Per-request rows bypass the logging stack entirely (see _pd_drain_samples).
# RAY_PD_TRACE is a path; put the rows beside it so the collector finds them.
_PD_ROWS_PATH = (
    f"{os.environ['RAY_PD_TRACE']}.ingress_rows.{os.getpid()}"
    if _PD_TRACE_ENABLED
    else ""
)


def _pd_route_log_sample(
    total_s: float,
    pick_replica_s: float,
    body_parse_s: float,
    inflight_on_entry: int,
    body_len: int,
) -> None:
    if _PD_TRACE_ENABLED:
        _pd_samples.append(
            (total_s, pick_replica_s, body_parse_s, inflight_on_entry, body_len)
        )


def _pd_pct(sorted_vals, q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[idx]


def _pd_drain_samples() -> None:
    """Emit one aggregate line for the samples buffered since the last drain.

    Runs off the request path. Emits the same field names the per-request
    version used, so existing log parsing keeps working, plus the count and
    the percentiles that matter for a serialization point.
    """
    # The serve-side fast-path buffer lives in the same actor; drain it on the
    # same tick so both traces leave the hot path together.
    try:
        from ray.serve._private.router import _pd_fastpath_drain

        _pd_fastpath_drain()
    except Exception:
        pass
    if not _pd_samples:
        return
    batch = _pd_samples[:]
    del _pd_samples[: len(batch)]
    picks = sorted(s[1] for s in batch)
    totals = sorted(s[0] for s in batch)
    inflights = sorted(s[3] for s in batch)
    logger.info(
        f"[pd_ingress_route_agg] n={len(batch)} "
        f"pick_replica_ms_p50={_pd_pct(picks, 0.50) * 1000:.3f} "
        f"pick_replica_ms_p95={_pd_pct(picks, 0.95) * 1000:.3f} "
        f"pick_replica_ms_max={picks[-1] * 1000:.3f} "
        f"total_ms_p50={_pd_pct(totals, 0.50) * 1000:.3f} "
        f"inflight_p50={_pd_pct(inflights, 0.50)} "
        f"inflight_max={inflights[-1]}"
    )
    # Per-request rows, written to their OWN file with a single write() and no
    # logging handler in between. Routing them through logger.info would put
    # one synchronous flush per buffered request onto this actor's event loop
    # (Serve installs MemoryHandler(capacity=RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE),
    # which defaults to 1 -- i.e. flush-per-record). At c=64 that is hundreds
    # of flushes in one uninterrupted block every drain, stalling whatever
    # /internal/route calls are mid-flight and landing inside exactly the
    # pick_replica window this instrumentation exists to measure. Moving the
    # cost off the per-request path only to concentrate it into a periodic
    # stall would be no better than what it replaced.
    if _PD_ROWS_PATH:
        try:
            with open(_PD_ROWS_PATH, "a") as fh:
                fh.write(
                    "".join(
                        f"[pd_ingress_route] total_ms={t * 1000:.3f} "
                        f"pick_replica_ms={p * 1000:.3f} "
                        f"body_parse_ms={b * 1000:.3f} "
                        f"inflight={i} body_len={n}\n"
                        for t, p, b, i, n in batch
                    )
                )
        except OSError:
            # Diagnostics must never take the deployment down.
            pass


async def _pd_drain_loop() -> None:
    # Drain immediately on start, then on the interval: a short run (the c=1
    # arm is 30 requests) can finish inside one window, and the bench driver
    # kills the Serve process after the sweep, so a buffer that has not been
    # drained is simply lost.
    while True:
        try:
            _pd_drain_samples()
        except Exception:
            logger.exception("[pd_ingress_route] drain failed")
        await asyncio.sleep(_PD_DRAIN_INTERVAL_S)


def _pd_start_drain_task() -> None:
    """Start the drain loop once, from inside the actor's running loop."""
    global _pd_drain_task
    if not _PD_TRACE_ENABLED or _pd_drain_task is not None:
        return
    try:
        _pd_drain_task = asyncio.get_running_loop().create_task(_pd_drain_loop())
    except RuntimeError:
        # No running loop yet; the next request-path call will try again.
        _pd_drain_task = None
        return
    # The bench driver kills the Serve process once the sweep ends, so without
    # this the final window's samples never reach disk.
    atexit.register(_pd_final_drain)


def _pd_final_drain() -> None:
    """Flush whatever is buffered at process exit."""
    try:
        _pd_drain_samples()
    except Exception:
        pass


# A request body routes on one of these fields. Body-aware routers read it off
# the namespace; a body without any of them degrades to load-balancing. Extend
# as routers learn to route additional request types.
_ROUTING_KEY_FIELDS = ("messages", "prompt")

router_app = FastAPI()


def _parse_routing_payload(body: bytes) -> Optional[SimpleNamespace]:
    """Wrap a request body as a namespace a body-aware router routes on.

    Routers read a routing field (``messages`` or ``prompt``) off the first
    positional routing arg, the parsed request the normal ingress forwards.
    Direct streaming has only the raw body, so this wraps the parsed body in a
    namespace exposing every field by attribute, which a router reads the same
    way regardless of request type. Returns ``None`` for an empty, non-object,
    unparseable, or keyless body, so the caller falls back to load-balancing.
    """
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if not any(data.get(field) for field in _ROUTING_KEY_FIELDS):
        return None
    return SimpleNamespace(**data)


@serve.ingress(router_app)
class LLMRouter:
    """Ingress request router for direct streaming.

    When direct streaming is enabled, HAProxy calls /internal/route on this
    deployment to get a data plane replica, then forwards traffic directly
    to the matching LLMServer replica's backend HTTP port.

    Replica selection is delegated to the underlying deployment's configured
    request router, and this class translates the resulting pick into a backend
    HTTP endpoint.

    /internal/route HTTP contract
    -----------------------------
    Request:
        POST /internal/route
        Content-Type: application/json
        Body: the target ChatCompletions or Completions request payload.
            Wrapped in a namespace by ``_parse_routing_payload`` and passed to
            ``choose_replica`` positionally, exposing the request fields the way
            the parsed request does. Body-aware policies then score replicas the
            same way on both paths.

    Truncated bodies:
        HAProxy may forward only a prefix of the body for routing and sets the
        ``x-body-truncated`` header. A truncated prefix is usually not valid
        JSON, so no routing key is derived and the request falls back to the
        default load-balanced pick.

    Session affinity:
        If the client request carried the session-id header configured by
        ``RAY_SERVE_SESSION_ID_HEADER_KEY`` (default ``x-session-id``),
        HAProxy's Lua action forwards it to ``/internal/route`` on the same
        name. This handler reads it and applies
        ``handle.options(session_id=...)`` before calling
        ``choose_replica`` so session-aware policies (e.g.
        ``ConsistentHashRouter``) pin all turns of a session to one replica.

    Responses:
        200 ``{"host": str, "port": int, "replica_id": str}``: pick
            succeeded.
        4xx/5xx FastAPI ``{"detail": str}``: informational only; HAProxy
            treats any non-200 as a routing failure.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    # Warn once per replica when no routing key is derived. Class-level default
    # keeps the guard safe before __init__ runs.
    _warned_no_routing_key: bool = False

    async def __init__(self, server: DeploymentHandle):
        self._handle: DeploymentHandle = server
        self._handle._init()

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        # Keep this as ONE method with the original signature: @serve.ingress /
        # FastAPI introspect the decorated function, so splitting it into a
        # wrapper + impl would change what they see. Diagnostics ride inside a
        # try/finally instead.
        global _pd_inflight
        _pd_t0 = time.monotonic()
        # Sampled BEFORE incrementing so it reads "how many were already in
        # here when I arrived" -- the queue this request actually waited behind.
        _pd_inflight_on_entry = _pd_inflight
        _pd_inflight += 1
        if _pd_drain_task is None:
            _pd_start_drain_task()
        try:
            return await self._route_inner(request, _pd_t0, _pd_inflight_on_entry)
        finally:
            _pd_inflight -= 1

    async def _route_inner(
        self,
        request: Request,
        _pd_t0: float,
        _pd_inflight_on_entry: int,
    ):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        routing_payload = _parse_routing_payload(body)
        _pd_t_parsed = time.monotonic()
        if routing_payload is None and not self._warned_no_routing_key:
            self._warned_no_routing_key = True
            logger.warning(
                "Could not derive a routing key from the request body. "
                "body_truncated=%s. Falling back to load-balanced replica "
                "selection. A configured body-aware router such as "
                "PrefixCacheAffinityRouter cannot take effect for these "
                "requests. For truncated bodies, raise HAProxy's routing body "
                "limit.",
                body_truncated,
            )
        # HAProxy forwards the configured session header on the same name,
        # but use the same case-insensitive, separator-tolerant matcher as
        # proxy.py / ingress.py so a `-`/`_` rewrite anywhere in the path
        # doesn't silently drop session affinity.
        session_id = next(
            (v for k, v in request.headers.items() if _matches_session_id_header(k)),
            None,
        )
        handle = (
            self._handle.options(session_id=session_id) if session_id else self._handle
        )
        _pd_t_pick_start = time.monotonic()
        try:
            host, port, replica_id = await self._pick_replica(
                handle=handle,
                routing_payload=routing_payload,
            )
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))
        _pd_t_end = time.monotonic()
        _pd_route_log_sample(
            total_s=_pd_t_end - _pd_t0,
            pick_replica_s=_pd_t_end - _pd_t_pick_start,
            body_parse_s=_pd_t_parsed - _pd_t0,
            inflight_on_entry=_pd_inflight_on_entry,
            body_len=len(body),
        )
        return {"host": host, "port": port, "replica_id": replica_id}

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    async def _pick_replica(
        self,
        handle: DeploymentHandle,
        routing_payload: Optional[SimpleNamespace] = None,
    ) -> Tuple[str, int, str]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured
        with ``.options(session_id=...)`` by the caller so session-aware
        routers see the session id on ``RequestMetadata``.

        ``routing_payload``, when present, is passed to ``choose_replica``
        positionally. It lands in ``pending_request.args`` where the normal
        ingress puts the parsed request, so a body-aware policy scores replicas
        as on the normal path. When ``None``, nothing is forwarded. The router
        sees empty ``args`` and falls back to its default load-balanced pick.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        actor RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()
        async with handle.choose_replica(
            *route_args,
            _reserve=False,
        ) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            return host, port, replica.replica_id.to_full_id_str()
