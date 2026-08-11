import asyncio
import json
import time
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
    KV_TOKEN_METADATA_KEY,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
)
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

# Type-only import as LLMConfig transitively pulls in vLLM. This file should
# remain engine-agnostic.
if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"
_PD_REQUEST_PATH_HEADER = "x-serve-router-request-path"
_PD_GENERATION_PATHS = frozenset({"/v1/chat/completions", "/v1/completions"})

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
        200 ``{"host": str, "port": int, "replica_id": str, "request_headers"?: dict}``:
            pick succeeded. ``request_headers["x-serve-router-kv-token-key"]``
            is present only when prompt token IDs were enqueued to the selected
            replica's best-effort ZMQ side channel; the engine falls back to
            tokenization when it is absent or missing at consume time.
        4xx/5xx FastAPI ``{"detail": str}``: informational only; HAProxy
            treats any non-200 as a routing failure. When using KV aware routing,
            a pre-routing ``/tokenize`` rejection is surfaced here.

    Health:
        ``GET /health`` is exposed as a human-operator convenience.
        Serve uses ``check_health()`` for replica readiness, not HTTP.
    """

    # Warn once per replica when no routing key is derived. Class-level default
    # keeps the guard safe before __init__ runs.
    _warned_no_routing_key: bool = False
    _warned_no_token_endpoint: bool = False

    async def __init__(
        self,
        server: DeploymentHandle,
        llm_config: Optional["LLMConfig"] = None,
        prefill_server: Optional[DeploymentHandle] = None,
        prefill_llm_config: Optional["LLMConfig"] = None,
    ):
        self._handle: DeploymentHandle = server
        self._tokenizer = None
        self._prefill_tokenizer = None
        self._token_sender = None
        # Holds the KVTokenTracker (KV-aware deployments only) so the
        # engine-facing on_lifecycle_events method can book load into it.
        self._kv_token_tracker = None
        self._pd_pair_tracker = None
        self._pd_owner_replica_id = None
        # A non-None llm_config signals pre-routing tokenization, which the
        # builder binds only for a KV-aware request router.
        if prefill_server is not None:
            if llm_config is None or prefill_llm_config is None:
                raise ValueError(
                    "P/D LLMRouter requires both prefill and decode configs"
                )
            from ray.llm._internal.serve.routing_policies.kv_aware.pd_router import (
                PDPairTracker,
            )
            from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (  # noqa: E501
                get_llm_router_handle,
            )
            from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
                Tokenizer,
            )
            from ray.llm._internal.serve.routing_policies.kv_aware import (
                token_channel,
            )

            ticket_ttl_s = float(
                llm_config.experimental_configs.get("pd_ticket_ttl_s", 120.0)
            )
            pending_decode_load_scale = float(
                llm_config.experimental_configs.get("pending_decode_load_scale", 1.0)
            )
            selection_policy = llm_config.experimental_configs.get(
                "pd_selection_policy", "kv_aware"
            )
            prefill_selection_policy = prefill_llm_config.experimental_configs.get(
                "pd_selection_policy", selection_policy
            )
            if prefill_selection_policy != selection_policy:
                raise ValueError(
                    "prefill and decode pd_selection_policy values must match"
                )
            self._pd_pair_tracker = PDPairTracker(
                prefill_config=prefill_llm_config,
                decode_config=llm_config,
                prefill_deployment_id=prefill_server.deployment_id,
                decode_deployment_id=server.deployment_id,
                ticket_ttl_s=ticket_ttl_s,
                pending_decode_load_scale=pending_decode_load_scale,
                selection_policy=selection_policy,
            )
            self._pd_pair_tracker.start_cleanup()
            self._tokenizer = await asyncio.to_thread(Tokenizer, llm_config)
            # P/D KV transfer is valid only if both engines see exactly the
            # same prompt IDs. Config-level tokenizer fingerprints are not a
            # sufficient substitute for rendering the actual request.
            self._prefill_tokenizer = await asyncio.to_thread(
                Tokenizer, prefill_llm_config
            )
            self._token_sender = token_channel.TokenSender()
            self._pd_owner_replica_id = serve.get_replica_context().replica_id.unique_id
            self._pd_pair_tracker.start_reservation_broadcast(
                get_llm_router_handle(), self._pd_owner_replica_id
            )
        elif llm_config is not None:
            # Build the tracker before _handle._init() below, which initializes
            # the KVAwareRouter that looks it up. server.deployment_id is the
            # tracked LLMServer deployment.
            from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (  # noqa: E501
                build_kv_token_tracker,
                get_llm_router_handle,
            )

            self._kv_token_tracker = build_kv_token_tracker(
                llm_config, server.deployment_id
            )
            self._kv_token_tracker.start_reservation_broadcast(get_llm_router_handle())
            # Lazy import: this module pulls in vLLM's renderer;
            # keep it off the non-KV ingress import path.
            from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
                Tokenizer,
            )

            self._tokenizer = await asyncio.to_thread(Tokenizer, llm_config)
            # Lazy import: pyzmq is not a Ray runtime dependency, so keep it off
            # the non-KV ingress import path.
            from ray.llm._internal.serve.routing_policies.kv_aware import (
                token_channel,
            )

            self._token_sender = token_channel.TokenSender()
        self._handle._init()
        if prefill_server is not None:
            prefill_server._init()

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        routing_payload = _parse_routing_payload(body)
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
        if self._pd_pair_tracker is not None:
            return await self._route_pd(request, routing_payload)
        # Tokenize only a parseable, routable body; a truncated or unparseable
        # body has no routing payload, so fall back to token-less routing.
        request_token_ids = None
        if self._tokenizer is not None and routing_payload is not None:
            from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
                TokenizeError,
            )

            try:
                request_token_ids = await self._tokenizer.tokenize(
                    vars(routing_payload)
                )
            except TokenizeError as e:
                raise HTTPException(status_code=e.status_code, detail=e.message)
        return await self._route_to_backend(
            request=request,
            routing_payload=routing_payload,
            request_token_ids=request_token_ids,
        )

    async def _route_to_backend(
        self,
        *,
        request: Request,
        routing_payload: Optional[SimpleNamespace],
        request_token_ids: Optional[List[int]],
    ) -> Dict[str, Any]:
        """Route to decode without creating a P/D ticket."""
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
        try:
            host, port, replica_id, token_endpoint = await self._pick_replica(
                handle=handle,
                routing_payload=routing_payload,
                request_token_ids=request_token_ids,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))

        response = {"host": host, "port": port, "replica_id": replica_id}
        if request_token_ids:
            token_key = self._push_prompt_tokens(
                token_endpoint=token_endpoint,
                replica_id=replica_id,
                request_token_ids=request_token_ids,
            )
            if token_key:
                response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD] = {
                    KV_TOKEN_KEY_HEADER: token_key
                }
        return response

    async def _route_pd(
        self, request: Request, routing_payload: Optional[SimpleNamespace]
    ) -> Dict[str, Any]:
        """Select/reserve a P/D pair and return only the decode data-plane hop."""
        if not self._is_pd_generation_request(request):
            return await self._route_to_backend(
                request=request,
                routing_payload=routing_payload,
                request_token_ids=None,
            )
        if routing_payload is None:
            return await self._route_to_backend(
                request=request,
                routing_payload=None,
                request_token_ids=None,
            )
        from ray.llm._internal.serve.routing_policies.kv_aware.pd_router import (
            PD_D_REPLICA_ID_HEADER,
            PD_D_RESERVATION_ID_HEADER,
            PD_EXPIRY_MS_HEADER,
            PD_OWNER_REPLICA_ID_HEADER,
            PD_P_REPLICA_ID_HEADER,
            PD_P_RESERVATION_ID_HEADER,
            PD_TOKEN_KEY_HEADER,
            PD_VERSION_HEADER,
        )
        from ray.llm._internal.serve.routing_policies.kv_aware.tokenizer import (
            TokenizeError,
        )

        try:
            decode_token_ids = await self._tokenizer.tokenize(vars(routing_payload))
            prefill_token_ids = await self._prefill_tokenizer.tokenize(
                vars(routing_payload)
            )
        except TokenizeError:
            # Preserve the established P/D path when pre-routing cannot render
            # a prompt. It will tokenize in the engine and must not retain a
            # reservation the ticket flow cannot safely describe.
            return await self._route_to_backend(
                request=request,
                routing_payload=routing_payload,
                request_token_ids=None,
            )
        if prefill_token_ids != decode_token_ids:
            raise HTTPException(
                status_code=400,
                detail="prefill and decode tokenizers produced different token IDs",
            )

        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        expected_output_tokens = None
        for field in ("max_completion_tokens", "max_tokens"):
            value = getattr(routing_payload, field, None)
            if isinstance(value, int) and value > 0:
                expected_output_tokens = value
                break
        try:
            ticket = await self._pd_pair_tracker.reserve_pair(
                request_id=request_id,
                prefill_token_ids=prefill_token_ids,
                decode_token_ids=decode_token_ids,
                expected_output_tokens=expected_output_tokens,
            )
            token_key = self._push_pd_prompt_tokens(
                p_token_endpoint=ticket.p_route.get("token_endpoint"),
                d_token_endpoint=ticket.d_route.get("token_endpoint"),
                prefill_token_ids=prefill_token_ids,
                decode_token_ids=decode_token_ids,
            )
            self._pd_pair_tracker.set_token_key(ticket, token_key)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))

        headers = {
            PD_P_REPLICA_ID_HEADER: ticket.p_route["replica_id"],
            PD_D_REPLICA_ID_HEADER: ticket.d_route["replica_id"],
            PD_P_RESERVATION_ID_HEADER: ticket.p_reservation_id,
            PD_D_RESERVATION_ID_HEADER: ticket.d_reservation_id,
            PD_EXPIRY_MS_HEADER: str(ticket.expiry_ms),
            PD_VERSION_HEADER: "1",
            PD_OWNER_REPLICA_ID_HEADER: self._pd_owner_replica_id,
        }
        if token_key is not None:
            headers[PD_TOKEN_KEY_HEADER] = token_key
        return {
            "host": ticket.d_route["host"],
            "port": ticket.d_route["port"],
            "replica_id": ticket.d_route["full_replica_id"],
            RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD: headers,
        }

    @staticmethod
    def _is_pd_generation_request(request: Request) -> bool:
        """Return whether HAProxy is routing one of the P/D generation APIs."""
        path = request.headers.get(_PD_REQUEST_PATH_HEADER)
        return path is not None and any(
            path.rstrip("/").endswith(generation_path)
            for generation_path in _PD_GENERATION_PATHS
        )

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    def __del__(self) -> None:
        """Close the token channel ZMQ sockets upon cleanup."""
        token_sender = getattr(self, "_token_sender", None)
        if token_sender is not None:
            token_sender.close()
        pd_pair_tracker = getattr(self, "_pd_pair_tracker", None)
        if pd_pair_tracker is not None:
            pd_pair_tracker.close()

    async def on_lifecycle_events(self, batch):
        """Engine-facing intake for request lifecycle events.

        Engine replicas broadcast each batch to every LLMRouter replica to
        book request load; this applies it to the KVTokenTracker on this
        ingress replica's event loop.
        """
        if self._pd_pair_tracker is not None:
            # New token trackers include the emitting worker id.  Ignore P
            # lifecycle events here: P is released only by the transfer-safe
            # completion acknowledged by PDDecodeServer.
            for event in batch:
                if len(event) != 3:
                    continue
                worker_id, hook_name, args = event
                if worker_id in self._pd_pair_tracker.decode._replica_id_by_worker:
                    await self._pd_pair_tracker.on_decode_lifecycle_events(
                        worker_id, [(hook_name, args)]
                    )
            return
        events = [(event[1], event[2]) if len(event) == 3 else event for event in batch]
        return await self._kv_token_tracker.on_lifecycle_events(events)

    async def on_reservations_created(self, batch):
        """Ingress-facing intake for already-selected reservation bookings."""
        if self._pd_pair_tracker is not None:
            return await self._pd_pair_tracker.on_reservations_created(batch)
        return await self._kv_token_tracker.on_reservations_created(batch)

    async def on_pd_reservation_events(self, batch) -> None:
        """Ingress-facing intake for P/D transfer and compensation events."""
        if self._pd_pair_tracker is not None:
            await self._pd_pair_tracker.on_reservation_events(batch)

    async def claim_pd_ticket(self, headers: Dict[str, str]) -> Dict[str, Any]:
        """Validate and claim the selected prefill half of a direct P/D route."""
        if self._pd_pair_tracker is None:
            raise RuntimeError("P/D tickets are not enabled for this application")
        from ray.llm._internal.serve.routing_policies.kv_aware.pd_router import (
            PD_D_REPLICA_ID_HEADER,
            PD_D_RESERVATION_ID_HEADER,
            PD_EXPIRY_MS_HEADER,
            PD_P_REPLICA_ID_HEADER,
            PD_P_RESERVATION_ID_HEADER,
            PD_VERSION_HEADER,
        )

        normalized = {key.lower(): value for key, value in headers.items()}
        if normalized.get(PD_VERSION_HEADER) != "1":
            raise ValueError("unsupported P/D ticket version")
        try:
            expiry_ms = int(normalized[PD_EXPIRY_MS_HEADER])
        except (KeyError, ValueError) as e:
            raise ValueError("P/D ticket has invalid expiry") from e
        if expiry_ms < int(time.time() * 1000):
            raise ValueError("P/D ticket expired")
        ticket = self._pd_pair_tracker.claim_prefill(
            d_reservation_id=normalized[PD_D_RESERVATION_ID_HEADER],
            p_reservation_id=normalized[PD_P_RESERVATION_ID_HEADER],
            d_replica_id=normalized[PD_D_REPLICA_ID_HEADER],
            p_replica_id=normalized[PD_P_REPLICA_ID_HEADER],
        )
        # Do not trust a supplied token key.  The router's mutable ticket state
        # is authoritative and makes a stale/mutated envelope harmless.
        return {
            "p_replica_id": ticket.p_route["replica_id"],
            "p_reservation_id": ticket.p_reservation_id,
            "d_reservation_id": ticket.d_reservation_id,
            "token_key": ticket.token_key,
        }

    async def pd_prefill_complete(self, d_reservation_id: str) -> None:
        if self._pd_pair_tracker is None:
            raise RuntimeError("P/D tickets are not enabled for this application")
        await self._pd_pair_tracker.prefill_complete(d_reservation_id)

    async def release_pd_ticket(self, d_reservation_id: str) -> None:
        if self._pd_pair_tracker is not None:
            await self._pd_pair_tracker.release(d_reservation_id)

    def _push_prompt_tokens(
        self,
        *,
        token_endpoint: Optional[str],
        replica_id: str,
        request_token_ids: List[int],
    ) -> Optional[str]:
        # Only reachable on the KV path, where __init__ built the sender.
        if not token_endpoint or self._token_sender is None:
            if not self._warned_no_token_endpoint:
                self._warned_no_token_endpoint = True
                logger.warning(
                    "Selected replica %s did not advertise a prompt-token "
                    "ZMQ endpoint; falling back to engine tokenization.",
                    replica_id,
                )
            return None

        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel

        key = uuid.uuid4().hex
        try:
            payload = token_channel.encode_prompt_token_ids(request_token_ids)
        except Exception as e:
            logger.warning(
                "Failed to encode prompt token IDs for selected replica %s; "
                "falling back to engine tokenization: %s",
                replica_id,
                e,
            )
            return None
        if self._token_sender.push(token_endpoint, key, payload):
            return key
        return None

    def _push_pd_prompt_tokens(
        self,
        *,
        p_token_endpoint: Optional[str],
        d_token_endpoint: Optional[str],
        prefill_token_ids: List[int],
        decode_token_ids: List[int],
    ) -> Optional[str]:
        """Stage one key at both selected replicas or fall back at both.

        The stores are replica-local, so compatible tokenizers receive the same
        uint32 payload under the same key.  A mismatched but supported P/D
        tokenizer pair receives separately-rendered IDs under that same key.
        Partial delivery intentionally omits the key: correctness never depends
        on a best-effort staging channel and the orphaned entry expires quickly.
        """
        if self._token_sender is None or not p_token_endpoint or not d_token_endpoint:
            return None
        from ray.llm._internal.serve.routing_policies.kv_aware import token_channel

        key = uuid.uuid4().hex
        try:
            p_payload = token_channel.encode_prompt_token_ids(prefill_token_ids)
            d_payload = token_channel.encode_prompt_token_ids(decode_token_ids)
        except Exception as e:
            logger.warning("Failed to encode staged P/D prompt tokens: %s", e)
            return None
        if not self._token_sender.push(p_token_endpoint, key, p_payload):
            return None
        if not self._token_sender.push(d_token_endpoint, key, d_payload):
            return None
        return key

    async def _pick_replica(
        self,
        handle: DeploymentHandle,
        routing_payload: Optional[SimpleNamespace] = None,
        request_token_ids: Optional[List[int]] = None,
    ) -> Tuple[str, int, str, Optional[str]]:
        """Pick a backend HTTP replica via the deployment's request router.

        ``handle`` is the LLMServer deployment handle, optionally configured
        with ``.options(session_id=...)`` by the caller so session-aware
        routers see the session id on ``RequestMetadata``.

        ``routing_payload``, when present, is passed to ``choose_replica``
        positionally. It lands in ``pending_request.args`` where the normal
        ingress puts the parsed request, so a body-aware policy scores replicas
        as on the normal path. When ``None``, nothing is forwarded. The router
        sees empty ``args`` and falls back to its default load-balanced pick.

        ``request_token_ids``, when present, is forwarded as a keyword arg so a
        KV-aware request router can score replicas on prompt-prefix overlap.

        ``_reserve=False`` short-circuits the replica-side ``reserve_slot``
        RPC and the rejection-retry loop: the real request goes out via
        HAProxy, so Serve's capacity semaphore isn't load-bearing here, and
        the extra RPC + retry introduced burstiness compared to the prior
        local round-robin implementation.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()
        choose_replica_kwargs = {"_reserve": False}
        if request_token_ids is not None:
            choose_replica_kwargs[REQUEST_TOKEN_IDS_KWARG] = request_token_ids
        async with handle.choose_replica(
            *route_args, **choose_replica_kwargs
        ) as selection:
            replica = selection._replica
            endpoint = replica.backend_http_endpoint
            if endpoint is None:
                raise RuntimeError(
                    f"replica {selection.replica_id} has no backend HTTP endpoint"
                )
            host, port = endpoint
            prompt_token_metadata = replica.routing_stats.get(KV_TOKEN_METADATA_KEY)
            prompt_token_endpoint = (
                prompt_token_metadata.get("endpoint")
                if isinstance(prompt_token_metadata, dict)
                else None
            )
            return (
                host,
                port,
                replica.replica_id.to_full_id_str(),
                prompt_token_endpoint,
            )
