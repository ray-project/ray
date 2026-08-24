import asyncio
import json
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request

from ray import serve
from ray.llm._internal.common.utils.lora_utils import get_base_model_id
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
    KV_TOKEN_METADATA_KEY,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_MULTIPLEXED_MODEL_ID,
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

# A request body routes on one of these fields. Body-aware routers read it off
# the namespace; a body without any of them degrades to load-balancing. Extend
# as routers learn to route additional request types.
_ROUTING_KEY_FIELDS = ("messages", "prompt")

router_app = FastAPI()


def _parse_request_body(body: bytes) -> Optional[Dict[str, Any]]:
    """Parse an OpenAI JSON request body into an object."""
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _get_routing_payload_from_body(
    data: Optional[Dict[str, Any]],
) -> Optional[SimpleNamespace]:
    """Wrap a request body as a namespace a body-aware router routes on.

    Routers read a routing field (``messages`` or ``prompt``) off the first
    positional routing arg, the parsed request the normal ingress forwards.
    Direct streaming has only the raw body, so this wraps the parsed body in a
    namespace exposing every field by attribute, which a router reads the same
    way regardless of request type. Returns ``None`` for an empty, non-object,
    unparseable, or keyless body, so the caller falls back to load-balancing.
    """
    if data is None or not any(data.get(field) for field in _ROUTING_KEY_FIELDS):
        return None
    return SimpleNamespace(**data)


def _parse_routing_payload(body: bytes) -> Optional[SimpleNamespace]:
    return _get_routing_payload_from_body(_parse_request_body(body))


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
            pick succeeded. With LoRA, ``request_headers`` carries the
            router-derived multiplexed model ID for the direct replica. Its
            ``"x-serve-router-kv-token-key"`` entry is present only when prompt
            token IDs were enqueued to the selected replica's best-effort ZMQ
            side channel; the engine falls back to tokenization when it is
            absent or missing at consume time.
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
    _base_model_id: Optional[str] = None

    async def __init__(
        self,
        server: DeploymentHandle,
        llm_config: Optional["LLMConfig"] = None,
        base_model_id: Optional[str] = None,
    ):
        self._handle: DeploymentHandle = server
        self._base_model_id = base_model_id
        self._multiplexed_handles: Dict[str, DeploymentHandle] = {}
        self._tokenizer = None
        self._token_sender = None
        # Holds the KVTokenTracker (KV-aware deployments only) so the
        # engine-facing on_lifecycle_events method can book load into it.
        self._kv_token_tracker = None
        # A non-None llm_config signals pre-routing tokenization, which the
        # builder binds only for a KV-aware request router.
        if llm_config is not None:
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
            from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (  # noqa: E501
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

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        request_body = _parse_request_body(body)
        routing_payload = _get_routing_payload_from_body(request_body)
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
        # Tokenize only a parseable, routable body; a truncated or unparseable
        # body has no routing payload, so fall back to token-less routing.
        request_token_ids = None
        if self._tokenizer is not None and routing_payload is not None:
            from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (  # noqa: E501
                TokenizeError,
            )

            try:
                request_token_ids = await self._tokenizer.tokenize(
                    vars(routing_payload)
                )
            except TokenizeError as e:
                raise HTTPException(status_code=e.status_code, detail=e.message)
        # HAProxy forwards the configured session header on the same name,
        # but use the same case-insensitive, separator-tolerant matcher as
        # proxy.py / ingress.py so a `-`/`_` rewrite anywhere in the path
        # doesn't silently drop session affinity.
        session_id = next(
            (v for k, v in request.headers.items() if _matches_session_id_header(k)),
            None,
        )
        multiplexed_model_id = self._get_multiplexed_model_id(request_body)
        handle = self._get_handle(multiplexed_model_id)
        if session_id:
            handle = handle.options(session_id=session_id)
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
        request_headers = {}
        if multiplexed_model_id:
            request_headers[SERVE_MULTIPLEXED_MODEL_ID] = multiplexed_model_id
        if request_token_ids:
            token_key = self._push_prompt_tokens(
                token_endpoint=token_endpoint,
                replica_id=replica_id,
                request_token_ids=request_token_ids,
            )
            if token_key:
                request_headers[KV_TOKEN_KEY_HEADER] = token_key
        if request_headers:
            response[
                RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD
            ] = request_headers
        return response

    def _get_multiplexed_model_id(self, request_body: Optional[Dict[str, Any]]) -> str:
        """Return the requested adapter ID when this deployment enables LoRA."""
        if self._base_model_id is None or request_body is None:
            return ""

        model_id = request_body.get("model")
        if not isinstance(model_id, str) or model_id == self._base_model_id:
            return ""
        if get_base_model_id(model_id) != self._base_model_id:
            # A non-200 route response is translated by HAProxy into a 503.
            # Let the selected native ASGI app return its ordinary unknown-model
            # response instead of turning a client error into a routing failure.
            return ""
        return model_id

    def _get_handle(self, multiplexed_model_id: str) -> DeploymentHandle:
        if not multiplexed_model_id:
            return self._handle

        handle = self._multiplexed_handles.get(multiplexed_model_id)
        if handle is None:
            handle = self._handle.options(
                multiplexed_model_id=multiplexed_model_id,
            )
            self._multiplexed_handles[multiplexed_model_id] = handle
        return handle

    @router_app.get("/health")
    async def health(self):
        return {"status": "ok"}

    def __del__(self) -> None:
        """Close the token channel ZMQ sockets upon cleanup."""
        token_sender = getattr(self, "_token_sender", None)
        if token_sender is not None:
            token_sender.close()

    async def on_lifecycle_events(self, batch):
        """Engine-facing intake for request lifecycle events.

        Engine replicas broadcast each batch to every LLMRouter replica to
        book request load; this applies it to the KVTokenTracker on this
        ingress replica's event loop.
        """
        return await self._kv_token_tracker.on_lifecycle_events(batch)

    async def on_reservations_created(self, batch):
        """Ingress-facing intake for already-selected reservation bookings."""
        return await self._kv_token_tracker.on_reservations_created(batch)

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
