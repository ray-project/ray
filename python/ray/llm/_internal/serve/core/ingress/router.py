import asyncio
import json
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple

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
)
from ray.serve._private.http_util import _matches_session_id_header
from ray.serve._private.thirdparty.get_asgi_route_name import (
    ASGIRoutePatternMatcher,
    RoutePattern,
)
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

# Type-only import as LLMConfig transitively pulls in vLLM. This file should
# remain engine-agnostic.
if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

logger = get_logger(__name__)

_BODY_TRUNCATED_HEADER = "x-body-truncated"

# HAProxy forwards the original request line on these headers so the router can
# tell a request the application ingress owns (e.g. `GET /v1/models`) from model
# traffic. Sent regardless of whether body forwarding is enabled. Looked up through
# Starlette's case-insensitive headers.
_REQUEST_METHOD_HEADER = "x-serve-request-method"
_REQUEST_PATH_HEADER = "x-serve-request-path"

# A request body routes on one of these fields. Body-aware routers read it off
# the namespace; a body without any of them degrades to load-balancing. Extend
# as routers learn to route additional request types.
_ROUTING_KEY_FIELDS = ("messages", "prompt")

router_app = FastAPI()


def _parse_body(body: bytes) -> Optional[Dict[str, Any]]:
    """Parse a request body as a JSON object.

    Returns ``None`` for an empty, unparseable (including truncated), or
    non-object body. Parsed once here and shared by model selection and the
    routing payload below, so a body with a ``model`` but no routing key (e.g. an
    embeddings request) can still select a deployment.
    """
    if not body:
        return None
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _build_replica_routing_payload(
    data: Optional[Mapping[str, Any]],
) -> Optional[SimpleNamespace]:
    """Wrap a parsed body as a namespace a body-aware router routes on.

    Routers read a routing field (``messages`` or ``prompt``) off the first
    positional routing arg, the parsed request the normal ingress forwards.
    Direct streaming has only the raw body, so this wraps it in a namespace
    exposing every field by attribute, which a router reads the same way
    regardless of request type. Returns ``None`` for a missing or keyless body,
    so the caller falls back to load-balancing.
    """
    if data is None:
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

    The router first selects a deployment, then delegates replica selection to
    that deployment's configured request router. ``servers`` maps each model ID
    to its direct-HTTP deployment handle.

    When a separate application ingress is configured, ``ingress`` is its
    deployment handle and ``ingress_route_patterns`` identifies the HTTP routes
    it owns. Ingress routes take priority over model selection.

    Model selection:
        * A configured ``model`` selects its corresponding deployment.
        * A LoRA-style ``base:adapter`` model falls back to the configured base
          model deployment.
        * If ``model`` is omitted and exactly one server is configured, that
          server is selected.
        * If ``model`` is omitted and multiple servers are configured, the
          router returns 400 rather than selecting an arbitrary server.
        * An unknown or non-string ``model`` returns 404.

        HAProxy treats any non-200 from this endpoint as a routing failure and
        responds to the client with 503. The status codes above are visible to
        direct callers and in router logs.

    /internal/route HTTP contract
    -----------------------------
    Request:
        POST /internal/route
        Content-Type: application/json
        Body: the target ChatCompletions or Completions request payload.
            Parsed by ``_parse_body``, then wrapped by
            ``_build_replica_routing_payload`` and passed to ``choose_replica``
            positionally, exposing the request fields the way the parsed request
            does. Body-aware policies then score replicas the same way on both
            paths.

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
        200 ``{"host": str, "port": int, "deployment": str, "replica_id": str,
        "request_headers"?: dict}``:
            pick succeeded. ``deployment`` and ``replica_id`` together address
            the chosen replica;
            ``request_headers["x-serve-router-kv-token-key"]`` is present only
            when prompt token IDs were enqueued to the selected
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
    # keeps the below fields safe before __init__ runs.
    _warned_no_routing_key: bool = False
    _warned_no_token_endpoint: bool = False
    _ingress: Optional[DeploymentHandle] = None
    _ingress_routes = None

    async def __init__(
        self,
        servers: Dict[str, DeploymentHandle],
        llm_config: Optional["LLMConfig"] = None,
        ingress: Optional[DeploymentHandle] = None,
        ingress_route_patterns: Optional[List[RoutePattern]] = None,
    ):
        if not servers:
            raise ValueError(
                "LLMRouter requires at least one model id -> deployment handle."
            )
        if (ingress is None) != (ingress_route_patterns is None):
            raise ValueError(
                "`ingress` and `ingress_route_patterns` must be provided together."
            )
        if ingress is not None and not ingress_route_patterns:
            raise ValueError(
                "A separate ingress must declare at least one HTTP route pattern."
            )
        # model id -> handle to the `_direct_http` deployment serving that model.
        self._servers: Dict[str, DeploymentHandle] = dict(servers)
        self._ingress = ingress
        # Route ownership is build-time metadata, separate from the deployment
        # handle used for replica selection. Compile it once so request routing
        # stays local and does not need an RPC to the ingress deployment.
        self._ingress_routes = (
            ASGIRoutePatternMatcher(ingress_route_patterns)
            if ingress_route_patterns is not None
            else None
        )
        self._tokenizer = None
        self._token_sender = None
        # Holds the KVTokenTracker (KV-aware deployments only) so the
        # engine-facing on_lifecycle_events method can book load into it.
        self._kv_token_tracker = None
        # A non-None llm_config signals pre-routing tokenization, which the
        # builder binds only for a KV-aware request router. The tracker is a
        # process global and the tokenizer is per-model, so this path is
        # single-model; the builder rejects KV-aware routing with several
        # models before it can reach here.
        if llm_config is not None:
            if len(self._servers) != 1:
                raise ValueError(
                    "KV-aware routing (llm_config given) supports exactly one "
                    f"model per LLMRouter; got {sorted(self._servers)}."
                )
            (server,) = self._servers.values()
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
        for handle in self._servers.values():
            handle._init()

        if self._ingress is not None:
            self._ingress._init()

    def _select_handle(self, model: Any) -> DeploymentHandle:
        """Resolve the request's ``model`` to a deployment handle or raises."""
        if model is None:
            if (
                len(self._servers) == 1
            ):  # By design: its okay if no model is specified for single model.
                (handle,) = self._servers.values()
                return handle
            raise HTTPException(
                status_code=400,
                detail=(
                    "Model parameter is required when multiple models are "
                    f"configured. Available models: {sorted(self._servers)}"
                ),
            )
        if not isinstance(model, str):
            raise HTTPException(
                status_code=400,
                detail="Model parameter must be a string.",
            )
        handle = self._servers.get(model)
        if handle is None:
            # Try LoRA base model id
            handle = self._servers.get(get_base_model_id(model))
        if handle is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f'Got request for model "{model}". Could not find a '
                    f"configured model with that id or base model id. Available "
                    f"models: {sorted(self._servers)}"
                ),
            )
        return handle

    async def _route_to_ingress(self) -> dict:
        """Select an ingress replica for a request the ingress owns."""
        try:
            host, port, replica_id, _ = await self._pick_replica(
                handle=self._ingress,
                routing_payload=None,
                request_token_ids=None,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except (RuntimeError, DeploymentUnavailableError) as e:
            raise HTTPException(status_code=503, detail=str(e))
        return {
            "host": host,
            "port": port,
            "deployment": self._ingress.deployment_id.name,
            "replica_id": replica_id,
        }

    def _matches_ingress_route(self, request: Request) -> bool:
        """Whether this request belongs to a route the application ingress owns."""
        if self._ingress_routes is None:
            return False
        method = request.headers.get(_REQUEST_METHOD_HEADER)
        path = request.headers.get(_REQUEST_PATH_HEADER)
        if not method or not path:
            return False
        return self._ingress_routes.matches_http_route(method, path)

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        if self._matches_ingress_route(request):
            return await self._route_to_ingress()

        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        data = _parse_body(body)
        handle = self._select_handle(data.get("model") if data is not None else None)
        routing_payload = _build_replica_routing_payload(data)
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

        response = {
            "host": host,
            "port": port,
            "deployment": handle.deployment_id.name,
            "replica_id": replica_id,
        }
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
