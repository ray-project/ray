import asyncio
import json
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

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
# traffic. Sent regardless of whether body forwarding is enabled, since a
# deployment choice must not depend on that escape hatch. Looked up through
# Starlette's case-insensitive headers.
_REQUEST_METHOD_HEADER = "x-serve-request-method"
_REQUEST_PATH_HEADER = "x-serve-request-path"

# A request body routes on one of these fields. Body-aware routers read it off
# the namespace; a body without any of them degrades to load-balancing. Extend
# as routers learn to route additional request types.
_ROUTING_KEY_FIELDS = ("messages", "prompt")

router_app = FastAPI()


def _parse_body(body: bytes) -> Optional[dict]:
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


def _routing_payload(data: Optional[dict]) -> Optional[SimpleNamespace]:
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


def _parse_routing_payload(body: bytes) -> Optional[SimpleNamespace]:
    """``_routing_payload`` of ``_parse_body``; kept for callers and tests."""
    return _routing_payload(_parse_body(body))


@serve.ingress(router_app)
class LLMRouter:
    """Ingress request router for direct streaming.

    When direct streaming is enabled, HAProxy calls /internal/route on this
    deployment to get a data plane replica, then forwards traffic directly
    to the matching LLMServer replica's backend HTTP port.

    The router holds one ``DeploymentHandle`` per served model, keyed by model
    id (``servers``). A request selects its deployment by the ``model`` field of
    its body, then replica selection within that deployment is delegated to the
    deployment's configured request router, and this class translates the
    resulting pick into a backend HTTP endpoint.

    ``servers`` is an allow-list the builder constructs: it is the only place
    that both marks deployments ``_direct_http`` and hands them here, so the
    router never has to know which deployments own HTTP ports. Were a handle to
    a deployment without one ever passed, HAProxy's map has no entry for it and
    the request fails closed with ``unknown_deployment``.

    Model selection:
        * ``model`` names a configured id -> that deployment.
        * Otherwise its base model id (``get_base_model_id``) is tried, so a
          LoRA-style ``base:adapter`` id resolves to the base deployment.
        * No ``model`` and exactly one server -> that server (single-model apps
          need not send the field).
        * No ``model`` and several servers -> 400. A body HAProxy truncated or
          that is not a JSON object has no readable ``model`` and is treated the
          same way; selection never falls back to an arbitrary deployment.
        * Unknown ``model`` -> 404.
        HAProxy treats any non-200 from this endpoint as a routing failure and
        answers the client with 503 ``X-Serve-Reason: router_non_200``; the
        status codes above are for logs and direct callers.

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
    # keeps the guard safe before __init__ runs.
    _warned_no_routing_key: bool = False
    _warned_no_token_endpoint: bool = False

    # No application ingress to route to, and so no ingress-owned routes to
    # match against. Class-level defaults for the same reason as the flags
    # above: a router built without __init__ (as the unit tests do, to skip the
    # handle setup) still takes the model-selection path rather than raising.
    _ingress: Optional[DeploymentHandle] = None
    _ingress_routes = None

    async def __init__(
        self,
        servers: Dict[str, DeploymentHandle],
        llm_config: Optional["LLMConfig"] = None,
        ingress: Optional[DeploymentHandle] = None,
    ):
        if not servers:
            raise ValueError(
                "LLMRouter requires at least one model id -> deployment handle."
            )
        # model id -> handle to the `_direct_http` deployment serving that model.
        self._servers: Dict[str, DeploymentHandle] = dict(servers)
        # Application ingress, when it is a separate deployment that owns routes
        # of its own (model discovery, control plane). None for the builders
        # whose ingress *is* the model server.
        self._ingress = ingress
        self._tokenizer = None
        self._token_sender = None
        # Holds the KVTokenTracker (KV-aware deployments only) so the
        # engine-facing on_lifecycle_events method can book load into it.
        self._kv_token_tracker = None
        # A non-None llm_config signals pre-routing tokenization, which the
        # builder binds only for a KV-aware request router. The tracker is a
        # process global and the tokenizer is per-model, so this path is
        # single-model; the builder rejects KV-aware routing with several
        # models before it can reach here. TODO (celinky): multi model KV-aware
        # routing support.
        if llm_config is not None:
            if len(self._servers) != 1:
                raise ValueError(
                    "KV-aware routing (llm_config given) supports exactly one "
                    f"model per LLMRouter; got {sorted(self._servers)}."
                )
            (server,) = self._servers.values()
            # Build the tracker before the handles' _init() below, which
            # initializes the KVAwareRouter that looks it up. server.deployment_id
            # is the tracked LLMServer deployment.
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
            # Ask the running replica what it actually serves, rather than
            # reading build-time metadata off the controller, which can be stale
            # or missing before replicas report in. Fetched once and cached for
            # this replica's life; an ingress-only route change needs a restart.
            #
            # Deliberately unguarded: if this fails the router has no idea which
            # paths the ingress owns, and every control request would fall
            # through to model selection and be answered by a model deployment.
            # Failing initialization is the safe outcome.
            from ray.serve._private.thirdparty.get_asgi_route_name import (
                ASGIRoutePatternMatcher,
            )

            patterns = await self._ingress.__serve_route_patterns__.remote()
            self._ingress_routes = ASGIRoutePatternMatcher(patterns)

    def _select_handle(self, model: Optional[str]) -> DeploymentHandle:
        """Resolve the request's ``model`` to a deployment handle.

        See the class docstring for the rules. Raises ``HTTPException`` (400 for
        an ambiguous request, 404 for an unknown model) so ``route`` surfaces a
        non-200 and HAProxy fails closed rather than picking a deployment the
        client did not ask for.
        """
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
        # Exact id first so a configured id that itself contains ':' is not
        # mistaken for a LoRA id and stripped to a base that does not exist.
        handle = self._servers.get(model)
        if handle is None:
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
        """Select an ingress replica for a request the ingress owns.

        Deliberately does none of the model-request work: the body is never read
        (a control request has nothing to route on, and HAProxy may have
        truncated it anyway), no session affinity is applied (session pinning is
        a model-deployment concern), and no KV tokenization is attempted.
        Selection still goes through the handle's normal request router rather
        than reaching into a replica set directly.
        """
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
        """Whether this request belongs to a route the application ingress owns.

        HAProxy forwards the original method and path (it consults the router for
        every request now, not just model traffic). Both are required: `GET /foo`
        and `POST /foo` can legitimately belong to different destinations. An
        older HAProxy that does not send them simply never matches, so the
        router keeps its previous model-only behavior.
        """
        if self._ingress_routes is None:
            return False
        method = request.headers.get(_REQUEST_METHOD_HEADER)
        path = request.headers.get(_REQUEST_PATH_HEADER)
        if not method or not path:
            return False
        return self._ingress_routes.matches(method, path)

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        # A route the ingress declares is the ingress's, whatever the body says.
        # Checked before the body is read at all, so a control request is never
        # subject to model selection or its 400/404s.
        if self._matches_ingress_route(request):
            return await self._route_to_ingress()

        body = await request.body()
        body_truncated = _BODY_TRUNCATED_HEADER in request.headers
        data = _parse_body(body)
        # Select the deployment before anything else: an unreadable body cannot
        # name a model, and with several models that is a 400, not a guess.
        handle = self._select_handle(data.get("model") if data is not None else None)
        routing_payload = _routing_payload(data)
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
