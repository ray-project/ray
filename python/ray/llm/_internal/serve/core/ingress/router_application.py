"""OpenAI router application for multi-application direct streaming.

Each model is served by an independently deployed Serve application. The
router receives HAProxy's internal POST decision requests, answers with the
replica that should receive inference requests, and directly serves lightweight
global control-plane responses.
"""

import asyncio
import copy
import hashlib
import json
import os
import secrets
import time
import uuid
from types import SimpleNamespace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import unquote

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from ray import serve
from ray.exceptions import RayError
from ray.llm._internal.serve.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    get_llm_serve_runtime_env,
)
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    _get_routing_payload_from_body,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
    KV_TOKEN_METADATA_KEY,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER,
    SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER,
)
from ray.serve._private.http_util import session_id_from_headers
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

# Below HAProxy's decision timeout, so clients get an OpenAI-shaped 503.
CHOOSE_REPLICA_TIMEOUT_S = 0.8 * RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S
# Pick-only waits silently with no replicas; then reserve, which registers demand.
PICK_ONLY_TIMEOUT_S = 0.5
# Backoff for resolving a model application that is not deployed yet.
HANDLE_RETRY_INITIAL_S = 0.5
HANDLE_RETRY_MAX_S = 5.0
# How often to re-log an application that is still unavailable.
HANDLE_RETRY_LOG_INTERVAL_S = 300.0
# Clients retry after this many seconds while a model application starts.
RETRY_AFTER_S = 1

# The router is an independent, lightweight Serve deployment on the request path
# for every model application. These are starting points that should be tuned
# separately from model deployments based on aggregate request volume and routing
# latency.
DEFAULT_INGRESS_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "ray_actor_options": {"num_cpus": 1},
    "autoscaling_config": {
        "min_replicas": 1,
        "initial_replicas": 2,
        "max_replicas": 10,
        "target_ongoing_requests": 100,
    },
}


class _RouterTokenizeError(Exception):
    def __init__(self, message: str, status_code: int, type: str):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.type = type


def _response(
    content: Dict[str, Any],
    status_code: int = status.HTTP_200_OK,
    headers: Optional[Dict[str, str]] = None,
) -> JSONResponse:
    """Return a response that HAProxy should pass directly to the client."""
    return JSONResponse(
        content,
        status_code=status_code,
        headers={
            SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER: "1",
            **(headers or {}),
        },
    )


def _error(
    status_code: int,
    message: str,
    type: str,
    headers: Optional[Dict[str, str]] = None,
) -> JSONResponse:
    """Return an error in the OpenAI ingress's shape."""
    return _response(
        {
            "error": {
                "message": message,
                "type": type,
                "param": None,
                "code": status_code,
            }
        },
        status_code=status_code,
        headers=headers,
    )


def _model_card(model_id: str) -> Dict[str, Any]:
    """Build lightweight metadata without waking the model application."""
    return {
        "id": model_id,
        "object": "model",
        "owned_by": "organization-owner",
        "permission": [],
        "metadata": {"model_id": model_id},
    }


def _api_key_hashes(llm_config: LLMConfig) -> Sequence[bytes]:
    api_keys = llm_config.get_engine_config().frontend_kwargs.get("api_key")
    if not api_keys:
        api_key = (llm_config.runtime_env or {}).get("env_vars", {}).get(
            "VLLM_API_KEY"
        ) or os.getenv("VLLM_API_KEY")
        api_keys = [api_key] if api_key else []
    elif isinstance(api_keys, str):
        api_keys = [api_keys]
    return tuple(hashlib.sha256(key.encode()).digest() for key in api_keys if key)


def _has_valid_api_key(
    authorization: Optional[str], key_hashes: Sequence[bytes]
) -> bool:
    if not key_hashes:
        return True
    if not authorization:
        return False
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return False
    token_hash = hashlib.sha256(token.encode()).digest()
    return any(secrets.compare_digest(token_hash, key_hash) for key_hash in key_hashes)


_CORS_OPTIONS = {
    "allow_origins": ["*"],
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}

router_app = FastAPI()
router_app.add_middleware(CORSMiddleware, **_CORS_OPTIONS)
_cors_middleware = CORSMiddleware(router_app, **_CORS_OPTIONS)


@serve._router_application
@serve.ingress(router_app)
class RouterApplication:
    """Route OpenAI requests across independently deployed model applications.

    Inference requests return ``{"application", "replica_id"}`` for HAProxy to
    dispatch directly. Global model-list endpoints return marked direct
    responses, so listing models never starts a scale-to-zero model application.
    """

    def __init__(
        self,
        model_applications: Mapping[str, str],
        llm_configs: List[LLMConfig],
    ):
        self._model_applications = dict(model_applications)
        self._api_key_hashes = {
            config.model_id: _api_key_hashes(config) for config in llm_configs
        }
        self._llm_configs = {config.model_id: config for config in llm_configs}
        self._kv_model_ids = {
            config.model_id for config in llm_configs if is_kv_aware(config)
        }
        self._kv_trackers = {}
        self._tokenizers = {}
        self._token_sender = None
        self._warned_no_token_endpoint = False
        if self._kv_model_ids:
            from ray.llm._internal.serve.routing_policies.kv_aware import (
                token_channel,
            )
            from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (  # noqa: E501
                Tokenizer,
            )

            context = serve.get_replica_context()
            self._self_handle = serve.get_deployment_handle(
                context.deployment, app_name=context.app_name
            )
            self._token_sender = token_channel.TokenSender()
            self._tokenizers = {
                model_id: asyncio.create_task(
                    asyncio.to_thread(Tokenizer, self._llm_configs[model_id])
                )
                for model_id in self._kv_model_ids
            }
        # Resolve every handle in parallel without gating router health on them.
        self._handles: Dict[str, "asyncio.Task[DeploymentHandle]"] = {
            model_id: asyncio.create_task(self._resolve_handle(model_id, app_name))
            for model_id, app_name in self._model_applications.items()
        }

    async def check_health(self):
        for tokenizer_task in self._tokenizers.values():
            if tokenizer_task.done():
                tokenizer_task.result()

    @router_app.post("/internal/route")
    async def route(self, request: Request):
        request_target = request.headers.get(
            SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER, ""
        )
        path = request_target.partition("?")[0]
        method = request.headers.get(
            SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER, ""
        ).upper()

        if (
            method == "OPTIONS"
            and "origin" in request.headers
            and "access-control-request-method" in request.headers
        ):
            response = _cors_middleware.preflight_response(request.headers)
            response.headers[SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER] = "1"
            return response

        if path == "/v1/models" or path.startswith("/v1/models/"):
            if method not in {"GET", "HEAD"}:
                return _error(
                    status.HTTP_405_METHOD_NOT_ALLOWED,
                    "Method not allowed.",
                    "MethodNotAllowed",
                    headers={"Allow": "GET, HEAD"},
                )
            if path == "/v1/models":
                return await self.models(request)
            return await self.model_data(
                unquote(path.removeprefix("/v1/models/")), request
            )

        # The selected model application's native ASGI app owns its public API
        # routes. The control plane only chooses an application and replica; it
        # must not maintain a duplicate allowlist of vLLM routes.
        return await self._route_to_model_application(request)

    async def models(self, request: Request):
        model_ids = [
            model_id
            for model_id in self._model_applications
            if self._is_authorized(model_id, request)
        ]
        if not model_ids:
            return self._unauthorized()
        return _response(
            {
                "object": "list",
                "data": [_model_card(model_id) for model_id in model_ids],
            }
        )

    async def model_data(self, model: str, request: Request):
        # Like the OpenAI ingress, accept `--` for `/`.
        model = model.replace("--", "/")
        if model not in self._model_applications:
            return _error(
                status.HTTP_404_NOT_FOUND,
                f"Unable to find {model}. Please ensure that the model exists and "
                "you have permission.",
                "InvalidModel",
            )
        if not self._is_authorized(model, request):
            return self._unauthorized()
        return _response(_model_card(model))

    def _is_authorized(self, model_id: str, request: Request) -> bool:
        return _has_valid_api_key(
            request.headers.get("authorization"), self._api_key_hashes[model_id]
        )

    @staticmethod
    def _unauthorized() -> JSONResponse:
        return _response({"error": "Unauthorized"}, status.HTTP_401_UNAUTHORIZED)

    async def _route_to_model_application(self, request: Request):
        data = None
        model_id = None
        content_type = request.headers.get("content-type", "").lower()
        if content_type.startswith(
            ("multipart/form-data", "application/x-www-form-urlencoded")
        ):
            try:
                model_id = (await request.form()).get("model")
            except Exception:
                if len(self._model_applications) > 1:
                    return self._invalid_routing_body(
                        request, "Request body is not valid form data."
                    )
        else:
            body = await request.body()
            try:
                data = json.loads(body)
            except (ValueError, TypeError):
                if len(self._model_applications) > 1:
                    return self._invalid_routing_body(
                        request, "Request body is not valid JSON."
                    )
            else:
                if not isinstance(data, dict):
                    if len(self._model_applications) > 1:
                        return _error(
                            status.HTTP_400_BAD_REQUEST,
                            "Request body must be a JSON object.",
                            "BadRequestError",
                        )
                    data = None
                else:
                    model_id = data.get("model")

        if model_id is None and len(self._model_applications) == 1:
            model_id = next(iter(self._model_applications))
        if model_id is None:
            return _error(
                status.HTTP_400_BAD_REQUEST,
                "Model parameter is required when multiple models are configured. "
                f"Available models: {list(self._model_applications)}",
                "BadRequestError",
            )
        if not isinstance(model_id, str):
            return _error(
                status.HTTP_400_BAD_REQUEST,
                f"Model parameter must be a string, got {type(model_id).__name__}.",
                "BadRequestError",
            )
        if model_id not in self._model_applications:
            return _error(
                status.HTTP_404_NOT_FOUND,
                f'Could not find model with id "{model_id}". '
                f"Available models: {list(self._model_applications)}",
                "NotFoundError",
            )
        return await self._decide(
            model_id, _get_routing_payload_from_body(data), request
        )

    @staticmethod
    def _invalid_routing_body(request: Request, message: str) -> JSONResponse:
        if _BODY_TRUNCATED_HEADER in request.headers:
            return _error(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                "Request body is too large to route by model. Raise "
                "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE.",
                "RequestTooLarge",
            )
        return _error(
            status.HTTP_400_BAD_REQUEST,
            message,
            "BadRequestError",
        )

    async def _resolve_handle(self, model_id: str, app_name: str) -> DeploymentHandle:
        """Look up the application's handle, retrying until it is deployed."""
        backoff_s = HANDLE_RETRY_INITIAL_S
        first_failure_s = None
        last_log_s = 0.0
        while True:
            try:
                handle = await asyncio.to_thread(serve.get_app_handle, app_name)
                break
            # Not deployed yet (or misnamed), no controller yet, or controller restarting.
            except (RayServeException, RayError) as e:
                now = time.monotonic()
                if first_failure_s is None:
                    first_failure_s = last_log_s = now
                    logger.warning(
                        "Model application %s is not available yet; retrying: %r",
                        app_name,
                        e,
                    )
                elif now - last_log_s >= HANDLE_RETRY_LOG_INTERVAL_S:
                    last_log_s = now
                    logger.warning(
                        "Model application %s is still not available after %.0fs: %r",
                        app_name,
                        now - first_failure_s,
                        e,
                    )
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, HANDLE_RETRY_MAX_S)
        if first_failure_s is not None:
            logger.info("Model application %s is now available.", app_name)
        if model_id in self._kv_model_ids:
            from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (  # noqa: E501
                build_kv_token_tracker,
            )

            tracker = build_kv_token_tracker(
                self._llm_configs[model_id], handle.deployment_id
            )
            tracker.start_reservation_broadcast(self._self_handle, model_id=model_id)
            self._kv_trackers[model_id] = tracker
        # Start tracking replicas now, on the replica's event loop.
        if not handle.is_initialized:
            handle._init()
        return handle

    async def _decide(
        self,
        model_id: str,
        routing_payload: Optional[SimpleNamespace],
        request: Request,
    ):
        application_name = self._model_applications[model_id]
        deadline_s = time.monotonic() + CHOOSE_REPLICA_TIMEOUT_S
        try:
            handle = await asyncio.wait_for(
                asyncio.shield(self._handles[model_id]),
                timeout=max(0, deadline_s - time.monotonic()),
            )
            session_id = session_id_from_headers(request.headers)
            if session_id:
                handle = handle.options(session_id=session_id)
            request_token_ids = await self._tokenize(
                model_id,
                routing_payload,
                request.headers.get(SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER),
                timeout_s=max(0, deadline_s - time.monotonic()),
            )
            replica_id, token_endpoint = await self._choose_replica(
                handle,
                routing_payload,
                request_token_ids=request_token_ids,
                timeout_s=max(0, deadline_s - time.monotonic()),
            )
            request_headers = self._stage_prompt_tokens(
                replica_id, token_endpoint, request_token_ids, routing_payload
            )
        except _RouterTokenizeError as e:
            return _error(e.status_code, e.message, e.type)
        except ValueError as e:
            return _error(
                status.HTTP_400_BAD_REQUEST,
                str(e),
                "BadRequestError",
            )
        except (
            asyncio.TimeoutError,
            RayServeException,
            RuntimeError,
        ) as e:
            logger.warning(
                "No replica of application %s is available: %r",
                application_name,
                e,
            )
            return _error(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                f"Model '{model_id}' (application '{application_name}') is not "
                "available yet. Try again later.",
                "ServiceUnavailableError",
                headers={"Retry-After": str(RETRY_AFTER_S)},
            )
        decision = {"application": application_name, "replica_id": replica_id}
        if request_headers:
            decision[
                RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD
            ] = request_headers
        return decision

    async def _tokenize(
        self,
        model_id: str,
        routing_payload: Optional[SimpleNamespace],
        request_path: Optional[str],
        timeout_s: float,
    ) -> Optional[List[int]]:
        tokenizer_task = self._tokenizers.get(model_id)
        if tokenizer_task is None or routing_payload is None:
            return None
        tokenizer = await asyncio.wait_for(
            asyncio.shield(tokenizer_task), timeout=timeout_s
        )
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm.tokenizer import (  # noqa: E501
            TokenizeError,
        )

        try:
            return await tokenizer.tokenize(
                vars(routing_payload), request_path=request_path
            )
        except TokenizeError as e:
            raise _RouterTokenizeError(e.message, e.status_code, e.type) from e

    def _stage_prompt_tokens(
        self,
        replica_id: str,
        token_endpoint: Optional[str],
        request_token_ids: Optional[List[int]],
        routing_payload: Optional[SimpleNamespace],
    ) -> Dict[str, str]:
        if not request_token_ids or routing_payload is None:
            return {}
        from ray.llm._internal.serve.routing_policies.kv_aware.vllm.prompt_token_forwarding import (  # noqa: E501
            is_text_only_chat,
        )

        if not is_text_only_chat(getattr(routing_payload, "messages", [])):
            return {}
        if not token_endpoint or self._token_sender is None:
            if not self._warned_no_token_endpoint:
                self._warned_no_token_endpoint = True
                logger.warning(
                    "Selected replica %s did not advertise a prompt-token "
                    "ZMQ endpoint; falling back to engine tokenization.",
                    replica_id,
                )
            return {}

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
            return {}
        if not self._token_sender.push(token_endpoint, key, payload):
            return {}
        return {KV_TOKEN_KEY_HEADER: key}

    async def on_model_lifecycle_events(self, model_id: str, batch):
        tracker = self._kv_trackers.get(model_id)
        if tracker is not None:
            await tracker.on_lifecycle_events(batch)

    async def on_model_reservations_created(self, model_id: str, batch):
        tracker = self._kv_trackers.get(model_id)
        if tracker is not None:
            await tracker.on_reservations_created(batch)

    @staticmethod
    async def _choose_replica(
        handle: DeploymentHandle,
        routing_payload: Optional[SimpleNamespace],
        request_token_ids: Optional[List[int]],
        timeout_s: float,
    ) -> Tuple[str, Optional[str]]:
        """Pick a replica without dispatching; HAProxy sends the request.

        Pick-only registers no autoscaling demand, so if it finds no replica
        quickly, wait on the reserving path, which lets the model scale from zero.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()
        choose_replica_kwargs = {}
        if request_token_ids is not None:
            choose_replica_kwargs[REQUEST_TOKEN_IDS_KWARG] = request_token_ids
        deadline_s = time.monotonic() + timeout_s

        async def pick(**kwargs) -> Tuple[str, Optional[str]]:
            async with handle.choose_replica(
                *route_args, **choose_replica_kwargs, **kwargs
            ) as selection:
                replica = selection._replica
                token_metadata = replica.routing_stats.get(KV_TOKEN_METADATA_KEY)
                token_endpoint = (
                    token_metadata.get("endpoint")
                    if isinstance(token_metadata, dict)
                    else None
                )
                return replica.replica_id.to_full_id_str(), token_endpoint

        try:
            return await asyncio.wait_for(
                pick(_reserve=False),
                timeout=min(PICK_ONLY_TIMEOUT_S, max(0, deadline_s - time.monotonic())),
            )
        except asyncio.TimeoutError:
            return await pick(
                _selection_timeout_s=max(0, deadline_s - time.monotonic())
            )

    @classmethod
    def get_deployment_options(
        cls, llm_configs: Optional[List[LLMConfig]] = None
    ) -> Dict[str, Any]:
        options = copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
        env_vars = {}
        for config in llm_configs or []:
            if not is_kv_aware(config):
                continue
            for name, value in (config.runtime_env or {}).get("env_vars", {}).items():
                if name == "VLLM_API_KEY":
                    continue
                if name in env_vars and env_vars[name] != value:
                    raise ValueError(
                        f"KV-aware model configs must agree on runtime env var {name}."
                    )
                env_vars[name] = value
        options["ray_actor_options"]["runtime_env"] = get_llm_serve_runtime_env(
            {"env_vars": env_vars}
        )
        return options

    def __del__(self) -> None:
        token_sender = getattr(self, "_token_sender", None)
        if token_sender is not None:
            token_sender.close()
