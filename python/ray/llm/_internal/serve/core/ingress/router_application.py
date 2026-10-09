"""OpenAI router application for multi-application direct streaming.

Each model is served by an independently deployed Serve application. The
router answers HAProxy with the replica that should receive inference requests
and directly serves lightweight global control-plane responses.
"""

import asyncio
import copy
import json
import time
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Optional

from fastapi import FastAPI, Request, status
from starlette.responses import JSONResponse

from ray import serve
from ray.exceptions import RayActorError
from ray.llm._internal.serve.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    get_llm_serve_runtime_env,
)
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    _get_routing_payload_from_body,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER,
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


router_app = FastAPI()


@serve._router_application
@serve.ingress(router_app)
class RouterApplication:
    """Route OpenAI requests across independently deployed model applications.

    Inference requests return ``{"application", "replica_id"}`` for HAProxy to
    dispatch directly. Global model-list endpoints return marked direct
    responses, so listing models never starts a scale-to-zero model application.
    """

    def __init__(self, model_applications: Mapping[str, str]):
        self._model_applications = dict(model_applications)
        # Resolve every handle in parallel without gating router health on them.
        self._handles: Dict[str, "asyncio.Task[DeploymentHandle]"] = {
            model_id: asyncio.create_task(self._resolve_handle(app_name))
            for model_id, app_name in self._model_applications.items()
        }

    async def check_health(self):
        pass

    @router_app.get("/v1/models")
    async def models(self):
        return _response(
            {
                "object": "list",
                "data": [
                    _model_card(model_id) for model_id in self._model_applications
                ],
            }
        )

    @router_app.get("/v1/models/{model:path}")
    async def model_data(self, model: str):
        # Like the OpenAI ingress, accept `--` for `/`.
        model = model.replace("--", "/")
        if model not in self._model_applications:
            return _error(
                status.HTTP_404_NOT_FOUND,
                f"Unable to find {model}. Please ensure that the model exists and "
                "you have permission.",
                "InvalidModel",
            )
        return _response(_model_card(model))

    @router_app.post("/v1/chat/completions")
    async def chat(self, request: Request):
        body = await request.body()
        try:
            data = json.loads(body)
        except (ValueError, TypeError):
            if _BODY_TRUNCATED_HEADER in request.headers:
                return _error(
                    status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    "Request body is too large to route by model. Raise "
                    "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE.",
                    "RequestTooLarge",
                )
            return _error(
                status.HTTP_400_BAD_REQUEST,
                "Request body is not valid JSON.",
                "BadRequestError",
            )
        if not isinstance(data, dict):
            return _error(
                status.HTTP_400_BAD_REQUEST,
                "Request body must be a JSON object.",
                "BadRequestError",
            )

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
    async def _resolve_handle(app_name: str) -> DeploymentHandle:
        """Look up the application's handle, retrying until it is deployed."""
        backoff_s = HANDLE_RETRY_INITIAL_S
        first_failure_s = None
        last_log_s = 0.0
        while True:
            try:
                handle = await asyncio.to_thread(serve.get_app_handle, app_name)
                break
            # Not deployed yet (or misnamed), no controller yet, or controller restarting.
            except (RayServeException, RayActorError) as e:
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
        try:

            async def resolve_and_choose() -> str:
                handle = await asyncio.shield(self._handles[model_id])
                session_id = session_id_from_headers(request.headers)
                if session_id:
                    handle = handle.options(session_id=session_id)
                return await self._choose_replica(handle, routing_payload)

            replica_id = await asyncio.wait_for(
                resolve_and_choose(), timeout=CHOOSE_REPLICA_TIMEOUT_S
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
        return {"application": application_name, "replica_id": replica_id}

    @staticmethod
    async def _choose_replica(
        handle: DeploymentHandle, routing_payload: Optional[SimpleNamespace]
    ) -> str:
        """Pick a replica without dispatching; HAProxy sends the request.

        Pick-only registers no autoscaling demand, so if it finds no replica
        quickly, wait on the reserving path, which lets the model scale from zero.
        """
        route_args = (routing_payload,) if routing_payload is not None else ()

        async def pick(**kwargs) -> str:
            async with handle.choose_replica(*route_args, **kwargs) as selection:
                return selection._replica.replica_id.to_full_id_str()

        try:
            return await asyncio.wait_for(
                pick(_reserve=False), timeout=PICK_ONLY_TIMEOUT_S
            )
        except asyncio.TimeoutError:
            return await pick()

    @classmethod
    def get_deployment_options(cls) -> Dict[str, Any]:
        options = copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
        options["ray_actor_options"]["runtime_env"] = get_llm_serve_runtime_env()
        return options
