"""OpenAI router application for multi-application direct streaming.

Each model is served by an independently deployed Serve application. The
router answers HAProxy with the replica that should receive inference requests
and directly serves lightweight global control-plane responses.
"""

import asyncio
import copy
import json
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Optional

from fastapi import FastAPI, Request, status
from starlette.responses import JSONResponse

from ray import serve
from ray.llm._internal.serve.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.core.ingress.router import (
    _BODY_TRUNCATED_HEADER,
    _routing_payload_from_dict,
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

# Same defaults as the OpenAI ingress. The router owns global control routes, so
# it remains available even when every model application has scaled to zero.
DEFAULT_INGRESS_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "autoscaling_config": {
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
}


def _response(
    content: Dict[str, Any], status_code: int = status.HTTP_200_OK
) -> JSONResponse:
    """Return a response that HAProxy should pass directly to the client."""
    return JSONResponse(
        content,
        status_code=status_code,
        headers={SERVE_ROUTER_APPLICATION_DIRECT_RESPONSE_HEADER: "1"},
    )


def _error(status_code: int, message: str, type: str) -> JSONResponse:
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


@serve.ingress(router_app)
class RouterApplication:
    """Route OpenAI requests across independently deployed model applications.

    Inference requests return ``{"application", "replica_id"}`` for HAProxy to
    dispatch directly. Global model-list endpoints return marked direct
    responses, so listing models never starts a scale-to-zero model application.
    """

    def __init__(self, model_applications: Mapping[str, str]):
        self._model_applications = dict(model_applications)
        self._handles: Dict[str, DeploymentHandle] = {}

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
        return await self._decide(model_id, _routing_payload_from_dict(data), request)

    def _get_handle(self, model_id: str) -> DeploymentHandle:
        handle = self._handles.get(model_id)
        if handle is None:
            handle = serve.get_app_handle(self._model_applications[model_id])
            # Start tracking replicas now rather than inside choose_replica.
            handle._init()
            self._handles[model_id] = handle
        return handle

    async def _decide(
        self,
        model_id: str,
        routing_payload: Optional[SimpleNamespace],
        request: Request,
    ):
        application_name = self._model_applications[model_id]
        try:
            handle = self._get_handle(model_id)
            session_id = session_id_from_headers(request.headers)
            if session_id:
                handle = handle.options(session_id=session_id)
            replica_id = await asyncio.wait_for(
                self._choose_replica(handle, routing_payload),
                timeout=CHOOSE_REPLICA_TIMEOUT_S,
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
                "No replica is available to serve the request. Try again later.",
                "ServiceUnavailableError",
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
        return copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
