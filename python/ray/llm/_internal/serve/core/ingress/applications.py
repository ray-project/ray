"""Router and control applications for multi-application direct streaming.

Each model is its own application. The router application answers HAProxy
with the replica that serves a request (see `Application._as_router_application`);
the control application serves `/v1/models`.
"""

import asyncio
import copy
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, status
from starlette.responses import JSONResponse

from ray import serve
from ray.llm._internal.serve.constants import (
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
)
from ray.llm._internal.serve.core.ingress.router import _routing_payload_from_dict
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.http_util import session_id_from_headers
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

# Below HAProxy's decision timeout, so clients get an OpenAI-shaped 503.
CHOOSE_REPLICA_TIMEOUT_S = 4.0
# Pick-only waits silently with no replicas; then reserve, which registers demand.
PICK_ONLY_TIMEOUT_S = 0.5

_BODY_TRUNCATED_HEADER = "x-body-truncated"

# Same defaults as the OpenAI ingress.
DEFAULT_INGRESS_OPTIONS = {
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    "autoscaling_config": {
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
}


@dataclass(frozen=True)
class ApplicationDescriptor:
    """An application the router can select a replica of."""

    application_name: str
    ingress_deployment_name: str


@dataclass(frozen=True)
class ModelApplication(ApplicationDescriptor):
    """An application serving one model, with its LLMServer as the ingress."""

    model_id: str


def _error(status_code: int, message: str, type: str) -> JSONResponse:
    """An error in the OpenAI ingress's shape."""
    return JSONResponse(
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


def _ingress_options(scale_to_zero: bool) -> Dict[str, Any]:
    options = copy.deepcopy(DEFAULT_INGRESS_OPTIONS)
    if scale_to_zero:
        options["autoscaling_config"]["min_replicas"] = 0
    return options


control_app = FastAPI()


@serve.ingress(control_app)
class ControlApplication:
    """Serves the OpenAI model list from static model cards."""

    def __init__(self, model_cards: Dict[str, Dict[str, Any]]):
        self._model_cards = dict(model_cards)

    async def check_health(self):
        pass

    @control_app.get("/v1/models")
    async def models(self):
        return {"object": "list", "data": list(self._model_cards.values())}

    @control_app.get("/v1/models/{model:path}")
    async def model_data(self, model: str):
        # Like the OpenAI ingress, accept `--` for `/`.
        model = model.replace("--", "/")
        card = self._model_cards.get(model)
        if card is None:
            return _error(
                status.HTTP_404_NOT_FOUND,
                f"Unable to find {model}. Please ensure that the model exists and "
                "you have permission.",
                "InvalidModel",
            )
        return card

    @classmethod
    def get_deployment_options(cls, scale_to_zero: bool) -> Dict[str, Any]:
        return _ingress_options(scale_to_zero)


router_app = FastAPI()


@serve.ingress(router_app)
class RouterApplication:
    """Returns `{"application", "replica_id"}` for each request HAProxy holds.

    Replicas are chosen by the target ingress deployment's own request router,
    run locally with the parsed body and session ID; the target application's
    `LLMRouter` is not called.
    """

    def __init__(
        self,
        model_applications: List[ModelApplication],
        control_application: ApplicationDescriptor,
    ):
        self._models: Dict[str, ModelApplication] = {
            m.model_id: m for m in model_applications
        }
        self._control = control_application
        self._handles: Dict[str, DeploymentHandle] = {
            app.application_name: self._resolve_handle(app)
            for app in [*model_applications, control_application]
        }

    @staticmethod
    def _resolve_handle(app: ApplicationDescriptor) -> DeploymentHandle:
        # Siblings deploy concurrently and may not be registered yet.
        handle = serve.get_deployment_handle(
            app.ingress_deployment_name,
            app_name=app.application_name,
            _check_exists=False,
        )
        # Start tracking replicas now rather than on the first request.
        handle._init()
        return handle

    async def check_health(self):
        pass

    @router_app.get("/v1/models")
    async def models(self, request: Request):
        return await self._decide(self._control, None, request)

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
        if model_id is None and len(self._models) == 1:
            model_id = next(iter(self._models))
        if model_id is None:
            return _error(
                status.HTTP_400_BAD_REQUEST,
                "Model parameter is required when multiple models are configured. "
                f"Available models: {list(self._models)}",
                "BadRequestError",
            )
        if not isinstance(model_id, str):
            return _error(
                status.HTTP_400_BAD_REQUEST,
                f"Model parameter must be a string, got {type(model_id).__name__}.",
                "BadRequestError",
            )
        model = self._models.get(model_id)
        if model is None:
            return _error(
                status.HTTP_404_NOT_FOUND,
                f'Could not find model with id "{model_id}". '
                f"Available models: {list(self._models)}",
                "NotFoundError",
            )
        return await self._decide(model, _routing_payload_from_dict(data), request)

    async def _decide(
        self,
        app: ApplicationDescriptor,
        routing_payload: Optional[Any],
        request: Request,
    ):
        handle = self._handles[app.application_name]
        session_id = session_id_from_headers(request.headers)
        if session_id:
            handle = handle.options(session_id=session_id)
        try:
            replica_id = await asyncio.wait_for(
                self._choose_replica(handle, routing_payload),
                timeout=CHOOSE_REPLICA_TIMEOUT_S,
            )
        except (asyncio.TimeoutError, DeploymentUnavailableError, RuntimeError) as e:
            logger.warning(
                "No replica of application %s is available: %r",
                app.application_name,
                e,
            )
            return _error(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "No replica is available to serve the request. Try again later.",
                "ServiceUnavailableError",
            )
        return {"application": app.application_name, "replica_id": replica_id}

    @staticmethod
    async def _choose_replica(handle: DeploymentHandle, routing_payload) -> str:
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
        except (asyncio.TimeoutError, RuntimeError):
            return await pick()

    @classmethod
    def get_deployment_options(cls, scale_to_zero: bool) -> Dict[str, Any]:
        return _ingress_options(scale_to_zero)
