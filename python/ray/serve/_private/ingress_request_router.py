from typing import Any, Dict

from starlette.requests import Request
from starlette.responses import JSONResponse

from ray import serve
from ray.serve._private.constants import SERVE_MULTIPLEXED_MODEL_ID
from ray.serve._private.http_util import session_id_from_headers
from ray.serve.handle import DeploymentHandle


@serve.deployment
class IngressRequestRouter:
    """Select an ingress replica through its Serve request router."""

    def __init__(self, ingress: DeploymentHandle):
        self._ingress = ingress
        self._ingress._init()

    async def __call__(self, request: Request) -> JSONResponse:
        model_id = request.headers.get(SERVE_MULTIPLEXED_MODEL_ID)
        if model_id is None:
            model_id = request.headers.get(SERVE_MULTIPLEXED_MODEL_ID.replace("_", "-"))

        handle = self._ingress
        if model_id is not None:
            handle = handle.options(multiplexed_model_id=model_id)

        session_id = session_id_from_headers(dict(request.headers))
        if session_id is not None:
            handle = handle.options(session_id=session_id)

        async with handle.choose_replica(request, _reserve=False) as selection:
            response: Dict[str, Any] = {
                "replica_id": selection._replica.replica_id.to_full_id_str()
            }
            if model_id is not None:
                response["request_headers"] = {SERVE_MULTIPLEXED_MODEL_ID: model_id}
            return JSONResponse(response)
