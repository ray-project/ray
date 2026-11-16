"""Health probes for the direct-streaming routing path."""

import asyncio
import json
from typing import Any, Awaitable, Callable, Dict, Optional

from starlette.requests import Request
from starlette.responses import Response

from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER,
)
from ray.serve.handle import DeploymentHandle


def _has_running_replicas(handle: DeploymentHandle) -> bool:
    if not handle.is_initialized:
        return False
    # Read the current set without initializing a request router on the user
    # loop. running_replicas_populated() stays true after scale-down.
    request_router = handle._router._asyncio_router._request_router
    return request_router is not None and bool(request_router.curr_replicas)


async def check_router_health(
    handle: DeploymentHandle,
    route: Callable[[Request], Awaitable[Any]],
    model_id: Optional[str] = None,
) -> None:
    """Exercise routing for a live model without sending an inference request."""
    if not _has_running_replicas(handle):
        return

    # A plain prompt also works for tokenizers without a chat template.
    payload: Dict[str, Any] = {"prompt": "health check"}
    if model_id is not None:
        payload["model"] = model_id

    async def receive():
        return {"type": "http.request", "body": json.dumps(payload).encode()}

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/internal/route",
            "headers": [
                (
                    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER.encode(),
                    b"/v1/completions",
                ),
                (SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER.encode(), b"POST"),
                (b"content-type", b"application/json"),
            ],
        },
        receive=receive,
    )
    try:
        response = await asyncio.wait_for(
            route(request), timeout=RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S
        )
        if isinstance(response, Response) and response.status_code != 200:
            raise RuntimeError(
                f"Routing health check failed for model {model_id!r}: "
                f"HTTP {response.status_code}: {response.body.decode()}"
            )
    except Exception:
        # A model can scale down while the probe is in flight.
        if _has_running_replicas(handle):
            raise
