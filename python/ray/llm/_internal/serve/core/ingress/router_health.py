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
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle


async def check_router_health(
    handle: DeploymentHandle,
    route: Callable[[Request], Awaitable[Any]],
    model_id: Optional[str] = None,
) -> None:
    """Exercise routing without sending an inference request."""
    if not handle.running_replicas_populated():
        return

    deployment_info, _ = await asyncio.wait_for(
        asyncio.to_thread(
            lambda: _get_global_client().get_deployment_info(
                handle.deployment_name, handle.app_name
            )
        ),
        timeout=RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    )
    autoscaling_config = deployment_info.deployment_config.autoscaling_config
    if autoscaling_config is not None and autoscaling_config.min_replicas == 0:
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
    response = await asyncio.wait_for(
        route(request), timeout=RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S
    )
    if isinstance(response, Response) and response.status_code != 200:
        raise RuntimeError(
            f"Routing health check failed for model {model_id!r}: "
            f"HTTP {response.status_code}: {response.body.decode()}"
        )
