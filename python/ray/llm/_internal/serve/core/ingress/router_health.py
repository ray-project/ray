"""Health probes for the direct-streaming routing path."""

import asyncio
import json
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional

from starlette.requests import Request
from starlette.responses import Response

from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_ROUTER_APPLICATION_ORIGINAL_METHOD_HEADER,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


def _get_min_replicas_from_llm_config(config: "LLMConfig") -> Optional[int]:
    autoscaling_config = config.deployment_config.get("autoscaling_config")
    if autoscaling_config is None:
        return None
    if isinstance(autoscaling_config, dict):
        return autoscaling_config.get("min_replicas")
    return getattr(autoscaling_config, "min_replicas", None)


async def check_router_health(
    route: Callable[[Request], Awaitable[Any]],
    model_id: Optional[str] = None,
) -> None:
    """Exercise routing without sending an inference request."""
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
