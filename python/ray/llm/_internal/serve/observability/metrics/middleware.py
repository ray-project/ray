import time
from asyncio import CancelledError
from typing import Dict

from fastapi import FastAPI
from starlette.requests import Request
from starlette.routing import Match
from starlette.types import Message

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.observability.metrics.fastapi_utils import (
    FASTAPI_API_SERVER_TAG_KEY,
    FASTAPI_HTTP_HANDLER_TAG_KEY,
    FASTAPI_HTTP_METHOD_TAG_KEY,
    FASTAPI_HTTP_PATH_TAG_KEY,
    FASTAPI_HTTP_RESPONSE_CODE_TAG_KEY,
    FASTAPI_HTTP_USER_ID_TAG_KEY,
    get_app_name,
)
from ray.llm._internal.serve.deployments.routers.middleware import (
    get_request_id,
    get_user_id,
)

logger = get_logger("ray.serve")


class MeasureHTTPRequestMetricsMiddleware:
    """Measures and stores HTTP request metrics."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)

        # If the status_code isn't set by send_wrapper,
        # we should consider that an error.
        status_code = 500
        send_wrapper_failed_exc_info = "Status code was never set by send_wrapper."
        exception_info = send_wrapper_failed_exc_info

        async def send_wrapper(message: Message) -> None:
            """Wraps the send message.

            Enables this middleware to access the response headers.
            """

            nonlocal status_code, exception_info
            if message["type"] == "http.response.start":
                status_code = message.get("status", 500)

                # Clear the send_wrapper_failed_exc_info.
                if exception_info == send_wrapper_failed_exc_info:
                    exception_info = None

            await send(message)

        request = Request(scope)
        req_id = get_request_id(request)
        now = time.monotonic()
        try:
            logger.info(f"Starting handling of the request {req_id}")
            await self.app(scope, receive, send_wrapper)

        except CancelledError as ce:
            status_code = -1
            exception_info = ce
            raise

        except BaseException as e:
            status_code = 500
            exception_info = e
            raise

        finally:
            duration_s = time.monotonic() - now

            tags = _get_tags(request, status_code, request.app)
            # NOTE: Custom decorators are not applied to histogram-based metrics
            #       to make sure we can keep cardinality of those in check
            truncated_tags = {
                **tags,
                FASTAPI_HTTP_USER_ID_TAG_KEY: "truncated",
            }

            request.app.state.http_requests_metrics.inc(1, tags)
            request.app.state.http_requests_latency_metrics.observe(
                duration_s, truncated_tags
            )

            extra_context = {
                "status_code": status_code,
                "duration_ms": duration_s * 1000,
            }

            if status_code >= 400:
                log = logger.error if status_code >= 500 else logger.warning
                log(
                    f"Handling of the request {req_id} failed",
                    exc_info=exception_info,
                    extra={"ray_serve_extra_fields": extra_context},
                )
            elif status_code == -1:
                logger.info(
                    f"Handling of the request {req_id} have been cancelled",
                    extra={"ray_serve_extra_fields": extra_context},
                )
            else:
                logger.info(
                    f"Handling of the request {req_id} successfully completed",
                    extra={"ray_serve_extra_fields": extra_context},
                )


def _get_route_details(scope):
    """
    Function to retrieve Starlette route from scope.
    TODO: there is currently no way to retrieve http.route from
    a starlette application from scope.
    See: https://github.com/encode/starlette/pull/804
    Args:
        scope: A Starlette scope
    Returns:
        A string containing the route or None
    """
    app = scope["app"]
    route = None

    for starlette_route in app.routes:
        match, _ = starlette_route.matches(scope)
        if match == Match.FULL:
            route = starlette_route.path
            break
        if match == Match.PARTIAL:
            route = starlette_route.path
    return route


def _get_tags(request: Request, status_code: int, app: FastAPI) -> Dict[str, str]:
    """Generates tags for the request's metrics."""

    route = str(_get_route_details(request.scope)) or "unknown"
    path = str(request.url.path) or "unknown"
    method = str(request.method) or "unknown"
    user_id = str(get_user_id(request) or "unknown")

    return {
        FASTAPI_API_SERVER_TAG_KEY: get_app_name(app),
        FASTAPI_HTTP_RESPONSE_CODE_TAG_KEY: str(status_code),
        FASTAPI_HTTP_PATH_TAG_KEY: path,
        FASTAPI_HTTP_HANDLER_TAG_KEY: route,
        FASTAPI_HTTP_METHOD_TAG_KEY: method,
        FASTAPI_HTTP_USER_ID_TAG_KEY: user_id,
    }
