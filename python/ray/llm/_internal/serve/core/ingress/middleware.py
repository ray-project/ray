import uuid
from asyncio import CancelledError
from typing import Optional

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.utils.server_utils import (
    get_response_for_error,
)

logger = get_logger(__file__)


def get_request_id(request) -> str:
    """Fetches request-id from Starlette's request object.

    NOTE: This method relies on "request_id" value to be injected into the
    Starlette's `request.state` via `inject_request_id` middleware

    :param request: Starlette request object
    :return: (optional) Id allowing to identify particular user
    """
    return getattr(request.state, "request_id", None)


async def _handle_validation_error(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle pydantic validation errors in an OpenAI-like format."""
    error_details = exc.errors()[0] if exc.errors() else {"msg": "Invalid request"}

    error_msg = error_details.get("msg", "Unknown validation error")
    error_loc = error_details.get("loc", ("body"))
    error_input = error_details.get("input", None)
    msg = f"Invalid request format: {error_msg} at {error_loc}"

    error_response = {
        "error": {
            "message": msg,
            "type": error_details.get("type", "invalid_request_error"),
            "param": error_input,
            "code": "invalid_parameter",
        }
    }

    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=error_response)


def _uncaught_exception_handler(request: Request, e: Exception):
    """This method serves as an uncaught exception handler being
    the last resort to return properly formatted response.

    NOTE: Exceptions from application handlers should NOT be reaching this point,
          this handler is here to intercept "fly-away" exceptions and should not
          be handled for handling of converting application exceptions into
          appropriate responses
    """

    if isinstance(e, CancelledError):
        return JSONResponse(content={}, status_code=204)

    request_id = get_request_id(request)

    logger.error(f"Uncaught exception while handling request {request_id}", exc_info=e)

    error_response = get_response_for_error(e, request_id)

    return JSONResponse(
        content=error_response.model_dump(), status_code=error_response.error.code
    )


def add_exception_handling_middleware(router: FastAPI):
    # NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
    #
    # Starlette has different behavior depending on the Exception class being handled
    # that we unfortunately have to take into account here:
    #
    #   - Handler for `Exception` will be added as uncaught exception handler (of last resort)
    #     that is going to be executed absolute last, making sure that in case of any fly-away
    #     (uncaught) exception
    #   - Handlers for any other classes of exceptions will be executed as last middleware layer,
    #     therefore being to intercept any exceptions originating from the handler before it
    #     propagates to the middleware above it
    #
    # As such we're aiming for 2 goals here:
    #   - Intercepting exceptions from the handlers, converting them into proper user-facing
    #   response (avoiding exception propagation up the middleware stack)
    #   - Adding uncaught exception handler (of last resort) to intercept any exceptions that
    #     might be originating from the middleware itself

    async def _handle_application_exceptions(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """This method intercepts application level exceptions not handled by the
        application code converting them into appropriately formatted (JSON) response
        """

        try:
            return await call_next(request)
        except CancelledError as ce:
            # NOTE: We re-raise CancelledError as is to let other middleware handle it.
            #       Since no response is expected in this case, it's deferred to uncaught
            #       exception handler to ultimately handle it
            raise ce
        except RequestValidationError as e:
            return await _handle_validation_error(request, e)
        except Exception as e:
            request_id = get_request_id(request)
            error_response = get_response_for_error(e, request_id)

            return JSONResponse(
                content=error_response.model_dump(),
                status_code=error_response.error.code,
            )

    # This adds last-resort uncaught exception handler into Starlette
    router.add_exception_handler(Exception, _uncaught_exception_handler)
    # Add validation error handler
    router.add_exception_handler(RequestValidationError, _handle_validation_error)
    # This adds application exception handler, allowing to convert application
    # exceptions into properly formatted responses
    router.add_middleware(
        BaseHTTPMiddleware,
        dispatch=_handle_application_exceptions,
    )


class SetRequestIdMiddleware:
    """Injects request ID into the request's state.

    The ID is either:
        1. the value of the request's "x-request-id" header, set by Ray
           Serve's Proxy, or
        2. if "x-request-id" header is unavailable, this middleware creates
           a UUIDv4 request ID.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = list(scope.get("headers", []))
            request_id = None
            for name, value in headers:
                if name.lower() == b"x-request-id" and value:
                    request_id = value.decode()
                    break

            if request_id is None:
                request_id = str(uuid.uuid4())
                headers.append((b"x-request-id", request_id.encode()))

            scope["headers"] = headers
            request = Request(scope)
            request.state.request_id = request_id

        return await self.app(scope, receive, send)


def get_user_id(request) -> Optional[str]:
    """Fetches user id inside Starlette's request object.

    NOTE: This method relies on "user_id" value to be injected into the
    Starlette's `request.state` via authentication middleware

    :param request: Starlette request object
    :return: (optional) Id identifying particular user
    """
    return getattr(request.state, "user_id", None)
