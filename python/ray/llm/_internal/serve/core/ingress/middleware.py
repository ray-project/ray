import os
import secrets
import uuid
from asyncio import CancelledError
from typing import Iterable, Optional

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.utils.server_utils import (
    get_response_for_error,
)

logger = get_logger(__file__)


def get_request_id(request: Request) -> str:
    """Fetches request-id from Starlette's request object.

    NOTE: This method relies on "request_id" value to be injected into the
    Starlette's ``request.state`` via ``inject_request_id`` middleware.

    Args:
        request: Starlette request object.

    Returns:
        Id allowing to identify the particular request, or ``None`` if not set.
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


def get_user_id(request: Request) -> Optional[str]:
    """Fetches user id inside Starlette's request object.

    NOTE: This method relies on "user_id" value to be injected into the
    Starlette's ``request.state`` via authentication middleware.

    Args:
        request: Starlette request object.

    Returns:
        Id identifying the particular user, or ``None`` if not set.
    """
    return getattr(request.state, "user_id", None)


# Marker written into ``request.state.user_id`` once a request has passed the
# bearer-token check, so ``get_user_id`` (and the metrics layer that consumes
# it) can distinguish authenticated traffic without exposing the key itself.
AUTHENTICATED_USER_ID = "authenticated"

# Environment variable the ingress reads for its bearer key, matching the
# variable the standalone vLLM OpenAI server enforces.
VLLM_API_KEY_ENV_VAR = "VLLM_API_KEY"


class AuthMiddleware:
    """Enforces bearer-token authentication on the OpenAI-compatible ingress.

    Ray Serve LLM serves requests from a FastAPI ingress that is a separate
    process from the model replicas, so the engine-level ``VLLM_API_KEY`` /
    ``api_key`` never reaches the HTTP boundary. This middleware restores that
    enforcement at the ingress: when ``api_key`` is set, a request must carry
    an ``Authorization: Bearer <key>`` header matching it, or it is rejected
    with a 401 before it reaches any handler or model deployment.

    When ``api_key`` is empty/``None`` the middleware is a no-op, preserving
    the endpoint's open-by-default behavior (enforcement is opt-in).

    On success the request is tagged via ``request.state.user_id`` (see
    ``AUTHENTICATED_USER_ID``), filling the slot ``get_user_id`` documents.

    NOTE: This is a raw ASGI middleware (mirroring ``SetRequestIdMiddleware``)
          so it can short-circuit with a 401 without invoking downstream
          layers. CORS preflight (``OPTIONS``) requests and ``exempt_paths``
          (e.g. health/metrics) bypass the check.
    """

    def __init__(
        self,
        app,
        *,
        api_key: Optional[str] = None,
        api_key_env_var: Optional[str] = None,
        exempt_paths: Iterable[str] = (),
    ):
        self.app = app
        # An explicit key wins; otherwise fall back to the env var. This resolves
        # here, not at build time, because Starlette instantiates the middleware
        # in the ingress replica process -- so the env read reflects the replica
        # (the process that actually owns the HTTP boundary), not the driver that
        # assembled the app.
        resolved = api_key or (
            os.environ.get(api_key_env_var) if api_key_env_var else None
        )
        self.api_key = resolved or None
        self.exempt_paths = frozenset(exempt_paths)

    def _is_authorized(self, request: Request) -> bool:
        auth_header = request.headers.get("authorization", "")
        scheme, _, param = auth_header.partition(" ")
        if scheme.lower() != "bearer":
            return False
        token = param.strip()
        if not token:
            return False
        # Constant-time comparison to avoid leaking the key via response timing.
        # Compare as UTF-8 bytes: secrets.compare_digest raises TypeError on
        # non-ASCII str inputs, which would otherwise surface as a 500 (the
        # middleware runs outside the exception-handling middleware) for a
        # token/key containing non-ASCII characters.
        return secrets.compare_digest(
            token.encode("utf-8"), self.api_key.encode("utf-8")
        )

    async def __call__(self, scope, receive, send):
        # No key configured, or non-HTTP scope (lifespan/websocket): pass through.
        if not self.api_key or scope["type"] != "http":
            return await self.app(scope, receive, send)

        # CORS preflight carries no Authorization header; rejecting it would
        # break browsers before they ever send the real request.
        if scope.get("method") == "OPTIONS":
            return await self.app(scope, receive, send)

        if scope.get("path") in self.exempt_paths:
            return await self.app(scope, receive, send)

        request = Request(scope)
        if not self._is_authorized(request):
            response = JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "error": {
                        "message": (
                            "Incorrect API key provided. You can set the API "
                            "key via the VLLM_API_KEY environment variable or "
                            "the ingress api_key configuration."
                        ),
                        "type": "authentication_error",
                        "param": None,
                        "code": "invalid_api_key",
                    }
                },
                headers={"WWW-Authenticate": "Bearer"},
            )
            return await response(scope, receive, send)

        # Authenticated: tag the request for downstream consumers (e.g. metrics).
        request.state.user_id = AUTHENTICATED_USER_ID
        return await self.app(scope, receive, send)


def add_auth_middleware(
    app: FastAPI,
    *,
    api_key: Optional[str] = None,
    api_key_env_var: Optional[str] = None,
    exempt_paths: Iterable[str] = (),
) -> None:
    """Install :class:`AuthMiddleware` on ``app``.

    A no-op when neither ``api_key`` nor ``api_key_env_var`` is provided. When
    ``api_key_env_var`` is given the middleware is installed even without an
    explicit key, since the key may be supplied via the environment at replica
    startup; in that case an unset variable makes the middleware a runtime
    no-op (open endpoint), preserving backwards compatibility.
    """
    if not (api_key or api_key_env_var):
        return
    app.add_middleware(
        AuthMiddleware,
        api_key=api_key,
        api_key_env_var=api_key_env_var,
        exempt_paths=frozenset(exempt_paths),
    )
