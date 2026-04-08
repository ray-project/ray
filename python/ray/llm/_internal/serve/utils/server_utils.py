import asyncio
import threading
import time
import traceback
from functools import partial
from typing import Awaitable, Callable, TypeVar

from fastapi import HTTPException, status
from httpx import HTTPStatusError as HTTPXHTTPStatusError
from pydantic import ValidationError as PydanticValidationError

from ray import serve
from ray.llm._internal.serve.constants import DEFAULT_FATAL_ERROR_COOLDOWN_S
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ErrorInfo,
    ErrorResponse,
    OpenAIHTTPException,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

# Keep in sync with _VLLM_FATAL_ERRORS in batch/stages/vllm_engine_stage.py
_FATAL_ERROR_TYPE_NAMES = frozenset(
    {
        "EngineDeadError",
    }
)


def _is_fatal_engine_error(e: Exception) -> bool:
    """
    Detect fatal engine errors
    """
    type_name = type(e).__name__
    return any(name in type_name for name in _FATAL_ERROR_TYPE_NAMES)


class _FatalEngineErrorLogHandler:
    """Rate limits logging for fatal engine errors.

    - First occurrence: logs with full traceback.
    - Subsequent occurrences within the cooldown window: suppressed.
    - Every ``cooldown_s`` seconds: emits a summary with the count of
      suppressed errors.
    - Non fatal errors are never touched by this handler.
    """

    def __init__(self, cooldown_s: float = DEFAULT_FATAL_ERROR_COOLDOWN_S):
        self._cooldown_s = cooldown_s
        self._first_logged = False
        self._suppressed_count = 0
        self._last_summary_time = 0.0
        self._lock = threading.Lock()

    def log(
        self,
        e: Exception,
        request_id: str,
        status_code: int,
    ) -> None:
        """Log the error, rate limiting fatal engine errors."""
        is_fatal = _is_fatal_engine_error(e)

        if not is_fatal:
            log_fn = logger.error if status_code >= 500 else logger.warning
            log_fn(
                f"Encountered failure while handling request {request_id}",
                exc_info=e,
                extra={"ray_serve_extra_fields": {"status_code": status_code}},
            )
            return

        with self._lock:
            if not self._first_logged:
                self._first_logged = True
                self._last_summary_time = time.monotonic()
                logger.error(
                    "Encountered failure while handling request %s",
                    request_id,
                    exc_info=e,
                    extra={"ray_serve_extra_fields": {"status_code": status_code}},
                )
                return

            self._suppressed_count += 1
            now = time.monotonic()
            elapsed = now - self._last_summary_time
            if elapsed >= self._cooldown_s:
                logger.error(
                    "Suppressed %d fatal engine error(s) in the last %.0fs. "
                    "Engine is dead, awaiting replica restart.",
                    self._suppressed_count,
                    elapsed,
                )
                self._suppressed_count = 0
                self._last_summary_time = now


_fatal_error_log_handler = _FatalEngineErrorLogHandler()


def make_async(_func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """Take a blocking function, and run it on in an executor thread.

    This function prevents the blocking function from blocking the asyncio event loop.
    The code in this function needs to be thread safe.
    """

    def _async_wrapper(*args, **kwargs) -> asyncio.Future:
        loop = asyncio.get_event_loop()
        func = partial(_func, *args, **kwargs)
        return loop.run_in_executor(executor=None, func=func)

    return _async_wrapper


def extract_message_from_exception(e: Exception) -> str:
    # If the exception is a Ray exception, we need to dig through the text to get just
    # the exception message without the stack trace
    # This also works for normal exceptions (we will just return everything from
    # format_exception_only in that case)
    message_lines = traceback.format_exception_only(type(e), e)[-1].strip().split("\n")
    message = ""
    # The stack trace lines will be prefixed with spaces, so we need to start from the bottom
    # and stop at the last line before a line with a space
    found_last_line_before_stack_trace = False
    for line in reversed(message_lines):
        if not line.startswith(" "):
            found_last_line_before_stack_trace = True
        if found_last_line_before_stack_trace and line.startswith(" "):
            break
        message = line + "\n" + message
    message = message.strip()
    return message


def _extract_message(e):
    if isinstance(e, OpenAIHTTPException) and e.internal_message is not None:
        internal_message = e.internal_message
    else:
        internal_message = extract_message_from_exception(e)

    if isinstance(e, HTTPException):
        message = e.detail
    elif isinstance(e, OpenAIHTTPException):
        message = e.message
    else:
        message = internal_message

    return internal_message, message


def get_response_for_error(
    e: Exception,
    request_id: str,
) -> ErrorResponse:
    if isinstance(e, HTTPException):
        status_code = e.status_code
    elif isinstance(e, OpenAIHTTPException):
        status_code = e.status_code
    elif isinstance(e, PydanticValidationError):
        status_code = 400
    elif isinstance(e, HTTPXHTTPStatusError):
        status_code = e.response.status_code
    else:
        # Try to get the status code attribute from exception,
        # if not present, fallback to generic 500
        status_code = int(
            getattr(e, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR)
        )

    _fatal_error_log_handler.log(e, request_id, status_code)

    if status_code == status.HTTP_500_INTERNAL_SERVER_ERROR:
        internal_message = message = "Internal Server Error"
        exc_type = "InternalServerError"
    else:
        internal_message, message = _extract_message(e)
        exc_type = e.__class__.__name__

    # TODO make this more robust
    if "(Request ID: " not in message:
        message += f" (Request ID: {request_id})"

    if "(Request ID: " not in internal_message:
        internal_message += f" (Request ID: {request_id})"

    error_info = ErrorInfo(
        message=f"Message: {message}, Internal exception: {internal_message}, original exception: {str(e)}",
        code=status_code,
        type=exc_type,
    )
    error_response = ErrorResponse(error=error_info)
    return error_response


def get_serve_request_id() -> str:
    """Get request id from serve request context."""
    context = serve.context._serve_request_context.get()
    if context is not None:
        return context.request_id
    return ""


def get_model_request_id(model: str):
    return model + "-" + get_serve_request_id()


def replace_prefix(model: str) -> str:
    """Replace -- with / in model name to handle slashes within the URL path segment"""
    return model.replace("--", "/")
