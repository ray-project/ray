import asyncio
import time
import traceback
from collections import defaultdict
from functools import partial
from typing import Awaitable, Callable, TypeVar, Dict, Set

from fastapi import HTTPException, status
from httpx import HTTPStatusError as HTTPXHTTPStatusError
from pydantic import ValidationError as PydanticValidationError

from ray import serve
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ErrorInfo,
    ErrorResponse,
    OpenAIHTTPException,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

# Global state for error rate limiting
# Format: {(exception_type, exception_message): {'first_seen': timestamp, 'count': int, 'last_logged': timestamp}}
_error_tracker: Dict[tuple, Dict[str, float]] = defaultdict(lambda: {'first_seen': 0, 'count': 0, 'last_logged': 0})

# Engine dead error types that should be treated as fatal and circuit-broken
_ENGINE_DEAD_ERROR_PATTERNS: Set[str] = {'EngineDeadError'}

# Rate limiting settings
_TRACEBACK_LOG_COOLDOWN_SECONDS = 30  # Don't log full tracebacks more than once per 30 seconds for same error
_CIRCUIT_BREAKER_THRESHOLD = 5  # After 5 similar errors, start fast-failing
_SUMMARY_LOG_INTERVAL_SECONDS = 60  # Log summary of suppressed errors every minute


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


def _get_error_key(e: Exception) -> tuple:
    """Generate a key for error tracking based on exception type and core message."""
    exc_type = e.__class__.__name__
    # Extract the core error message without request-specific details
    message = extract_message_from_exception(e)
    # Remove request IDs and other variable parts for better grouping
    core_message = message.split('(Request ID:')[0].strip()
    return (exc_type, core_message)


def _should_log_full_traceback(error_key: tuple) -> bool:
    """Determine if we should log the full traceback for this error."""
    now = time.time()
    error_info = _error_tracker[error_key]
    
    # First time seeing this error - always log
    if error_info['first_seen'] == 0:
        error_info['first_seen'] = now
        error_info['last_logged'] = now
        error_info['count'] = 1
        return True
    
    error_info['count'] += 1
    
    # Check if we should log based on cooldown
    time_since_last_log = now - error_info['last_logged']
    if time_since_last_log >= _TRACEBACK_LOG_COOLDOWN_SECONDS:
        error_info['last_logged'] = now
        return True
    
    return False


def _log_error_summary():
    """Log a summary of suppressed errors periodically."""
    now = time.time()
    suppressed_errors = []
    
    for error_key, error_info in _error_tracker.items():
        if error_info['count'] > 1:  # Only include errors that occurred multiple times
            exc_type, core_message = error_key
            time_since_first = now - error_info['first_seen']
            suppressed_errors.append({
                'type': exc_type,
                'message': core_message,
                'count': error_info['count'],
                'duration_minutes': round(time_since_first / 60, 1)
            })
    
    if suppressed_errors:
        logger.warning(
            f"Error rate-limiting summary: {len(suppressed_errors)} error types with repeated occurrences. "
            f"Details: {suppressed_errors}. Full tracebacks logged at most once per {_TRACEBACK_LOG_COOLDOWN_SECONDS}s per error type."
        )


def _is_engine_dead_error(e: Exception) -> bool:
    """Check if this is an engine dead error that indicates fatal engine failure."""
    exc_type = e.__class__.__name__
    return exc_type in _ENGINE_DEAD_ERROR_PATTERNS or 'EngineDeadError' in str(e)


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

    # Rate-limited error logging
    error_key = _get_error_key(e)
    should_log_traceback = _should_log_full_traceback(error_key)
    
    log = logger.error if status_code >= 500 else logger.warning
    
    if should_log_traceback:
        # Log full traceback for first occurrence or after cooldown
        log(
            f"Encountered failure while handling request {request_id}",
            exc_info=e,
            extra={"ray_serve_extra_fields": {"status_code": status_code}},
        )
        
        # For engine dead errors, add circuit breaker info
        if _is_engine_dead_error(e) and _error_tracker[error_key]['count'] >= _CIRCUIT_BREAKER_THRESHOLD:
            logger.error(
                f"Engine dead error has occurred {_error_tracker[error_key]['count']} times. "
                f"This indicates a fatal engine failure. Consider implementing fast-fail logic "
                f"to avoid processing additional requests until replica restart."
            )
    else:
        # Log concise message without traceback
        error_count = _error_tracker[error_key]['count']
        time_since_first = time.time() - _error_tracker[error_key]['first_seen']
        
        log(
            f"Encountered failure while handling request {request_id} "
            f"(same error type occurred {error_count} times in {round(time_since_first/60, 1)}m, "
            f"traceback suppressed to reduce log spam)",
            extra={"ray_serve_extra_fields": {"status_code": status_code, "error_suppressed": True}},
        )
        
        # Periodically log summary
        if error_count % 50 == 0:  # Every 50th occurrence
            _log_error_summary()

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