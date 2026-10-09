import functools
import logging
import random
import re
import time
import traceback
from collections.abc import Sequence
from typing import Callable, Optional, TypeVar

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)

R = TypeVar("R")
P = ParamSpec("P")


def format_exception(exc: BaseException, include_cause: bool = False) -> str:
    """Format ``exc`` as ``"ClassName: message"`` for substring/regex matching.

    Uses `traceback.format_exception_only` so the class name is preserved
    and callers can match on either the class name or the message.

    Args:
        exc: The exception to format.
        include_cause: If True and ``exc.__cause__`` is set (``raise X from Y``),
            append the cause exception after the base exception. This is useful when
            we want to match on an exception encountered in the UDF (e.g. ``RateLimitError``)
            which is wrapped in a ``UserCodeException`` by Ray Data.

    Returns:
        A single-string representation of ``exc`` in the form
        ``"ClassName: message"``. When ``include_cause`` is True and
        ``exc.__cause__`` is set, the cause's formatted form is appended
        after a single space. See the example below.

    Example:
        For a ``UserCodeException`` wrapping a ``RateLimitError``, calling ``format_exception(e, include_cause=True)``
        returns::

            ray.exceptions.UserCodeException: UDF failed to process a data block. RateLimitError: Error code: 429 - rate limited
    """
    s = "".join(traceback.format_exception_only(type(exc), exc)).rstrip("\n")
    if include_cause and exc.__cause__:
        cause = exc.__cause__
        s += " " + "".join(traceback.format_exception_only(type(cause), cause)).rstrip(
            "\n"
        )
    return s


def matches_error(pattern: str, error_str: str) -> bool:
    """True if ``pattern`` matches ``error_str`` as a substring or as a regex.

    Substring is tried first so literal patterns are not interpreted as regex.
    Invalid regex patterns return False instead of raising.

    Args:
        pattern: Pattern to match, tried first as a substring then as a regex.
        error_str: Formatted exception string.

    Returns:
        True if ``pattern`` matches ``error_str`` as a substring or as a regex.
    """
    if pattern in error_str:
        return True
    try:
        return bool(re.search(pattern, error_str))
    except re.error:
        return False


def call_with_retry(
    f: Callable[P, R],
    description: str,
    match: Optional[Sequence[str]] = None,
    max_attempts: int = 10,
    max_backoff_s: int = 32,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """Retry a function with exponential backoff.

    Args:
        f: The function to retry.
        description: An imperative description of the function being retried. For
            example, "open the file".
        match: A sequence of patterns to match in the exception message. Each
            pattern is first checked as a substring, then as a regex. If
            ``None``, any error is retried.
        max_attempts: The maximum number of attempts to retry.
        max_backoff_s: The maximum number of seconds to backoff.
        *args: Arguments to pass to the function.
        **kwargs: Keyword arguments to pass to the function.

    Returns:
        The result of the function.
    """
    # TODO: consider inverse match and matching exception type
    assert max_attempts >= 1, f"`max_attempts` must be positive. Got {max_attempts}."

    for i in range(max_attempts):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            exception_str = format_exception(e)
            is_retryable = match is None or any(
                matches_error(pattern, exception_str) for pattern in match
            )
            if is_retryable and i + 1 < max_attempts:
                # Retry with binary exponential backoff with 20% random jitter.
                backoff = min(2**i, max_backoff_s) * (random.uniform(0.8, 1.2))
                logger.debug(
                    f"Retrying {i+1} attempts to {description} after {backoff} seconds."
                )
                time.sleep(backoff)
            else:
                if is_retryable:
                    logger.debug(
                        f"Failed to {description} after {max_attempts} attempts. Raising."
                    )
                else:
                    logger.debug(
                        f"Did not find a match for {exception_str}. Raising after {i+1} attempts."
                    )
                raise e from None


def retry(
    description: str,
    match: Optional[Sequence[str]] = None,
    max_attempts: int = 10,
    max_backoff_s: int = 32,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator-based version of call_with_retry.

    Args:
        description: An imperative description of the function being retried. For
            example, "open the file".
        match: A sequence of patterns to match in the exception message. Each
            pattern is first checked as a substring, then as a regex. If
            ``None``, any error is retried.
        max_attempts: The maximum number of attempts to retry.
        max_backoff_s: The maximum number of seconds to backoff.

    Returns:
        A Callable that can be applied in a normal decorator fashion.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def inner(*args: P.args, **kwargs: P.kwargs) -> R:
            return call_with_retry(
                func, description, match, max_attempts, max_backoff_s, *args, **kwargs
            )

        return inner

    return decorator
