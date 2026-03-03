import functools
import logging
import random
import time
from collections.abc import Sequence
from typing import Callable, Optional, TypeVar

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)

R = TypeVar("R")
P = ParamSpec("P")


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
        match: A sequence of strings to match in the exception message.
            If ``None``, any error is retried.
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
            exception_str = str(e)
            is_retryable = match is None or any(
                pattern in exception_str for pattern in match
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
        match: A sequence of strings to match in the exception message.
            If ``None``, any error is retried.
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
