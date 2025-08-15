import os
import warnings
from typing import Callable, List, Optional, Type, TypeVar


def str_to_list(s: str) -> List[str]:
    """Return a list from a comma-separated string.

    Trims whitespace and skips empty entries.
    """
    return [part for part in (part.strip() for part in s.split(",")) if part]


def parse_latency_buckets(bucket_str: str, default_buckets: List[float]) -> List[float]:
    """Parse a comma-separated string of latency bucket values.

    Args:
        bucket_str: A comma-separated string of positive numbers in ascending order.
        default_buckets: Default bucket values to use if bucket_str is empty.

    Returns:
        A list of parsed float values.

    Raises:
        ValueError: If the format is invalid or values don't meet requirements.
    """
    if bucket_str.strip() == "":
        return default_buckets
    try:
        # Convert string to list of floats
        buckets = [float(x.strip()) for x in bucket_str.split(",")]

        if not buckets:
            raise ValueError("Empty bucket list")
        if any(x <= 0 for x in buckets):
            raise ValueError("Bucket values must be positive")
        if sorted(set(buckets)) != buckets:
            raise ValueError("Bucket values must be in strictly ascending order")

        return buckets
    except Exception as e:
        raise ValueError(
            f"Invalid format for `{bucket_str}`. "
            f"Expected comma-separated positive numbers in ascending order. Error: {str(e)}"
        ) from e


T = TypeVar("T")


def _get_env_value(
    name: str,
    default: Optional[T],
    value_type: Type[T],
    validation_func: Optional[Callable[[T], bool]] = None,
    expected_value_description: str = None,
) -> Optional[T]:
    """Get environment variable with type conversion and validation.

    This function retrieves an environment variable, converts it to the specified type,
    and optionally validates the converted value.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.
               If None, the function will return None without validation.
        value_type: Type to convert the environment variable value to (e.g., int, float, str).
        validation_func: Optional function that takes the converted value and returns
                       a boolean indicating whether the value is valid.
        expected_value_description: Description of the expected value characteristics
                                 (e.g., "positive", "non negative") used in error messages.

    Returns:
        The environment variable value converted to the specified type and validated,
        or the default value if the environment variable is not set.

    Raises:
        ValueError: If the environment variable value cannot be converted to the specified
            type, or if it fails the optional validation check.
    """
    raw = os.environ.get(name, default)
    if raw is None:
        return None

    try:
        value = value_type(raw)
    except ValueError as e:
        raise ValueError(
            f"Environment variable `{name}` value `{raw}` cannot be converted to `{value_type.__name__}`!"
        ) from e

    if validation_func and not validation_func(value):
        raise ValueError(
            f"Got unexpected value `{value}` for `{name}` environment variable! "
            f"Expected {expected_value_description} `{value_type.__name__}`."
        )

    return value


def get_env_int(name: str, default: Optional[int]) -> Optional[int]:
    """Get environment variable as an integer.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as an integer.

    Raises:
        ValueError: If the value cannot be converted to an integer.
    """
    return _get_env_value(name, default, int)


def get_env_int_positive(name: str, default: Optional[int]) -> Optional[int]:
    """Get environment variable as a positive integer.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a positive integer.

    Raises:
        ValueError: If the value cannot be converted to an integer or is not positive.
    """
    return _get_env_value(name, default, int, lambda x: x > 0, "positive")


def get_env_int_non_negative(name: str, default: Optional[int]) -> Optional[int]:
    """Get environment variable as a non-negative integer.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a non-negative integer.

    Raises:
        ValueError: If the value cannot be converted to an integer or is negative.
    """
    return _get_env_value(name, default, int, lambda x: x >= 0, "non negative")


def get_env_float(name: str, default: Optional[float]) -> Optional[float]:
    """Get environment variable as a float.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a float.

    Raises:
        ValueError: If the value cannot be converted to a float.
    """
    return _get_env_value(name, default, float)


def get_env_float_positive(name: str, default: Optional[float]) -> Optional[float]:
    """Get environment variable as a positive float.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a positive float.

    Raises:
        ValueError: If the value cannot be converted to a float or is not positive.
    """
    return _get_env_value(name, default, float, lambda x: x > 0, "positive")


def get_env_float_non_negative(name: str, default: Optional[float]) -> Optional[float]:
    """Get environment variable as a non-negative float.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a non-negative float.

    Raises:
        ValueError: If the value cannot be converted to a float or is negative.
    """
    return _get_env_value(name, default, float, lambda x: x >= 0, "non negative")


def get_env_str(name: str, default: Optional[str]) -> str:
    """Get environment variable as a string.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a string.
    """
    return os.environ.get(name, default)


def get_env_bool(name: str, default: Optional[str]) -> bool:
    """Get environment variable as a boolean.

    Environment variable values of "1" are interpreted as True, all others as False.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        True if the environment variable value is "1", False otherwise.
    """
    return os.environ.get(name, default) == "1"


def get_env_float_non_zero_with_warning(
    name: str, default: Optional[float]
) -> Optional[float]:
    """Introduced for backward compatibility for constants:

    PROXY_HEALTH_CHECK_TIMEOUT_S
    PROXY_HEALTH_CHECK_PERIOD_S
    PROXY_READY_CHECK_TIMEOUT_S
    PROXY_MIN_DRAINING_PERIOD_S
    RAY_SERVE_KV_TIMEOUT_S

    todo: replace this function with 'get_env_float_positive' for the '2.50.0' release.
    """
    removal_version = "2.50.0"

    env_value = get_env_float(name, default)
    backward_compatible_result = env_value or default

    if env_value is not None and env_value <= 0:
        # warning message if unexpected value
        warnings.warn(
            f"Got unexpected value `{env_value}` for `{name}` environment variable! "
            f"Starting from version `{removal_version}`, the environment variable will require a positive value. "
            f"Setting `{name}` to `{backward_compatible_result}`. ",
            FutureWarning,
            stacklevel=2,
        )
    return backward_compatible_result
