import os
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
    default: T,
    value_type: Type[T],
    value_requirement: Optional[Callable[[T], bool]] = None,
    error_message: str = None,
) -> T:
    """Get environment variable with type conversion and validation.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.
        value_type: Type to convert the environment variable value to.
        value_requirement: Optional function to validate the converted value.
        error_message: Error message description for validation failures.

    Returns:
        The converted and validated environment variable value.

    Raises:
        ValueError: If type conversion fails or validation fails.
    """
    raw = os.environ.get(name, default)
    try:
        value = value_type(raw)
    except ValueError as e:
        raise ValueError(
            f"Environment variable `{name}` value `{raw}` cannot be converted to `{value_type.__name__}`!"
        ) from e

    if value_requirement and not value_requirement(value):
        raise ValueError(
            f"Got unexpected value `{value}` for `{name}` environment variable! Expected {error_message} `{value_type.__name__}`."
        )

    return value


def get_env_int(name: str, default: int) -> int:
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


def get_env_int_positive(name: str, default: int) -> int:
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


def get_env_int_non_negative(name: str, default: int) -> int:
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


def get_env_float(name: str, default: float) -> float:
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


def get_env_float_positive(name: str, default: float) -> float:
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


def get_env_float_non_negative(name: str, default: float) -> float:
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
