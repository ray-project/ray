import os
from typing import List


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


def get_env_int(name: str, default: int) -> int:
    """Get an integer value from an environment variable.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The integer value of the environment variable or the default.

    Raises:
        ValueError: If the environment variable value cannot be converted to an integer.
    """
    value = os.environ.get(name, default)
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable `{name}` value `{value}` cannot be converted to int!"
        )


def get_env_int_positive(name: str, default: int) -> int:
    """Get a positive integer value from an environment variable.

    Args:
        name: The name of the environment variable.
        default: Default positive value to use if the environment variable is not set.

    Returns:
        The positive integer value of the environment variable or the default.

    Raises:
        ValueError: If the environment variable value is not a positive integer.
    """
    value = get_env_int(name, default)
    if value <= 0:
        raise ValueError(
            f"Got unexpected value `{value}` for `{name}` environment variable! Expected positive integer."
        )
    return value


def get_env_float(name: str, default: float) -> float:
    """Get a float value from an environment variable.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The float value of the environment variable or the default.

    Raises:
        ValueError: If the environment variable value cannot be converted to a float.
    """
    value = os.environ.get(name, default)
    try:
        return float(value)
    except ValueError:
        raise ValueError(
            f"Environment variable `{name}` value `{value}` cannot be converted to float!"
        )


def get_env_str(name: str, default: str | None) -> str:
    """Get a string value from an environment variable.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The string value of the environment variable or the default.
    """
    return os.environ.get(name, default)


def get_env_bool(name: str, default: str) -> bool:
    """Get a boolean value from an environment variable.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.
               "1" will evaluate to True, anything else to False.

    Returns:
        True if the environment variable value is "1", otherwise False.
    """
    return os.environ.get(name, default) == "1"
