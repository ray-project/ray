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

# todo: remove for the '3.0.0' release.
_wrong_names_white_list = {
    "MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT",
    "MAX_PER_REPLICA_RETRY_COUNT",
    "REQUEST_LATENCY_BUCKETS_MS",
    "MODEL_LOAD_LATENCY_BUCKETS_MS",
    "MAX_CACHED_HANDLES",
    "CONTROLLER_MAX_CONCURRENCY",
    "SERVE_REQUEST_PROCESSING_TIMEOUT_S",
}


def _validate_name(name: str) -> None:
    """Validate Ray Serve environment variable name."""
    required_prefix = "RAY_SERVE_"

    if not name.startswith(required_prefix):
        if name in _wrong_names_white_list:
            return

        raise ValueError(
            f"Got unexpected environment variable name `{name}`! "
            f"Ray Serve environment variables require prefix `{required_prefix}`. "
        )


def _get_env_value(
    name: str,
    default: Optional[T],
    value_type: Type[T],
    validation_func: Optional[Callable[[T], bool]] = None,
    expected_value_description: Optional[str] = None,
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
            (e.g., "positive", "non-negative") used in error messages.
            Optional, expected only if validation_func is provided.

    Returns:
        The environment variable value converted to the specified type and validated,
        or the default value if the environment variable is not set.

    Raises:
        ValueError: If the environment variable value cannot be converted to the specified
            type, or if it fails the optional validation check. Also, if name validation fails.
    """
    _validate_name(name)

    explicitly_defined_value = os.environ.get(name)
    if explicitly_defined_value is None:
        if default is None:
            return None
        else:
            raw = default
    else:
        _deprecation_warning(name)
        raw = explicitly_defined_value

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


def get_env_str(name: str, default: Optional[str]) -> Optional[str]:
    """Get environment variable as a string.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.

    Returns:
        The environment variable value as a string.
        Returns `None` if default is `None` and value not found.
    """
    return _get_env_value(name, default, str)


def get_env_bool(name: str, default: str) -> bool:
    """Get environment variable as a boolean.

    Environment variable values of "1" are interpreted as True, all others as False.

    Args:
        name: The name of the environment variable.
        default: Default value to use if the environment variable is not set.
            Expects "0" or "1".

    Returns:
        True if the environment variable value is "1", False otherwise.
    """
    env_value_str = _get_env_value(name, default, str)
    return env_value_str == "1"


def _deprecation_warning(name: str) -> None:
    """Log replacement warning for wrong or legacy environment variables.

    TODO: remove this function for the '3.0.0' release.

    :param name: environment variable name
    """

    def get_new_name(name: str) -> str:
        if name == "RAY_SERVE_HANDLE_METRIC_PUSH_INTERVAL_S":
            return "RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S"
        elif name == "SERVE_REQUEST_PROCESSING_TIMEOUT_S":
            return "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S"
        else:
            return f"{required_prefix}{name}"

    change_version = "3.0.0"
    required_prefix = "RAY_SERVE_"

    if (
        name in _wrong_names_white_list
        or name == "RAY_SERVE_HANDLE_METRIC_PUSH_INTERVAL_S"
    ):
        new_name = get_new_name(name)
        warnings.warn(
            f"Starting from version `{change_version}` environment variable "
            f"`{name}` will be deprecated. Please use `{new_name}` instead.",
            FutureWarning,
            stacklevel=4,
        )
