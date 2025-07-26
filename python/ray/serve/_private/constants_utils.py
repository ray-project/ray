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
