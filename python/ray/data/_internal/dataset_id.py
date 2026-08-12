import os
import string
import time
from typing import Callable

# ASCII order is required so fixed-width IDs preserve lexicographic sorting.
_BASE62_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase
_ULID_LENGTH = 22
_NUM_TIMESTAMP_BITS = 48
_NUM_RANDOM_BITS = 80


def _encode_base62(value: int) -> str:
    """Encode a ULID integer as a fixed-width Base62 string."""
    assert value >= 0, f"Received a negative value to encode: {value}"

    encoded = ""
    while value:
        value, remainder = divmod(value, len(_BASE62_ALPHABET))
        encoded = _BASE62_ALPHABET[remainder] + encoded

    encoded = encoded.rjust(_ULID_LENGTH, "0")
    assert len(encoded) == _ULID_LENGTH, encoded
    return encoded


def generate_dataset_ulid(*, get_time_ns: Callable[[], int] = time.time_ns) -> str:
    """Generate a time-sortable, practically unique Dataset ULID.

    Args:
        get_time_ns: A callable that returns the current time in nanoseconds.

    Returns:
        A 22-character Base62-encoded Dataset ULID.
    """
    timestamp_ms = get_time_ns() // 1_000_000
    assert 0 <= timestamp_ms < 1 << _NUM_TIMESTAMP_BITS, (
        f"Timestamp {timestamp_ms} ms does not fit in " f"{_NUM_TIMESTAMP_BITS} bits"
    )

    # Byte order doesn't affect randomness. Big-endian is used to interpret the
    # random bytes as an integer.
    random_value = int.from_bytes(os.urandom(_NUM_RANDOM_BITS // 8), byteorder="big")
    return _encode_base62((timestamp_ms << _NUM_RANDOM_BITS) | random_value)
