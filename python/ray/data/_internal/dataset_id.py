import os
import time

_BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_ULID_LENGTH = 22
_TIMESTAMP_BITS = 48
_RANDOM_BITS = 80


def _encode_base62(value: int) -> str:
    """Encode a ULID integer as a fixed-width Base62 string."""
    if value < 0:
        raise ValueError("Cannot encode a negative value in Base62")

    encoded = ""
    while value:
        value, remainder = divmod(value, len(_BASE62_ALPHABET))
        encoded = _BASE62_ALPHABET[remainder] + encoded

    return encoded.rjust(_ULID_LENGTH, "0")


def generate_dataset_ulid() -> str:
    """Generate a time-sortable, practically unique Dataset ULID."""
    timestamp_ms = time.time_ns() // 1_000_000
    if not 0 <= timestamp_ms < 1 << _TIMESTAMP_BITS:
        raise ValueError("Timestamp must fit in 48 bits")

    random_value = int.from_bytes(os.urandom(_RANDOM_BITS // 8), byteorder="big")
    return _encode_base62((timestamp_ms << _RANDOM_BITS) | random_value)
