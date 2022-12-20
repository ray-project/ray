import hashlib
from typing import List


def simple_split_tokenizer(value: str) -> List[str]:
    """Tokenize a string using a split on spaces."""
    return value.split(" ")


def simple_hash(value: object, num_features: int) -> int:
    """Deterministically hash a value into the integer space."""
    encoded_value = str(value).encode()
    hashed_value = hashlib.sha1(encoded_value)
    hashed_value_int = int(hashed_value.hexdigest(), 16)
    return hashed_value_int % num_features
