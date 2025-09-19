import hashlib
from collections import deque
from typing import TYPE_CHECKING, Any, Callable, Deque, Dict, List, Optional, Union

from ray.data.aggregate import AggregateFnV2
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data.dataset import Dataset


@DeveloperAPI
def simple_split_tokenizer(value: str) -> List[str]:
    """Tokenize a string using a split on spaces."""
    return value.split(" ")


@DeveloperAPI
def simple_hash(value: object, num_features: int) -> int:
    """Deterministically hash a value into the integer space."""
    encoded_value = str(value).encode()
    hashed_value = hashlib.sha1(encoded_value)
    hashed_value_int = int(hashed_value.hexdigest(), 16)
    return hashed_value_int % num_features
