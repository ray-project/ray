import hashlib
import string
from typing import List


def simple_split_tokenizer(value: str) -> List[str]:
    """Tokenize a string using a split on spaces."""
    return value.split(" ")


def default_tokenization_fn(value: str) -> List[str]:
    """Split a string into a list of tokens.

    This function converts ``value`` to lowercase, removes punctuation, and then
    delimits tokens by whitespace.

    Args:
        value: The string to tokenize.

    Examples:
        >>> string = "Hello, world!\nReadability counts."
        >>> default_tokenization_fn(string)
        ['hello', 'world', 'readability', 'counts']
    """
    value = value.lower()
    for character in string.punctuation:
        value = value.replace(character, "")
    return value.split()


def simple_hash(value: object, num_features: int) -> int:
    """Deterministically hash a value into the integer space."""
    encoded_value = str(value).encode()
    hashed_value = hashlib.sha1(encoded_value)
    hashed_value_int = int(hashed_value.hexdigest(), 16)
    return hashed_value_int % num_features
