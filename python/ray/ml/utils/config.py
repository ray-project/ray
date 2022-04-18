import dataclasses
from typing import Iterable


def ensure_only_allowed_keys_updated(
    dataclass: dataclasses.dataclass,
    allowed_keys: Iterable[str],
):
    """
    Validate dataclass by raising an exception if any key not included in
    ``allowed_keys`` differs from the default value.

    Args:
        dataclass: Dict or dataclass to check.
        allowed_keys: dataclass attribute keys that can have a value different than
        the default one.
    """
    if not dataclasses.is_dataclass(dataclass):
        raise ValueError(
            f"Can only perform allowed keys check on dataclass objects, got "
            f"{type(dataclass)}"
        )

    default_data = dataclass.__class__()

    allowed_keys = set(allowed_keys)

    bad_allowed_keys = [key for key in allowed_keys if key not in dataclass.__dict__]
    if bad_allowed_keys:
        raise KeyError(
            f"Allowed key(s) {bad_allowed_keys} are not valid. "
            f"Valid keys: {list(dataclass.__dict__.keys())}"
        )

    prohibited_keys = set(default_data.__dict__) - allowed_keys

    bad_keys = [
        key
        for key in prohibited_keys
        if dataclass.__dict__[key] != default_data.__dict__[key]
    ]
    if bad_keys:
        raise ValueError(
            f"Key(s) {bad_keys} are not allowed in the current context. "
            "Remove them from the dataclass."
        )
