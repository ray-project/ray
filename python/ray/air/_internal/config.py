import dataclasses
from typing import Iterable


def ensure_only_allowed_dataclass_keys_updated(
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
    default_data = dataclass.__class__()

    allowed_keys = set(allowed_keys)

    # These keys should not have been updated in the `dataclass` object
    prohibited_keys = set(default_data.__dict__) - allowed_keys

    bad_keys = [
        key
        for key in prohibited_keys
        if dataclass.__dict__[key] != default_data.__dict__[key]
    ]
    if bad_keys:
        raise ValueError(
            f"Key(s) {bad_keys} are not allowed to be updated in the current context. "
            "Remove them from the dataclass."
        )
