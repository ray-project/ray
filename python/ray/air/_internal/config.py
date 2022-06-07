import dataclasses
from typing import Iterable


def ensure_only_allowed_dict_keys_set(
    data: dict,
    allowed_keys: Iterable[str],
):
    """
    Validate dict by raising an exception if any key not included in
    ``allowed_keys`` is set.

    Args:
        data: Dict to check.
        allowed_keys: Iterable of keys that can be contained in dict keys.
    """
    allowed_keys_set = set(allowed_keys)
    bad_keys = [key for key in data.keys() if key not in allowed_keys_set]

    if bad_keys:
        raise ValueError(
            f"Key(s) {bad_keys} are not allowed to be set in the current context. "
            "Remove them from the dict."
        )


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
