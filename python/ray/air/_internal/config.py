import dataclasses
from typing import Iterable


def _validate_allowed_keys_exist(
    dataclass_name: str, data_dict: dict, allowed_keys: set
):
    keys_not_in_dict = allowed_keys.difference(data_dict)
    if keys_not_in_dict:
        raise ValueError(
            f"Key(s) {sorted(keys_not_in_dict)} are not present in {dataclass_name}. "
            "Remove them from `allowed_keys`. "
            f"Valid keys: {sorted(data_dict.keys())}"
        )


def ensure_only_allowed_dataclass_keys_updated(
    dataclass: dataclasses.dataclass,
    allowed_keys: Iterable[str],
):
    """
    Validate dataclass by raising an exception if any key not included in
    ``allowed_keys`` differs from the default value.

    A ``ValueError`` will also be raised if any of the ``allowed_keys``
    is not present in ``dataclass.__dict__``.

    Args:
        dataclass: Dict or dataclass to check.
        allowed_keys: dataclass attribute keys that can have a value different than
            the default one.
    """
    default_data = dataclass.__class__()
    default_data_dict = default_data.__dict__

    allowed_keys = set(allowed_keys)
    _validate_allowed_keys_exist(
        dataclass.__class__.__name__, default_data_dict, allowed_keys
    )

    # These keys should not have been updated in the `dataclass` object
    prohibited_keys = set(default_data_dict) - allowed_keys
    dataclass_dict = dataclass.__dict__

    bad_keys = [
        key for key in prohibited_keys if dataclass_dict[key] != default_data_dict[key]
    ]
    if bad_keys:
        raise ValueError(
            f"Key(s) {bad_keys} are not allowed to be updated in the current context. "
            "Remove them from the dataclass."
        )
