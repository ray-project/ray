from typing import Dict, List, Union, Optional, TypeVar
import copy
from collections import deque
from collections.abc import Mapping, Sequence

T = TypeVar("T")


def merge_dicts(d1: dict, d2: dict) -> dict:
    """
    Args:
        d1 (dict): Dict 1.
        d2 (dict): Dict 2.

    Returns:
         dict: A new dict that is d1 and d2 deep merged.
    """
    merged = copy.deepcopy(d1)
    deep_update(merged, d2, True, [])
    return merged


def deep_update(
    original: dict,
    new_dict: dict,
    new_keys_allowed: bool = False,
    allow_new_subkey_list: Optional[List[str]] = None,
    override_all_if_type_changes: Optional[List[str]] = None,
    override_all_key_list: Optional[List[str]] = None,
) -> dict:
    """Updates original dict with values from new_dict recursively.

    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the allow_new_subkey_list, then new subkeys can be introduced.

    Args:
        original: Dictionary with default values.
        new_dict: Dictionary with values to be updated
        new_keys_allowed: Whether new keys are allowed.
        allow_new_subkey_list: List of keys that
            correspond to dict values where new subkeys can be introduced.
            This is only at the top level.
        override_all_if_type_changes: List of top level
            keys with value=dict, for which we always simply override the
            entire value (dict), iff the "type" key in that value dict changes.
        override_all_key_list: List of top level keys
            for which we override the entire value if the key is in the new_dict.
    """
    allow_new_subkey_list = allow_new_subkey_list or []
    override_all_if_type_changes = override_all_if_type_changes or []
    override_all_key_list = override_all_key_list or []

    for k, value in new_dict.items():
        if k not in original and not new_keys_allowed:
            raise Exception("Unknown config parameter `{}` ".format(k))

        # Both orginal value and new one are dicts.
        if (
            isinstance(original.get(k), dict)
            and isinstance(value, dict)
            and k not in override_all_key_list
        ):
            # Check old type vs old one. If different, override entire value.
            if (
                k in override_all_if_type_changes
                and "type" in value
                and "type" in original[k]
                and value["type"] != original[k]["type"]
            ):
                original[k] = value
            # Allowed key -> ok to add new subkeys.
            elif k in allow_new_subkey_list:
                deep_update(
                    original[k],
                    value,
                    True,
                    override_all_key_list=override_all_key_list,
                )
            # Non-allowed key.
            else:
                deep_update(
                    original[k],
                    value,
                    new_keys_allowed,
                    override_all_key_list=override_all_key_list,
                )
        # Original value not a dict OR new value not a dict:
        # Override entire value.
        else:
            original[k] = value
    return original


def flatten_dict(
    dt: Dict,
    delimiter: str = "/",
    prevent_delimiter: bool = False,
    flatten_list: bool = False,
):
    """Flatten dict.

    Output and input are of the same dict type.
    Input dict remains the same after the operation.
    """

    def _raise_delimiter_exception():
        raise ValueError(
            f"Found delimiter `{delimiter}` in key when trying to flatten "
            f"array. Please avoid using the delimiter in your specification."
        )

    dt = copy.copy(dt)
    if prevent_delimiter and any(delimiter in key for key in dt):
        # Raise if delimiter is any of the keys
        _raise_delimiter_exception()

    while_check = (dict, list) if flatten_list else dict

    while any(isinstance(v, while_check) for v in dt.values()):
        remove = []
        add = {}
        for key, value in dt.items():
            if isinstance(value, dict):
                for subkey, v in value.items():
                    if prevent_delimiter and delimiter in subkey:
                        # Raise if delimiter is in any of the subkeys
                        _raise_delimiter_exception()

                    add[delimiter.join([key, str(subkey)])] = v
                remove.append(key)
            elif flatten_list and isinstance(value, list):
                for i, v in enumerate(value):
                    if prevent_delimiter and delimiter in subkey:
                        # Raise if delimiter is in any of the subkeys
                        _raise_delimiter_exception()

                    add[delimiter.join([key, str(i)])] = v
                remove.append(key)

        dt.update(add)
        for k in remove:
            del dt[k]
    return dt


def unflatten_dict(dt: Dict[str, T], delimiter: str = "/") -> Dict[str, T]:
    """Unflatten dict. Does not support unflattening lists."""
    dict_type = type(dt)
    out = dict_type()
    for key, val in dt.items():
        path = key.split(delimiter)
        item = out
        for k in path[:-1]:
            item = item.setdefault(k, dict_type())
            if not isinstance(item, dict_type):
                raise TypeError(
                    f"Cannot unflatten dict due the key '{key}' "
                    f"having a parent key '{k}', which value is not "
                    f"of type {dict_type} (got {type(item)}). "
                    "Change the key names to resolve the conflict."
                )
        item[path[-1]] = val
    return out


def unflatten_list_dict(dt: Dict[str, T], delimiter: str = "/") -> Dict[str, T]:
    """Unflatten nested dict and list.

    This function now has some limitations:
    (1) The keys of dt must be str.
    (2) If unflattened dt (the result) contains list, the index order must be
        ascending when accessing dt. Otherwise, this function will throw
        AssertionError.
    (3) The unflattened dt (the result) shouldn't contain dict with number
        keys.

    Be careful to use this function. If you want to improve this function,
    please also improve the unit test. See #14487 for more details.

    Args:
        dt: Flattened dictionary that is originally nested by multiple
            list and dict.
        delimiter: Delimiter of keys.

    Example:
        >>> dt = {"aaa/0/bb": 12, "aaa/1/cc": 56, "aaa/1/dd": 92}
        >>> unflatten_list_dict(dt)
        {'aaa': [{'bb': 12}, {'cc': 56, 'dd': 92}]}
    """
    out_type = list if list(dt)[0].split(delimiter, 1)[0].isdigit() else type(dt)
    out = out_type()
    for key, val in dt.items():
        path = key.split(delimiter)

        item = out
        for i, k in enumerate(path[:-1]):
            next_type = list if path[i + 1].isdigit() else dict
            if isinstance(item, dict):
                item = item.setdefault(k, next_type())
            elif isinstance(item, list):
                if int(k) >= len(item):
                    item.append(next_type())
                    assert int(k) == len(item) - 1
                item = item[int(k)]

        if isinstance(item, dict):
            item[path[-1]] = val
        elif isinstance(item, list):
            item.append(val)
            assert int(path[-1]) == len(item) - 1
    return out


def unflattened_lookup(
    flat_key: str, lookup: Union[Mapping, Sequence], delimiter: str = "/", **kwargs
) -> Union[Mapping, Sequence]:
    """
    Unflatten `flat_key` and iteratively look up in `lookup`. E.g.
    `flat_key="a/0/b"` will try to return `lookup["a"][0]["b"]`.
    """
    if flat_key in lookup:
        return lookup[flat_key]
    keys = deque(flat_key.split(delimiter))
    base = lookup
    while keys:
        key = keys.popleft()
        try:
            if isinstance(base, Mapping):
                base = base[key]
            elif isinstance(base, Sequence):
                base = base[int(key)]
            else:
                raise KeyError()
        except KeyError as e:
            if "default" in kwargs:
                return kwargs["default"]
            raise e
    return base
