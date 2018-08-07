import copy

from ray.rllib.utils.filter_manager import FilterManager
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.policy_server import PolicyServer

__all__ = ["Filter", "FilterManager", "PolicyClient", "PolicyServer"]


def merge_dicts(d1, d2):
    """Returns a new dict that is d1 and d2 deep merged."""
    merged = copy.deepcopy(d1)
    deep_update(merged, d2, True, [])
    return merged


def deep_update(original, new_dict, new_keys_allowed, whitelist):
    """Updates original dict with values from new_dict recursively.
    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the whitelist, then new subkeys can be introduced.

    Args:
        original (dict): Dictionary with default values.
        new_dict (dict): Dictionary with values to be updated
        new_keys_allowed (bool): Whether new keys are allowed.
        whitelist (list): List of keys that correspond to dict values
            where new subkeys can be introduced. This is only at
            the top level.
    """
    for k, value in new_dict.items():
        if k not in original:
            if not new_keys_allowed:
                raise Exception("Unknown config parameter `{}` ".format(k))
        if type(original.get(k)) is dict:
            if k in whitelist:
                deep_update(original[k], value, True, [])
            else:
                deep_update(original[k], value, new_keys_allowed, [])
        else:
            original[k] = value
    return original
