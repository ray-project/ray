from typing import Any, Dict


def deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two dictionaries hierarchically, creating a new dictionary without modifying inputs.

    For each key:
    - If the key exists in both dicts and both values are dicts, recursively merge them
    - Otherwise, the value from override takes precedence

    Args:
        base: The base dictionary
        override: The dictionary with values that should override the base

    Returns:
        A new merged dictionary

    Example:
        >>> base = {"a": 1, "b": {"c": 2, "d": 3}}
        >>> override = {"b": {"c": 10}, "e": 5}
        >>> result = deep_merge_dicts(base, override)
        >>> result
        {'a': 1, 'b': {'c': 10, 'd': 3}, 'e': 5}
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            result[key] = deep_merge_dicts(result[key], value)
        else:
            # Override the value (or add new key)
            result[key] = value

    return result
