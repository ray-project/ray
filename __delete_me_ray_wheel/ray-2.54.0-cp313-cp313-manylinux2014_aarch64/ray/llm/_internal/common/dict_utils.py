from typing import Any, Dict, Optional


def maybe_apply_llm_deployment_config_defaults(
    defaults: Dict[str, Any],
    user_deployment_config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Apply defaults and merge with user-provided deployment config.

    If the user has explicitly set 'num_replicas' in their deployment config,
    we remove 'autoscaling_config' from the defaults since Ray Serve
    does not allow both to be set simultaneously. Then merges the defaults
    with the user config.

    Args:
        defaults: The default deployment options dictionary.
        user_deployment_config: The user-provided deployment configuration.

    Returns:
        The merged deployment options with conflicts resolved.
    """
    if user_deployment_config and "num_replicas" in user_deployment_config:
        defaults = defaults.copy()
        defaults.pop("autoscaling_config", None)
    return deep_merge_dicts(defaults, user_deployment_config or {})


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
