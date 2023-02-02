from typing import Any, Dict, Tuple

from ray.tune.search.sample import Categorical, Domain, Function
from ray.tune.search.variant_generator import assign_value
from ray.util.annotations import DeveloperAPI


class _FunctionResolver:
    """Replaced value for function typed objects."""

    TOKEN = "__fn_ph"

    def __init__(self, fn):
        self._fn = fn

    def resolve(self, config: Dict):
        """Some functions take a resolved spec dict as input.

        Note: Function placeholders are independently sampled during
        resolution. Therefore their random states are not restored.
        """
        return self._fn.sample(config=config)

    def get_placeholder(self, path) -> str:
        return (self.TOKEN,) + path


class _RefResolver:
    """Replaced value for all other non-primitive objects."""

    TOKEN = "__ref_ph"

    def __init__(self, value):
        self._value = value

    def resolve(self):
        return self._value

    def get_placeholder(self, path) -> str:
        return (self.TOKEN,) + path


def _is_primitive(x):
    """Returns True if x is a primitive type.

    Primitive types are int, float, str, bool, and None.
    """
    return isinstance(x, (int, float, str, bool)) or not x


@DeveloperAPI
def inject_placeholders(config: Any, resolvers: Dict, prefix: Tuple = ()) -> Dict:
    """Replaces reference objects contained by a config dict with placeholders.

    Given a config dict, this function replaces all reference objects contained
    by this dict with placeholder strings. It recursively expands nested dicts
    and lists, and properly handles Tune native search objects such as Categorical
    and Function.
    This makes sure the config dict only contains primitive typed values, which
    can then be handled by different search algorithms.

    Args:
        config: The config dict to replace references in.
        resolvers: A dict from path to replaced objects.
        prefix: The prefix to prepend to all paths.

    Returns:
        The config with all references replaced.
    """
    if isinstance(config, dict) and "grid_search" in config and len(config) == 1:
        config["grid_search"] = [
            inject_placeholders(choice, resolvers, prefix + (i,))
            for i, choice in enumerate(config["grid_search"])
        ]
        return config
    elif isinstance(config, dict):
        return {
            k: inject_placeholders(v, resolvers, prefix + (k,))
            for k, v in config.items()
        }
    elif isinstance(config, list):
        return [
            inject_placeholders(elem, resolvers, prefix + (i,))
            for i, elem in enumerate(config)
        ]
    elif isinstance(config, tuple):
        return tuple(
            inject_placeholders(elem, resolvers, prefix + (i,))
            for i, elem in enumerate(config)
        )
    elif _is_primitive(config):
        # Primitive types.
        return config
    elif isinstance(config, Categorical):
        config.categories = [
            inject_placeholders(choice, resolvers, prefix + (i,))
            for i, choice in enumerate(config.categories)
        ]
        return config
    elif isinstance(config, Function):
        # Function type.
        v = _FunctionResolver(config)
        resolvers[prefix] = v
        return v.get_placeholder(prefix)
    elif not isinstance(config, Domain):
        # Other non-search space reference objects, dataset, actor handle, etc.
        v = _RefResolver(config)
        resolvers[prefix] = v
        return v.get_placeholder(prefix)
    else:
        # All the other cases, do nothing.
        return config


def _get_placeholder(config: Any, prefix: Tuple, path: Tuple):
    if not path:
        return prefix, config

    key = path[0]
    if (isinstance(config, dict) and key in config) or (
        isinstance(config, list) and key < len(config)
    ):
        # Expand config tree recursively.
        return _get_placeholder(config[key], prefix=prefix + (path[0],), path=path[1:])
    if isinstance(config, tuple):
        if config[0] in (_FunctionResolver.TOKEN, _RefResolver.TOKEN) and config[
            1:
        ] == (prefix + path):
            # Found the matching placeholder.
            return prefix, config
        elif key < len(config):
            return _get_placeholder(
                config[key], prefix=prefix + (path[0],), path=path[1:]
            )

    # Can not find a matching placeholder.
    # This is ok, since the path may represent a value that is not chosen.
    return None, None


@DeveloperAPI
def resolve_placeholders(config: Any, replaced: Dict):
    """Replaces placeholders contained by a config dict with the original values.

    Args:
        config: The config to replace placeholders in.
        replaced: A dict from path to replaced objects.
    """
    for path, resolver in replaced.items():
        # RefResolvers first.
        if isinstance(resolver, _RefResolver):
            prefix, ph = _get_placeholder(config, (), path)
            if not ph:
                # Represents an unchosen value. Just skip.
                continue
            assign_value(config, prefix, resolver.resolve())

    for path, resolver in replaced.items():
        # Functions need to be resolved after RefResolvers, in case they are
        # referencing values from the RefResolvers.
        if isinstance(resolver, _FunctionResolver):
            prefix, ph = _get_placeholder(config, (), path)
            if not ph:
                # Represents an unchosen value. Just skip.
                continue
            assign_value(config, prefix, resolver.resolve(config))
