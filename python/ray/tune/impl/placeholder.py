from collections import defaultdict
import hashlib
from typing import Any, Dict, Tuple

from ray.tune.search.sample import Categorical, Domain, Function
from ray.tune.search.variant_generator import assign_value
from ray.util.annotations import DeveloperAPI


ID_HASH_LENGTH = 8


def create_resolvers_map():
    return defaultdict(list)


def _id_hash(path_tuple):
    """Compute a hash for the specific placeholder based on its path."""
    return hashlib.sha1(str(path_tuple).encode("utf-8")).hexdigest()[:ID_HASH_LENGTH]


class _FunctionResolver:
    """Replaced value for function typed objects."""

    TOKEN = "__fn_ph"

    def __init__(self, hash, fn):
        self.hash = hash
        self._fn = fn

    def resolve(self, config: Dict):
        """Some functions take a resolved spec dict as input.

        Note: Function placeholders are independently sampled during
        resolution. Therefore their random states are not restored.
        """
        return self._fn.sample(config=config)

    def get_placeholder(self) -> str:
        return (self.TOKEN, self.hash)


class _RefResolver:
    """Replaced value for all other non-primitive objects."""

    TOKEN = "__ref_ph"

    def __init__(self, hash, value):
        self.hash = hash
        self._value = value

    def resolve(self):
        return self._value

    def get_placeholder(self) -> str:
        return (self.TOKEN, self.hash)


def _is_primitive(x):
    """Returns True if x is a primitive type.

    Primitive types are int, float, str, bool, and None.
    """
    return isinstance(x, (int, float, str, bool)) or x is None


@DeveloperAPI
def inject_placeholders(
    config: Any,
    resolvers: defaultdict,
    id_prefix: Tuple = (),
    path_prefix: Tuple = (),
) -> Dict:
    """Replaces reference objects contained by a config dict with placeholders.

    Given a config dict, this function replaces all reference objects contained
    by this dict with placeholder strings. It recursively expands nested dicts
    and lists, and properly handles Tune native search objects such as Categorical
    and Function.
    This makes sure the config dict only contains primitive typed values, which
    can then be handled by different search algorithms.

    A few details about id_prefix and path_prefix. Consider the following config,
    where "param1" is a simple grid search of 3 tuples.

    config = {
        "param1": tune.grid_search([
            (Cat, None, None),
            (None, Dog, None),
            (None, None, Fish),
        ]),
    }

    We will replace the 3 objects contained with placeholders. And after trial
    expansion, the config may look like this:

    config = {
        "param1": (None, (placeholder, hash), None)
    }

    Now you need 2 pieces of information to resolve the placeholder. One is the
    path of ("param1", 1), which tells you that the first element of the tuple
    under "param1" key is a placeholder that needs to be resolved.
    The other is the mapping from the placeholder to the actual object. In this
    case hash -> Dog.

    id and path prefixes serve exactly this purpose here. The difference between
    these two is that id_prefix is the location of the value in the pre-injected
    config tree. So if a value is the second option in a grid_search, it gets an
    id part of 1. Injected placeholders all get unique id prefixes. path prefix
    identifies a placeholder in the expanded config tree. So for example, all
    options of a single grid_search will get the same path prefix. This is how
    we know which location has a placeholder to be resolved in the post-expansion
    tree.

    Args:
        config: The config dict to replace references in.
        resolvers: A dict from path to replaced objects.
        id_prefix: The prefix to prepend to id every single placeholders.
        path_prefix: The prefix to prepend to every path identifying
            potential locations of placeholders in an expanded tree.

    Returns:
        The config with all references replaced.
    """
    if isinstance(config, dict) and "grid_search" in config and len(config) == 1:
        config["grid_search"] = [
            # Different options gets different id prefixes.
            # But we should omit appending to path_prefix because after expansion,
            # this level will not be there.
            inject_placeholders(choice, resolvers, id_prefix + (i,), path_prefix)
            for i, choice in enumerate(config["grid_search"])
        ]
        return config
    elif isinstance(config, dict):
        return {
            k: inject_placeholders(v, resolvers, id_prefix + (k,), path_prefix + (k,))
            for k, v in config.items()
        }
    elif isinstance(config, list):
        return [
            inject_placeholders(elem, resolvers, id_prefix + (i,), path_prefix + (i,))
            for i, elem in enumerate(config)
        ]
    elif isinstance(config, tuple):
        return tuple(
            inject_placeholders(elem, resolvers, id_prefix + (i,), path_prefix + (i,))
            for i, elem in enumerate(config)
        )
    elif _is_primitive(config):
        # Primitive types.
        return config
    elif isinstance(config, Categorical):
        config.categories = [
            # Different options gets different id prefixes.
            # But we should omit appending to path_prefix because after expansion,
            # this level will not be there.
            inject_placeholders(choice, resolvers, id_prefix + (i,), path_prefix)
            for i, choice in enumerate(config.categories)
        ]
        return config
    elif isinstance(config, Function):
        # Function type.
        id_hash = _id_hash(id_prefix)
        v = _FunctionResolver(id_hash, config)
        resolvers[path_prefix].append(v)
        return v.get_placeholder()
    elif not isinstance(config, Domain):
        # Other non-search space reference objects, dataset, actor handle, etc.
        id_hash = _id_hash(id_prefix)
        v = _RefResolver(id_hash, config)
        resolvers[path_prefix].append(v)
        return v.get_placeholder()
    else:
        # All the other cases, do nothing.
        return config


def _get_placeholder(config: Any, prefix: Tuple, path: Tuple):
    if not path:
        return prefix, config

    key = path[0]
    if isinstance(config, tuple):
        if config[0] in (_FunctionResolver.TOKEN, _RefResolver.TOKEN):
            # Found a matching placeholder.
            # Note that we do not require that the full path are consumed before
            # declaring a match. Because this placeholder may be part of a nested
            # search space. For example, the following config:
            #   config = {
            #       "param1": tune.grid_search([
            #           tune.grid_search([Object1, 2, 3]),
            #           tune.grid_search([Object2, 5, 6]),
            #       ]),
            #   }
            # will result in placeholders under path ("param1", 0, 0).
            # After expansion though, the choosen placeholder will live under path
            # ("param1", 0) like this: config = {"param1": (Placeholder1, 2, 3)}
            return prefix, config
        elif key < len(config):
            return _get_placeholder(
                config[key], prefix=prefix + (path[0],), path=path[1:]
            )
    elif (isinstance(config, dict) and key in config) or (
        isinstance(config, list) and key < len(config)
    ):
        # Expand config tree recursively.
        return _get_placeholder(config[key], prefix=prefix + (path[0],), path=path[1:])

    # Can not find a matching placeholder.
    return None, None


@DeveloperAPI
def resolve_placeholders(config: Any, replaced: defaultdict):
    """Replaces placeholders contained by a config dict with the original values.

    Args:
        config: The config to replace placeholders in.
        replaced: A dict from path to replaced objects.
    """

    def __resolve(resolver_type, args):
        for path, resolvers in replaced.items():
            assert resolvers

            if not isinstance(resolvers[0], resolver_type):
                continue

            prefix, ph = _get_placeholder(config, (), path)
            if not ph:
                # Represents an unchosen value. Just skip.
                continue

            for resolver in resolvers:
                if resolver.hash != ph[1]:
                    continue
                # Found the matching resolver.
                assign_value(config, prefix, resolver.resolve(*args))

    # RefResolvers first.
    __resolve(_RefResolver, args=())
    # Functions need to be resolved after RefResolvers, in case they are
    # referencing values from the RefResolvers.
    __resolve(_FunctionResolver, args=(config,))
