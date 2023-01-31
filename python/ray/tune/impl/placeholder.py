from typing import Any, Dict, List, Tuple

from ray.tune.search.sample import Categorical, Function
from ray.tune.search.variant_generator import assign_value, _get_value
from ray.util.annotations import DeveloperAPI


class _CategoricalResolver:
    """Replaced value for categorical typed objects."""

    def __init__(self, choices):
        self._choices = choices

    def resolve(self, placeholder: str) -> Any:
        ca, i = placeholder.split("_")
        assert ca == "cat", "Categorical placeholder should start with cat_"
        return self._choices[int(i)]

    def get_placeholders(self) -> List[str]:
        return [f"cat_{i}" for i in range(len(self._choices))]


class _FunctionResolver:
    """Replaced value for function typed objects."""

    def __init__(self, fn):
        self._fn = fn

    def resolve(self, config):
        """Some functions take a resolved spec dict as input.

        Note: Function placeholders are independently sampled during
        resolution. Therefore their random states are not restored.
        """
        return self._fn.sample(config=config)

    def get_placeholder(self) -> str:
        return "fn_ph"


class _RefResolver:
    """Replaced value for all other non-primitive objects."""

    def __init__(self, value):
        self._value = value

    def resolve(self):
        return self._value

    def get_placeholder(self) -> str:
        return "ref_ph"


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
        # Special case for grid search spec.
        v = _CategoricalResolver(config["grid_search"])
        resolvers[prefix] = v
        # Here we return the original grid_search spec, but with the choices replaced.
        config["grid_search"] = v.get_placeholders()
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
        return (
            inject_placeholders(elem, resolvers, prefix + (i,))
            for i, elem in enumerate(config)
        )
    elif isinstance(config, (int, float, str)):
        # Primitive types.
        return config
    elif isinstance(config, Categorical):
        # Categorical type.
        v = _CategoricalResolver(config.categories)
        resolvers[prefix] = v
        # Here we return the original Categorical spec, but with the choices replaced.
        config.categories = v.get_placeholders()
        return config
    elif isinstance(config, Function):
        # Function type.
        v = _FunctionResolver(config)
        resolvers[prefix] = v
        return v.get_placeholder()
    else:
        # Other reference objects, dataset, actor handle, etc.
        v = _RefResolver(config)
        resolvers[prefix] = v
        return v.get_placeholder()


@DeveloperAPI
def resolve_placeholders(config: Any, replaced: Dict):
    """Replaces placeholders contained by a config dict with the original values.

    Args:
        config: The config to replace placeholders in.
        replaced: A dict from path to replaced objects.
    """
    for path, resolver in replaced.items():
        resolved = None

        if isinstance(resolver, _CategoricalResolver):
            sampled = _get_value(config, path)
            resolved = resolver.resolve(sampled)
        elif isinstance(resolver, _FunctionResolver):
            # Function domain expects the full spec dict.
            resolved = resolver.resolve(config)
        elif isinstance(resolver, _RefResolver):
            resolved = resolver.resolve()

        assign_value(config, path, resolved)
