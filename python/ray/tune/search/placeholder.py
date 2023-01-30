from typing import Any, Dict, Tuple
from ray.tune.search.sample import Categorical, Function
from ray.tune.search.variant_generator import assign_value, _get_value

from ray.util.annotations import DeveloperAPI


class _Categorical_Value:
    """Replaced value for categorical typed objects.
    """
    def __init__(self, choices):
        self._choices = choices

    def get_value(self, placeholder: str) -> Any:
        ca, i = ph.split("_")
        assert ca == "cat", "Categorical placeholder should start with cat_"
        return self._choices[int(i)]

    def get_ph(self):
        return [f"cat_{i}" for i in range(len(self._choices))]


class _Function_Value:
    """Replaced value for function typed objects.
    """
    def __init__(self, fn):
        self._fn = fn

    def get(self, spec):
        """Some functions take a resolved spec dict as input.
        """
        return self._fn.sample(spec=spec)

    def get_ph(self):
        return "fn_ph"


class _Ref_Value:
    """Replaced value for all other non-primitive objects.
    """
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value

    def get_ph(self):
        return "ref_ph"


@DeveloperAPI
def replace_references(spec: Any, replaced: Dict, prefix: Tuple = ()) -> Dict:
    """Replaces reference objects contained by a spec dict with placeholders.

    Args:
        spec: The spec to replace references in.
        replacements: A dict from path to replaced objects.
        prefix: The prefix to prepend to all paths.

    Returns:
        The spec with all references replaced.
    """
    if (isinstance(spec, dict) and
        "grid_search" in spec and
        len(spec) == 1
    ):
        # Special case for grid search spec.
        v = _Categorical_Value(spec["grid_search"])
        replaced[prefix] = v
        # Here we return the original grid_search spec, but with the choices replaced. 
        spec["grid_search"] = v.get_ph()
        return spec
    elif isinstance(spec, dict):
        return {
            k: replace_references(v, replaced, prefix + (k,))
            for k, v in spec.items()
        }
    elif isinstance(spec, (list, tuple)):
        return [
            replace_references(elem, replaced, prefix + (i,))
            for i, elem in enumerate(spec)
        ]
    elif isinstance(spec, (int, float, str)):
        # Primitive types.
        return spec
    elif isinstance(spec, Categorical):
        # Categorical type.
        v = _Categorical_Value(spec.categories)
        replaced[prefix] = v
        # Here we return the original Categorical spec, but with the choices replaced. 
        spec.categories = v.get_ph()
        return spec
    elif isinstance(spec, Function):
        # Function type.
        v = _Function_Value(spec)
        replaced[prefix] = v
        return v.get_ph()
    else:
        # Other reference objects, dataset, actor handle, etc.
        v = _Ref_Value(spec)
        replaced[prefix] = v
        return v.get_ph()


@DeveloperAPI
def resolve_placeholders(spec: Any, replaced: Dict):
    """Replaces placeholders contained by a spec dict with the original values.

    Args:
        spec: The spec to replace placeholders in.
        replaced: A dict from path to replaced objects.
    """
    for path, value in replaced.items():
        resolved = None

        if isinstance(value, _Categorical_Value):
            sampled = _get_value(spec, path)
            resolved = value.get(sampled)
        elif isinstance(value, _Function_Value):
            resolved = value.get(spec)
        elif isinstance(value, _Ref_Value):
            resolved = value.get()

        assign_value(spec, path, resolved)
