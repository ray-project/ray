from typing import Union, Type, Tuple, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.core.models.specs.specs_base import Spec


NestedKeys = List[Union[str, Tuple[str, ...]]]
Constraint = Union[Type, Tuple[Type, ...], "Spec"]
# Either a flat list of nested keys or a tree of constraints
SpecType = Union[NestedKeys]
