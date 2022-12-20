
from typing import Union, Type, Tuple, Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.utils.nested_dict import NestedDict
    from ray.rllib.models.specs.specs_base import Spec


NestedKeys = List[Union[str, Tuple[str, ...]]]
Constraint = Union[Type, Tuple[Type, ...], Spec]
# Either a flat list of nested keys or a tree of constraints
SpecType = Union[NestedKeys, NestedDict[Optional[Constraint]]]
