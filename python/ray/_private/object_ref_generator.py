from __future__ import annotations
from ray.util.annotations import DeveloperAPI
from typing import Iterator, List, TYPE_CHECKING

if TYPE_CHECKING:
    import ray


@DeveloperAPI
class DynamicObjectRefGenerator:
    def __init__(self, refs: List["ray.ObjectRef"]):
        # TODO(swang): As an optimization, can also store the generator
        # ObjectID so that we don't need to keep individual ref counts for the
        # inner ObjectRefs.
        self._refs: List["ray.ObjectRef"] = refs

    def __iter__(self) -> Iterator("ray.ObjectRef"):
        for ref in self._refs:
            yield ref

    def __len__(self) -> int:
        return len(self._refs)
