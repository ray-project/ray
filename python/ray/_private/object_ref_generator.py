from __future__ import annotations

import collections
from typing import TYPE_CHECKING, Deque, Iterator

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import ray


@DeveloperAPI
class DynamicObjectRefGenerator:
    def __init__(self, refs: Deque["ray.ObjectRef"]):
        # TODO(swang): As an optimization, can also store the generator
        # ObjectID so that we don't need to keep individual ref counts for the
        # inner ObjectRefs.
        self._refs: Deque["ray.ObjectRef"] = collections.deque(refs)

    def __iter__(self) -> Iterator("ray.ObjectRef"):
        while self._refs:
            yield self._refs.popleft()

    def __len__(self) -> int:
        return len(self._refs)
