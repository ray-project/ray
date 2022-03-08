from typing import List

from ray.types import ObjectRef


def checkpoint(refs: List[ObjectRef]) -> None:
    raise NotImplementedError
