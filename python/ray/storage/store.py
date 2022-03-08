from dataclasses import dataclass
from typing import Dict, Union, Optional, Any, List

from ray.types import ObjectRef


@dataclass
class ObjectEntry:
    path: str
    size: int


def save_objects(
    namespace: str,
    objects: Dict[str, Union[Any, ObjectRef]],
    overwrite_if_exists: bool = True,
):
    raise NotImplementedError


def delete_objects(namespace: str, paths: List[str]):
    raise NotImplementedError


def get_object_info(namespace: str, paths: List[str]) -> List[Optional[ObjectEntry]]:
    raise NotImplementedError


def list_objects(
    namespace: str, prefix: str, limit: Optional[int] = None
) -> List[ObjectEntry]:
    raise NotImplementedError


def load_objects(namespace: str, paths: List[str]) -> List[ObjectRef]:
    raise NotImplementedError
