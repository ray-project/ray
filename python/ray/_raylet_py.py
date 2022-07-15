from typing import Awaitable, TypeVar
from ._raylet import ObjectRef as _CyObjectRef
from ._raylet import ObjectID as _CyObjectID

T = TypeVar("T")


class ObjectRef(_CyObjectRef, Awaitable[T]):
    def __init__(self, __value: T) -> None:
        super().__init__(__value)  # type: ignore


class ObjectID(_CyObjectID, Awaitable[T]):
    def __init__(self, __value: T) -> None:
        super().__init__(__value)  # type: ignore
