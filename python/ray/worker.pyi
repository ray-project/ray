from typing import TypeVar, Generic, Callable, List, Any, Dict, Union
from ray import ObjectID

T = TypeVar('T')


def get(inp: ObjectID[T]) -> T:
    pass


def put(inp: T) -> ObjectID[T]:
    pass


Y = TypeVar('Y')
Z = TypeVar('Z')


class RemoteFunction(Y, Z):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def remote(self, a: Union[Y, ObjectID[Y]]) -> ObjectID[Z]:
        pass


def remote(func: Callable[[Y], Z]) -> RemoteFunction[Y, Z]:
    pass