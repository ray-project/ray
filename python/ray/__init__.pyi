from typing import TypeVar, Generic, Callable, List, Any, Dict, Union
T = TypeVar('T')


class ObjectID(Generic[T]):
    pass


def get(inp: ObjectID[T]) -> T:
    pass


def put(inp: T) -> ObjectID[T]:
    pass


F = TypeVar('F', bound=Callable[..., Any])

Y = TypeVar('Y')
Z = TypeVar('Z')


class RemoteFunction(Generic[Y], Generic[Z]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    # def remote(self, a: F[0], b: Dict[Any]) -> ObjectID[Z]:
    # def remote(self, a: Y, b: List[..., Any], c: Dict[None, Any]) -> ObjectID[Z]:
    #  *args: Any, **kwargs: Any
    # def remote(self, a: Union(Y, ObjectID[Y])) -> ObjectID[Z]:
    def remote(self, a: Union[Y, ObjectID[Y]]) -> ObjectID[Z]:
        pass


# @overload
def remote(func: Callable[[Y], Z]) -> RemoteFunction[Y, Z]:
    pass


# @overload
# def remote(func: Callable[[Y], Z]) -> RemoteFunction[Y, Z]:
#     pass