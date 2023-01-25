from typing import Any, Awaitable, TypeVar

R = TypeVar("R")


class ObjectRef(Awaitable[R]): # type: ignore
    pass


class ObjectID(Awaitable[R]): # type: ignore
    pass
