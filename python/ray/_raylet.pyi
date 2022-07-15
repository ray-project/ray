from typing import Any, Awaitable, TypeVar

R = TypeVar("R")


class ObjectRef(Awaitable[R]):
    pass


class ObjectID(Awaitable[R]):
    pass
