import sys
from typing import TypeVar, List, Generic, Any

# TODO(ekl) how do we express ObjectRef[Block]?
BlockRef = List
T = TypeVar("T")


class Block(Generic[T]):
    def __init__(self, items: List[Any]):
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError

    def __len__(self):
        raise NotImplementedError

    def size_bytes(self):
        raise NotImplementedError


class ListBlock(Block):
    def __init__(self, items: List[Any]):
        self._items = items

    def __iter__(self):
        return self._items.__iter__()

    def __len__(self):
        return len(self._items)

    def size_bytes(self):
        # TODO
        return sys.getsizeof(self._items)
