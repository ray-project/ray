from typing import Iterator, List, TypeVar, Dict, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas
    import pyarrow

from ray.data.block import Block, BlockAccessor
from ray.data.impl.block_builder import BlockBuilder

T = TypeVar("T")


# TODO(ekl) switch to pyarrow.Tensor as the block type; currently there is a
# serialization issue with pyarrow tensors.
class TensorBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._rows = []
        self._tensors: List[np.ndarray] = []
        self._num_rows = 0

    def add(self, row: np.ndarray) -> None:
        self._rows.append(row)
        self._num_rows += 1

    def add_block(self, block: np.ndarray) -> None:
        assert isinstance(block, np.ndarray), block
        self._tensors.append(block)
        self._num_rows += len(block)

    def build(self) -> Block:
        tensors = self._tensors.copy()
        if self._rows:
            tensors.append(np.stack(self._rows, axis=0))
        return np.concatenate(tensors, axis=0)

    def num_rows(self) -> int:
        return self._num_rows


class TensorBlockAccessor(BlockAccessor):
    def __init__(self, tensor: np.ndarray):
        self._tensor = tensor

    def iter_rows(self) -> Iterator[np.ndarray]:
        return iter(self._tensor)

    def slice(self, start: int, end: int,
              copy: bool) -> "TensorBlockAccessor[T]":
        view = self._tensor[start:end]
        if copy:
            view = view.copy()
        return view

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._tensor)

    def to_arrow(self) -> "pyarrow.Tensor":
        import pyarrow
        return pyarrow.Tensor.from_numpy(self._tensor)

    def schema(self) -> Dict:
        shape = self._tensor.shape
        shape = (None, ) + shape[1:]
        return {"shape": shape, "dtype": self._tensor.dtype.name}

    def num_rows(self) -> int:
        return len(self._tensor)

    def size_bytes(self) -> int:
        return self._tensor.nbytes

    @staticmethod
    def builder() -> TensorBlockBuilder[T]:
        return TensorBlockBuilder()
