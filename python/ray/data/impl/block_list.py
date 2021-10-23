import math
from typing import List, Iterator, Tuple

import numpy as np

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockMetadata


class BlockList:
    """A list of blocks that may be computed or pending computation.

    The BlockList tracks a set of tasks that produce List[Block] object refs
    as output. The set of tasks may be accessed via ``.iter_tasks()``. Each
    task produces a list of one or more Blocks, the flat list of which can be
    accessed via ``.iter_output_blocks()``. Note that the number of outputs may
    be greater than the number of tasks, if some tasks return multiple objects.
    """

    def __init__(self, blocks: List[ObjectRef[List[Block]]],
                 metadata: List[BlockMetadata]):
        assert len(blocks) == len(metadata), (blocks, metadata)
        self._block_futures: List[ObjectRef[List[Block]]] = blocks
        self._num_tasks = len(self._block_futures)
        self._metadata = metadata

    def set_metadata(self, i: int, metadata: BlockMetadata) -> None:
        self._metadata[i] = metadata

    def get_metadata(self) -> List[BlockMetadata]:
        return self._metadata.copy()

    def copy(self) -> "BlockList":
        return BlockList(self._block_futures, self._metadata)

    def clear(self):
        self._block_futures = None

    def _check_if_cleared(self):
        if self._block_futures is None:
            raise ValueError(
                "This Dataset's blocks have been moved, which means that you "
                "can no longer use this Dataset.")

    def split(self, split_size: int) -> List["BlockList"]:
        self._check_if_cleared()
        num_splits = math.ceil(len(self._block_futures) / split_size)
        blocks = np.array_split(self._block_futures, num_splits)
        meta = np.array_split(self._metadata, num_splits)
        output = []
        for b, m in zip(blocks, meta):
            output.append(BlockList(b.tolist(), m.tolist()))
        return output

    def divide(self, block_idx: int) -> ("BlockList", "BlockList"):
        self._check_if_cleared()
        return (BlockList(self._block_futures[:block_idx],
                          self._metadata[:block_idx]),
                BlockList(self._block_futures[block_idx:],
                          self._metadata[block_idx:]))

    def iter_tasks(self) -> Iterator[ObjectRef[List[Block]]]:
        return iter(self._block_futures)

    def iter_output_blocks_with_orig_metadata(
            self) -> Iterator[Tuple[ObjectRef[Block], BlockMetadata, int]]:
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = zip(outer.iter_tasks(), outer._metadata)
                self._buffer = []

            def __iter__(self):
                return self

            def __next__(self):
                while not self._buffer:
                    refs, orig_meta = next(self._base_iter)
                    refs = ray.get(refs)
                    for ref in refs:
                        if not isinstance(ref, ray.ObjectRef):
                            raise AssertionError(
                                "Expected list of block refs, but got a list "
                                "of raw blocks elements: {}. Probably a "
                                "task returned a block ref directly instead "
                                "of [ray.put(block)].".format(refs))
                        self._buffer.append((ref, orig_meta, len(refs)))
                return self._buffer.pop(0)

        return Iter()

    def iter_output_blocks(self) -> Iterator[ObjectRef[Block]]:
        self._check_if_cleared()
        outer = self

        class Iter:
            def __init__(self):
                self._base_iter = outer.iter_output_blocks_with_orig_metadata()

            def __iter__(self):
                return self

            def __next__(self):
                ref, meta, n = next(self._base_iter)
                assert isinstance(ref, ray.ObjectRef), (ref, meta, n)
                return ref

        return Iter()

    def num_tasks(self) -> int:
        return self._num_tasks

    def num_output_blocks(self) -> int:
        self._check_if_cleared()
        return len(list(self.iter_output_blocks()))
