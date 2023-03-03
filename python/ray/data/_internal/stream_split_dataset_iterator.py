from typing import List, Literal, Optional, Iterator, Callable, Any

import ray
from ray.data import Dataset, DatasetIterator
from ray.data.block import Block, DataBatch
from ray.data._internal.block_batching import batch_block_refs
from ray.types import ObjectRef


class StreamSplitDatasetIterator(DatasetIterator):
    @staticmethod
    def create(
        self,
        base: Dataset,
        n: int,
        equal: bool,
        locality_hints: Optional[List[ray.actor.ActorHandle]],
    ) -> List["StreamSplitDatasetIterator"]:
        coord_actor = SplitCoordinator.remote(base, n, equal, locality_hints)
        return [StreamSplitDatasetIterator(coord_actor, i) for i in range(n)]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._coord_actor = coord_actor
        self.output_split_idx = output_split_idx

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 256,
        batch_format: Literal["default", "numpy", "pandas"] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], Any]] = None,
    ) -> Iterator[DataBatch]:

        block_iterator = self._gen_blocks()

        yield from batch_block_refs(
            block_iterator,
            stats=None,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            collate_fn=_collate_fn,
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )

    def _gen_blocks(self) -> Iterator[ObjectRef[Block]]:
        raise NotImplementedError


@ray.remote(num_cpus=0)
class SplitCoordinator:
    def __init__(
        self,
        base: Dataset,
        n: int,
        equal: bool,
        locality_hints: Optional[List[ray.actor.ActorHandle]],
    ):
        pass
