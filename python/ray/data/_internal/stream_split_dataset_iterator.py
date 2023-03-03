from typing import List, Literal, Optional, Iterator, Callable, Any
import time

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
        coord_actor.start_processing.remote()
        return [StreamSplitDatasetIterator(coord_actor, i) for i in range(n)]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx

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
        while True:
            blocks = ray.get(self._coord_actor.get(self._output_split_idx))
            if not blocks:
                break
            else:
                yield from blocks


@ray.remote(num_cpus=0)
class SplitCoordinator:
    def __init__(
        self,
        dataset: Dataset,
        n: int,
        equal: bool,
        locality_hints: Optional[List[ray.actor.ActorHandle]],
    ):
        self._base_dataset = dataset
        self._n = n
        self._equal = equal
        self._locality_hints = locality_hints
        self._outboxes = [[] for _ in range(n)]
        self._finished = False

    async def start_processing(self) -> None:
        try:
            print("START PROCESSING LOOP")
            ds = self._base_dataset
            block_iterator, stats, executor = ds._plan.execute_to_iterator()
            # TODO: backpressure???
            for block in block_iterator:
                self._outboxes[block.output_split_idx].append(block)
            print("END PROCESSING LOOP")
        finally:
            self._finished = True

    async def get(self, output_split_idx: int) -> List[ObjectRef[Block]]:
        result = []
        outbox = self._outboxes[output_split_idx]
        while not self._finished:
            while outbox:
                result.append(outbox.pop(0))
            if result:
                return result
            time.sleep(0.1)  # Polling loop.
        return []  # End of stream.
