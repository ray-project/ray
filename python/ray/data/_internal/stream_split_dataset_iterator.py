import asyncio
from collections import deque
import copy
import threading
from typing import (
    List,
    Literal,
    Optional,
    Iterator,
    Callable,
    Any,
    Union,
    TYPE_CHECKING,
)

import ray
from ray.data.dataset_iterator import DatasetIterator
from ray.data.block import Block, DataBatch
from ray.data.context import DatasetContext
from ray.data._internal.block_batching import batch_block_refs
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow
    from ray.data import Dataset


class StreamSplitDatasetIterator(DatasetIterator):
    @staticmethod
    def create(
        base_dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[ray.actor.ActorHandle]],
    ) -> List["StreamSplitDatasetIterator"]:
        coord_actor = SplitCoordinator.remote(base_dataset, n, equal, locality_hints)
        return [
            StreamSplitDatasetIterator(base_dataset, coord_actor, i) for i in range(n)
        ]

    def __init__(
        self,
        base_dataset: "Dataset",
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._base_dataset = base_dataset
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx

    def stats(self) -> str:
        return self._base_dataset.stats()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        return self._base_dataset.schema()

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
        future = self._coord_actor.get.remote(self._output_split_idx)
        while True:
            block = ray.get(future)
            if not block:
                break
            else:
                future = self._coord_actor.get.remote(self._output_split_idx)
                yield block


# TODO schedule on same node
@ray.remote(num_cpus=0)
class SplitCoordinator:
    def __init__(
        self,
        dataset: "Dataset",
        n: int,
        equal: bool,
        locality_hints: Optional[List[ray.actor.ActorHandle]],
    ):
        self._base_dataset = dataset
        self._n = n
        self._equal = equal
        self._locality_hints = locality_hints
        self._outboxes = [deque() for _ in range(n)]
        self._finished = False

        outer = self

        class Runner(threading.Thread):
            def run(self) -> None:
                from ray.data._internal.execution.streaming_executor import (
                    StreamingExecutor,
                )
                from ray.data._internal.execution.legacy_compat import (
                    execute_to_legacy_bundle_iterator,
                )

                ctx = DatasetContext.get_current()
                executor = StreamingExecutor(copy.deepcopy(ctx.execution_options))
                try:
                    print("START PROCESSING LOOP")
                    ds = outer._base_dataset

                    def add_split_op(dag):
                        return OutputSplitter(dag, outer._n, outer._equal)

                    bundle_iterator = execute_to_legacy_bundle_iterator(
                        executor,
                        ds._plan,
                        True,
                        ds._plan._dataset_uuid,
                        dag_rewrite=add_split_op,
                    )
                    for bundle in bundle_iterator:
                        for block, _ in bundle.blocks:
                            outer._outboxes[bundle.output_split_idx].append(block)
                    print("END PROCESSING LOOP")
                finally:
                    outer._finished = True

        self._runner = Runner()
        self._runner.start()

    async def get(self, output_split_idx: int) -> ObjectRef[Block]:
        outbox = self._outboxes[output_split_idx]
        while not self._finished:
            if outbox:
                return outbox.popleft()
            await asyncio.sleep(0.1)  # Polling loop.
        return None  # End of stream.
