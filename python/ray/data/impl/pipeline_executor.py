from typing import Any, Callable, List, Optional, TYPE_CHECKING
import time

import ray
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, T
from ray.data.impl.progress_bar import ProgressBar, set_progress_bars
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data.dataset_pipeline import DatasetPipeline


# Temporarily use an actor here to avoid ownership issues with tasks:
# https://github.com/ray-project/ray/issues/20554
@ray.remote(num_cpus=0, placement_group=None)
class _StageRunner:
    def run(self, fn: Callable[[], Dataset[T]], context: DatasetContext) -> Dataset[T]:
        DatasetContext._set_current(context)
        try:
            prev = set_progress_bars(False)
            # Force eager evaluation of all blocks in the pipeline stage. This
            # prevents resource deadlocks due to overlapping stage execution
            # (e.g., task -> actor stage).
            return fn().force_reads()
        finally:
            set_progress_bars(prev)


class PipelineExecutor:
    def __init__(self, pipeline: "DatasetPipeline[T]"):
        self._pipeline: "DatasetPipeline[T]" = pipeline
        self._stages: List[ObjectRef[Dataset[Any]]] = [None] * (
            len(self._pipeline._stages) + 1
        )
        self._stage_runners = [_StageRunner.remote() for _ in self._stages]
        self._iter = iter(self._pipeline._base_iterable)
        self._stages[0] = self._stage_runners[0].run.remote(
            next(self._iter), DatasetContext.get_current()
        )

        if self._pipeline._length and self._pipeline._length != float("inf"):
            length = self._pipeline._length
        else:
            length = 1

        if self._pipeline._progress_bars:
            self._bars = [
                ProgressBar("Stage {}".format(i), length, position=i)
                for i in range(len(self._stages))
            ]
        else:
            self._bars = None

    def __iter__(self):
        return self

    def __next__(self):
        output = None
        start = time.perf_counter()

        while output is None:
            if all(s is None for s in self._stages):
                raise StopIteration

            # Wait for any completed stages.
            pending = [s for s in self._stages if s is not None]
            ready, _ = ray.wait(pending, timeout=0.1, num_returns=len(pending))

            # Bubble elements down the pipeline as they become ready.
            for i in range(len(self._stages))[::-1]:
                is_last = i + 1 >= len(self._stages)
                next_slot_free = is_last or self._stages[i + 1] is None
                if not next_slot_free:
                    continue

                slot_ready = self._stages[i] in ready
                if not slot_ready:
                    continue

                # Bubble.
                result = ray.get(self._stages[i])
                if self._bars:
                    self._bars[i].update(1)
                self._stages[i] = None
                if is_last:
                    output = result
                else:
                    fn = self._pipeline._stages[i]
                    self._stages[i + 1] = self._stage_runners[i].run.remote(
                        lambda: fn(result), DatasetContext.get_current()
                    )

            # Pull a new element for the initial slot if possible.
            if self._stages[0] is None:
                try:
                    self._stages[0] = self._stage_runners[0].run.remote(
                        next(self._iter), DatasetContext.get_current()
                    )
                except StopIteration:
                    pass

        self._pipeline._stats.wait_time_s.append(time.perf_counter() - start)
        self._pipeline._stats.add(output._stats)
        return output


@ray.remote(num_cpus=0, placement_group=None)
class PipelineSplitExecutorCoordinator:
    def __init__(
        self,
        pipeline: "DatasetPipeline[T]",
        n: int,
        splitter: Callable[[Dataset], "DatasetPipeline[T]"],
        context: DatasetContext,
    ):
        DatasetContext._set_current(context)
        self.executor = PipelineExecutor(pipeline)
        self.n = n
        self.splitter = splitter
        self.cur_splits = [None] * self.n

    def next_dataset_if_ready(self, split_index: int) -> Optional[Dataset[T]]:
        # Pull the next dataset once all splits are fully consumed.
        if all(s is None for s in self.cur_splits):
            ds = next(self.executor)
            self.cur_splits = self.splitter(ds)

        # Return the dataset at the split index once per split.
        ret = self.cur_splits[split_index]
        self.cur_splits[split_index] = None
        return ret

    def get_stats(self):
        return self.executor._pipeline._stats
