from typing import Any, Callable, List, Optional, TYPE_CHECKING

import ray
from ray.data.dataset import Dataset, T
from ray.data.impl.progress_bar import ProgressBar, \
    set_progress_bars
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data.dataset_pipeline import DatasetPipeline


@ray.remote
def pipeline_stage(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
    try:
        prev = set_progress_bars(False)
        return fn()
    finally:
        set_progress_bars(prev)


class PipelineExecutor:
    def __init__(self, pipeline: "DatasetPipeline[T]"):
        self._pipeline: "DatasetPipeline[T]" = pipeline
        self._stages: List[ObjectRef[Dataset[
            Any]]] = [None] * (len(self._pipeline._stages) + 1)
        self._iter = iter(self._pipeline._base_iterable)
        self._stages[0] = pipeline_stage.remote(next(self._iter))

        if self._pipeline._progress_bars:
            self._bars = [
                ProgressBar(
                    "Stage {}".format(i),
                    self._pipeline._length or 1,
                    position=i) for i in range(len(self._stages))
            ]
        else:
            self._bars = None

    def __iter__(self):
        return self

    def __next__(self):
        output = None

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
                    self._stages[i +
                                 1] = pipeline_stage.remote(lambda: fn(result))

            # Pull a new element for the initial slot if possible.
            if self._stages[0] is None:
                try:
                    self._stages[0] = pipeline_stage.remote(next(self._iter))
                except StopIteration:
                    pass

        return output


@ray.remote
class PipelineSplitExecutorCoordinator:
    def __init__(self, pipeline: "DatasetPipeline[T]", n: int, equal: bool,
                 locality_hints: List[Any]):
        self.executor = PipelineExecutor(pipeline)
        self.n = n
        self.locality_hints = locality_hints
        self.equal = equal
        self.cur_splits = [None] * self.n

    def next_dataset_if_ready(self, split_index: int) -> Optional[Dataset[T]]:
        # Pull the next dataset once all splits are fully consumed.
        if all(s is None for s in self.cur_splits):
            ds = next(self.executor)
            self.cur_splits = ds.split(
                self.n, equal=self.equal, locality_hints=self.locality_hints)

        # Return the dataset at the split index once per split.
        ret = self.cur_splits[split_index]
        self.cur_splits[split_index] = None
        return ret
