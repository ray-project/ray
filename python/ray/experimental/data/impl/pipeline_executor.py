from typing import Any, Callable, List, Iterator, Generic, Union, TYPE_CHECKING

import ray
from ray.experimental.data.dataset import Dataset, T, U, BatchType
from ray.experimental.data.impl.progress_bar import ProgressBar, \
    set_progress_bars
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.experimental.data.dataset_pipeline import DatasetPipeline


@ray.remote
def do(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
    try:
        prev = set_progress_bars(False)
        return fn()
    finally:
        set_progress_bars(prev)



class PipelineExecutor:
    def __init__(self, pipeline: "DatasetPipeline[T]"):
        self._pipeline: "DatasetPipeline[T]" = pipeline
        self._stages: List[ObjectRef[Dataset[Any]]] = [None] * (
            len(self._pipeline._stage_transforms) + 1)
        self._stages[0] = do.remote(
            next(self._pipeline._base_iterator))
        self._bars = [
            ProgressBar(
                "Stage {}".format(i),
                self._pipeline._length or 1,
                position=i) for i in range(len(self._stages))
        ]

    def __iter__(self):
        return self

    def __next__(self):
        output = None
        ready = []

        while output is None:
            if all(s is None for s in self._stages):
                raise StopIteration

            # Wait for any completed stages.
            pending = [s for s in self._stages if s is not None]
            ready, _ = ray.wait(
                pending, timeout=0.1, num_returns=len(pending))

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
                self._bars[i].update(1)
                self._stages[i] = None
                if is_last:
                    output = result
                else:
                    fn = self._pipeline._stage_transforms[i]
                    self._stages[i + 1] = do.remote(lambda: fn(result))

            # Pull a new element for the initial slot if possible.
            if self._stages[0] is None:
                try:
                    self._stages[0] = do.remote(
                        next(self._pipeline._base_iterator))
                except StopIteration:
                    pass

        return output
