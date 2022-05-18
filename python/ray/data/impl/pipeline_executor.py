from typing import Any, Callable, List, Optional, TYPE_CHECKING
import time
import concurrent.futures
import logging

import ray
from ray.data.context import DatasetContext
from ray.data.dataset import Dataset, T
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl import progress_bar

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.data.dataset_pipeline import DatasetPipeline


def pipeline_stage(fn: Callable[[], Dataset[T]]) -> Dataset[T]:
    # Force eager evaluation of all blocks in the pipeline stage. This
    # prevents resource deadlocks due to overlapping stage execution (e.g.,
    # task -> actor stage).
    return fn().fully_executed(preserve_original=False)


class PipelineExecutor:
    def __init__(self, pipeline: "DatasetPipeline[T]"):
        self._pipeline: "DatasetPipeline[T]" = pipeline
        self._stages: List[concurrent.futures.Future[Dataset[Any]]] = [None] * (
            len(self._pipeline._optimized_stages) + 1
        )
        self._iter = iter(self._pipeline._base_iterable)
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._stages)
        )
        self._stages[0] = self._pool.submit(
            lambda n: pipeline_stage(n), next(self._iter)
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

    def __del__(self):
        for f in self._stages:
            if f is not None:
                f.cancel()
        self._pool.shutdown(wait=False)

        # Signal to all remaining threads to shut down.
        with progress_bar._canceled_threads_lock:
            for t in self._pool._threads:
                if t.is_alive():
                    progress_bar._canceled_threads.add(t)

        # Wait for 1s for all threads to shut down.
        start = time.time()
        while time.time() - start < 1:
            self._pool.shutdown(wait=False)
            if not [t for t in self._pool._threads if t.is_alive()]:
                break

        if [t for t in self._pool._threads if t.is_alive()]:
            logger.info(
                "Failed to shutdown all DatasetPipeline execution threads. "
                "These threads will be destroyed once all current stages "
                "complete or when the driver exits"
            )

    def __iter__(self):
        return self

    def __next__(self):
        output = None
        start = time.perf_counter()

        while output is None:
            if all(s is None for s in self._stages):
                raise StopIteration

            # Wait for any completed stages.
            pending = [f for f in self._stages if f is not None]
            ready, _ = concurrent.futures.wait(pending, timeout=0.1)

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
                result = self._stages[i].result()
                if self._bars:
                    self._bars[i].update(1)
                self._stages[i] = None
                if is_last:
                    output = result
                else:
                    self._stages[i + 1] = self._pool.submit(
                        lambda r, fn: pipeline_stage(lambda: fn(r)),
                        result,
                        self._pipeline._optimized_stages[i],
                    )

            # Pull a new element for the initial slot if possible.
            if self._stages[0] is None:
                try:
                    self._stages[0] = self._pool.submit(
                        lambda n: pipeline_stage(n), next(self._iter)
                    )
                except StopIteration:
                    pass

        self._pipeline._stats.wait_time_s.append(time.perf_counter() - start)
        self._pipeline._stats.add(output._plan.stats())
        return output


@ray.remote(num_cpus=0)
class PipelineSplitExecutorCoordinator:
    def __init__(
        self,
        pipeline: "DatasetPipeline[T]",
        n: int,
        splitter: Callable[[Dataset], "DatasetPipeline[T]"],
        context: DatasetContext,
    ):
        DatasetContext._set_current(context)
        pipeline._optimize_stages()
        self.executor = PipelineExecutor(pipeline)
        self.n = n
        self.splitter = splitter
        self.cur_splits = [None] * self.n

    def next_dataset_if_ready(self, split_index: int) -> Optional[Dataset[T]]:
        # TODO(swang): This will hang if one of the consumers fails and is
        # re-executed from the beginning. To make this fault-tolerant, we need
        # to make next_dataset_if_ready idempotent.
        # Pull the next dataset once all splits are fully consumed.
        if all(s is None for s in self.cur_splits):
            ds = next(self.executor)
            self.cur_splits = self.splitter(ds)
            assert len(self.cur_splits) == self.n, (self.cur_splits, self.n)

        # Return the dataset at the split index once per split.
        ret = self.cur_splits[split_index]
        self.cur_splits[split_index] = None
        return ret

    def get_stats(self):
        return self.executor._pipeline._stats
