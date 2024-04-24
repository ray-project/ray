from typing import Iterable, Iterator, Optional

from .execution_options import ExecutionOptions
from .physical_operator import PhysicalOperator
from .ref_bundle import RefBundle
from ray.data._internal.stats import DatasetStats


class OutputIterator(Iterator[RefBundle]):
    """Iterator used to access the output of an Executor execution.

    This is a blocking iterator. Datasets guarantees that all its iterators are
    thread-safe (i.e., multiple threads can block on them at the same time).
    """

    def __init__(self, base: Iterable[RefBundle]):
        self._it = iter(base)

    def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
        """Can be used to pull outputs by a specified output index.

        This is used to support the streaming_split() API, where the output of a
        streaming execution is to be consumed by multiple processes.

        Args:
            output_split_idx: The output split index to get results for. This arg is
                only allowed for iterators created by `Dataset.streaming_split()`.

        Raises:
            StopIteration if there are no more outputs to return.
        """
        if output_split_idx is not None:
            raise NotImplementedError()
        return next(self._it)

    def __next__(self) -> RefBundle:
        return self.get_next()


class Executor:
    """Abstract class for executors, which implement physical operator execution.

    Subclasses:
        StreamingExecutor
    """

    def __init__(self, options: ExecutionOptions):
        """Create the executor."""
        options.validate()
        self._options = options

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> OutputIterator:
        """Start execution.

        Args:
            dag: The operator graph to execute.
            initial_stats: The DatasetStats to prepend to the stats returned by the
                executor. These stats represent actions done to compute inputs.
        """
        raise NotImplementedError

    def shutdown(self):
        """Shutdown an executor, which may still be running.

        This should interrupt execution and clean up any used resources.
        """
        pass

    def get_stats(self) -> DatasetStats:
        """Return stats for the execution so far.

        This is generally called after `execute` has completed, but may be called
        while iterating over `execute` results for streaming execution.
        """
        raise NotImplementedError
