from abc import ABC, abstractmethod
from typing import ContextManager, Iterator, Optional

from .execution_options import ExecutionOptions
from .physical_operator import PhysicalOperator
from .ref_bundle import RefBundle
from ray.data._internal.stats import DatasetStats


class OutputIterator(Iterator[RefBundle], ABC):
    """Iterator used to access the output of an Executor execution.

    This is a blocking iterator. Datasets guarantees that all its iterators are
    thread-safe (i.e., multiple threads can block on them at the same time).
    """

    @abstractmethod
    def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
        """Can be used to pull outputs by a specified output index.

        This is used to support the streaming_split() API, where the output of a
        streaming execution is to be consumed by multiple processes.

        Args:
            output_split_idx: The output split index to get results for. This arg is
                only allowed for iterators created by `Dataset.streaming_split()`.

        Raises:
            StopIteration: If there are no more outputs to return.
        """
        ...

    def __next__(self) -> RefBundle:
        return self.get_next()


class Executor(ContextManager, ABC):
    """Abstract class for executors, which implement physical operator execution.

    Subclasses:
        StreamingExecutor
    """

    def __init__(self, options: ExecutionOptions):
        """Create the executor."""
        options.validate()
        self._options = options

    @abstractmethod
    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> OutputIterator:
        """Start execution.

        Args:
            dag: The operator graph to execute.
            initial_stats: The DatasetStats to prepend to the stats returned by the
                executor. These stats represent actions done to compute inputs.
        """
        ...

    def shutdown(self, force: bool, exception: Optional[Exception] = None):
        """Shutdown an executor, which may still be running.

        This should interrupt execution and clean up any used resources.

        Args:
            force: Controls whether shutdown should forcefully terminate all execution
                   activity (making sure that upon returning from this method all
                   activities are stopped). When force=False, some activities could be
                   terminated asynchronously (ie this method won't provide guarantee
                   that they stop executing before returning from this method)
            exception: The exception that causes the executor to shut down, or None if
                the executor finishes successfully.
        """
        pass

    @abstractmethod
    def get_stats(self) -> DatasetStats:
        """Return stats for the execution so far.

        This is generally called after `execute` has completed, but may be called
        while iterating over `execute` results for streaming execution.
        """
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback, /):
        # NOTE: ``ContextManager`` semantic must guarantee that executor
        #       fully shutdown upon returning from this method
        self.shutdown(force=True, exception=exc_value)
