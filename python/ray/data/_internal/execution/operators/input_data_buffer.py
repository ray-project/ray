import time
from typing import Callable, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class InputDataBuffer(PhysicalOperator):
    """Defines the input data for the operator DAG.

    For example, this may hold cached blocks from a previous Dataset execution, or
    the arguments for read tasks.
    """

    def __init__(
        self,
        data_context: DataContext,
        input_data: Optional[List[RefBundle]] = None,
        input_data_factory: Optional[Callable[[int], List[RefBundle]]] = None,
        streaming: bool = False,
        polling_new_tasks_interval_s: Optional[float] = None,
    ):
        """Create an InputDataBuffer.

        Args:
            data_context: :class:`~ray.data.context.DataContext`
                object to use injestion.
            input_data: The list of bundles to output from this operator.
            input_data_factory: The factory to get input data, if input_data is None.
            streaming: Whether the input data factory is streaming.
            polling_new_tasks_interval_s: Optional metadata fetch interval in seconds for
                micro-batching. If None, falls back to DataContext.polling_new_tasks_interval_s,
                or defaults to 5 seconds if not set in context.
        """
        super().__init__("Input", [], data_context)
        self._streaming = streaming
        self._input_data_index = 0
        # Track last fetch time for micro-batching with controlled intervals
        self._last_fetch_time = None
        self._polling_new_tasks_interval_s = polling_new_tasks_interval_s
        # Initialize metadata fields before _initialize_metadata() may be called
        self._estimated_num_output_bundles = None
        self._estimated_num_output_rows = None
        self._stats = None

        if input_data is not None:
            assert input_data_factory is None
            # Copy the input data to avoid mutating the original list.
            self._input_data = input_data[:]
            # TODO(youcheng): We might need to clear the input data list after it is used it is possible that the input data list will become very large.
            self._is_input_initialized = True
            self._initialize_metadata(self._input_data)
        else:
            # Initialize input lazily when execution is started.
            assert input_data_factory is not None
            self._input_data_factory = input_data_factory
            self._input_data = None
            self._is_input_initialized = False
        # Only mark as finished if we have input_data directly (non-streaming case).
        # For streaming sources with factory, we don't mark it finished since
        # streaming datasources are long-running and never finish.
        if input_data is not None:
            self.mark_execution_finished()

    def start(self, options: ExecutionOptions) -> None:
        if not self._is_input_initialized:
            self._input_data = self._input_data_factory(
                self.target_max_block_size_override
                or self.data_context.target_max_block_size
            )
            self._is_input_initialized = True
            self._initialize_metadata(self._input_data)
            # InputDataBuffer does not take inputs from other operators,
            # so we record input metrics here
            for bundle in self._input_data:
                self._metrics.on_input_received(bundle)

            if self._streaming:
                self._last_fetch_time = time.time()
            else:
                self.mark_execution_finished()
        super().start(options)

    def has_next(self) -> bool:
        if not self._is_input_initialized or self._input_data is None:
            return False

        # For streaming sources with micro-batching, periodically fetch new data
        if (
            self._streaming
            and self._polling_new_tasks_interval_s is not None
            and self._last_fetch_time is not None
        ):
            current_time = time.time()
            time_since_last_fetch = current_time - self._last_fetch_time

            if time_since_last_fetch >= self._polling_new_tasks_interval_s:
                new_bundles = self._input_data_factory(
                    self.target_max_block_size_override
                    or self.data_context.target_max_block_size
                )
                if new_bundles:
                    self._initialize_metadata(new_bundles)
                    self._input_data.extend(new_bundles)
                    for bundle in new_bundles:
                        self._metrics.on_input_received(bundle)
                self._last_fetch_time = current_time

            return self._input_data_index < len(self._input_data)

        return self._input_data_index < len(self._input_data)

    def _get_next_inner(self) -> RefBundle:
        # We can't pop the input data. If we do, Ray might garbage collect the block
        # references, and Ray won't be able to reconstruct downstream objects.
        bundle = self._input_data[self._input_data_index]
        self._input_data_index += 1
        return bundle

    def has_execution_finished(self) -> bool:
        """Override to prevent auto-completion for streaming sources.

        The default implementation returns True when _inputs_complete=True and
        num_active_tasks()=0. For InputDataBuffer (a root operator with no
        dependencies), these conditions are always satisfied, which would cause
        streaming pipelines to terminate immediately.

        For streaming, we only return True when explicitly marked finished
        (e.g., when a downstream LimitOperator reaches its limit via take(N),
        or when the consumer stops pulling via iter_batches()).
        """
        if self._streaming:
            return self._is_execution_marked_finished
        return super().has_execution_finished()

    def get_stats(self) -> StatsDict:
        return {}

    def _add_input_inner(self, refs, input_index) -> None:
        raise ValueError("Inputs are not allowed for this operator.")

    def _initialize_metadata(self, bundles: List[RefBundle]):
        """Update metadata incrementally with the given bundles."""
        if self._estimated_num_output_bundles is None:
            self._estimated_num_output_bundles = len(bundles)
        else:
            self._estimated_num_output_bundles += len(bundles)

        block_metadata = []
        total_rows = 0
        for bundle in bundles:
            block_metadata.extend(bundle.metadata)
            bundle_num_rows = bundle.num_rows()
            if total_rows is not None and bundle_num_rows is not None:
                total_rows += bundle_num_rows
            else:
                # total row is unknown
                total_rows = None

        # Accumulate row count (only if both old and new are known)
        if total_rows:
            if self._estimated_num_output_rows is not None:
                self._estimated_num_output_rows += total_rows
            else:
                self._estimated_num_output_rows = total_rows

        # Extend existing stats rather than overwriting
        if self._stats:
            self._stats.setdefault("input", []).extend(block_metadata)
        else:
            self._stats = {"input": block_metadata}
