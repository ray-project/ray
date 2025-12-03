import abc
from typing import List, Optional

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import FIFOBundleQueue
from ray.data._internal.execution.bundle_queue.base import BaseBundleQueue
from ray.data._internal.execution.interfaces import (
    AllToAllTransformFn,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.physical_operator import _create_sub_pb
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext


class InternalQueueOperatorMixin(PhysicalOperator, abc.ABC):
    @property
    @abc.abstractmethod
    def input_buffers(self) -> List["BaseBundleQueue"]:
        """Return all the internal input buffer queues for this operator."""
        ...

    @property
    @abc.abstractmethod
    def output_buffers(self) -> List["BaseBundleQueue"]:
        """Return all the internal output buffer queues for this operator."""
        ...

    def internal_input_queue_num_blocks(self) -> int:
        """Returns Operator's internal input queue size (in blocks)"""
        return sum(input_buffer.num_blocks() for input_buffer in self.input_buffers)

    def internal_input_queue_num_bytes(self) -> int:
        """Returns Operator's internal input queue size (in bytes)"""
        return sum(
            input_buffer.estimate_size_bytes() for input_buffer in self.input_buffers
        )

    def internal_output_queue_num_blocks(self) -> int:
        """Returns Operator's internal output queue size (in blocks)"""
        return sum(output_buffer.num_blocks() for output_buffer in self.output_buffers)

    def internal_output_queue_num_bytes(self) -> int:
        """Returns Operator's internal output queue size (in bytes)"""
        return sum(
            output_buffer.estimate_size_bytes() for output_buffer in self.output_buffers
        )

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queue(s)."""
        for input_buffer in self.input_buffers:
            input_buffer.clear()

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue(s)."""
        for output_buffer in self.output_buffers:
            output_buffer.clear()

    def mark_execution_finished(self) -> None:
        """Mark execution as finished and clear internal queues.

        This default implementation calls the parent's mark_execution_finished()
        and then clears internal input and output queues.
        """
        super().mark_execution_finished()
        self.clear_internal_input_queue()
        self.clear_internal_output_queue()


class OneToOneOperator(PhysicalOperator):
    """An operator that has one input and one output dependency.

    This operator serves as the base for map, filter, limit, etc.
    """

    def __init__(
        self,
        name: str,
        input_op: PhysicalOperator,
        data_context: DataContext,
        target_max_block_size_override: Optional[int] = None,
    ):
        """Create a OneToOneOperator.
        Args:
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            target_max_block_size_override: The target maximum number of bytes to
                include in an output block.
        """
        super().__init__(name, [input_op], data_context, target_max_block_size_override)

    @property
    def input_dependency(self) -> PhysicalOperator:
        return self.input_dependencies[0]


class AllToAllOperator(
    InternalQueueOperatorMixin, SubProgressBarMixin, PhysicalOperator
):
    """A blocking operator that executes once its inputs are complete.

    This operator implements distributed sort / shuffle operations, etc.
    """

    def __init__(
        self,
        bulk_fn: AllToAllTransformFn,
        input_op: PhysicalOperator,
        data_context: DataContext,
        target_max_block_size_override: Optional[int] = None,
        num_outputs: Optional[int] = None,
        sub_progress_bar_names: Optional[List[str]] = None,
        name: str = "AllToAll",
    ):
        """Create an AllToAllOperator.
        Args:
            bulk_fn: The blocking transformation function to run. The inputs are the
                list of input ref bundles, and the outputs are the output ref bundles
                and a stats dict.
            input_op: Operator generating input data for this op.
            data_context: The DataContext instance containing configuration settings.
            target_max_block_size_override: The target maximum number of bytes to
                include in an output block.
            num_outputs: The number of expected output bundles for progress bar.
            sub_progress_bar_names: The names of internal sub progress bars.
            name: The name of this operator.
        """
        self._bulk_fn = bulk_fn
        self._next_task_index = 0
        self._num_outputs = num_outputs
        self._output_rows = 0
        self._sub_progress_bar_names = sub_progress_bar_names
        self._sub_progress_bar_dict = None
        self._input_buffer: FIFOBundleQueue = FIFOBundleQueue()
        self._output_buffer: FIFOBundleQueue = FIFOBundleQueue()
        self._stats: StatsDict = {}
        super().__init__(name, [input_op], data_context, target_max_block_size_override)

    @property
    @override
    def input_buffers(self) -> List["BaseBundleQueue"]:
        return [self._input_buffer]

    @property
    @override
    def output_buffers(self) -> List["BaseBundleQueue"]:
        return [self._output_buffer]

    def num_outputs_total(self) -> Optional[int]:
        return (
            self._num_outputs
            if self._num_outputs
            else self.input_dependencies[0].num_outputs_total()
        )

    def num_output_rows_total(self) -> Optional[int]:
        return (
            self._output_rows
            if self._output_rows
            else self.input_dependencies[0].num_output_rows_total()
        )

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert input_index == 0, input_index
        self._input_buffer.add(refs)
        self._metrics.on_input_queued(refs)

    def all_inputs_done(self) -> None:
        ctx = TaskContext(
            task_idx=self._next_task_index,
            op_name=self.name,
            sub_progress_bar_dict=self._sub_progress_bar_dict,
            target_max_block_size_override=self.target_max_block_size_override,
        )
        # NOTE: We don't account object store memory use from intermediate `bulk_fn`
        # outputs (e.g., map outputs for map-reduce).
        output_buffer, self._stats = self._bulk_fn(self._input_buffer.to_list(), ctx)
        self._output_buffer = FIFOBundleQueue(output_buffer)

        while self._input_buffer:
            refs = self._input_buffer.get_last()
            self._metrics.on_input_dequeued(refs)

        for ref in self._output_buffer:
            self._metrics.on_output_queued(ref)

        self._next_task_index += 1

        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle = self._output_buffer.get_next()
        self._metrics.on_output_dequeued(bundle)
        self._output_rows += bundle.num_rows()
        return bundle

    def get_stats(self) -> StatsDict:
        return self._stats

    def get_transformation_fn(self) -> AllToAllTransformFn:
        return self._bulk_fn

    def progress_str(self) -> str:
        return f"{self.num_output_rows_total() or 0} rows output"

    def initialize_sub_progress_bars(self, position: int) -> int:
        """Initialize all internal sub progress bars, and return the number of bars."""
        if self._sub_progress_bar_names is not None:
            self._sub_progress_bar_dict = {}
            for name in self._sub_progress_bar_names:
                bar, position = _create_sub_pb(
                    name, self.num_output_rows_total(), position
                )
                self._sub_progress_bar_dict[name] = bar
            return len(self._sub_progress_bar_dict)
        else:
            return 0

    def close_sub_progress_bars(self):
        """Close all internal sub progress bars."""
        if self._sub_progress_bar_dict is not None:
            for sub_bar in self._sub_progress_bar_dict.values():
                sub_bar.close()

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return self._sub_progress_bar_names

    def set_sub_progress_bar(self, name, pg):
        # not type-checking due to circular imports
        if self._sub_progress_bar_dict is None:
            self._sub_progress_bar_dict = {}
        self._sub_progress_bar_dict[name] = pg

    def supports_fusion(self):
        return True

    def implements_accurate_memory_accounting(self):
        return True


class NAryOperator(PhysicalOperator):
    """An operator that has multiple input dependencies and one output.

    This operator serves as the base for union, zip, etc.
    """

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: LogicalOperator,
    ):
        """Create a OneToOneOperator.
        Args:
            input_op: Operator generating input data for this op.
            name: The name of this operator.
        """
        input_names = ", ".join([op._name for op in input_ops])
        op_name = f"{self.__class__.__name__}({input_names})"
        super().__init__(
            op_name,
            list(input_ops),
            data_context,
        )
