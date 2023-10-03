from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Union

import ray
from .ref_bundle import RefBundle
from ray._raylet import StreamingObjectRefGenerator
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.logical.interfaces import Operator
from ray.data._internal.stats import StatsDict

# TODO(hchen): Ray Core should have a common interface for these two types.
Waitable = Union[ray.ObjectRef, StreamingObjectRefGenerator]


class OpTask(ABC):
    """Abstract class that represents a task that is created by an PhysicalOperator.

    The task can be either a regular task or an actor task.
    """

    @abstractmethod
    def get_waitable(self) -> Waitable:
        """Return the ObjectRef or StreamingObjectRefGenerator to wait on."""
        pass

    @abstractmethod
    def on_waitable_ready(self):
        """Called when the waitable is ready.

        This method may get called multiple times if the waitable is a
        streaming generator.
        """
        pass


class DataOpTask(OpTask):
    """Represents an OpTask that handles Block data."""

    def __init__(
        self,
        streaming_gen: StreamingObjectRefGenerator,
        output_ready_callback: Callable[[RefBundle], None],
        task_done_callback: Callable[[], None],
    ):
        """
        Args:
            streaming_gen: The streaming generator of this task. It should yield blocks.
            output_ready_callback: The callback to call when a new RefBundle is output
                from the generator.
            task_done_callback: The callback to call when the task is done.
        """
        # TODO(hchen): Right now, the streaming generator is required to yield a Block
        # and a BlockMetadata each time. We should unify task submission with an unified
        # interface. So each individual operator don't need to take care of the
        # BlockMetadata.
        self._streaming_gen = streaming_gen
        self._output_ready_callback = output_ready_callback
        self._task_done_callback = task_done_callback

    def get_waitable(self) -> StreamingObjectRefGenerator:
        return self._streaming_gen

    def on_waitable_ready(self):
        # Handle all the available outputs of the streaming generator.
        while True:
            try:
                block_ref = self._streaming_gen._next_sync(0)
                if block_ref.is_nil():
                    # The generator currently doesn't have new output.
                    # And it's not stopped yet.
                    return
            except StopIteration:
                self._task_done_callback()
                return

            try:
                meta = ray.get(next(self._streaming_gen))
            except StopIteration:
                # The generator should always yield 2 values (block and metadata)
                # each time. If we get a StopIteration here, it means an error
                # happened in the task.
                # And in this case, the block_ref is the exception object.
                # TODO(hchen): Ray Core should have a better interface for
                # detecting and obtaining the exception.
                ex = ray.get(block_ref)
                self._task_done_callback()
                raise ex
            self._output_ready_callback(
                RefBundle([(block_ref, meta)], owns_blocks=True)
            )


class MetadataOpTask(OpTask):
    """Represents an OpTask that only handles metadata, instead of Block data."""

    def __init__(
        self, object_ref: ray.ObjectRef, task_done_callback: Callable[[], None]
    ):
        """
        Args:
            object_ref: The ObjectRef of the task.
            task_done_callback: The callback to call when the task is done.
        """
        self._object_ref = object_ref
        self._task_done_callback = task_done_callback

    def get_waitable(self) -> ray.ObjectRef:
        return self._object_ref

    def on_waitable_ready(self):
        self._task_done_callback()


class PhysicalOperator(Operator):
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Physical operators are stateful and non-serializable; they live on the driver side
    of the Dataset only.

    Here's a simple example of implementing a basic "Map" operator:

        class MapOperator(PhysicalOperator):
            def __init__(self):
                self.active_tasks = []

            def add_input(self, refs, _):
                self.active_tasks.append(map_task.remote(refs))

            def has_next(self):
                ready, _ = ray.wait(self.active_tasks, timeout=0)
                return len(ready) > 0

            def get_next(self):
                ready, remaining = ray.wait(self.active_tasks, num_returns=1)
                self.active_tasks = remaining
                return ready[0]

    Note that the above operator fully supports both bulk and streaming execution,
    since `add_input` and `get_next` can be called in any order. In bulk execution,
    all inputs would be added up-front, but in streaming execution the calls could
    be interleaved.
    """

    def __init__(self, name: str, input_dependencies: List["PhysicalOperator"]):
        super().__init__(name, input_dependencies)
        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x
        self._inputs_complete = not input_dependencies
        self._dependents_complete = False
        self._started = False

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

    def completed(self) -> bool:
        """Return True when this operator is completed.

        An operator is completed if any of the following conditions are met:
        - All upstream operators are completed and all outputs are taken.
        - All downstream operators are completed.
        """
        return (
            self._inputs_complete
            and self.num_active_tasks() == 0
            and not self.has_next()
        ) or self._dependents_complete

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatasetStats."""
        raise NotImplementedError

    def get_metrics(self) -> Dict[str, int]:
        """Returns dict of metrics reported from this operator.

        These should be instant values that can be queried at any time, e.g.,
        obj_store_mem_allocated, obj_store_mem_freed.
        """
        return {}

    def progress_str(self) -> str:
        """Return any extra status to be displayed in the operator progress bar.

        For example, `<N> actors` to show current number of actors in an actor pool.
        """
        return ""

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator, if known.

        This is useful for reporting progress.
        """
        if len(self.input_dependencies) == 1:
            return self.input_dependencies[0].num_outputs_total()
        return None

    def start(self, options: ExecutionOptions) -> None:
        """Called by the executor when execution starts for an operator.

        Args:
            options: The global options used for the overall execution.
        """
        self._started = True

    def should_add_input(self) -> bool:
        """Return whether it is desirable to add input to this operator right now.

        Operators can customize the implementation of this method to apply additional
        backpressure (e.g., waiting for internal actors to be created).
        """
        return True

    def need_more_inputs(self) -> bool:
        """Return true if the operator still needs more inputs.

        Once this return false, it should never return true again.
        """
        return True

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        """Called when an upstream result is available.

        Inputs may be added in any order, and calls to `add_input` may be interleaved
        with calls to `get_next` / `has_next` to implement streaming execution.

        Args:
            refs: The ref bundle that should be added as input.
            input_index: The index identifying the input dependency producing the
                input. For most operators, this is always `0` since there is only
                one upstream input operator.
        """
        raise NotImplementedError

    def input_done(self, input_index: int) -> None:
        """Called when the upstream operator at index `input_index` has completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for the given input index.
        """
        pass

    def all_inputs_done(self) -> None:
        """Called when all upstream operators have completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for any input index.
        """
        self._inputs_complete = True

    def all_dependents_complete(self) -> None:
        """Called when all downstream operators have completed().

        After this is called, the operator is marked as completed.
        """
        self._dependents_complete = True

    def has_next(self) -> bool:
        """Returns when a downstream output is available.

        When this returns true, it is safe to call `get_next()`.
        """
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Get the next downstream output.

        It is only allowed to call this if `has_next()` has returned True.
        """
        raise NotImplementedError

    def get_active_tasks(self) -> List[OpTask]:
        """Get a list of the active tasks of this operator."""
        return []

    def num_active_tasks(self) -> int:
        """Return the number of active tasks.

        Subclasses can override this as a performance optimization.
        """
        return len(self.get_active_tasks())

    def throttling_disabled(self) -> bool:
        """Whether to disable resource throttling for this operator.

        This should return True for operators that only manipulate bundle metadata
        (e.g., the OutputSplitter operator). This hints to the execution engine that
        these operators should not be throttled based on resource usage.
        """
        return False

    def internal_queue_size(self) -> int:
        """If the operator has an internal input queue, return its size.

        This is used to report tasks pending submission to actor pools.
        """
        return 0

    def shutdown(self) -> None:
        """Abort execution and release all resources used by this operator.

        This release any Ray resources acquired by this operator such as active
        tasks, actors, and objects.
        """
        if not self._started:
            raise ValueError("Operator must be started before being shutdown.")

    def current_resource_usage(self) -> ExecutionResources:
        """Returns the current estimated resource usage of this operator.

        This method is called by the executor to decide how to allocate resources
        between different operators.
        """
        return ExecutionResources()

    def base_resource_usage(self) -> ExecutionResources:
        """Returns the minimum amount of resources required for execution.

        For example, an operator that creates an actor pool requiring 8 GPUs could
        return ExecutionResources(gpu=8) as its base usage.
        """
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        """Returns the incremental resources required for processing another input.

        For example, an operator that launches a task per input could return
        ExecutionResources(cpu=1) as its incremental usage.
        """
        return ExecutionResources()

    def notify_resource_usage(
        self, input_queue_size: int, under_resource_limits: bool
    ) -> None:
        """Called periodically by the executor.

        Args:
            input_queue_size: The number of inputs queued outside this operator.
            under_resource_limits: Whether this operator is under resource limits.
        """
        pass
