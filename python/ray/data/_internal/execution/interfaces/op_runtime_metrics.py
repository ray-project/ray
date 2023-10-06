from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_allocation


@dataclass
class ObjectStoreMetrics:
    """Metrics for object store memory allocations."""

    alloc: int = 0
    freed: int = 0
    cur: int = 0
    peak: int = 0
    spilled: int = 0

    def to_metrics_dict(self) -> Dict[str, int]:
        return {
            "obj_store_mem_alloc": self.alloc,
            "obj_store_mem_freed": self.freed,
            "obj_store_mem_peak": self.peak,
            "obj_store_mem_spilled": self.spilled,
        }


@dataclass
class RunningTaskInfo:
    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int


@dataclass
class OpRuntimeMetrics:
    """Runtime metrics for a PhysicalOperator."""

    # Object store metrics
    object_store: ObjectStoreMetrics = field(default_factory=ObjectStoreMetrics)

    # === Inputs-related metrics ===

    # Number of received input blocks.
    num_inputs_received: int = 0
    # Total size in bytes of received input blocks.
    bytes_inputs_received: int = 0

    # Number of processed input blocks.
    num_inputs_processed: int = 0
    # Total size in bytes of processed input blocks.
    bytes_inputs_processed: int = 0

    # === Outputs-related metrics ===

    # Number of generated output blocks.
    num_outputs_generated: int = 0
    # Total size in bytes of generated output blocks.
    bytes_outputs_generated: int = 0

    # Number of output blocks that are already taken by the downstream.
    num_outputs_taken: int = 0
    # Size in bytes of output blocks that are already taken by the downstream.
    bytes_outputs_taken: int = 0

    # Number of generated output blocks that are from finished tasks.
    num_outputs_of_finished_tasks: int = 0
    # Size in bytes of generated output blocks that are from finished tasks.
    bytes_outputs_of_finished_tasks: int = 0

    # === Tasks-related metrics ===

    # Number of submitted tasks.
    num_tasks_submitted: int = 0
    # Number of running tasks.
    num_tasks_running: int = 0
    # Number of tasks that have at least one output block.
    num_tasks_have_outputs: int = 0
    # Number of finished tasks.
    num_tasks_finished: int = 0

    # Keep track of running tasks.
    _running_tasks: Dict[int, RunningTaskInfo] = field(default_factory=dict)

    @property
    def average_num_outputs_per_task(self) -> Optional[float]:
        """Average number of output blocks per task, or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.num_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def average_bytes_outputs_per_task(self) -> Optional[float]:
        """Average size in bytes of output blocks per task, or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.bytes_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def input_buffer_bytes(self) -> int:
        """Size in bytes of input blocks that are not processed yet."""
        return self.bytes_inputs_received - self.bytes_inputs_processed

    @property
    def output_buffer_bytes(self) -> int:
        """Size in bytes of output blocks that are not taken by the downstream yet."""
        return self.bytes_outputs_generated - self.bytes_outputs_taken

    def on_input_received(self, input: RefBundle):
        self.num_inputs_received += 1
        input_size = input.size_bytes()
        self.bytes_inputs_received += input_size
        # Update object store metrics.
        self.object_store.cur += input_size
        if self.object_store.cur > self.object_store.peak:
            self.object_store.peak = self.object_store.cur

    def on_task_submitted(self, task_index: int, inputs: RefBundle):
        self.num_tasks_submitted += 1
        self.num_tasks_running += 1
        self._running_tasks[task_index] = RunningTaskInfo(inputs, 0, 0)

    def on_output_generated(self, task_index: int, output: RefBundle):
        num_outputs = len(output)
        output_bytes = output.size_bytes()

        self.num_outputs_generated += num_outputs
        self.bytes_outputs_generated += output_bytes

        task_info = self._running_tasks[task_index]
        if task_info.num_outputs == 0:
            self.num_tasks_have_outputs += 1
        task_info.num_outputs += num_outputs
        task_info.bytes_outputs += output_bytes

        # Update object store metrics.
        self.object_store.alloc += output_bytes
        self.object_store.cur += output_bytes
        if self.object_store.cur > self.object_store.peak:
            self.object_store.peak = self.object_store.cur

        for block_ref, _ in output.blocks:
            trace_allocation(block_ref, "operator_output")

    def on_task_finished(self, task_index: int):
        self.num_tasks_running -= 1
        self.num_tasks_finished += 1

        task_info = self._running_tasks[task_index]
        self.num_outputs_of_finished_tasks += task_info.num_outputs
        self.bytes_outputs_of_finished_tasks += task_info.bytes_outputs

        inputs = self._running_tasks[task_index].inputs
        self.num_inputs_processed += len(inputs)
        total_input_size = inputs.size_bytes()
        self.bytes_inputs_processed += total_input_size

        blocks = [input[0] for input in inputs.blocks]
        metadata = [input[1] for input in inputs.blocks]
        ctx = ray.data.context.DataContext.get_current()
        if ctx.enable_get_object_locations_for_metrics:
            locations = ray.experimental.get_object_locations(blocks)
        else:
            locations = {ref: {"did_spill": False} for ref in blocks}
        for block, meta in zip(blocks, metadata):
            if locations[block]["did_spill"]:
                assert meta.size_bytes != None
                self.object_store.spilled += meta.size_bytes

        self.object_store.freed += total_input_size
        self.object_store.cur -= total_input_size

        inputs.destroy_if_owned()
        del self._running_tasks[task_index]

    def on_output_taken(self, output: RefBundle):
        output_bytes = output.size_bytes()
        self.num_outputs_taken += 1
        self.bytes_outputs_taken += output_bytes
        self.object_store.cur -= output_bytes

