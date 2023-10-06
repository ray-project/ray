from dataclasses import dataclass, field, fields
from typing import Dict, Optional

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_allocation


@dataclass
class RunningTaskInfo:
    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int


@dataclass
class OpRuntimeMetrics:
    """Runtime metrics for a PhysicalOperator.

    Metrics are updated dynamically during the execution of the Dataset.
    This class can be used for either observablity or scheduling purposes.
    """

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

    # === Object store memory metrics ===

    # Allocated memory size in the object store.
    obj_store_mem_alloc: int = 0
    # Freed memory size in the object store.
    obj_store_mem_freed: int = 0
    # Current memory size in the object store.
    obj_store_mem_cur: int = 0
    # Peak memory size in the object store.
    obj_store_mem_peak: int = 0
    # Spilled memory size in the object store.
    obj_store_mem_spilled: int = 0

    def as_dict(self):
        """Return a dict representation of the metrics."""
        result = []
        for f in fields(self):
            if f.name.startswith("_"):
                continue
            value = getattr(self, f.name)
            result.append((f.name, value))
        return dict(result)

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
        """Callback when the operator receives a new input."""
        self.num_inputs_received += 1
        input_size = input.size_bytes()
        self.bytes_inputs_received += input_size
        # Update object store metrics.
        self.obj_store_mem_cur += input_size
        if self.obj_store_mem_cur > self.obj_store_mem_peak:
            self.obj_store_mem_peak = self.obj_store_mem_cur

    def on_output_taken(self, output: RefBundle):
        """Callback when an output is taken from the operator."""
        output_bytes = output.size_bytes()
        self.num_outputs_taken += 1
        self.bytes_outputs_taken += output_bytes
        self.obj_store_mem_cur -= output_bytes

    def on_task_submitted(self, task_index: int, inputs: RefBundle):
        """Callback when the operator submits a task."""
        self.num_tasks_submitted += 1
        self.num_tasks_running += 1
        self._running_tasks[task_index] = RunningTaskInfo(inputs, 0, 0)

    def on_output_generated(self, task_index: int, output: RefBundle):
        """Callback when a new task generates an output."""
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
        self.obj_store_mem_alloc += output_bytes
        self.obj_store_mem_cur += output_bytes
        if self.obj_store_mem_cur > self.obj_store_mem_peak:
            self.obj_store_mem_peak = self.obj_store_mem_cur

        for block_ref, _ in output.blocks:
            trace_allocation(block_ref, "operator_output")

    def on_task_finished(self, task_index: int):
        """Callback when a task is finished."""
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
                self.obj_store_mem_spilled += meta.size_bytes

        self.obj_store_mem_freed += total_input_size
        self.obj_store_mem_cur -= total_input_size

        inputs.destroy_if_owned()
        del self._running_tasks[task_index]

