import threading
import time
from collections import deque
from dataclasses import _MISSING_TYPE, dataclass, field, fields
from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np

import ray
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.memory_tracing import trace_allocation

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )


@dataclass
class RunningTaskInfo:
    inputs: RefBundle
    num_outputs: int
    bytes_outputs: int
    start_time: float
    bytes_rss: int


class SlidingWindowMetric:
    """Class to maintain metrics datapoints for the last X seconds."""

    # Length in seconds of sliding window.
    WINDOW_LENGTH_SECONDS = 10
    # Percentiles to compute.
    PERCENTILES = [50, 90, 99]

    def __init__(self):
        self._window = deque()
        self._window_lock = threading.Lock()
        self._computed_percentiles = [
            (percentile, 0) for percentile in SlidingWindowMetric.PERCENTILES
        ]

    def append(self, value):
        with self._window_lock:
            self._window.append((value, time.time()))
            self._slide_window()

    def percentiles(self):
        with self._window_lock:
            self._slide_window()
            values = [item[0] for item in self._window]
        results = []
        for percentile in SlidingWindowMetric.PERCENTILES:
            results.append(
                (percentile, np.percentile(values, percentile) if values else 0)
            )
        self._computed_percentiles = results
        return results

    def get_last_computed_percentiles(self):
        return self._computed_percentiles

    def _slide_window(self):
        while (
            len(self._window) > 0
            and time.time() - self._window[0][1]
            > SlidingWindowMetric.WINDOW_LENGTH_SECONDS
        ):
            self._window.popleft()


@dataclass
class OpRuntimeMetrics:
    """Runtime metrics for a PhysicalOperator.

    Metrics are updated dynamically during the execution of the Dataset.
    This class can be used for either observablity or scheduling purposes.

    DO NOT modify the fields of this class directly. Instead, use the provided
    callback methods.

    Field metadata:
        - export: Metrics to export for `as_dict()`
        - map_only: Metrics only available for map operator.
        - export_metric: Metrics to export for `as_dict()` to
            Ray Data Prometheus metrics.
        - window: A field that stores individual datapoints of the last X
            seconds in a deque. Used for querying percentiles.
    """

    # === Inputs-related metrics ===

    # Number of received input blocks.
    num_inputs_received: int = 0
    # Total size in bytes of received input blocks.
    bytes_inputs_received: int = 0

    # Number of processed input blocks.
    # TODO(hchen): Fields tagged with "map_only" currently only work for MapOperator.
    # We should make them work for all operators by unifying the task execution code.
    num_inputs_processed: int = field(default=0, metadata={"map_only": True})
    # Total size in bytes of processed input blocks.
    bytes_inputs_processed: int = field(default=0, metadata={"map_only": True})
    # Size in bytes of input blocks passed to submitted tasks.
    bytes_inputs_of_submitted_tasks: int = field(default=0, metadata={"map_only": True})

    # === Outputs-related metrics ===

    # Number of generated output blocks.
    num_outputs_generated: int = field(default=0, metadata={"map_only": True})
    # Total size in bytes of generated output blocks.
    bytes_outputs_generated: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )
    # Number of rows of generated output blocks that are from finished tasks.
    rows_outputs_generated: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )

    # Number of output blocks that are already taken by the downstream.
    num_outputs_taken: int = 0
    # Size in bytes of output blocks that are already taken by the downstream.
    bytes_outputs_taken: int = 0

    # Number of generated output blocks that are from finished tasks.
    num_outputs_of_finished_tasks: int = field(default=0, metadata={"map_only": True})
    # Size in bytes of generated output blocks that are from finished tasks.
    bytes_outputs_of_finished_tasks: int = field(default=0, metadata={"map_only": True})

    # === Tasks-related metrics ===

    # Number of submitted tasks.
    num_tasks_submitted: int = field(default=0, metadata={"map_only": True})
    # Number of running tasks.
    num_tasks_running: int = field(default=0, metadata={"map_only": True})
    # Number of tasks that have at least one output block.
    num_tasks_have_outputs: int = field(default=0, metadata={"map_only": True})
    # Number of finished tasks.
    num_tasks_finished: int = field(default=0, metadata={"map_only": True})
    # Number of failed tasks.
    num_tasks_failed: int = field(default=0, metadata={"map_only": True})
    # Runtime of finished tasks.
    task_runtime: SlidingWindowMetric = field(
        default_factory=lambda: SlidingWindowMetric(),
        metadata={"export_metric": True, "map_only": True, "window": True},
    )
    # RSS of finished tasks.
    task_rss: SlidingWindowMetric = field(
        default_factory=lambda: SlidingWindowMetric(),
        metadata={"export_metric": True, "map_only": True, "window": True},
    )

    # === Object store memory metrics ===

    # Allocated memory size in the object store.
    obj_store_mem_alloc: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )
    # Freed memory size in the object store.
    obj_store_mem_freed: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )
    # Current memory size in the object store.
    obj_store_mem_cur: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )
    # Peak memory size in the object store.
    obj_store_mem_peak: int = field(default=0, metadata={"map_only": True})
    # Spilled memory size in the object store.
    obj_store_mem_spilled: int = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )

    # === Miscellaneous metrics ===

    # Time spent generating blocks.
    block_generation_time: float = field(
        default=0, metadata={"map_only": True, "export_metric": True}
    )

    def __init__(self, op: "PhysicalOperator"):
        from ray.data._internal.execution.operators.map_operator import MapOperator

        self._op = op
        self._is_map = isinstance(op, MapOperator)
        self._running_tasks: Dict[int, RunningTaskInfo] = {}
        self._extra_metrics: Dict[str, Any] = {}

        # default_factory fields are not initialized automatically
        # because we define __init__.
        for f in fields(self):
            if not isinstance(f.default_factory, _MISSING_TYPE):
                setattr(self, f.name, f.default_factory())

    @property
    def extra_metrics(self) -> Dict[str, Any]:
        """Return a dict of extra metrics."""
        return self._extra_metrics

    def as_dict(self, metrics_only: bool = False, compute_percentiles: bool = False):
        """Return a dict representation of the metrics.

        Args:
            metrics_only: Whether to include `metrics_only` tagged fields.
            compute_percentiles: Whether to compute updated percentiles for
                `window` tagged fields. This can be expensive to compute, and should
                only be set when fetching stats for the _StatsActor.
        """
        result = []
        for f in fields(self):
            if f.metadata.get("export", True):
                if (not self._is_map and f.metadata.get("map_only", False)) or (
                    metrics_only and not f.metadata.get("export_metric", False)
                ):
                    continue
                value = getattr(self, f.name)
                if f.metadata.get("window", False):
                    percentiles = (
                        value.percentiles()
                        if compute_percentiles
                        else value.get_last_computed_percentiles()
                    )
                    for percentile, value in percentiles:
                        result.append((f"{f.name}_p{percentile}", value))
                else:
                    result.append((f.name, value))

        # TODO: record resource usage in OpRuntimeMetrics,
        # avoid calling self._op.current_resource_usage()
        resource_usage = self._op.current_resource_usage()
        result.extend(
            [
                ("cpu_usage", resource_usage.cpu or 0),
                ("gpu_usage", resource_usage.gpu or 0),
            ]
        )
        result.extend(self._extra_metrics.items())
        return dict(result)

    @classmethod
    def get_metric_keys(cls):
        """Return a list of metric keys."""
        return [
            f.name for f in fields(cls) if f.metadata.get("export_metric", False)
        ] + ["cpu_usage", "gpu_usage"]

    @property
    def average_num_outputs_per_task(self) -> Optional[float]:
        """Average number of output blocks per task, or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.num_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def average_bytes_inputs_per_task(self) -> Optional[float]:
        """Average size in bytes of ref bundles passed to tasks, or ``None`` if no
        tasks have been submitted."""
        if self.num_tasks_submitted == 0:
            return None
        else:
            return self.bytes_inputs_of_submitted_tasks / self.num_tasks_submitted

    @property
    def average_bytes_outputs_per_task(self) -> Optional[float]:
        """Average size in bytes of output blocks per task,
        or None if no task has finished."""
        if self.num_tasks_finished == 0:
            return None
        else:
            return self.bytes_outputs_of_finished_tasks / self.num_tasks_finished

    @property
    def average_bytes_change_per_task(self) -> Optional[float]:
        """Average size difference in bytes of input ref bundles and output ref
        bundles per task."""
        if (
            self.average_bytes_inputs_per_task is None
            or self.average_bytes_outputs_per_task is None
        ):
            return None

        return self.average_bytes_outputs_per_task - self.average_bytes_inputs_per_task

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
        self.bytes_inputs_of_submitted_tasks += inputs.size_bytes()
        self._running_tasks[task_index] = RunningTaskInfo(inputs, 0, 0, time.time(), 0)

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

        bundle_rss = 0
        for block_ref, meta in output.blocks:
            assert meta.exec_stats and meta.exec_stats.wall_time_s
            self.block_generation_time += meta.exec_stats.wall_time_s
            assert meta.num_rows is not None
            self.rows_outputs_generated += meta.num_rows
            bundle_rss += meta.exec_stats.max_rss_bytes
            trace_allocation(block_ref, "operator_output")
        self._running_tasks[task_index].bytes_rss += bundle_rss

    def on_task_finished(self, task_index: int, exception: Optional[Exception]):
        """Callback when a task is finished."""
        self.num_tasks_running -= 1
        self.num_tasks_finished += 1
        if exception is not None:
            self.num_tasks_failed += 1

        task_info = self._running_tasks[task_index]
        self.num_outputs_of_finished_tasks += task_info.num_outputs
        self.bytes_outputs_of_finished_tasks += task_info.bytes_outputs
        task_runtime = time.time() - task_info.start_time
        self.task_runtime.append(task_runtime)
        self.task_rss.append(task_info.bytes_rss)

        inputs = self._running_tasks[task_index].inputs
        self.num_inputs_processed += len(inputs)
        total_input_size = inputs.size_bytes()
        self.bytes_inputs_processed += total_input_size

        blocks = [input[0] for input in inputs.blocks]
        metadata = [input[1] for input in inputs.blocks]
        ctx = ray.data.context.DataContext.get_current()
        if ctx.enable_get_object_locations_for_metrics:
            locations = ray.experimental.get_object_locations(blocks)
            for block, meta in zip(blocks, metadata):
                if locations[block].get("did_spill", False):
                    assert meta.size_bytes is not None
                    self.obj_store_mem_spilled += meta.size_bytes

        self.obj_store_mem_freed += total_input_size
        self.obj_store_mem_cur -= total_input_size

        inputs.destroy_if_owned()
        del self._running_tasks[task_index]
