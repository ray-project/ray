import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    import pyarrow as pa

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import (
    BaseRefBundler,
    MapOperator,
    _map_task,
)
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.context import DataContext


class TaskPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on a task pool."""

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        data_context: DataContext,
        name: str = "TaskPoolMap",
        target_max_block_size_override: Optional[int] = None,
        min_rows_per_bundle: Optional[int] = None,
        ref_bundler: Optional[BaseRefBundler] = None,
        max_concurrency: Optional[int] = None,
        supports_fusion: bool = True,
        map_task_kwargs: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        on_start: Optional[Callable[[Optional["pa.Schema"]], None]] = None,
    ):
        """Create an TaskPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            target_max_block_size_override: Override for target max-block-size.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ref_bundler: The ref bundler to use for this operator.
            max_concurrency: The maximum number of Ray tasks to use concurrently,
                or None to use as many tasks as possible.
            supports_fusion: Whether this operator supports fusion with other operators.
            map_task_kwargs: A dictionary of kwargs to pass to the map task. You can
                access these kwargs through the `TaskContext.kwargs` dictionary.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Customize the :func:`ray.remote` args for this op's tasks.
            on_start: Optional callback invoked with the schema from the first input
                bundle before any tasks are submitted.
        """
        super().__init__(
            map_transformer,
            input_op,
            data_context,
            name,
            target_max_block_size_override,
            min_rows_per_bundle,
            ref_bundler,
            supports_fusion,
            map_task_kwargs,
            ray_remote_args_fn,
            ray_remote_args,
            on_start,
        )

        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError(f"max_concurrency have to be > 0 (got {max_concurrency})")

        self._max_concurrency = max_concurrency

        # NOTE: Unlike static Ray remote args, dynamic arguments extracted from the
        #       blocks themselves are going to be passed inside `fn.options(...)`
        #       invocation
        ray_remote_static_args = {
            **(self._ray_remote_args or {}),
            "num_returns": "streaming",
            "_labels": {self._OPERATOR_ID_LABEL_KEY: self.id},
        }

        self._map_task = cached_remote_fn(_map_task, **ray_remote_static_args)

    def _add_bundled_input(self, bundle: RefBundle):
        # Notify first input for deferred initialization (e.g., Iceberg schema evolution).
        self._notify_first_input(bundle)
        # Submit the task as a normal Ray task.
        ctx = TaskContext(
            task_idx=self._next_data_task_idx,
            op_name=self.name,
            target_max_block_size_override=self.target_max_block_size_override,
        )

        dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(input_bundle=bundle)
        dynamic_ray_remote_args["name"] = self.name

        if (
            "_generator_backpressure_num_objects" not in dynamic_ray_remote_args
            and self.data_context._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # The `_generator_backpressure_num_objects` parameter should be
            # `2 * _max_num_blocks_in_streaming_gen_buffer` because we yield
            # 2 objects for each block: the block and the block metadata.
            dynamic_ray_remote_args["_generator_backpressure_num_objects"] = (
                2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
            )

        data_context = self.data_context

        gen = self._map_task.options(**dynamic_ray_remote_args).remote(
            self._map_transformer_ref,
            data_context,
            ctx,
            *bundle.block_refs,
            slices=bundle.slices,
            **self.get_map_task_kwargs(),
        )
        self._submit_data_task(gen, bundle)

    def progress_str(self) -> str:
        return ""

    def current_processor_usage(self) -> ExecutionResources:
        num_active_workers = self.num_active_tasks()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
        )

    def pending_processor_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        return self.per_task_resource_allocation().copy(
            object_store_memory=(
                self._metrics.obj_store_mem_max_pending_output_per_task or 0
            ),
        )

    def per_task_resource_allocation(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0),
            gpu=self._ray_remote_args.get("num_gpus", 0),
            memory=self._ray_remote_args.get("memory", 0),
        )

    def min_scheduling_resources(
        self: "PhysicalOperator",
    ) -> ExecutionResources:
        return self.incremental_resource_usage()

    def get_max_concurrency_limit(self) -> Optional[int]:
        return self._max_concurrency

    def all_inputs_done(self):
        super().all_inputs_done()

        if (
            self._max_concurrency is not None
            and self._metrics.num_inputs_received < self._max_concurrency
        ):
            warnings.warn(
                f"The maximum number of concurrent tasks for '{self.name}' is set to "
                f"{self._max_concurrency}, but the operator only received "
                f"{self._metrics.num_inputs_received} input(s). This means that the "
                f"operator can launch at most {self._metrics.num_inputs_received} "
                "task(s), which is less than the concurrency limit. You might be able "
                "to increase the number of concurrent tasks by configuring "
                "`override_num_blocks` earlier in the pipeline."
            )
