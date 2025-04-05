import copy
from typing import Any, Callable, Dict, Optional

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.map_operator import MapOperator, _map_task
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class TaskPoolMapOperator(MapOperator):
    """A MapOperator implementation that executes tasks on a task pool."""

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        data_context: DataContext,
        target_max_block_size: Optional[int],
        name: str = "TaskPoolMap",
        min_rows_per_bundle: Optional[int] = None,
        concurrency: Optional[int] = None,
        supports_fusion: bool = True,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Create an TaskPoolMapOperator instance.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            target_max_block_size: The target maximum number of bytes to
                include in an output block.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            concurrency: The maximum number of Ray tasks to use concurrently,
                or None to use as many tasks as possible.
            supports_fusion: Whether this operator supports fusion with other operators.
            ray_remote_args_fn: A function that returns a dictionary of remote args
                passed to each map worker. The purpose of this argument is to generate
                dynamic arguments for each actor/task, and will be called each time
                prior to initializing the worker. Args returned from this dict will
                always override the args in ``ray_remote_args``. Note: this is an
                advanced, experimental feature.
            ray_remote_args: Customize the :func:`ray.remote` args for this op's tasks.
        """
        super().__init__(
            map_transformer,
            input_op,
            data_context,
            name,
            target_max_block_size,
            min_rows_per_bundle,
            supports_fusion,
            ray_remote_args_fn,
            ray_remote_args,
        )
        self._concurrency = concurrency

        # NOTE: Unlike static Ray remote args, dynamic arguments extracted from the
        #       blocks themselves are going to be passed inside `fn.options(...)`
        #       invocation
        ray_remote_static_args = {
            **(self._ray_remote_args or {}),
            "num_returns": "streaming",
            "_labels": {self._OPERATOR_ID_LABEL_KEY: self.id},
        }

        self._map_task = cached_remote_fn(_map_task, **ray_remote_static_args)
        self._ray_remote_args_factory_actor_locality = None

    def _add_bundled_input(self, bundle: RefBundle):
        # Submit the task as a normal Ray task.
        ctx = TaskContext(
            task_idx=self._next_data_task_idx,
            target_max_block_size=self.actual_target_max_block_size,
        )

        dynamic_ray_remote_args = self._get_runtime_ray_remote_args(input_bundle=bundle)
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
            **self.get_map_task_kwargs(),
        )
        self._submit_data_task(gen, bundle)

    def _get_runtime_ray_remote_args(
        self, input_bundle: Optional[RefBundle] = None
    ) -> Dict[str, Any]:
        ray_remote_args = copy.deepcopy(self._ray_remote_args)

        # Override parameters from user provided remote args function.
        if self._ray_remote_args_fn:
            new_remote_args = self._ray_remote_args_fn()
            for k, v in new_remote_args.items():
                ray_remote_args[k] = v
        # For tasks with small args, we will use SPREAD by default to optimize for
        # compute load-balancing. For tasks with large args, we will use DEFAULT to
        # allow the Ray locality scheduler a chance to optimize task placement.
        if "scheduling_strategy" not in ray_remote_args:
            ctx = self.data_context
            if input_bundle and input_bundle.size_bytes() > ctx.large_args_threshold:
                ray_remote_args[
                    "scheduling_strategy"
                ] = ctx.scheduling_strategy_large_args
                # Takes precedence over small args case. This is to let users know
                # when the large args case is being triggered.
                self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
            else:
                ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
                # Only save to metrics if we haven't already done so.
                if "scheduling_strategy" not in self._remote_args_for_metrics:
                    self._remote_args_for_metrics = copy.deepcopy(ray_remote_args)
        # This should take precedence over previously set scheduling strategy, as it
        # implements actor-based locality overrides.
        if self._ray_remote_args_factory_actor_locality:
            return self._ray_remote_args_factory_actor_locality(ray_remote_args)
        return ray_remote_args

    def start(self, options: ExecutionOptions):
        super().start(options)

        if options.locality_with_output:
            if isinstance(options.locality_with_output, list):
                locs = options.locality_with_output
            else:
                locs = [ray.get_runtime_context().get_node_id()]

            class RoundRobinAssign:
                def __init__(self, locs):
                    self.locs = locs
                    self.i = 0

                def __call__(self, args):
                    args = copy.deepcopy(args)
                    args["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                        self.locs[self.i],
                        soft=True,
                        _spill_on_unavailable=True,
                    )
                    self.i += 1
                    self.i %= len(self.locs)
                    return args

            self._ray_remote_args_factory_actor_locality = RoundRobinAssign(locs)

    def shutdown(self, force: bool = False):
        # Cancel all active tasks.
        for _, task in self._data_tasks.items():
            ray.cancel(task.get_waitable())
        # Wait until all tasks have failed or been cancelled.
        for _, task in self._data_tasks.items():
            try:
                ray.get(task.get_waitable())
            except ray.exceptions.RayError:
                # Cancellation either succeeded, or the task had already failed with
                # a different error, or cancellation failed. In all cases, we
                # swallow the exception.
                pass

        super().shutdown(force)

    def progress_str(self) -> str:
        return ""

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def current_processor_usage(self) -> ExecutionResources:
        num_active_workers = self.num_active_tasks()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
        )

    def pending_processor_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0),
            gpu=self._ray_remote_args.get("num_gpus", 0),
            object_store_memory=self._metrics.obj_store_mem_max_pending_output_per_task
            or 0,
        )

    def get_concurrency(self) -> Optional[int]:
        return self._concurrency
