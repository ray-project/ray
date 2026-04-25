import copy
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple

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
        self._current_logical_usage = ExecutionResources.zero()

        # NOTE: Unlike static Ray remote args, dynamic arguments extracted from the
        #       blocks themselves are going to be passed inside `fn.options(...)`
        #       invocation
        ray_remote_static_args = {
            **(self._ray_remote_args or {}),
            "num_returns": "streaming",
            "_labels": {self._OPERATOR_ID_LABEL_KEY: self.id},
        }

        self._map_task = cached_remote_fn(_map_task, **ray_remote_static_args)

        # Pre-build the `.options(...)` wrapper and derived logical-usage for
        # the two per-bundle-size dispatch paths: small args (SPREAD) and
        # large args (DEFAULT / locality-preferring). The per-task decision is
        # a binary flip on `bundle.size_bytes() > large_args_threshold`, so
        # two cached wrappers cover all dispatches. When the user provided a
        # dynamic `ray_remote_args_fn` we cannot cache (the callback may
        # return different args per call); in that case we fall back to the
        # original per-task path, preserving semantics without regression.
        (
            self._cached_small_args,
            self._cached_small_options,
            self._cached_small_metrics_args,
        ) = (
            self._build_cached_dispatch(self.data_context.scheduling_strategy)
            if ray_remote_args_fn is None
            else (None, None, None)
        )
        (
            self._cached_large_args,
            self._cached_large_options,
            self._cached_large_metrics_args,
        ) = (
            self._build_cached_dispatch(
                self.data_context.scheduling_strategy_large_args
            )
            if ray_remote_args_fn is None
            else (None, None, None)
        )

    def _build_cached_dispatch(
        self, scheduling_strategy: Any
    ) -> Tuple[Dict[str, Any], Any, Dict[str, Any]]:
        """Prebuild a `(remote_args_dict, options_wrapper, metrics_args)`
        triple for one dispatch path.

        Mirrors the dict construction of `_get_dynamic_ray_remote_args` +
        `_try_schedule_task`, but with the scheduling strategy fixed. Called
        only in `__init__` when `ray_remote_args_fn` is None, so the dict
        contents never change after construction.

        ``metrics_args`` is a snapshot captured BEFORE ``name`` and
        ``_generator_backpressure_num_objects`` are added, matching pristine's
        ``_remote_args_for_metrics = copy.deepcopy(ray_remote_args)`` capture
        point inside ``_get_dynamic_ray_remote_args``. Aliasing the full args
        dict for metrics would leak those internal keys into the
        user-visible ``Dataset.stats()`` output.
        """
        args = copy.deepcopy(self._ray_remote_args) if self._ray_remote_args else {}
        # max_calls isn't supported in `.options(...)`.
        args.pop("max_calls", None)
        # Only assign the size-aware scheduling strategy if the caller
        # hasn't pinned one via `ray_remote_args`. Some upstream datasources
        # (e.g. ReadParquet with local-node affinity) specify an explicit
        # strategy that must not be overridden — clobbering it was the
        # cause of a regression on `test_consumption::test_read_write_local_node`.
        args.setdefault("scheduling_strategy", scheduling_strategy)
        # Snapshot for metrics BEFORE the dispatch-only keys (`name`,
        # `_generator_backpressure_num_objects`) are added.
        metrics_args = copy.deepcopy(args)
        args["name"] = self.name
        ctx = self.data_context
        if (
            "_generator_backpressure_num_objects" not in args
            and ctx._max_num_blocks_in_streaming_gen_buffer is not None
        ):
            # `_generator_backpressure_num_objects` = 2 *
            # `_max_num_blocks_in_streaming_gen_buffer` because the map task
            # yields 2 objects per block (the block and its metadata).
            args["_generator_backpressure_num_objects"] = (
                2 * ctx._max_num_blocks_in_streaming_gen_buffer
            )
        return args, self._map_task.options(**args), metrics_args

    def _try_schedule_task(self, bundle: RefBundle, strict: bool):
        # Notify first input for deferred initialization (e.g., Iceberg schema evolution).
        self._notify_first_input(bundle)
        # Submit the task as a normal Ray task.
        ctx = TaskContext(
            task_idx=self._next_data_task_idx,
            op_name=self.name,
            target_max_block_size_override=self.target_max_block_size_override,
        )

        # Fast path: no dynamic remote-args callback. Use the cached
        # (args_dict, options_wrapper) pair matching this bundle's size
        # classification.
        #
        # Both clauses must hold to take the fast path:
        #   * the cache was built (only true when ``ray_remote_args_fn`` was
        #     None at __init__), AND
        #   * no callback has been installed since __init__. The
        #     ``ConfigureMapTaskMemoryRule`` planner pass installs
        #     ``op._ray_remote_args_fn`` AFTER operator construction to
        #     inject per-task ``memory`` for OOM protection. Bypassing the
        #     cache check on a now-non-None callback silently disables that
        #     memory configuration for every dispatch.
        if (
            self._cached_small_options is not None
            and self._ray_remote_args_fn is None
        ):
            # ``self.name`` can change between ``__init__`` and the first
            # dispatch — ``SetReadParallelismRule`` calls
            # ``set_additional_split_factor(k)`` after plan construction,
            # which makes ``self.name`` return e.g.
            # ``ReadCSV->SplitBlocks(10)`` instead of ``ReadCSV``. Ray's
            # core task metrics are keyed on the dispatch-time task name,
            # so a stale cached name would record tasks under the wrong
            # identifier (seen as
            # ``test_splitblocks::test_small_file_split`` regression).
            # Re-bake the cached args + options wrapper the first time we
            # detect a name drift; cached thereafter.
            if self._cached_small_args["name"] != self.name:
                self._cached_small_args["name"] = self.name
                self._cached_large_args["name"] = self.name
                self._cached_small_options = self._map_task.options(
                    **self._cached_small_args
                )
                self._cached_large_options = self._map_task.options(
                    **self._cached_large_args
                )
            is_large = (
                bundle.size_bytes() > self.data_context.large_args_threshold
            )
            if is_large:
                dynamic_ray_remote_args = self._cached_large_args
                options_wrapper = self._cached_large_options
            else:
                dynamic_ray_remote_args = self._cached_small_args
                options_wrapper = self._cached_small_options
            # Update the metrics snapshot the first time we dispatch along
            # this path, matching the preserved `_get_dynamic_ray_remote_args`
            # behavior. Large args takes precedence once triggered.
            #
            # Use the pre-built ``_cached_*_metrics_args`` (snapshot taken in
            # ``_build_cached_dispatch`` BEFORE ``name`` and
            # ``_generator_backpressure_num_objects`` were added), not the
            # full dispatch dict — those keys are dispatch-internal and
            # leaking them into the user-visible ``Dataset.stats()`` output
            # diverges from pristine.
            if is_large or "scheduling_strategy" not in self._remote_args_for_metrics:
                self._remote_args_for_metrics = (
                    self._cached_large_metrics_args
                    if is_large
                    else self._cached_small_metrics_args
                )
        else:
            # Slow path: user provided a dynamic `ray_remote_args_fn`; args
            # may change per call, rebuild the wrapper fresh.
            dynamic_ray_remote_args = self._get_dynamic_ray_remote_args(
                input_bundle=bundle
            )
            dynamic_ray_remote_args["name"] = self.name
            if (
                "_generator_backpressure_num_objects" not in dynamic_ray_remote_args
                and self.data_context._max_num_blocks_in_streaming_gen_buffer
                is not None
            ):
                dynamic_ray_remote_args["_generator_backpressure_num_objects"] = (
                    2 * self.data_context._max_num_blocks_in_streaming_gen_buffer
                )
            options_wrapper = self._map_task.options(**dynamic_ray_remote_args)

        logical_usage = ExecutionResources.from_resource_dict(
            dynamic_ray_remote_args
        )

        gen = options_wrapper.remote(
            self._map_transformer_ref,
            self._data_context_ref,
            ctx,
            *bundle.block_refs,
            slices=bundle.slices,
            **self.get_map_task_kwargs(),
        )

        self._current_logical_usage = self._current_logical_usage.add(logical_usage)

        def task_done_callback():
            self._current_logical_usage = self._current_logical_usage.subtract(
                logical_usage
            )

        self._submit_data_task(gen, bundle, task_done_callback=task_done_callback)

    def progress_str(self) -> str:
        return ""

    def current_logical_usage(self) -> ExecutionResources:
        return self._current_logical_usage

    def pending_logical_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        return self.per_task_resource_allocation()

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

    def min_max_resource_requirements(
        self,
    ) -> Tuple[ExecutionResources, ExecutionResources]:
        """Returns min/max resource requirements for this operator.

        - Min: resources needed for one task (minimum to make progress)
        - Max: resources for max_concurrency tasks (if set), else infinite
        """
        per_task = self.per_task_resource_allocation()
        obj_store_per_task = (
            self._metrics.obj_store_mem_max_pending_output_per_task or 0
        )

        min_resource_usage = per_task.copy(object_store_memory=obj_store_per_task)

        # Cap resources to 0 if this operator doesn't use them.
        # This prevents operators from hoarding resource budget they don't need.
        max_concurrency = (
            self._max_concurrency if self._max_concurrency is not None else float("inf")
        )
        max_resource_usage = ExecutionResources(
            cpu=0 if per_task.cpu == 0 else per_task.cpu * max_concurrency,
            gpu=0 if per_task.gpu == 0 else per_task.gpu * max_concurrency,
            memory=0 if per_task.memory == 0 else per_task.memory * max_concurrency,
            # Set the max `object_store_memory` requirement to 'inf', because we
            # don't know how much data each task can output.
            object_store_memory=float("inf"),
        )

        return min_resource_usage, max_resource_usage

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
