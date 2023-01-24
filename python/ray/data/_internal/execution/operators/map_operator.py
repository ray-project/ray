from typing import List, Iterator, Any, Dict, Callable, Optional

import ray
from ray.data.block import Block, BlockMetadata
from ray.data._internal.stats import StatsDict
from ray.data._internal.compute import (
    ComputeStrategy,
    TaskPoolStrategy,
)
from ray.data._internal.execution.interfaces import (
    RefBundle,
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.map_operator_state import (
    MapOperatorState,
)


class MapOperator(PhysicalOperator):
    """A streaming operator that maps input bundles 1:1 to output bundles.

    This operator implements the distributed map operation, supporting both task
    and actor compute strategies.
    """

    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        input_op: PhysicalOperator,
        name: str = "Map",
        # TODO(ekl): slim down ComputeStrategy to only specify the compute
        # config and not contain implementation code.
        compute_strategy: Optional[ComputeStrategy] = None,
        min_rows_per_bundle: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Create a MapOperator.

        Args:
            transform_fn: The function to apply to each ref bundle input.
            input_op: Operator generating input data for this op.
            name: The name of this operator.
            compute_strategy: Customize the compute strategy for this op.
            min_rows_per_bundle: The number of rows to gather per batch passed to the
                transform_fn, or None to use the block size. Setting the batch size is
                important for the performance of GPU-accelerated transform functions.
                The actual rows passed may be less if the dataset is small.
            ray_remote_args: Customize the ray remote args for this op's tasks.
        """
        ray_remote_args = _canonicalize_ray_remote_args(ray_remote_args or {})
        compute_strategy = compute_strategy or TaskPoolStrategy()
        self._ray_remote_args = ray_remote_args
        self._execution_state = MapOperatorState(
            transform_fn,
            compute_strategy,
            ray_remote_args,
            min_rows_per_bundle,
        )
        self._output_metadata: List[BlockMetadata] = []
        super().__init__(name, [input_op])

    def get_metrics(self) -> Dict[str, int]:
        return {
            "obj_store_mem_alloc": self._execution_state.obj_store_mem_alloc,
            "obj_store_mem_freed": self._execution_state.obj_store_mem_freed,
            "obj_store_mem_peak": self._execution_state.obj_store_mem_peak,
        }

    def progress_str(self) -> str:
        return self._execution_state.progress_str()

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, input_index
        self._execution_state.add_input(refs)

    def inputs_done(self) -> None:
        self._execution_state.inputs_done()
        super().inputs_done()

    def has_next(self) -> bool:
        assert self._started
        return self._execution_state.has_next()

    def get_next(self) -> RefBundle:
        assert self._started
        bundle = self._execution_state.get_next()
        for _, meta in bundle.blocks:
            self._output_metadata.append(meta)
        return bundle

    def get_work_refs(self) -> List[ray.ObjectRef]:
        return self._execution_state.get_work_refs()

    def num_active_work_refs(self) -> int:
        return self._execution_state.num_active_work_refs()

    def notify_work_completed(self, task: ray.ObjectRef) -> None:
        self._execution_state.work_completed(task)

    def get_stats(self) -> StatsDict:
        return {self._name: self._output_metadata}

    def start(self, options: ExecutionOptions) -> None:
        self._execution_state.start(options)
        super().start(options)

    def shutdown(self) -> None:
        self._execution_state.shutdown()
        super().shutdown()

    def base_resource_usage(self) -> ExecutionResources:
        return self._execution_state.base_resource_usage()

    def incremental_resource_usage(self) -> ExecutionResources:
        return self._execution_state.incremental_resource_usage()

    def current_resource_usage(self) -> ExecutionResources:
        return self._execution_state.current_resource_usage()


def _canonicalize_ray_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
    """Enforce rules on ray remote args for map tasks.

    Namely, args must explicitly specify either CPU or GPU, not both. Disallowing
    mixed resources avoids potential starvation and deadlock issues during scheduling,
    and should not be a serious limitation for users.
    """
    ray_remote_args = ray_remote_args.copy()
    if "num_cpus" not in ray_remote_args and "num_gpus" not in ray_remote_args:
        ray_remote_args["num_cpus"] = 1
    if ray_remote_args.get("num_gpus", 0) > 0:
        if ray_remote_args.get("num_cpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    elif ray_remote_args.get("num_cpus", 0) > 0:
        if ray_remote_args.get("num_gpus", 0) != 0:
            raise ValueError(
                "It is not allowed to specify both num_cpus and num_gpus for map tasks."
            )
    return ray_remote_args
