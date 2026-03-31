import abc
import copy
from typing import Any, Dict, Optional

from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import Rule
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

__all__ = [
    "ConfigureMapTaskMemoryRule",
    "ConfigureMapTaskMemoryUsingOpMetrics",
]


class ConfigureMapTaskMemoryRule(Rule, abc.ABC):
    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        for op in plan.dag.post_order_iter():
            if not isinstance(op, MapOperator):
                continue

            def ray_remote_args_fn(
                op: MapOperator = op, original_ray_remote_args_fn=op._ray_remote_args_fn
            ) -> Dict[str, Any]:
                assert isinstance(op, MapOperator), op

                static_ray_remote_args = copy.deepcopy(op._ray_remote_args)

                dynamic_ray_remote_args = {}
                if original_ray_remote_args_fn is not None:
                    dynamic_ray_remote_args = original_ray_remote_args_fn()

                if (
                    "memory" not in static_ray_remote_args
                    and "memory" not in dynamic_ray_remote_args
                    # If this rule configures memory but the user hasn't specified
                    # memory in the placement group, then Ray won't be able to
                    # schedule tasks.
                    and not any(
                        isinstance(
                            scheduling_strategy, PlacementGroupSchedulingStrategy
                        )
                        for scheduling_strategy in (
                            static_ray_remote_args.get("scheduling_strategy"),
                            dynamic_ray_remote_args.get("scheduling_strategy"),
                            op.data_context.scheduling_strategy,
                            op.data_context.scheduling_strategy_large_args,
                        )
                    )
                ):
                    memory = self.estimate_per_task_memory_requirement(op)
                    if memory is not None:
                        dynamic_ray_remote_args["memory"] = _find_memory_bin(memory)

                return dynamic_ray_remote_args

            op._ray_remote_args_fn = ray_remote_args_fn

        return plan

    @abc.abstractmethod
    def estimate_per_task_memory_requirement(self, op: MapOperator) -> Optional[int]:
        """Estimate the per-task memory requirement for the given map operator.

        This is used to configure the `memory` argument in `ray.remote`.
        """
        ...


# Group memory requirement into bins to reduce scheduling overhead.
def _find_memory_bin(raw_memory: float) -> int:
    MB = 1024**2
    GB = 1024**3
    if raw_memory <= 2 * GB:
        bin_size = 128 * MB
    else:
        bin_size = 256 * MB
    # Use ceiling division to ensure we always round up to the nearest bin.
    bin_idx = int(-(-raw_memory // bin_size))
    return bin_idx * bin_size


class ConfigureMapTaskMemoryUsingOpMetrics(ConfigureMapTaskMemoryRule):
    def estimate_per_task_memory_requirement(self, op: MapOperator) -> Optional[int]:
        # Typically, this configuration won't make a difference because
        # `average_bytes_per_output` is usually ~128 MiB and each core usually has
        # 4 GiB of memory. However, if `num_cpus` is small (e.g., 0.01) or
        # `target_max_block_size` is large (e.g., 1GB), then tasks can OOM even
        # if their peak USS or a single output block size fits only narrowly.
        #
        # We set `memory` based on runtime metrics: use the maximum of
        # `average_max_uss_per_task` and `average_bytes_per_output` when both are
        # available, or fall back to whichever metric is present.
        #
        # Note that, unless object store memory is manually specified, by default Ray's
        # "memory" resource is exclusive of the Object Store memory allocated on the
        # node (i.e., its total allocatable value is Total memory - Object Store
        # memory).
        uss = op.metrics.average_max_uss_per_task
        output = op.metrics.average_bytes_per_output
        if uss is not None and output is not None:
            return int(max(uss, output))
        elif uss is not None:
            return int(uss)
        elif output is not None:
            return int(output)
        else:
            return None
