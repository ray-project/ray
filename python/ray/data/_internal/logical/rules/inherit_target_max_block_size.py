from typing import List, Optional, Tuple

# TODO(Clark): Remove compute dependency once we delete the legacy compute.
from ray.data._internal.compute import get_compute, is_task_compute
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.logical.interfaces import PhysicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import (
    AbstractAllToAll,
    RandomShuffle,
    Repartition,
)
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.rules.split_read_output_blocks import (
    compute_additional_split_factor,
)
from ray.data._internal.stats import StatsDict
from ray.data.context import DataContext



class InheritTargetMaxBlockSizeRule(Rule):
    """Fuses linear chains of compatible physical operators."""

    def apply(self, plan: PhysicalPlan) -> PhysicalPlan:
        target_max_block_size = None
        dag = plan.dag
        while True:
            if dag.target_max_block_size is not None:
                # Set the target block size to inherit for
                # upstream ops.
                target_max_block_size = dag.target_max_block_size
            elif target_max_block_size is not None:
                # Inherit from downstream op.
                dag.target_max_block_size = target_max_block_size

            if len(dag.input_dependencies) != 1:
                break
            dag = dag.input_dependencies[0]
        return plan
