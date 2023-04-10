from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

import ray
from ray.experimental.parallel_ml.communicator import TorchBasedCommunicator
from ray.experimental.parallel_ml.engine import Config
from ray.experimental.parallel_ml.schedule import (
    ExecuteSchedule,
    InputSchedule,
    OutputSchedule,
    Schedule,
)
from ray.util.placement_group import PlacementGroup
from torch import Tensor, nn


@dataclass
class ModuleParition(object):
    partition_index: int
    module: nn.Module
    module_loader: Callable[[], nn.Module]
    input_tensor_shape: Any
    input_tensor_dtype: Tensor.dtype


@dataclass
class PhysicalPlan(object):
    num_stages: int
    replica_schedules: List[Schedule]
    replica_configs: List[Config]
    replica_placements: List[Tuple(PlacementGroup, int)]


class PhysicalPlanner(metaclass=ABCMeta):
    """The physical planner takes a logical plan (DAG) and number of
    availalble resources in the cluster, and generates a physical plan:
    i.e. number of replicas of each stage, and execution schedule of each replica."""

    @abstractmethod
    def plan(
        self, logical_plan: List[ModuleParition], pg: PlacementGroup
    ) -> PhysicalPlan:
        pass


def _get_device_name():
    gpu_ids = ray.get_gpu_ids()
    assert len(gpu_ids) == 1
    return f"cuda:{gpu_ids[0]}"


def _get_communicator(world_size: int, rank: int, master_addr: str):
    return TorchBasedCommunicator(world_size, rank, master_addr)


class SimplePhysicalPlanner(PhysicalPlanner):
    """Simple physical planner that simply deploys one replica per stage per GPU."""

    def plan(
        self, logical_plan: List[ModuleParition], pg: PlacementGroup
    ) -> PhysicalPlan:
        self._validate_pg(pg)
        assert len(logical_plan) >= len(pg.bundle_specs)
        world_size = len(logical_plan)
        schedules = []
        configs = []
        pgs = []

        for rank, partition in enumerate(logical_plan):
            schedule = ExecuteSchedule(rank - 1, rank + 1)
            if rank == 0:
                schedule = InputSchedule(rank + 1)
            elif rank == world_size - 1:
                schedule = OutputSchedule(rank - 1)
            schedules.append(schedule)

            config = Config(
                world_size,
                rank,
                partition.input_tensor_shape,
                partition.input_tensor_dtype,
                _get_device_name,
                _get_communicator,
                partition.module_loader,
                lambda: None,
            )
            configs.append(config)
            pgs.append((pg, rank))

        return PhysicalPlan(pg, schedules, configs, pgs)

    def _validate_pg(self, pg: PlacementGroup):
        for bundle in pg.bundle_specs:
            assert bundle == {
                "GPU": 1
            }, "SimplePhysicalPlanner only supports one GPU per replica"
