import logging
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

import ray
from ray.experimental.lightrails.communicator import (
    NaiveCommunicator,
    TorchBasedCommunicator,
)
from ray.experimental.lightrails.engine import Config
from ray.experimental.lightrails.schedule import (
    ExecuteSchedule,
    InputSchedule,
    OutputSchedule,
    Schedule,
)
from ray.util.placement_group import PlacementGroup
from torch import Tensor, nn

logger = logging.getLogger(__name__)


@dataclass
class ModuleParition(object):
    partition_index: int
    module_loader: Callable[[], nn.Module]
    input_tensor_shape: Any
    input_tensor_dtype: Tensor.dtype
    schedule: Schedule
    data_loader_builder: Callable[[], Any]


@dataclass
class PhysicalPlan(object):
    num_stages: int
    replica_schedules: List[Schedule]
    replica_configs: List[Config]
    replica_placements: List


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
    if len(gpu_ids) == 0:
        logger.warning("No GPU available, using CPU")
        return "cpu"
    assert len(gpu_ids) == 1
    return f"cuda:{gpu_ids[0]}"


def _get_communicator(world_size: int, rank: int, master_addr: str):
    return NaiveCommunicator(world_size, rank, master_addr=master_addr)


class SimplePhysicalPlanner(PhysicalPlanner):
    """Simple physical planner that simply deploys one replica per stage per GPU."""

    def plan(
        self,
        logical_plan: List[ModuleParition],
        pg: PlacementGroup,
        requires_gpu: bool = False,
    ) -> PhysicalPlan:
        self._validate_pg(pg, requires_gpu)
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
            if partition.schedule is not None:
                schedule = partition.schedule
            schedules.append(schedule)

            config = Config(
                world_size,
                rank,
                partition.input_tensor_shape,
                partition.input_tensor_dtype,
                _get_device_name,
                _get_communicator,
                partition.module_loader,
                partition.data_loader_builder,
                lambda: None,
            )
            configs.append(config)
            pgs.append((pg, rank))

        return PhysicalPlan(len(logical_plan), schedules, configs, pgs)

    def _validate_pg(self, pg: PlacementGroup, requires_gpu: bool = True):
        for bundle in pg.bundle_specs:
            if requires_gpu:
                assert bundle == {
                    "GPU": 1
                }, "SimplePhysicalPlanner only supports one GPU per replica"
