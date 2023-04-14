from typing import List

import ray
from ray.experimental.lightrails.engine import ExecutionEngine
from ray.experimental.lightrails.physical_plan import ModuleParition, PhysicalPlanner
from ray.util.placement_group import PlacementGroup, PlacementGroupSchedulingStrategy


class Coordinator(object):
    """The coordinator is responsible for scheduling the execution of the physical plan.
    It also responsible for reconfiguring the physical plan when necessary.
    """

    def __init__(
        self,
        logical_plan: List[ModuleParition],
        pg: PlacementGroup,
        planner: PhysicalPlanner,
        requires_gpu: bool = False,
    ) -> None:
        self._logical_plan = logical_plan
        self._pg = pg
        self._planner = planner
        self._actors = []
        self.requires_gpu = requires_gpu

    def start(self):
        self._physical_plan = self._planner.plan(
            self._logical_plan, self._pg, requires_gpu=self.requires_gpu
        )
        for rank in range(self._physical_plan.num_stages):
            self._actors.append(self._start_actor(rank))

        master_address = ray.get(self._actors[0].get_address.remote())

        return ray.get([actor.start.remote(master_address) for actor in self._actors])

    def wait_until_stopped(self):
        ray.get([actor.wait_until_stopped.remote() for actor in self._actors[:1]])

    def _start_actor(self, rank: int):
        pg, bundle_index = self._physical_plan.replica_placements[rank]
        return (
            ray.remote(ExecutionEngine)
            .options(
                num_gpus=1 if self.requires_gpu else 0,
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=bundle_index
                ),
            )
            .remote(
                self._physical_plan.replica_schedules[rank],
                self._physical_plan.replica_configs[rank],
            )
        )

    def reconfigure(
        self, new_logical_plan: List[ModuleParition], new_pg: PlacementGroup
    ) -> None:
        pass
