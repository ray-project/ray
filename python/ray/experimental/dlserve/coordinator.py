import ray
from ray.dag import DAGNode
from ray.experimental.dlserve.engine import ExecutionEngine
from ray.experimental.dlserve.physical_plan import PhysicalPlan, PhysicalPlanner
from ray.util.placement_group import PlacementGroup, PlacementGroupSchedulingStrategy


class Coordinator(object):
    def __init__(
        self, logical_plan: DAGNode, pg: PlacementGroup, planner: PhysicalPlanner
    ) -> None:
        self._logical_plan = logical_plan
        self._pg = pg
        self._planner = planner
        self._actors = []

    def start(self):
        self._physical_plan = self._planner.plan(self._logical_plan, self._pg)
        for rank in range(self._physical_plan.num_stages):
            self._actors.append(self._start_actor(rank))
        return ray.get([actor.start.remote() for actor in self._actors])

    def _start_actor(self, rank: int):
        pg, bundle_index = self._physical_plan.replica_placements[rank]
        return (
            ray.remote(ExecutionEngine)
            .options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_bundle_index=bundle_index
                )
            )
            .remote(
                self._physical_plan.replica_schedules[rank],
                self._physical_plan.replica_configs[rank],
            )
        )

    def reconfigure(self, new_logical_plan: DAGNode, new_pg: PlacementGroup) -> None:
        pass
