from abc import ABCMeta, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Tuple

import ray
from ray.dag import DAGNode
from ray.experimental.parallel_ml.dag.nn_module_node import NNModuleNode
from ray.experimental.parallel_ml.engine import Config
from ray.experimental.parallel_ml.schedule import (
    ExecuteSchedule,
    InputSchedule,
    OutputSchedule,
    Schedule,
)
from ray.util.placement_group import PlacementGroup


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
    def plan(self, logical_plan: DAGNode, pg: PlacementGroup) -> PhysicalPlan:
        pass


def _get_device_name():
    gpu_ids = ray.get_gpu_ids()
    assert len(gpu_ids) == 1
    return f"cuda:{gpu_ids[0]}"


class SimplePhysicalPlanner(PhysicalPlanner):
    """Simple physical planner that simply deploys one replica per stage per GPU."""

    def plan(self, logical_plan: DAGNode, pg: PlacementGroup) -> PhysicalPlan:
        self._validate_pg(pg)
        nodes, input_edges, output_edges = self._get_nodes_and_edges(logical_plan)
        assert len(nodes) >= len(pg.bundle_specs)
        world_size = len(nodes)
        schedules = []
        configs = []
        pgs = []

        for rank, node in enumerate(nodes):
            assert isinstance(node, NNModuleNode)
            schedule = ExecuteSchedule()
            if not input_edges[node]:
                schedule = InputSchedule()
            elif not output_edges[node]:
                schedule = OutputSchedule()
            schedules.append(schedule)

            config = Config(
                world_size,
                rank,
                node.get_input_tensor_shape(),
                node.get_input_tensor_dtype(),
                _get_device_name,
                node.get_model_builder(),
                node.get_data_loader_builder(),
            )
            configs.append(config)
            pgs.append((pg, rank))

        return PhysicalPlan(pg, schedules, configs, pgs)

    def _validate_pg(self, pg: PlacementGroup):
        for bundle in pg.bundle_specs:
            assert bundle == {
                "GPU": 1
            }, "SimplePhysicalPlanner only supports one GPU per replica"

    def _get_nodes_and_edges(dag: DAGNode):
        """Get all unique nodes and edges in the DAG.

        A basic dfs with memorization to get all unique nodes
        and edges in the DAG.
        Unique nodes will be used to generate unique names,
        while edges will be used to construct the graph.
        """

        input_edges = defaultdict(list)
        output_edges = defaultdict(list)
        nodes = []

        def _dfs(node):
            nodes.append(node)
            for child_node in node._get_all_child_nodes():
                input_edges[node].append(child_node)
                output_edges[child_node].append(node)
            return node

        dag.apply_recursive(_dfs)
        return nodes, input_edges, output_edges
