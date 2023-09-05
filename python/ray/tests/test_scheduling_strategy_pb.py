import os
import sys
import pytest
import ray
from ray._private.scheduling_strategies_util import covert_pb_to_scheduling_strategy
from ray.core.generated import common_pb2
from ray.util.scheduling_strategies import (
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)


def test_default_scheduling_strategy_pb():
    scheduling_strategy_pb = common_pb2.SchedulingStrategy()
    scheduling_strategy_pb.default_scheduling_strategy.CopyFrom(
        common_pb2.DefaultSchedulingStrategy()
    )
    assert covert_pb_to_scheduling_strategy(scheduling_strategy_pb) == "DEFAULT"


def test_spread_scheduling_strategy_pb():
    scheduling_strategy_pb = common_pb2.SchedulingStrategy()
    scheduling_strategy_pb.spread_scheduling_strategy.CopyFrom(
        common_pb2.SpreadSchedulingStrategy()
    )
    assert covert_pb_to_scheduling_strategy(scheduling_strategy_pb) == "SPREAD"


def test_placement_scheduling_strategy_pb(ray_start_cluster_head):
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    scheduling_strategy_pb = common_pb2.SchedulingStrategy()
    pg_pb = common_pb2.PlacementGroupSchedulingStrategy()
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    pg_pb.placement_group_id = pg.id.binary()
    pg_pb.placement_group_bundle_index = 0
    pg_pb.placement_group_capture_child_tasks = True
    scheduling_strategy_pb.placement_group_scheduling_strategy.CopyFrom(pg_pb)
    assert covert_pb_to_scheduling_strategy(
        scheduling_strategy_pb
    ) == PlacementGroupSchedulingStrategy(pg, 0, True)


def test_node_affinity_scheduling_strategy_pb():
    scheduling_strategy_pb = common_pb2.SchedulingStrategy()
    node_affinity_pb = common_pb2.NodeAffinitySchedulingStrategy()
    node_id = ray.NodeID.from_random()
    node_affinity_pb.node_id = node_id.binary()
    node_affinity_pb.soft = True
    node_affinity_pb.spill_on_unavailable = False
    node_affinity_pb.fail_on_unavailable = True
    scheduling_strategy_pb.node_affinity_scheduling_strategy.CopyFrom(node_affinity_pb)
    assert covert_pb_to_scheduling_strategy(
        scheduling_strategy_pb
    ) == NodeAffinitySchedulingStrategy(node_id.hex(), True, False, True)


def test_node_label_scheduling_strategy_pb():
    scheduling_strategy_pb = common_pb2.SchedulingStrategy()
    node_label_pb = common_pb2.NodeLabelSchedulingStrategy()

    label_in = common_pb2.LabelIn()
    label_in.values.extend(["value1", "value2"])
    label_operator = common_pb2.LabelOperator()
    label_operator.label_in.CopyFrom(label_in)

    label_match_expression = common_pb2.LabelMatchExpression()
    label_match_expression.key = "label_key"
    label_match_expression.operator.CopyFrom(label_operator)

    label_match_expressions = common_pb2.LabelMatchExpressions()
    label_match_expressions.expressions.append(label_match_expression)
    node_label_pb.hard.CopyFrom(label_match_expressions)

    scheduling_strategy_pb.node_label_scheduling_strategy.CopyFrom(node_label_pb)
    assert covert_pb_to_scheduling_strategy(
        scheduling_strategy_pb
    ) == NodeLabelSchedulingStrategy(hard={"label_key": In("value1", "value2")})


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
