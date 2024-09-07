from ray._private.utils import binary_to_hex
from ray._raylet import PlacementGroupID

from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import (
    DoesNotExist,
    Exists,
    In,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    NotIn,
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)


def covert_pb_to_label_match_expressions(label_match_expressions_pb):
    expressions_map = {}
    for expression in label_match_expressions_pb.expressions:
        key = expression.key
        operator = expression.operator
        if operator.HasField("label_in"):
            values = []
            for value in operator.label_in.values:
                values.append(value)
            expressions_map[key] = In(*values)
        elif operator.HasField("label_not_in"):
            values = []
            for value in operator.label_in.values:
                values.append(value)
            expressions_map[key] = NotIn(*values)
        elif operator.HasField("label_exists"):
            expressions_map[key] = Exists()
        elif operator.HasField("label_does_not_exist"):
            expressions_map[key] = DoesNotExist()
    return expressions_map


def covert_pb_to_scheduling_strategy(scheduling_strategy_pb) -> SchedulingStrategyT:
    if scheduling_strategy_pb.HasField("default_scheduling_strategy"):
        return "DEFAULT"
    elif scheduling_strategy_pb.HasField("spread_scheduling_strategy"):
        return "SPREAD"
    elif scheduling_strategy_pb.HasField("placement_group_scheduling_strategy"):
        pg_pb = scheduling_strategy_pb.placement_group_scheduling_strategy
        return PlacementGroupSchedulingStrategy(
            PlacementGroup(PlacementGroupID(pg_pb.placement_group_id)),
            pg_pb.placement_group_bundle_index,
            pg_pb.placement_group_capture_child_tasks,
        )
    elif scheduling_strategy_pb.HasField("node_affinity_scheduling_strategy"):
        node_affinity_pb = scheduling_strategy_pb.node_affinity_scheduling_strategy
        return NodeAffinitySchedulingStrategy(
            binary_to_hex(node_affinity_pb.node_id),
            node_affinity_pb.soft,
            node_affinity_pb.spill_on_unavailable,
            node_affinity_pb.fail_on_unavailable,
        )
    elif scheduling_strategy_pb.HasField("node_label_scheduling_strategy"):
        node_label_pb = scheduling_strategy_pb.node_label_scheduling_strategy
        return NodeLabelSchedulingStrategy(
            hard=covert_pb_to_label_match_expressions(node_label_pb.hard),
            soft=covert_pb_to_label_match_expressions(node_label_pb.soft),
        )
    else:
        return None
