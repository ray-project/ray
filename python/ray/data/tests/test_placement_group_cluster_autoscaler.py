from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.cluster_autoscaler import create_cluster_autoscaler
from ray.data._internal.cluster_autoscaler.fake_autoscaling_coordinator import (
    FakeAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.placement_group_cluster_autoscaler import (
    PlacementGroupClusterAutoscaler,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_sends_pg_bundles_as_resource_request():
    bundles = [{"CPU": 4, "GPU": 1}, {"CPU": 4, "GPU": 1}]
    pg = MagicMock(spec=PlacementGroup, id="pg-1", bundle_specs=bundles)
    strategy = PlacementGroupSchedulingStrategy(placement_group=pg)

    coordinator = FakeAutoscalingCoordinator()
    autoscaler = PlacementGroupClusterAutoscaler(
        execution_id="test",
        scheduling_strategy=strategy,
        scheduling_strategy_large_args=strategy,
        autoscaling_coordinator=coordinator,
        min_gap_between_autoscaling_requests_s=0,
    )

    resources = autoscaler.get_total_resources()
    assert resources == ExecutionResources(cpu=8, gpu=2)


def test_unions_bundles_from_two_distinct_pgs():
    pg1 = MagicMock(spec=PlacementGroup, id="pg-1", bundle_specs=[{"CPU": 2, "GPU": 1}])
    strategy1 = PlacementGroupSchedulingStrategy(placement_group=pg1)
    pg2 = MagicMock(spec=PlacementGroup, id="pg-2", bundle_specs=[{"CPU": 4}])
    strategy2 = PlacementGroupSchedulingStrategy(placement_group=pg2)

    coordinator = FakeAutoscalingCoordinator()
    autoscaler = PlacementGroupClusterAutoscaler(
        execution_id="test",
        scheduling_strategy=strategy1,
        scheduling_strategy_large_args=strategy2,
        autoscaling_coordinator=coordinator,
        min_gap_between_autoscaling_requests_s=0,
    )

    resources = autoscaler.get_total_resources()
    assert resources == ExecutionResources(cpu=6, gpu=1)


def test_deduplicates_same_pg():
    bundles = [{"CPU": 4, "GPU": 1}]
    pg1 = MagicMock(spec=PlacementGroup, id="same-pg", bundle_specs=bundles)
    strategy1 = PlacementGroupSchedulingStrategy(placement_group=pg1)
    pg2 = MagicMock(spec=PlacementGroup, id="same-pg", bundle_specs=bundles)
    strategy2 = PlacementGroupSchedulingStrategy(placement_group=pg2)

    coordinator = FakeAutoscalingCoordinator()
    autoscaler = PlacementGroupClusterAutoscaler(
        execution_id="test",
        scheduling_strategy=strategy1,
        scheduling_strategy_large_args=strategy2,
        autoscaling_coordinator=coordinator,
        min_gap_between_autoscaling_requests_s=0,
    )

    resources = autoscaler.get_total_resources()
    assert resources == ExecutionResources(cpu=4, gpu=1)


def test_create_cluster_autoscaler_returns_pg_autoscaler(restore_data_context):
    pg = MagicMock(spec=PlacementGroup, id="pg-1", bundle_specs=[{"CPU": 1}])
    strategy = PlacementGroupSchedulingStrategy(pg)
    data_context = ray.data.DataContext.get_current()
    data_context.scheduling_strategy = strategy
    data_context.scheduling_strategy_large_args = strategy

    autoscaler = create_cluster_autoscaler(
        topology={},
        resource_manager=MagicMock(spec=ResourceManager),
        data_context=data_context,
        execution_id="test-pg",
    )

    assert isinstance(autoscaler, PlacementGroupClusterAutoscaler)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
