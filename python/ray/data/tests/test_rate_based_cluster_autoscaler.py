import logging
from dataclasses import dataclass
from typing import List, Optional, Type
from unittest.mock import MagicMock, patch

import pytest

from ray.data._internal.cluster_autoscaler import (
    ClusterAutoscalingMetrics,
    DefaultClusterAutoscalerV2,
    RateBasedClusterAutoscaler,
    create_cluster_autoscaler,
)
from ray.data._internal.cluster_autoscaler.fake_autoscaling_coordinator import (
    FakeAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.rate_based_cluster_autoscaler import (
    _to_resource_bundle,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    ClusterUtil,
    ResourceUtilizationGauge,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data.context import DataContext
from ray.data.tests.conftest import propagate_logs  # noqa


class StubUtilizationGauge(ResourceUtilizationGauge):
    def __init__(self, utilization: Optional[ClusterUtil] = None):
        if utilization is None:
            utilization = ClusterUtil(cpu=1, gpu=1, object_store_memory=1, memory=1)
        self._utilization = utilization

    def observe(self):
        pass

    def get(self):
        return self._utilization


@dataclass(frozen=True)
class StubClusterAutoscalingMetrics(ClusterAutoscalingMetrics):
    """A stub `OpRuntimeMetrics` implementation for testing."""

    average_num_inputs_per_task: Optional[float] = None
    average_num_outputs_per_task: Optional[float] = None
    num_output_blocks_per_task_s: Optional[float] = None


def _make_fake_op(
    *,
    spec: Optional[Type] = None,
    per_task_resource_allocation: ExecutionResources = ExecutionResources(cpu=1),
    min_scheduling_resources: ExecutionResources = ExecutionResources(cpu=1),
    metrics: StubClusterAutoscalingMetrics = StubClusterAutoscalingMetrics(
        num_output_blocks_per_task_s=1,
        average_num_inputs_per_task=1,
        average_num_outputs_per_task=1,
    ),
    output_dependencies: Optional[List[PhysicalOperator]] = None,
    max_concurrency_limit: Optional[int] = None,
    completed: bool = False,
    min_resource_requirements: ExecutionResources = ExecutionResources.zero(),
    max_resource_requirements: ExecutionResources = ExecutionResources.for_limits(),
) -> MagicMock:
    """Create a fake that implements the ``SupportsClusterAutoscaling`` protocol."""
    op = MagicMock(spec=spec) if spec is not None else MagicMock(spec=[])
    op.metrics = metrics
    op.output_dependencies = output_dependencies or []
    op.per_task_resource_allocation = MagicMock(
        return_value=per_task_resource_allocation
    )
    op.min_scheduling_resources = MagicMock(return_value=min_scheduling_resources)
    op.get_max_concurrency_limit = MagicMock(return_value=max_concurrency_limit)
    op.has_completed = MagicMock(return_value=completed)
    op.min_max_resource_requirements = MagicMock(
        return_value=(min_resource_requirements, max_resource_requirements)
    )
    return op


def test_autoscaler_requests_resources_if_no_scalable_ops():
    """Test the autoscaler requests resources even if no ops support cluster
    autoscaling.

    Some operators don't support cluster autoscaling. If a DAG only contains these
    operators, the autoscaler should still request the remaining resources. Otherwise,
    the operators won't get any resources and the pipeline won't run.
    """
    time = 0
    autoscaler = RateBasedClusterAutoscaler(
        ops=[],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            get_time=lambda: time, initial_cluster_resources=[{"CPU": 1}]
        ),
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_request_expire_time_s=1,
    )

    # The autoscaler should immediately request the remaining resources.
    assert autoscaler.get_total_resources() == ExecutionResources(cpu=1)

    # After the specified `autoscaling_request_expire_time_s` has passed, the autoscaler
    # shouldn't get any resources.
    time += 2
    assert autoscaler.get_total_resources() == ExecutionResources()

    # Calling `try_trigger_scaling` should re-request the remaining resources, even if
    # there aren't any scalable ops.
    autoscaler.try_trigger_scaling()
    assert autoscaler.get_total_resources() == ExecutionResources(cpu=1)


@patch(
    "ray.data._internal.cluster_autoscaler.DEFAULT_CLUSTER_AUTOSCALER_VERSION",
    "invalid",
)
def test_invalid_cluster_autoscaler_env_value_raises_value_error():
    with pytest.raises(ValueError):
        create_cluster_autoscaler(
            topology={},
            data_context=DataContext(execution_options=ExecutionOptions()),
            resource_manager=MagicMock(spec=ResourceManager),
            execution_id="test",
        )


@pytest.mark.parametrize(
    "cluster_autoscaler_env_value, expected_autoscaler_type",
    [
        ("RATE_BASED", RateBasedClusterAutoscaler),
        ("V2", DefaultClusterAutoscalerV2),
    ],
)
def test_cluster_autoscaler_env_value_creates_correct_autoscaler(
    cluster_autoscaler_env_value, expected_autoscaler_type
):
    with patch(
        "ray.data._internal.cluster_autoscaler.DEFAULT_CLUSTER_AUTOSCALER_VERSION",
        cluster_autoscaler_env_value,
    ):
        autoscaler = create_cluster_autoscaler(
            topology={},
            data_context=DataContext(execution_options=ExecutionOptions()),
            resource_manager=MagicMock(spec=ResourceManager),
            execution_id="test",
        )

        assert isinstance(autoscaler, expected_autoscaler_type)


@pytest.mark.parametrize("cpu_usage", [0.25, 0.9])
@pytest.mark.parametrize("gpu_usage", [0.25, 0.9])
def test_autoscaler_utilization_threshold(cpu_usage, gpu_usage):
    """Test autoscaler scaling behavior based on cluster utilization thresholds.

    Tests all combinations of cpu and gpu utilization values.
    The autoscaler should scale up if CPU or GPU utilization exceeds the 0.75 threshold.
    """
    threshold = 0.75

    cpu_op = _make_fake_op()

    utilization = ClusterUtil(cpu=cpu_usage, gpu=gpu_usage)

    autoscaler = RateBasedClusterAutoscaler(
        ops=[cpu_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(utilization),
        autoscaling_coordinator=FakeAutoscalingCoordinator(),
        min_gap_between_autoscaling_requests_s=0,
        cluster_scaling_up_util_threshold=ClusterUtil(
            cpu=threshold,
            gpu=threshold,
            memory=threshold,
            object_store_memory=threshold,
        ),
    )

    result = autoscaler.try_trigger_scaling()

    over_threshold = cpu_usage >= threshold or gpu_usage >= threshold
    if over_threshold:
        # Should return non-empty list of resource bundles
        assert result is not None and len(result) > 0
    else:
        # Should return empty list when under threshold
        assert result == []


@pytest.mark.parametrize(
    "min_scheduling_resources,initial_allocation,max_cpu_delta,max_gpu_delta,expected_total_bundle_count",
    [
        # CPU-only operator: 4 tasks allocated, scaling factor 2x = 4 additional tasks
        # Max CPU delta 256 / 1 CPU per task = 256 bundles allowed, so no capping
        # Total = current (4) + additional (4) = 8
        (
            ExecutionResources(cpu=1),
            [{"CPU": 1}] * 4,
            256.0,
            32.0,
            8,  # 4 current + 4 additional
        ),
        # CPU-only operator: 100 tasks allocated, scaling factor 2x = 100 additional
        # Max CPU delta 50 / 1 CPU per task = 50 bundles allowed (capped)
        # Total = current (100) + additional (50 capped) = 150
        (
            ExecutionResources(cpu=1),
            [{"CPU": 1}] * 100,
            50.0,
            32.0,
            150,  # 100 current + 50 additional (capped by max_cpu_delta)
        ),
        # GPU operator: 8 GPUs allocated, 2 GPU per task = 4 tasks
        # scaling factor 2x = 4 additional tasks
        # Max GPU delta 32 / 2 GPU per task = 16 bundles allowed, so no capping
        # Total = current (4) + additional (4) = 8
        (
            ExecutionResources(gpu=2),
            [{"GPU": 2}] * 4,
            256.0,
            32.0,
            8,  # 4 current + 4 additional
        ),
        # GPU operator: Max GPU delta 4 / 2 GPU per task = 2 bundles allowed (capped)
        # Total = current (4) + additional (2 capped) = 6
        (
            ExecutionResources(gpu=2),
            [{"GPU": 2}] * 4,
            256.0,
            4.0,
            6,  # 4 current + 2 additional (capped by max_gpu_delta)
        ),
        # Mixed CPU+GPU operator: GPU is the limiting factor for both task count and delta
        # Total = current (4) + additional (2 capped) = 6
        (
            ExecutionResources(cpu=4, gpu=1),
            [{"CPU": 4, "GPU": 1}] * 4,  # 4 tasks based on GPU
            256.0,
            2.0,  # Allows only 2 additional bundles
            6,  # 4 current + 2 additional (capped by GPU delta)
        ),
    ],
)
def test_autoscaler_requests_correct_bundle_count(
    min_scheduling_resources: ExecutionResources,
    initial_allocation: ExecutionResources,
    max_cpu_delta: float,
    max_gpu_delta: float,
    expected_total_bundle_count: int,
):
    """Test that autoscaler requests total bundles (current + capped additional) and respects delta caps."""
    op = _make_fake_op(
        min_scheduling_resources=min_scheduling_resources,
        per_task_resource_allocation=min_scheduling_resources,
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=initial_allocation
        ),
        min_gap_between_autoscaling_requests_s=0,
        cluster_scaling_up_max_resource_delta=ExecutionResources(
            cpu=max_cpu_delta, gpu=max_gpu_delta
        ),
    )

    result = autoscaler.try_trigger_scaling()

    assert len(result) == expected_total_bundle_count
    # Each bundle should match the min_scheduling_resources (excluding object_store_memory and zeros)
    expected_bundle = _to_resource_bundle(min_scheduling_resources)
    for bundle in result:
        assert bundle == expected_bundle

    # Trigger scaling with low utilization. The cluster autoscaler should re-request the previous resources.
    autoscaler._utility_calculator = StubUtilizationGauge(ClusterUtil(cpu=0.1))
    requested_resources_low_util = autoscaler.try_trigger_scaling()
    assert requested_resources_low_util == result


@pytest.mark.parametrize(
    "max_concurrency_limit, initial_cluster_resources,min_scheduling_resources",
    [
        # Case 1: Current usage (4) + min_scheduling (1) = 5 > max (4) -> don't scale
        (
            4,
            [{"CPU": 4}],
            ExecutionResources(cpu=1),
        ),
        # Case 2: Current usage (3) + min_scheduling (1) = 4 <= max (4) -> scale
        (
            4,
            [{"CPU": 3}],
            ExecutionResources(cpu=1),
        ),
        # Case 3: Heterogeneous - CPU at limit (4) but GPU below limit (2)
        # Adding one more task: CPU 4+1=5 > 4 (exceeds), GPU 2+1=3 <= 4 (within limit)
        # Should not scale because CPU exceeds limit
        (
            4,
            [{"CPU": 4, "GPU": 2}],
            ExecutionResources(cpu=1, gpu=1),
        ),
    ],
)
def test_autoscaler_skips_scaling_when_at_max_schedulable_tasks(
    max_concurrency_limit: int,
    initial_cluster_resources: ExecutionResources,
    min_scheduling_resources: ExecutionResources,
):
    """Test that autoscaler skips scaling when bottleneck operator would exceed max resource limits."""

    # Set up operator with min_scheduling_resources
    op = _make_fake_op(
        min_scheduling_resources=min_scheduling_resources,
        per_task_resource_allocation=min_scheduling_resources,
        max_concurrency_limit=max_concurrency_limit,
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=initial_cluster_resources
        ),
        min_gap_between_autoscaling_requests_s=0,
    )

    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    expected_max_resources = min_scheduling_resources.scale(max_concurrency_limit)
    assert resources_after_scaling.satisfies_limit(expected_max_resources)


def test_does_not_fail_with_zero_logical_resources():
    # Regression test: an operator requiring zero logical resources (e.g. a
    # `num_cpus=0` map) yields an infinite bundle count, which used to trip the
    # assertion that bundle counts are finite.
    op = _make_fake_op(
        min_scheduling_resources=ExecutionResources.zero(),
        metrics=StubClusterAutoscalingMetrics(
            num_output_blocks_per_task_s=1,
            average_num_inputs_per_task=1,
            average_num_outputs_per_task=1,
        ),
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(),
        min_gap_between_autoscaling_requests_s=0,
    )

    # Should not raise an assertion error about non-finite bundle counts.
    autoscaler.try_trigger_scaling()

    assert autoscaler.get_total_resources() == ExecutionResources.zero()


def test_autoscaler_requests_at_least_one_bundle_when_no_allocation():
    """Test that autoscaler requests at least 1 bundle even when current allocation is 0."""
    op = _make_fake_op(
        min_scheduling_resources=ExecutionResources(cpu=2, gpu=1),
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(),
        min_gap_between_autoscaling_requests_s=0,
    )

    result = autoscaler.try_trigger_scaling()

    # Should still request at least 1 bundle
    assert len(result) >= 1
    # Bundle should have CPU=2, GPU=1 (no object_store_memory or zero values)
    assert result[0] == {"CPU": 2, "GPU": 1}


def test_object_store_memory_adds_cpu_bundles_when_global_util_high():
    """When global object store util is high, expect CPU bundles added."""
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=False)
    autoscaler = RateBasedClusterAutoscaler(
        ops=[all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(object_store_memory=1)),
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
    )

    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    assert resources_after_scaling.cpu == 8


def test_object_store_memory_skips_scaling_when_util_low():
    """When global object store util is below threshold, expect no scaling."""
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=False)
    autoscaler = RateBasedClusterAutoscaler(
        ops=[all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(object_store_memory=0)),
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
    )

    resources_before_scaling = autoscaler.get_total_resources()
    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    has_not_scaled = resources_after_scaling.satisfies_limit(resources_before_scaling)
    assert has_not_scaled, (resources_after_scaling, resources_before_scaling)


def test_object_store_memory_skips_scaling_when_all_all_to_all_completed():
    """When all all-to-all ops are completed, expect no obj mem scaling even if util high."""
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=True)
    autoscaler = RateBasedClusterAutoscaler(
        ops=[all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(object_store_memory=1.0)),
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
    )

    resources_before_scaling = autoscaler.get_total_resources()
    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    has_not_scaled = resources_after_scaling.satisfies_limit(resources_before_scaling)
    assert has_not_scaled, (resources_after_scaling, resources_before_scaling)


def test_object_store_memory_respects_cpu_delta_cap():
    """When requested bundles exceed max CPU delta, expect result capped."""
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=False)
    max_resource_delta = ExecutionResources(cpu=1)
    autoscaler = RateBasedClusterAutoscaler(
        ops=[all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(object_store_memory=1)),
        min_gap_between_autoscaling_requests_s=0,
        cluster_scaling_up_max_resource_delta=max_resource_delta,
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
    )

    resources_before_scaling = autoscaler.get_total_resources()
    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    delta = resources_after_scaling.subtract(resources_before_scaling)
    assert delta.satisfies_limit(max_resource_delta), (delta, max_resource_delta)


def test_combined_bottleneck_and_object_store_memory_adds_bundles_from_both():
    """When both bottleneck and object store memory need scaling, expect bundles from both."""
    map_op = _make_fake_op(
        min_scheduling_resources=ExecutionResources(gpu=1),
        per_task_resource_allocation=ExecutionResources(gpu=1),
    )
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=False)

    autoscaler = RateBasedClusterAutoscaler(
        ops=[map_op, all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(
            ClusterUtil(gpu=1, object_store_memory=1)
        ),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4, "GPU": 1}]
        ),
        min_gap_between_autoscaling_requests_s=0,
    )

    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    # Since both object store memory and logical resource utilization are above the
    # thresholds, the autoscaler should both double the throughput of the pipeline by
    # requesting another GPU, and also double the total number of CPUs to decrease
    # object store memory pressure.
    assert resources_after_scaling == ExecutionResources(cpu=8, gpu=2)


def test_log_resource_request_emits_correct_message(
    propagate_logs, caplog  # noqa: F811
):
    resource_request = [{"CPU": 1}, {"CPU": 2, "GPU": 1}, {"CPU": 1}]

    with caplog.at_level(logging.DEBUG):
        RateBasedClusterAutoscaler._log_resource_request(resource_request)

    expected_message = (
        "Sending resource request: [{'CPU': 1}] * 2, [{'CPU': 2, 'GPU': 1}] * 1"
    )
    assert expected_message in caplog.text


def test_autoscaler_scales_when_memory_utilization_high():
    op = _make_fake_op(
        min_scheduling_resources=ExecutionResources(memory=1 * 1024**3),
        per_task_resource_allocation=ExecutionResources(memory=1 * 1024**3),
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(memory=1)),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"memory": 1 * 1024**3}]
        ),
        min_gap_between_autoscaling_requests_s=0,
    )

    resources_before_scaling = autoscaler.get_total_resources()
    autoscaler.try_trigger_scaling()
    resources_after_scaling = autoscaler.get_total_resources()

    assert resources_after_scaling.memory > resources_before_scaling.memory


def test_autoscaler_respects_memory_delta_cap():
    op = _make_fake_op(
        min_scheduling_resources=ExecutionResources(memory=1 * 1024**3),
        per_task_resource_allocation=ExecutionResources(memory=1 * 1024**3),
    )
    max_resource_delta = ExecutionResources.for_limits(memory=1 * 1024**3)
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(memory=1)),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"memory": 1 * 1024**3}] * 4
        ),
        min_gap_between_autoscaling_requests_s=0,
        cluster_scaling_up_max_resource_delta=max_resource_delta,
    )

    resources_before = autoscaler.get_total_resources()
    autoscaler.try_trigger_scaling()
    resources_after = autoscaler.get_total_resources()

    delta = resources_after.subtract(resources_before)
    assert delta.satisfies_limit(max_resource_delta), (delta, max_resource_delta)
    assert delta.memory > 0, (delta, max_resource_delta)


def test_autoscaler_does_not_crash_when_task_produces_no_data():
    """Regression test for tasks that produce no output data.

    The autoscaler uses the average number of outputs per task to normalize
    throughput rates. If an operator produces no data, the implementation
    previously normalized the rate by 0 and failed an assertion that rates must
    be positive.

    This can happen in practice when UDFs filter data.
    """
    downstream_op = _make_fake_op(
        metrics=StubClusterAutoscalingMetrics(
            # Downstream operator hasn't produced any data yet.
            num_output_blocks_per_task_s=0.0,
            average_num_inputs_per_task=1.0,
            average_num_outputs_per_task=0.0,
        ),
        output_dependencies=[],
    )
    upstream_op = _make_fake_op(
        metrics=StubClusterAutoscalingMetrics(
            num_output_blocks_per_task_s=1.0,
            average_num_inputs_per_task=1.0,
            average_num_outputs_per_task=1.0,
        ),
        output_dependencies=[downstream_op],
    )

    autoscaler = RateBasedClusterAutoscaler(
        ops=[upstream_op, downstream_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
        min_gap_between_autoscaling_requests_s=0,
    )

    # Before the fix this raised: `AssertionError: Rates must be positive`
    autoscaler.try_trigger_scaling()


def test_autoscaler_passes_label_selector_to_coordinator(monkeypatch):
    """``RateBasedClusterAutoscaler`` forwards ``label_selector`` to the
    ``DefaultAutoscalingCoordinator`` it constructs as ``subcluster_selector``."""
    from ray.data._internal.cluster_autoscaler import rate_based_cluster_autoscaler

    captured = {}

    class _StubProxy:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def request_resources(self, *args, **kwargs):
            pass

    monkeypatch.setattr(
        rate_based_cluster_autoscaler, "DefaultAutoscalingCoordinator", _StubProxy
    )
    RateBasedClusterAutoscaler(
        ops=[],
        execution_id="exec-1",
        utility_calculator=StubUtilizationGauge(),
        label_selector={"ray-subcluster": "training"},
    )
    assert captured["subcluster_selector"] == {"ray-subcluster": "training"}


def test_create_reads_label_selector_from_execution_options(monkeypatch):
    """``RateBasedClusterAutoscaler.create`` reads ``label_selector`` from the
    ``ExecutionOptions`` and forwards it to the coordinator as
    ``subcluster_selector``."""
    from ray.data._internal.cluster_autoscaler import rate_based_cluster_autoscaler

    captured = {}

    class _StubProxy:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def request_resources(self, *args, **kwargs):
            pass

    monkeypatch.setattr(
        rate_based_cluster_autoscaler, "DefaultAutoscalingCoordinator", _StubProxy
    )
    execution_options = ExecutionOptions(label_selector={"ray-subcluster": "training"})
    RateBasedClusterAutoscaler.create(
        topology=[],
        execution_options=execution_options,
        resource_manager=MagicMock(spec=ResourceManager),
        execution_id="exec-1",
    )
    assert captured["subcluster_selector"] == {"ray-subcluster": "training"}


def test_fractional_resource_and_high_object_store_utilization_does_not_crash():
    """Regression test for fractional resources during object store padding.

    The autoscaler used to raise `TypeError: can't multiply sequence by non-int
    of type 'float'` when padding the resource request to indirectly request
    object store memory.

    This bug happens when:
    1. You have an incomplete all-to-all op.
    2. The object store utilization is higher than the threshold.
    3. You use fractional resources.
    """
    all_to_all_op = _make_fake_op(spec=AllToAllOperator, completed=False)
    op = _make_fake_op(
        min_scheduling_resources=ExecutionResources(cpu=0.1),
        per_task_resource_allocation=ExecutionResources(cpu=0.1),
        output_dependencies=[all_to_all_op],
    )
    autoscaler = RateBasedClusterAutoscaler(
        ops=[op, all_to_all_op],
        execution_id="test",
        utility_calculator=StubUtilizationGauge(ClusterUtil(object_store_memory=1)),
        autoscaling_coordinator=FakeAutoscalingCoordinator(
            initial_cluster_resources=[{"CPU": 4}]
        ),
        min_gap_between_autoscaling_requests_s=0,
    )

    autoscaler.try_trigger_scaling()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
