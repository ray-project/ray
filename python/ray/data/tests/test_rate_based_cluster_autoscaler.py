import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, List, Optional, Type
from unittest.mock import MagicMock

import pytest

from ray.data._internal.cluster_autoscaler import (
    CLUSTER_AUTOSCALER_ENV_KEY,
    DefaultClusterAutoscalerV2,
    RateBasedClusterAutoscaler,
    ResourceDict,
    create_cluster_autoscaler,
)
from ray.data._internal.cluster_autoscaler.base_autoscaling_coordinator import (
    STANDARD_RESOURCE_TYPES,
)
from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
    NodeResources,
)
from ray.data._internal.cluster_autoscaler.fake_autoscaling_coordinator import (
    FakeAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    ClusterUtil,
    ResourceUtilizationGauge,
)
from ray.data._internal.cluster_autoscaler.shape_requests import (
    ScaleUpKeepAlive,
    collect_active_requests,
    distribute_bundles,
    select_over_utilized_shapes,
    to_resource_bundle,
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
class StubClusterAutoscalingMetrics:
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
    resource_requests: Optional[List[ResourceDict]] = None,
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
    # The exact requests of the operator's in-flight tasks/actors. Empty for
    # every op that doesn't exercise the exact path, so the request falls back to
    # the operator's logical bundle.
    op.get_resource_requests = MagicMock(
        return_value=[] if resource_requests is None else resource_requests
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


def test_invalid_cluster_autoscaler_env_value_raises_value_error(monkeypatch):
    monkeypatch.setenv(CLUSTER_AUTOSCALER_ENV_KEY, "invalid")

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
    cluster_autoscaler_env_value, expected_autoscaler_type, monkeypatch
):
    monkeypatch.setenv(CLUSTER_AUTOSCALER_ENV_KEY, cluster_autoscaler_env_value)

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
    initial_allocation: List[ResourceDict],
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

    assert result is not None
    assert len(result) == expected_total_bundle_count
    # Each bundle should match the min_scheduling_resources (excluding object_store_memory and zeros)
    expected_bundle = to_resource_bundle(min_scheduling_resources)
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
    initial_cluster_resources: List[ResourceDict],
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

    assert result is not None
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
    resource_request = [{"CPU": 1.0}, {"CPU": 2.0, "GPU": 1.0}, {"CPU": 1.0}]

    with caplog.at_level(logging.DEBUG):
        RateBasedClusterAutoscaler._log_resource_request(resource_request)

    expected_message = (
        "Sending resource request: [{'CPU': 1.0}] * 2, [{'CPU': 2.0, 'GPU': 1.0}] * 1"
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
        topology={},
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


class _MutableUtilizationGauge(ResourceUtilizationGauge):
    """A gauge whose reading can be changed mid-test."""

    def __init__(self, utilization: ClusterUtil):
        self.utilization = utilization

    def observe(self):
        pass

    def get(self):
        return self.utilization


_HIGH_UTIL = ClusterUtil(cpu=0.9, gpu=0.9, memory=0.9, object_store_memory=0.9)
_LOW_UTIL = ClusterUtil(cpu=0.0, gpu=0.0, memory=0.0, object_store_memory=0.0)


def _make_release_delay_autoscaler(gauge, get_time) -> RateBasedClusterAutoscaler:
    return RateBasedClusterAutoscaler(
        ops=[_make_fake_op()],
        execution_id="test_low_util_release",
        utility_calculator=gauge,
        autoscaling_coordinator=FakeAutoscalingCoordinator(get_time=get_time),
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_request_expire_time_s=3600,
        low_util_request_release_delay_s=100,
        get_time=get_time,
    )


def test_low_utilization_grace_period_keeps_explicit_request():
    """Below the scale-up threshold, the last explicit request is resent briefly.

    This avoids immediately dropping explicit autoscaler demand. Port of OSS
    #62592's regression tests; same semantics as `DefaultClusterAutoscalerV2`.
    """
    current_time = {"t": 0.0}
    gauge = _MutableUtilizationGauge(_HIGH_UTIL)
    autoscaler = _make_release_delay_autoscaler(gauge, lambda: current_time["t"])

    current_time["t"] = 10.0
    request = autoscaler.try_trigger_scaling()
    assert request
    reserved = autoscaler.get_total_resources()
    assert reserved != ExecutionResources.zero()

    # Low utilization within the grace window: the request is kept alive verbatim.
    gauge.utilization = _LOW_UTIL
    current_time["t"] = 20.0
    assert autoscaler.try_trigger_scaling() == request
    assert autoscaler.get_total_resources() == reserved


def test_low_utilization_after_grace_sends_empty_request():
    """After the grace window, low utilization renews with an empty request.

    This is what lets idle termination scale the cluster down during a long
    low-utilization tail: without the release, the request sized during the
    high-throughput phase pins every node until the dataset finishes.
    """
    current_time = {"t": 0.0}
    gauge = _MutableUtilizationGauge(_HIGH_UTIL)
    autoscaler = _make_release_delay_autoscaler(gauge, lambda: current_time["t"])

    current_time["t"] = 10.0
    assert autoscaler.try_trigger_scaling()

    # 190s of low utilization > the 100s release delay: the request is released.
    gauge.utilization = _LOW_UTIL
    current_time["t"] = 200.0
    assert autoscaler.try_trigger_scaling() == []
    assert autoscaler.get_total_resources() == ExecutionResources.zero()


def test_high_utilization_after_release_rearms_grace_window():
    """A new non-empty request after a release re-arms the grace window.

    Also pins that keep-alive resends do NOT refresh the window: the window is
    measured from the last genuine non-empty request, otherwise it never closes.
    """
    current_time = {"t": 0.0}
    gauge = _MutableUtilizationGauge(_HIGH_UTIL)
    autoscaler = _make_release_delay_autoscaler(gauge, lambda: current_time["t"])

    current_time["t"] = 10.0
    assert autoscaler.try_trigger_scaling()

    gauge.utilization = _LOW_UTIL
    current_time["t"] = 200.0
    assert autoscaler.try_trigger_scaling() == []

    # Utilization recovers: a fresh non-empty request re-arms the window.
    gauge.utilization = _HIGH_UTIL
    current_time["t"] = 210.0
    request = autoscaler.try_trigger_scaling()
    assert request

    # Within the new window, the request is kept alive...
    gauge.utilization = _LOW_UTIL
    current_time["t"] = 220.0
    assert autoscaler.try_trigger_scaling() == request

    # ...still kept at t=305, just inside the window armed at t=210...
    current_time["t"] = 305.0
    assert autoscaler.try_trigger_scaling() == request

    # ...but the keep-alives did not refresh the window (armed at t=210),
    # so at t=315 the request is released again.
    current_time["t"] = 315.0
    assert autoscaler.try_trigger_scaling() == []
    assert autoscaler.get_total_resources() == ExecutionResources.zero()


class _RecordingCoordinator(FakeAutoscalingCoordinator):
    """Records every request the autoscaler sends to the coordinator."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.requests: List[ResourceDict] = []

    def request_resources(self, resources, **kwargs):
        self.requests.append(resources)
        super().request_resources(resources, **kwargs)


class _ScriptedClusterCoordinator(_RecordingCoordinator):
    """Records requests and reports a cluster view that can be scripted.

    The fake's cluster view normally follows whatever was requested last, which
    makes it impossible to model "the scale-up landed and freed up capacity".
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cluster_resources = list(self._initial_cluster_resources)

    def get_reserved_resources(self) -> NodeResources:
        return {f"node_{i}": dict(r) for i, r in enumerate(self.cluster_resources)}


def _make_exact_path_autoscaler(
    requests: List[ResourceDict],
    *,
    initial_cluster_resources: Optional[List[ResourceDict]] = None,
    utilization: ClusterUtil = _HIGH_UTIL,
    get_time: Optional[Callable[[], float]] = None,
    metrics: Optional[StubClusterAutoscalingMetrics] = None,
    coordinator_cls=FakeAutoscalingCoordinator,
):
    """Build a rate-based autoscaler with one operator reporting exact requests.

    Returns the coordinator, the autoscaler, and the operator. The operator's
    reported requests can be changed mid-test to simulate tasks finishing, and
    the gauge's utilization can be changed to move between the scaling path and
    the low-utilization keep-alive.
    """
    op_kwargs = {"resource_requests": requests}
    if metrics is not None:
        op_kwargs["metrics"] = metrics
    operator = _make_fake_op(**op_kwargs)
    # Both fakes default to ``time.time``; pass it through so a test can supply
    # a controllable clock.
    get_time = get_time or time.time
    coordinator = coordinator_cls(
        initial_cluster_resources=initial_cluster_resources, get_time=get_time
    )
    # The shape check measures demand against the cluster's worker groups. The
    # fakes script that cluster (mutating ``cluster_resources`` models a scale-up
    # landing), so expose it as the node view ``ray.nodes()`` would give.
    initial_cluster = list(initial_cluster_resources or [])

    def get_node_resources() -> NodeResources:
        cluster = getattr(coordinator, "cluster_resources", initial_cluster)
        return {f"node_{i}": dict(r) for i, r in enumerate(cluster)}

    autoscaler = RateBasedClusterAutoscaler(
        ops=[operator],
        execution_id="exact-path",
        utility_calculator=_MutableUtilizationGauge(utilization),
        autoscaling_coordinator=coordinator,
        min_gap_between_autoscaling_requests_s=0,
        get_time=get_time,
        get_node_resources=get_node_resources,
    )
    return coordinator, autoscaler, operator


@pytest.mark.parametrize(
    "shapes,count,expected",
    [
        # Fewer bundles than shapes: scale the most in-demand ones.
        (
            {(("CPU", 1),): 5, (("GPU", 1),): 1, (("memory", 1),): 1},
            1,
            [(("CPU", 1),)],
        ),
        (
            {(("CPU", 1),): 5, (("GPU", 1),): 1, (("memory", 1),): 1},
            2,
            [(("CPU", 1),), (("GPU", 1),)],
        ),
        # One bundle per shape once every shape has one, the rest by demand.
        (
            {(("CPU", 1),): 3, (("GPU", 1),): 1},
            2,
            [(("CPU", 1),), (("GPU", 1),)],
        ),
        (
            {(("CPU", 1),): 3, (("GPU", 1),): 1},
            4,
            [(("CPU", 1),), (("CPU", 1),), (("CPU", 1),), (("GPU", 1),)],
        ),
        # A single shape just repeats.
        ({(("CPU", 1),): 3}, 4, [(("CPU", 1),)] * 4),
        ({(("CPU", 1),): 3}, 0, []),
    ],
)
def test_distribute_bundles(shapes, count, expected):
    """Scale-up copies follow the operator's own mix of in-flight shapes."""
    assert distribute_bundles(shapes, count) == expected


def test_active_request_collection_from_all_ops():
    """Every operator's exact requests are collected, empty ones dropped."""
    actor_pool_operator = _make_fake_op(resource_requests=[{"GPU": 1}])
    regular_operator = _make_fake_op(resource_requests=[])
    # Shuffle operators report their aggregator / rank actors, which are real
    # demand carrying a shape of their own.
    shuffle_operator = _make_fake_op(
        spec=AllToAllOperator,
        resource_requests=[{"CPU": 1}],
    )

    active_requests = collect_active_requests(
        [actor_pool_operator, regular_operator, shuffle_operator]
    )

    assert active_requests[actor_pool_operator] == [{"GPU": 1}]
    assert active_requests[regular_operator] == []
    assert active_requests[shuffle_operator] == [{"CPU": 1}]


def test_exact_path_preserves_custom_resources():
    """A custom resource reaches the request instead of being flattened away.

    ``min_scheduling_resources`` is an `ExecutionResources`, which cannot express
    a custom resource, so requesting it instead would ask for a node the actors
    can never run on.
    """
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"GPU": 1, "worker_group_a": 1}],
        # No node can host ``worker_group_a``, so the shape is infeasible.
        initial_cluster_resources=[{"CPU": 1}],
    )

    autoscaler.try_trigger_scaling()

    # The active request plus one scale-up copy, both with the exact shape.
    assert coordinator._allocation.resources == [
        {"GPU": 1, "worker_group_a": 1},
        {"GPU": 1, "worker_group_a": 1},
    ]


def test_request_never_drops_below_in_flight_demand():
    """In-flight demand is retained even when the solver wants fewer bundles.

    An operator with no throughput rate yet only gets one bundle, so without
    this the request would fall below what is already running.
    """
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1}] * 3,
        initial_cluster_resources=[{"CPU": 100}],
        # Plenty of capacity, so nothing needs to scale up.
        utilization=_LOW_UTIL,
    )

    autoscaler.try_trigger_scaling()

    assert coordinator._allocation.resources == [{"CPU": 1}] * 3


def test_scale_up_bundles_follow_the_operators_own_shapes():
    """A shortage of one shape must not request another shape."""
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1}] * 3 + [{"GPU": 1}],
        initial_cluster_resources=[{"CPU": 4}],
    )

    autoscaler.try_trigger_scaling()

    bundle_counts = Counter(
        tuple(sorted(bundle.items())) for bundle in coordinator._allocation.resources
    )
    # 4 in-flight requests (3 CPU + 1 GPU) scaled to 8 bundles: the 4 extra
    # bundles follow the same 3:1 mix.
    assert bundle_counts == {
        (("CPU", 1),): 6,
        (("GPU", 1),): 2,
    }


def test_over_utilized_shape_scales_when_cluster_utilization_is_low():
    """The cluster-wide ratio can't see a custom resource, so the shape decides.

    A pool whose actors need a custom resource looks idle in the aggregate
    utilization, no matter how starved that resource is.
    """
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1, "worker_group_a": 1}],
        # The single node that can host the shape holds a single unit of it, so
        # the shape is at 100% of its own capacity...
        initial_cluster_resources=[{"CPU": 1, "worker_group_a": 1}],
        # ...while every standard resource looks idle.
        utilization=_LOW_UTIL,
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    autoscaler.try_trigger_scaling()

    assert coordinator._allocation.resources == [
        {"CPU": 1, "worker_group_a": 1},
        {"CPU": 1, "worker_group_a": 1},
    ]


def test_over_utilized_shape_scales_when_solver_is_silent():
    """The over-utilized shape must reach the request, not just the early return.

    Without a throughput rate the solver asks for nothing, so the shape's own
    utilization is the only signal left. It used to be dropped after suppressing
    the low-utilization return, which meant a starved custom resource requested
    a scale-up copy of nothing.
    """
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1, "worker_group_a": 1}],
        initial_cluster_resources=[{"CPU": 1, "worker_group_a": 1}],
        utilization=_LOW_UTIL,
        # No rate: the optimal allocation is undefined, so the solver is silent.
        metrics=StubClusterAutoscalingMetrics(num_output_blocks_per_task_s=0),
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    autoscaler.try_trigger_scaling()

    # The in-flight demand plus one copy of the saturated shape.
    assert coordinator._allocation.resources == [
        {"CPU": 1, "worker_group_a": 1},
        {"CPU": 1, "worker_group_a": 1},
    ]


def test_over_utilized_shape_is_not_requested_twice():
    """A shape the solver already scaled is not requested a second time."""
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1, "worker_group_a": 1}],
        initial_cluster_resources=[{"CPU": 1, "worker_group_a": 1}],
        utilization=_HIGH_UTIL,
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    autoscaler.try_trigger_scaling()

    # The solver wants a second bundle of the same shape; that copy already
    # covers the over-utilized shape, so no third bundle is added.
    assert coordinator._allocation.resources == [
        {"CPU": 1, "worker_group_a": 1},
        {"CPU": 1, "worker_group_a": 1},
    ]


@pytest.mark.parametrize(
    "initial_cluster_resources",
    [
        # The reservation covers the demand, so nothing needs to grow.
        [{"CPU": 100}],
        # Unknown: the first reservation hasn't landed yet, so no decision is
        # made. Treating that as "no capacity" would request a scale-up copy of
        # every active shape on the first tick after construction.
        None,
    ],
)
def test_exact_path_does_not_scale_up(initial_cluster_resources):
    """Only the in-flight demand is sent when nothing needs to grow."""
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"CPU": 1}],
        initial_cluster_resources=initial_cluster_resources,
        utilization=_LOW_UTIL,
    )

    autoscaler.try_trigger_scaling()

    assert coordinator._allocation.resources == [{"CPU": 1}]


def test_exact_path_scales_up_when_no_node_can_host_shape():
    """A known-but-infeasible shape (no node can host it) is still scaled up."""
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"GPU": 1}],
        initial_cluster_resources=[{"CPU": 1}],
        utilization=_LOW_UTIL,
    )

    autoscaler.try_trigger_scaling()

    assert coordinator._allocation.resources == [{"GPU": 1}, {"GPU": 1}]


def test_exact_path_does_not_hold_demand_for_finished_tasks():
    """Active requests must not be recorded as explicit autoscaler demand.

    Recording them would keep requesting resources for tasks that already
    finished until ``low_util_request_release_delay_s`` expires.
    """
    current_time = {"t": 0.0}

    def get_time() -> float:
        return current_time["t"]

    coordinator, autoscaler, operator = _make_exact_path_autoscaler(
        [{"CPU": 1}],
        initial_cluster_resources=[{"CPU": 100}],
        utilization=_LOW_UTIL,
        get_time=get_time,
        coordinator_cls=_RecordingCoordinator,
    )

    # Tick 1: an active request below the scale-up threshold.
    current_time["t"] = 10.0
    autoscaler.try_trigger_scaling()
    assert coordinator._allocation.resources == [{"CPU": 1}]

    # Tick 2 (inside the release delay window): the task finished, so nothing may
    # be requested on its behalf anymore.
    operator.get_resource_requests.return_value = []
    current_time["t"] = 11.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == []


def test_exact_path_reports_new_shapes_during_release_delay():
    """A shape appearing inside the release-delay window must still be reported.

    The keep-alive snapshot used to *replace* the current demand, so a shape that
    first appeared while a previous scale-up was still being held never reached
    the coordinator until the window expired.
    """
    current_time = {"t": 0.0}

    def get_time() -> float:
        return current_time["t"]

    coordinator, autoscaler, operator = _make_exact_path_autoscaler(
        [{"CPU": 1}],
        initial_cluster_resources=[{"CPU": 1}],
        get_time=get_time,
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    # Tick 1: the only known CPU is fully used, so the shape is scaled up and the
    # release-delay window opens.
    current_time["t"] = 10.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 1}, {"CPU": 1}]

    # The scale-up landed, so there is spare capacity now.
    coordinator.cluster_resources = [{"CPU": 100}]

    # Tick 2 (inside the window, at low utilization): the old task finished and a
    # different shape showed up. The held scale-up copy must be *added* to the
    # current demand, not sent in its place.
    operator.get_resource_requests.return_value = [{"CPU": 2}]
    autoscaler._utility_calculator.utilization = _LOW_UTIL
    current_time["t"] = 11.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 2}, {"CPU": 1}]


def test_custom_resources_stay_out_of_request_remaining():
    """Custom resources travel in the bundles, not in ``request_remaining``.

    The coordinator rejects any change to ``request_remaining`` on an ongoing
    request, and ``__init__`` already registered with the standard types. Adding
    a custom resource there would make the first request that carries one fail
    silently, so the set is left alone.
    """
    coordinator, autoscaler, _ = _make_exact_path_autoscaler(
        [{"GPU": 1, "worker_group_a": 1}],
        initial_cluster_resources=[{"CPU": 1}],
    )

    autoscaler.try_trigger_scaling()

    # The custom resource still reaches the request...
    assert coordinator._allocation.resources == [
        {"GPU": 1, "worker_group_a": 1},
        {"GPU": 1, "worker_group_a": 1},
    ]
    # ...and the leftover-reservation set is untouched.
    assert coordinator._allocation.request_remaining == frozenset(
        STANDARD_RESOURCE_TYPES
    )


_THRESHOLDS = ClusterUtil(cpu=0.5, gpu=0.5, memory=0.5, object_store_memory=0.5)


@pytest.mark.parametrize(
    "active_requests,node_resources,expected",
    [
        # 2 of 8 ``worker_group_a`` units is 25%: measured against the worker
        # groups that can host the shape, not against the whole cluster.
        (
            [{"CPU": 1, "worker_group_a": 1}],
            {
                "node_a": {"CPU": 8, "worker_group_a": 4},
                "node_b": {"CPU": 8, "worker_group_b": 4},
            },
            [],
        ),
        # The custom resource saturates the worker groups that provide it.
        (
            [{"CPU": 1, "worker_group_a": 1}] * 4,
            {"node_a": {"CPU": 16, "worker_group_a": 4}},
            [(("CPU", 1), ("worker_group_a", 1))],
        ),
        # Standard resources take part the same way: 6 of 8 CPUs.
        (
            [{"CPU": 1}] * 6,
            {"node_a": {"CPU": 8}},
            [(("CPU", 1),)],
        ),
        # Below the threshold: nothing is selected.
        ([{"CPU": 1}] * 3, {"node_a": {"CPU": 8}}, []),
        # No cluster view means "unknown", not "no capacity".
        ([{"CPU": 1}], {}, []),
        # A shape no worker group can host can never be satisfied.
        (
            [{"GPU": 1}],
            {"node_a": {"CPU": 100}},
            [(("GPU", 1),)],
        ),
    ],
)
def test_select_over_utilized_shapes(active_requests, node_resources, expected):
    """Utilization is computed per exact shape, against its worker groups."""
    assert (
        select_over_utilized_shapes(active_requests, node_resources, _THRESHOLDS)
        == expected
    )


def test_keep_alive_keeps_earlier_scale_up_shapes():
    """A later scale-up must not cancel an outstanding keep-alive.

    Only the current tick's copies used to be recorded, so a 4-CPU scale-up
    dropped a 1-CPU scale-up that was still inside the release-delay window.
    """
    current_time = {"t": 0.0}

    def get_time() -> float:
        return current_time["t"]

    coordinator, autoscaler, operator = _make_exact_path_autoscaler(
        [{"CPU": 1}],
        initial_cluster_resources=[{"CPU": 1}],
        get_time=get_time,
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    # Tick 1: the 1-CPU shape is saturated, so it is scaled up and the window opens.
    current_time["t"] = 10.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 1}, {"CPU": 1}]

    # Tick 2: a 4-CPU shape shows up. No node can host it, so it is scaled up too.
    operator.get_resource_requests.return_value = [{"CPU": 4}]
    current_time["t"] = 11.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 4}, {"CPU": 4}]

    # Tick 3 (inside the window, at low utilization): both scale-ups are held.
    autoscaler._utility_calculator.utilization = _LOW_UTIL
    operator.get_resource_requests.return_value = []
    current_time["t"] = 12.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 1}, {"CPU": 4}]


def test_keep_alive_copy_is_released_once_its_shape_stops_being_demanded():
    """A busy tick re-sends current demand only, not an obsolete held copy.

    The window is a release delay for the low-utilization path, not a minimum
    lifetime: a copy is held only while its shape still has in-flight work or is
    still above its own threshold, so a tick that no longer needs the shape does
    not pin capacity for work that is gone (the bug covered by
    ``test_exact_path_does_not_hold_demand_for_finished_tasks``). The release is
    not lossy: when the shape's work returns, the shape check requests it again.
    """
    current_time = {"t": 0.0}

    def get_time() -> float:
        return current_time["t"]

    coordinator, autoscaler, operator = _make_exact_path_autoscaler(
        [{"CPU": 1}],
        initial_cluster_resources=[{"CPU": 1}],
        get_time=get_time,
        coordinator_cls=_ScriptedClusterCoordinator,
    )

    # Tick 1 (busy): the 1-CPU shape is saturated, so a copy is requested and held.
    current_time["t"] = 10.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 1}, {"CPU": 1}]

    # Tick 2 (still busy): a different shape is in flight now. Nothing needs the
    # 1-CPU shape any more (no in-flight work, not above its own threshold), so
    # the request carries current demand only...
    operator.get_resource_requests.return_value = [{"CPU": 4}]
    current_time["t"] = 11.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 4}, {"CPU": 4}]
    # ...although the copy itself is still inside its window, which is what the
    # low-utilization path replays.
    assert autoscaler._scale_up_keep_alive.bundles(11.0) == [
        {"CPU": 1},
        {"CPU": 4},
    ]

    # Tick 3: the 1-CPU work comes back. The shape is requested again, while the
    # 4-CPU copy stays out of the request because nothing demands it now.
    operator.get_resource_requests.return_value = [{"CPU": 1}]
    current_time["t"] = 12.0
    autoscaler.try_trigger_scaling()
    assert coordinator.requests[-1] == [{"CPU": 1}, {"CPU": 1}]


def test_keep_alive_window_is_not_re_armed_by_empty_scale_up():
    """A tick that adds no copies must not extend an existing shape's window.

    The window is measured from when a shape was last requested. Re-arming it on
    every tick kept a copy the solver had already abandoned alive indefinitely,
    and re-sent it once utilization dropped.
    """
    keep_alive = ScaleUpKeepAlive(release_delay_s=100)
    delay = keep_alive.release_delay_s

    keep_alive.record([{"CPU": 1}], 10.0)
    assert keep_alive.bundles(10.0) == [{"CPU": 1}]

    # Ticks that scale up nothing keep the copy, but never re-arm its window.
    for elapsed in (delay / 2, delay - 1):
        keep_alive.record([], 10.0 + elapsed)
        assert keep_alive.bundles(10.0 + elapsed) == [{"CPU": 1}]

    # The window closes on schedule despite those later ticks.
    keep_alive.record([], 10.0 + delay)
    assert keep_alive.bundles(10.0 + delay) == []


def test_keep_alive_snapshot_does_not_grow_with_churning_shapes():
    """Shapes that stop being requested are released even under constant churn.

    Shapes used to be merged into one snapshot governed by a single timestamp,
    so a workload whose shape changed every tick accumulated every shape it had
    ever reported and never dropped any of them.
    """
    keep_alive = ScaleUpKeepAlive(release_delay_s=5.0)

    # A different shape every tick, one second apart.
    for i in range(100):
        keep_alive.record([{"CPU": 1, "memory": float(i)}], float(i))

    # Only the shapes still inside the window survive, not all 100.
    assert keep_alive.bundles(99.0) == [
        {"CPU": 1, "memory": float(i)} for i in range(95, 100)
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
