from typing import Dict, List

import pytest
from freezegun import freeze_time

from ray.anyscale.train._internal.execution.scaling_policy.autoscaling_requester import (  # noqa: E501
    AutoscalingRequester,
)
from ray.anyscale.train._internal.execution.scaling_policy.elastic import (
    ElasticScalingPolicy,
)
from ray.train.v2._internal.execution.scaling_policy import NoopDecision, ResizeDecision
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupStatus,
    WorkerStatus,
)
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.config import ScalingConfig


class MockAutoscalingRequester(AutoscalingRequester):
    def __init__(self):
        self._requested = []
        self._node_resources = []

    def request(self, bundles: List[Dict]):
        self._requested = bundles

    def node_resources(self) -> List[Dict[str, float]]:
        return self._node_resources

    def clear_request(self):
        self._requested = []

    # === Test methods ===
    def set_node_resources(self, node_resources):
        self._node_resources = node_resources

    def get_requested_bundles(self):
        return self._requested


@pytest.fixture(autouse=True)
def patch_autoscaling_requester(monkeypatch):
    monkeypatch.setattr(
        ElasticScalingPolicy, "autoscaling_requester_cls", MockAutoscalingRequester
    )


def _get_mock_worker_group_status(
    num_workers: int, latest_start_time: float
) -> WorkerGroupStatus:
    return WorkerGroupStatus(
        num_workers=num_workers,
        latest_start_time=latest_start_time,
        worker_statuses={
            i: WorkerStatus(running=True, error=None) for i in range(num_workers)
        },
    )


def test_recovery_decision():
    """Test recovery decisions being made when the worker group is not healthy.
    Ensure that the policy will request a resize as soon as resources are available."""
    min_workers, max_workers = 4, 64
    resources_per_worker = {"CPU": 8, "GPU": 1}

    scaling_config = ScalingConfig(
        num_workers=(min_workers, max_workers),
        resources_per_worker=resources_per_worker,
        use_gpu=True,
    )
    policy = ElasticScalingPolicy(scaling_config)
    autoscaling_requester = policy.autoscaling_requester
    assert isinstance(autoscaling_requester, MockAutoscalingRequester)

    # No resources are available
    worker_group_status = _get_mock_worker_group_status(0, float("-inf"))
    decision = policy.make_decision_for_non_running_worker_group(worker_group_status)
    assert isinstance(decision, NoopDecision)

    # Resources for < min workers are available
    autoscaling_requester.set_node_resources([resources_per_worker] * (min_workers - 1))
    decision = policy.make_decision_for_non_running_worker_group(worker_group_status)
    assert isinstance(decision, NoopDecision)

    # Resources for >= min workers are available
    autoscaling_requester.set_node_resources([resources_per_worker] * min_workers)
    decision = policy.make_decision_for_non_running_worker_group(worker_group_status)
    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == min_workers

    autoscaling_requester.set_node_resources([resources_per_worker] * max_workers)
    worker_group_status = _get_mock_worker_group_status(min_workers, time_monotonic())
    decision = policy.make_decision_for_non_running_worker_group(worker_group_status)
    assert isinstance(decision, ResizeDecision)
    assert decision.num_workers == max_workers


def test_monitor_recently_started_worker_group():
    """Test monitor decisions being made when the worker group is running.
    Ensures that resizing decisions are not made too soon after the worker group starts.
    """
    min_workers, max_workers = 4, 64
    monitor_interval_s = 60
    resources_per_worker = {"CPU": 8, "GPU": 1}

    scaling_config = ScalingConfig(
        num_workers=(min_workers, max_workers),
        resources_per_worker=resources_per_worker,
        use_gpu=True,
        elastic_resize_monitor_interval_s=monitor_interval_s,
    )
    policy = ElasticScalingPolicy(scaling_config)
    autoscaling_requester = policy.autoscaling_requester
    assert isinstance(autoscaling_requester, MockAutoscalingRequester)

    with freeze_time() as frozen_time:
        # The worker group just started
        worker_group_status = _get_mock_worker_group_status(
            min_workers, time_monotonic()
        )

        # Advance time partway through the monitor interval
        frozen_time.tick(delta=monitor_interval_s / 2)

        # Even though there are new resources available, we should not resize yet
        # because the monitor interval has not passed since
        autoscaling_requester.set_node_resources(
            [resources_per_worker] * (max_workers - 1)
        )
        assert isinstance(
            policy.make_decision_for_running_worker_group(worker_group_status),
            NoopDecision,
        )

        frozen_time.tick(delta=monitor_interval_s / 2)

        # The monitor interval has passed, should detect resources and resize
        decision = policy.make_decision_for_running_worker_group(worker_group_status)
        assert isinstance(decision, ResizeDecision)
        assert decision.num_workers == max_workers - 1


def test_monitor_long_running_worker_group():
    """Test monitor decisions being made when the worker group is running.
    Ensures that the resizing considerations are not made too frequently.
    """
    min_workers, max_workers = 4, 64
    monitor_interval_s = 60
    resources_per_worker = {"CPU": 8, "GPU": 1}

    scaling_config = ScalingConfig(
        num_workers=(min_workers, max_workers),
        resources_per_worker=resources_per_worker,
        use_gpu=True,
        elastic_resize_monitor_interval_s=monitor_interval_s,
    )
    policy = ElasticScalingPolicy(scaling_config)
    autoscaling_requester = policy.autoscaling_requester
    assert isinstance(autoscaling_requester, MockAutoscalingRequester)

    with freeze_time() as frozen_time:
        worker_group_status = _get_mock_worker_group_status(
            min_workers, time_monotonic()
        )
        autoscaling_requester.set_node_resources([resources_per_worker] * min_workers)

        # The worker group has been running for a while at the same size
        frozen_time.tick(monitor_interval_s * 60)

        # Consider resizing.
        decision = policy.make_decision_for_running_worker_group(worker_group_status)
        assert isinstance(decision, NoopDecision)

        # We recently considered resizing, so we should wait until the next interval
        # to consider again --> no-op even if new resources are available
        autoscaling_requester.set_node_resources([resources_per_worker] * max_workers)
        frozen_time.tick(monitor_interval_s / 2)
        decision = policy.make_decision_for_running_worker_group(worker_group_status)
        assert isinstance(decision, NoopDecision)

        frozen_time.tick(monitor_interval_s / 2)
        decision = policy.make_decision_for_running_worker_group(worker_group_status)
        assert isinstance(decision, ResizeDecision)
        assert decision.num_workers == max_workers


def test_count_possible_workers():
    """Test counting the number of workers that can be started with
    available node resources."""
    resources_per_worker = {"CPU": 8, "GPU": 1}
    scaling_config = ScalingConfig(
        use_gpu=True, resources_per_worker=resources_per_worker
    )
    policy = ElasticScalingPolicy(scaling_config)

    # No resources
    assert policy._count_possible_workers([]) == 0

    # Single node
    assert policy._count_possible_workers([{"CPU": 8, "GPU": 1}]) == 1
    assert policy._count_possible_workers([{"CPU": 16, "GPU": 2}]) == 2
    assert policy._count_possible_workers([{"CPU": 16, "GPU": 1}]) == 1

    # Multinode
    assert policy._count_possible_workers([{"CPU": 7, "GPU": 1}] * 2) == 0
    assert policy._count_possible_workers([{"CPU": 9, "GPU": 2}] * 8) == 8
    assert policy._count_possible_workers([{"CPU": 16, "GPU": 2}] * 2) == 4
    assert policy._count_possible_workers([{"CPU": 8, "GPU": 1}] * 4) == 4


def test_count_possible_workers_with_zero_resources():
    max_workers = 4
    scaling_config = ScalingConfig(
        num_workers=(1, max_workers),
        resources_per_worker={"CPU": 0, "GPU": 0, "memory": 0},
    )
    policy = ElasticScalingPolicy(scaling_config)

    assert (
        policy._count_possible_workers([{"CPU": 1, "GPU": 1, "memory": 1}])
        == max_workers
    )


def test_request_and_clear():
    """Tests that the policy makes resource requests and clears the requests."""
    resources_per_worker = {"CPU": 8, "GPU": 1}
    policy = ElasticScalingPolicy(
        scaling_config=ScalingConfig(
            use_gpu=True, resources_per_worker=resources_per_worker, num_workers=(2, 4)
        )
    )

    policy.on_controller_run_start()
    assert (
        policy.autoscaling_requester.get_requested_bundles()
        == [resources_per_worker] * 4
    )

    policy.on_controller_shutdown()
    assert policy.autoscaling_requester.get_requested_bundles() == []


if __name__ == "__main__":
    pytest.main(["-v", __file__])
