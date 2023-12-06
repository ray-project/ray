import asyncio
import logging
import os
import sys
import tempfile
import time
import zipfile
from typing import Dict, Iterable, List
from unittest import mock

import numpy as np
import pytest
import requests

import ray
import ray.util.state as state_api
from ray import serve
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.autoscaling_policy import (
    BasicAutoscalingPolicy,
    calculate_desired_num_replicas,
)
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    ReplicaState,
)
from ray.serve._private.constants import (
    CONTROL_LOOP_PERIOD_S,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.controller import ServeController
from ray.serve.config import AutoscalingConfig
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.schema import ServeDeploySchema


class TestCalculateDesiredNumReplicas:
    def test_bounds_checking(self):
        num_replicas = 10
        max_replicas = 11
        min_replicas = 9
        config = AutoscalingConfig(
            max_replicas=max_replicas,
            min_replicas=min_replicas,
            target_num_ongoing_requests_per_replica=100,
        )

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=[150] * num_replicas
        )
        assert desired_num_replicas == max_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=[50] * num_replicas
        )
        assert desired_num_replicas == min_replicas

        for i in range(50, 150):
            desired_num_replicas = calculate_desired_num_replicas(
                autoscaling_config=config,
                current_num_ongoing_requests=[i] * num_replicas,
            )
            assert min_replicas <= desired_num_replicas <= max_replicas

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_up(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=target_requests,
        )
        num_replicas = 10
        num_ongoing_requests = [2 * target_requests] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    @pytest.mark.parametrize("target_requests", [0.5, 1.0, 1.5])
    def test_scale_down(self, target_requests):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=target_requests,
        )
        num_replicas = 10
        num_ongoing_requests = [0.5 * target_requests] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 4 <= desired_num_replicas <= 6  # 10 * 0.5 = 5

    def test_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            smoothing_factor=0.5,
        )
        num_replicas = 10

        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25

    def test_upscale_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            upscale_smoothing_factor=0.5,
        )
        num_replicas = 10

        # Should use upscale smoothing factor of 0.5
        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        # Should use downscale smoothing factor of 1 (default)
        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 1 <= desired_num_replicas <= 4  # 10 + (2.5 - 10) = 2.5

    def test_downscale_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            downscale_smoothing_factor=0.5,
        )
        num_replicas = 10

        # Should use upscale smoothing factor of 1 (default)
        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 39 <= desired_num_replicas <= 41  # 10 + (40 - 10) = 40

        # Should use downscale smoothing factor of 0.5
        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25


class TestGetDecisionNumReplicas:
    def test_smoothing_factor_scale_up_from_0_replicas(self):
        """Test that the smoothing factor is respected when scaling up
        from 0 replicas.
        """

        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=2,
            smoothing_factor=10,
        )
        policy = BasicAutoscalingPolicy(config)
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
        )

        # 1 * 10
        assert new_num_replicas == 10

        config.smoothing_factor = 0.5
        policy = BasicAutoscalingPolicy(config)
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
        )

        # math.ceil(1 * 0.5)
        assert new_num_replicas == 1

    def test_smoothing_factor_scale_down_to_0_replicas(self):
        """Test that a deployment scales down to 0 for non-default smoothing factors."""

        # With smoothing factor > 1, the desired number of replicas should
        # immediately drop to 0 (while respecting upscale and downscale delay)
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=5,
            smoothing_factor=10,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )
        policy = BasicAutoscalingPolicy(config)
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=[0, 0, 0, 0, 0],
            curr_target_num_replicas=5,
            current_handle_queued_queries=0,
        )

        assert new_num_replicas == 0

        # With smoothing factor < 1, the desired number of replicas shouldn't
        # get stuck at a positive number, and instead should eventually drop
        # to zero
        config.smoothing_factor = 0.2
        policy = BasicAutoscalingPolicy(config)
        num_replicas = 5
        for _ in range(5):
            num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=[0] * num_replicas,
                curr_target_num_replicas=num_replicas,
                current_handle_queued_queries=0,
            )

        assert num_replicas == 0

    def test_upscale_downscale_delay(self):
        """Unit test for upscale_delay_s and downscale_delay_s."""

        upscale_delay_s = 30.0
        downscale_delay_s = 600.0

        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=2,
            target_num_ongoing_requests_per_replica=1,
            upscale_delay_s=30.0,
            downscale_delay_s=600.0,
        )

        policy = BasicAutoscalingPolicy(config)

        upscale_wait_periods = int(upscale_delay_s / CONTROL_LOOP_PERIOD_S)
        downscale_wait_periods = int(downscale_delay_s / CONTROL_LOOP_PERIOD_S)

        overload_requests = [100]

        # Scale up when there are 0 replicas and current_handle_queued_queries > 0
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=[],
            curr_target_num_replicas=0,
            current_handle_queued_queries=1,
        )
        assert new_num_replicas == 1

        # We should scale up only after enough consecutive scale-up decisions.
        for i in range(upscale_wait_periods):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 2

        no_requests = [0, 0]

        # We should scale down only after enough consecutive scale-down decisions.
        for i in range(downscale_wait_periods):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 0

        # Get some scale-up decisions, but not enough to trigger a scale up.
        for i in range(int(upscale_wait_periods / 2)):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        # Interrupt with a scale-down decision.
        policy.get_decision_num_replicas(
            current_num_ongoing_requests=[0],
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )

        # The counter should be reset, so it should require `upscale_wait_periods`
        # more periods before we actually scale up.
        for i in range(upscale_wait_periods):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=overload_requests,
                curr_target_num_replicas=1,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 1, i

        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 2

        # Get some scale-down decisions, but not enough to trigger a scale down.
        for i in range(int(downscale_wait_periods / 2)):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        # Interrupt with a scale-up decision.
        policy.get_decision_num_replicas(
            current_num_ongoing_requests=[100, 100],
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )

        # The counter should be reset so it should require `downscale_wait_periods`
        # more periods before we actually scale down.
        for i in range(downscale_wait_periods):
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=no_requests,
                curr_target_num_replicas=2,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 2, i

        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == 0

    def test_replicas_delayed_startup(self):
        """Unit test simulating replicas taking time to start up."""
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=200,
            target_num_ongoing_requests_per_replica=1,
            upscale_delay_s=0,
            downscale_delay_s=100000,
        )

        policy = BasicAutoscalingPolicy(config)

        new_num_replicas = policy.get_decision_num_replicas(1, [100], 0)
        assert new_num_replicas == 100

        # New target is 100, but no new replicas finished spinning up during this
        # timestep.
        new_num_replicas = policy.get_decision_num_replicas(100, [100], 0)
        assert new_num_replicas == 100

        # Two new replicas spun up during this timestep.
        new_num_replicas = policy.get_decision_num_replicas(100, [100, 20, 3], 0)
        assert new_num_replicas == 123

        # A lot of queries got drained and a lot of replicas started up, but
        # new_num_replicas should not decrease, because of the downscale delay.
        new_num_replicas = policy.get_decision_num_replicas(123, [6, 2, 1, 1], 0)
        assert new_num_replicas == 123

    @pytest.mark.parametrize("delay_s", [30.0, 0.0])
    def test_fluctuating_ongoing_requests(self, delay_s):
        """
        Simulates a workload that switches between too many and too few
        ongoing requests.
        """

        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            target_num_ongoing_requests_per_replica=50,
            upscale_delay_s=delay_s,
            downscale_delay_s=delay_s,
        )

        policy = BasicAutoscalingPolicy(config)

        if delay_s > 0:
            wait_periods = int(delay_s / CONTROL_LOOP_PERIOD_S)
            assert wait_periods > 1

        underload_requests, overload_requests = [20, 20], [100]
        trials = 1000

        new_num_replicas = None
        for trial in range(trials):
            if trial % 2 == 0:
                new_num_replicas = policy.get_decision_num_replicas(
                    current_num_ongoing_requests=overload_requests,
                    curr_target_num_replicas=1,
                    current_handle_queued_queries=0,
                )
                if delay_s > 0:
                    assert new_num_replicas == 1, trial
                else:
                    assert new_num_replicas == 2, trial
            else:
                new_num_replicas = policy.get_decision_num_replicas(
                    current_num_ongoing_requests=underload_requests,
                    curr_target_num_replicas=2,
                    current_handle_queued_queries=0,
                )
                if delay_s > 0:
                    assert new_num_replicas == 2, trial
                else:
                    assert new_num_replicas == 1, trial

    @pytest.mark.parametrize(
        "ongoing_requests", [[7, 1, 8, 4], [8, 1, 8, 4], [6, 1, 8, 4], [0, 1, 8, 4]]
    )
    def test_imbalanced_replicas(self, ongoing_requests):
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            target_num_ongoing_requests_per_replica=5,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        policy = BasicAutoscalingPolicy(config)

        # Check that as long as the average number of ongoing requests equals
        # the target_num_ongoing_requests_per_replica, the number of replicas
        # stays the same
        if np.mean(ongoing_requests) == config.target_num_ongoing_requests_per_replica:
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 4

        # Check downscaling behavior when average number of requests
        # is lower than target_num_ongoing_requests_per_replica
        elif np.mean(ongoing_requests) < config.target_num_ongoing_requests_per_replica:
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )

            if (
                config.target_num_ongoing_requests_per_replica
                - np.mean(ongoing_requests)
                <= 1
            ):
                # Autoscaling uses a ceiling operator, which means a slightly low
                # current_num_ongoing_requests value is insufficient to downscale
                assert new_num_replicas == 4
            else:
                assert new_num_replicas == 3

        # Check upscaling behavior when average number of requests
        # is higher than target_num_ongoing_requests_per_replica
        else:
            new_num_replicas = policy.get_decision_num_replicas(
                current_num_ongoing_requests=ongoing_requests,
                curr_target_num_replicas=4,
                current_handle_queued_queries=0,
            )
            assert new_num_replicas == 5

    @pytest.mark.parametrize(
        "ongoing_requests", [[20, 0, 0, 0], [100, 0, 0, 0], [10, 0, 0, 0]]
    )
    def test_single_replica_receives_all_requests(self, ongoing_requests):
        target_requests = 5

        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=50,
            target_num_ongoing_requests_per_replica=target_requests,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        policy = BasicAutoscalingPolicy(config)

        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=ongoing_requests,
            curr_target_num_replicas=4,
            current_handle_queued_queries=0,
        )
        assert new_num_replicas == sum(ongoing_requests) / target_requests


def get_deployment_status(controller, name) -> DeploymentStatus:
    ref = ray.get(controller.get_deployment_status.remote(name, SERVE_DEFAULT_APP_NAME))
    info = DeploymentStatusInfo.from_proto(DeploymentStatusInfoProto.FromString(ref))
    return info.status


def get_running_replicas(controller: ServeController, name: str) -> List:
    """Get the replicas currently running for given deployment"""
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(
            DeploymentID(name, SERVE_DEFAULT_APP_NAME)
        )
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return running_replicas


def get_running_replica_tags(controller: ServeController, name: str) -> List:
    """Get the replica tags of running replicas for given deployment"""
    running_replicas = get_running_replicas(controller, name)
    return [replica.replica_tag for replica in running_replicas]


def check_deployment_status(controller, name, expected_status) -> DeploymentStatus:
    ref = ray.get(controller.get_deployment_status.remote(name, SERVE_DEFAULT_APP_NAME))
    info = DeploymentStatusInfo.from_proto(DeploymentStatusInfoProto.FromString(ref))
    assert info.status == expected_status
    return True


def check_autoscale_num_replicas_gte(
    controller: ServeController, name: str, target: int
) -> int:
    """Check the number of replicas currently running for given
    deployment is greater than or equal to target.
    """
    assert len(get_running_replicas(controller, name)) >= target
    return True


def check_autoscale_num_replicas_eq(
    controller: ServeController, name: str, target: int
) -> int:
    """Check the number of replicas currently running for given deployment."""
    assert len(get_running_replicas(controller, name)) == target
    return True


def check_autoscale_num_replicas_lte(
    controller: ServeController, name: str, target: int
) -> int:
    """Check the number of replicas currently running for given deployment."""

    assert len(get_running_replicas(controller, name)) <= target
    return True


def assert_no_replicas_deprovisioned(
    replica_tags_1: Iterable[str], replica_tags_2: Iterable[str]
) -> None:
    """
    Checks whether any replica tags from replica_tags_1 are absent from
    replica_tags_2. Assumes that this indicates replicas were de-provisioned.

    replica_tags_1: Replica tags of running replicas at the first timestep
    replica_tags_2: Replica tags of running replicas at the second timestep
    """

    replica_tags_1, replica_tags_2 = set(replica_tags_1), set(replica_tags_2)
    num_matching_replicas = len(replica_tags_1.intersection(replica_tags_2))

    print(
        f"{num_matching_replicas} replica(s) stayed provisioned between "
        f"both deployments. All {len(replica_tags_1)} replica(s) were "
        f"expected to stay provisioned. "
        f"{len(replica_tags_1) - num_matching_replicas} replica(s) were "
        f"de-provisioned."
    )

    assert len(replica_tags_1) == num_matching_replicas


def test_assert_no_replicas_deprovisioned():
    replica_tags_1 = ["a", "b", "c"]
    replica_tags_2 = ["a", "b", "c", "d", "e"]

    assert_no_replicas_deprovisioned(replica_tags_1, replica_tags_2)
    with pytest.raises(AssertionError):
        assert_no_replicas_deprovisioned(replica_tags_2, replica_tags_1)


def get_deployment_start_time(controller: ServeController, name: str):
    """Return start time for given deployment"""
    deployments = ray.get(controller.list_deployments_internal.remote())
    deployment_info, _ = deployments[DeploymentID(name, SERVE_DEFAULT_APP_NAME)]
    return deployment_info.start_time_ms


@pytest.mark.parametrize("min_replicas", [1, 2])
def test_e2e_scale_up_down_basic(min_replicas, serve_instance):
    """Send 100 requests and check that we autoscale up, and then back down."""

    controller = serve_instance._controller
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": 3,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.5,
            "upscale_delay_s": 0,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1",
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        check_deployment_status,
        controller=controller,
        name="A",
        expected_status=DeploymentStatus.HEALTHY,
    )
    start_time = get_deployment_start_time(controller, "A")

    [handle.remote() for _ in range(100)]

    # scale up one more replica from min_replicas
    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=min_replicas + 1,
    )
    # check_deployment_status(controller, "A", DeploymentStatus.UPSCALING)
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_lte,
        controller=controller,
        name="A",
        target=min_replicas,
    )

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("smoothing_factor", [1, 0.2])
@pytest.mark.parametrize("use_upscale_downscale_config", [True, False])
def test_e2e_scale_up_down_with_0_replica(
    serve_instance, smoothing_factor, use_upscale_downscale_config
):
    """Send 100 requests and check that we autoscale up, and then back down."""

    controller = serve_instance._controller
    signal = SignalActor.remote()

    autoscaling_config = {
        "metrics_interval_s": 0.1,
        "min_replicas": 0,
        "max_replicas": 2,
        "look_back_period_s": 0.2,
        "downscale_delay_s": 0.5,
        "upscale_delay_s": 0,
    }
    if use_upscale_downscale_config:
        autoscaling_config["upscale_smoothing_factor"] = smoothing_factor
        autoscaling_config["downscale_smoothing_factor"] = smoothing_factor
    else:
        autoscaling_config["smoothing_factor"] = smoothing_factor

    @serve.deployment(
        autoscaling_config=autoscaling_config,
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1",
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind()).options(use_new_handle_api=True)
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    results = [handle.remote() for _ in range(100)]

    # After the blocking requests are sent, the number of replicas
    # should increase.
    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=1,
    )
    # Release the signal, which should unblock all requests.
    print("Number of replicas reached at least 1, releasing signal.")
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_eq,
        controller=controller,
        name="A",
        target=0,
    )
    # Make sure no requests were dropped.
    # If the deployment (unexpectedly) scaled down before the
    # blocking signal was released, chances are some requests failed b/c
    # they were assigned to a replica that died. Therefore, this for
    # loop is intended to help make sure that didn't happen.
    for res in results:
        res.result()

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@mock.patch.object(ServeController, "run_control_loop")
def test_initial_num_replicas(mock, serve_instance):
    """assert that the inital amount of replicas a deployment is launched with
    respects the bounds set by autoscaling_config.

    For this test we mock out the run event loop, make sure the number of
    replicas is set correctly before we hit the autoscaling procedure.
    """

    @serve.deployment(
        autoscaling_config={
            "min_replicas": 2,
            "max_replicas": 4,
        },
        version="v1",
    )
    class A:
        def __call__(self):
            return "ok!"

    serve.run(A.bind())

    controller = serve_instance._controller
    assert len(get_running_replicas(controller, "A")) == 2


def test_cold_start_time(serve_instance):
    """Test a request is served quickly by a deployment that's scaled to zero"""

    @serve.deployment(
        autoscaling_config={
            "min_replicas": 0,
            "max_replicas": 1,
            "look_back_period_s": 0.2,
        },
    )
    class A:
        def __call__(self):
            return "hello"

    handle = serve.run(A.bind())

    def check_running():
        assert serve.status().applications["default"].status == "RUNNING"
        return True

    wait_for_condition(check_running)

    start = time.time()
    result = handle.remote().result()
    cold_start_time = time.time() - start
    assert cold_start_time < 3
    print(
        "Time taken for deployment at 0 replicas to serve first request:",
        cold_start_time,
    )
    assert result == "hello"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_bursty(serve_instance):
    """
    Sends 100 requests in bursts. Uses delays for smooth provisioning.
    """

    controller = serve_instance._controller
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 2,
            "look_back_period_s": 0.5,
            "downscale_delay_s": 0.5,
            "upscale_delay_s": 0.5,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1",
    )
    class A:
        def __init__(self):
            logging.getLogger("ray.serve").setLevel(logging.ERROR)

        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    [handle.remote() for _ in range(100)]

    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=2,
    )

    num_replicas = len(get_running_replicas(controller, "A"))
    signal.send.remote()

    # Execute a bursty workload that issues 100 requests every 0.05 seconds
    # The SignalActor allows all requests in a burst to be queued before they
    # are all executed, which increases the
    # target_in_flight_requests_per_replica. Then the send method will bring
    # it back to 0. This bursty behavior should be smoothed by the delay
    # parameters.
    for _ in range(5):
        ray.get(signal.send.remote(clear=True))
        check_autoscale_num_replicas_eq(controller, "A", num_replicas)
        responses = [handle.remote() for _ in range(100)]
        signal.send.remote()
        [r.result() for r in responses]
        time.sleep(0.05)

    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_lte,
        controller=controller,
        name="A",
        target=1,
    )

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_intermediate_downscaling(serve_instance):
    """
    Scales up, then down, and up again.
    """

    controller = serve_instance._controller
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 0,
            "max_replicas": 20,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2,
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1",
    )
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    handle = serve.run(A.bind())
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    [handle.remote() for _ in range(50)]

    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=20,
        timeout=30,
    )
    signal.send.remote()

    wait_for_condition(
        check_autoscale_num_replicas_lte,
        controller=controller,
        name="A",
        target=1,
        timeout=30,
    )
    signal.send.remote(clear=True)

    [handle.remote() for _ in range(50)]
    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=20,
        timeout=30,
    )

    signal.send.remote()
    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_eq,
        controller=controller,
        name="A",
        target=0,
        timeout=30,
    )

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skip(reason="Currently failing with undefined behavior")
def test_e2e_update_autoscaling_deployment(serve_instance):
    # See https://github.com/ray-project/ray/issues/21017 for details

    controller = serve_instance._controller
    signal = SignalActor.options(name="signal123").remote()

    app_config = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "metrics_interval_s": 0.1,
                    "min_replicas": 0,
                    "max_replicas": 10,
                    "look_back_period_s": 0.2,
                    "downscale_delay_s": 0.2,
                    "upscale_delay_s": 0.2,
                },
                "graceful_shutdown_timeout_s": 1,
                "max_concurrent_queries": 1000,
            }
        ],
    }

    serve_instance.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Deployed A with min_replicas 1 and max_replicas 10.")
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )
    handle = serve.get_deployment_handle("A", "default")
    start_time = get_deployment_start_time(controller, "A")

    check_autoscale_num_replicas_eq(controller, "A", 0)
    [handle.remote() for _ in range(400)]
    print("Issued 400 requests.")

    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=10,
    )
    print("Scaled to 10 replicas.")
    first_deployment_replicas = get_running_replica_tags(controller, "A")

    check_autoscale_num_replicas_lte(controller, "A", 20)

    [handle.remote() for _ in range(458)]
    time.sleep(3)
    print("Issued 458 requests. Request routing in-progress.")

    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 2
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 20
    serve_instance.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A.")

    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=20,
    )
    print("Scaled up to 20 requests.")
    second_deployment_replicas = get_running_replica_tags(controller, "A")

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_lte,
        controller=controller,
        name="A",
        target=2,
    )
    check_autoscale_num_replicas_gte(controller, "A", 2)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time

    # scale down to 0
    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 0
    serve_instance.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A.")
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )

    wait_for_condition(
        check_autoscale_num_replicas_eq,
        controller=controller,
        name="A",
        target=0,
    )
    check_autoscale_num_replicas_eq(controller, "A", 0)

    # scale up
    [handle.remote() for _ in range(400)]
    wait_for_condition(
        check_autoscale_num_replicas_gte,
        controller=controller,
        name="A",
        target=0,
    )
    signal.send.remote()
    wait_for_condition(
        check_autoscale_num_replicas_eq,
        controller=controller,
        name="A",
        target=0,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_raise_min_replicas(serve_instance):
    controller = serve_instance._controller
    signal = SignalActor.options(name="signal123").remote()

    app_config = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "metrics_interval_s": 0.1,
                    "min_replicas": 0,
                    "max_replicas": 10,
                    "look_back_period_s": 0.2,
                    "downscale_delay_s": 0.2,
                    "upscale_delay_s": 0.2,
                },
                "graceful_shutdown_timeout_s": 1,
                "max_concurrent_queries": 1000,
            }
        ],
    }

    serve_instance.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Deployed A.")
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )
    start_time = get_deployment_start_time(controller, "A")

    check_autoscale_num_replicas_eq(controller, "A", 0)

    handle = serve.get_deployment_handle("A", "default")
    handle.remote()
    print("Issued one request.")

    time.sleep(2)
    check_autoscale_num_replicas_eq(controller, "A", 1)
    print("Scale up to 1 replica.")

    first_deployment_replicas = get_running_replica_tags(controller, "A")

    app_config["deployments"][0]["autoscaling_config"]["min_replicas"] = 2
    serve_instance.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    print("Redeployed A with min_replicas set to 2.")
    wait_for_condition(
        lambda: get_deployment_status(controller, "A") == DeploymentStatus.HEALTHY
    )

    # Confirm that autoscaler doesn't scale above 2 even after waiting
    time.sleep(5)
    check_autoscale_num_replicas_eq(controller, "A", 2)
    print("Autoscaled to 2 without issuing any new requests.")

    second_deployment_replicas = get_running_replica_tags(controller, "A")

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()
    time.sleep(1)
    print("Completed request.")

    # As the queue is drained, we should scale back down.
    wait_for_condition(
        check_autoscale_num_replicas_lte,
        controller=controller,
        name="A",
        target=2,
    )
    check_autoscale_num_replicas_gte(controller, "A", 2)
    print("Stayed at 2 replicas.")

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, "A") == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_initial_replicas(serve_instance):
    @serve.deployment(
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            initial_replicas=2,
            max_replicas=5,
            downscale_delay_s=3,
        ),
    )
    def f():
        return os.getpid()

    serve.run(f.bind())
    dep_id = DeploymentID("f", SERVE_DEFAULT_APP_NAME)

    # f should start with initial_replicas (2) deployments
    actors = state_api.list_actors(
        filters=[
            ("class_name", "=", dep_id.to_replica_actor_class_name()),
            ("state", "=", "ALIVE"),
        ]
    )
    print(actors)
    assert len(actors) == 2

    # f should scale down to min_replicas (1) deployments
    def check_one_replica():
        actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "ALIVE"),
            ]
        )
        return len(actors) == 1

    wait_for_condition(check_one_replica, timeout=20)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(
        max_concurrent_queries=5,
        # The config makes the deployment scale up really quickly and then
        # wait nearly forever to downscale.
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            max_replicas=2,
            downscale_delay_s=600,
            upscale_delay_s=0,
            metrics_interval_s=1,
            look_back_period_s=1,
        ),
    )
    def scaler():
        ray.get(signal.wait.remote())
        time.sleep(0.2)
        return os.getpid()

    handle = serve.run(scaler.bind())
    dep_id = DeploymentID("scaler", SERVE_DEFAULT_APP_NAME)
    responses = [handle.remote() for _ in range(10)]

    def check_two_replicas():
        actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "ALIVE"),
            ]
        )
        print(actors)
        return len(actors) == 2

    wait_for_condition(check_two_replicas, retry_interval_ms=1000, timeout=20)

    ray.get(signal.send.remote())

    pids = {r.result() for r in responses}
    assert len(pids) == 2

    # Now re-deploy the application, make sure it is still 2 replicas and it shouldn't
    # be scaled down.
    handle = serve.run(scaler.bind())
    responses = [handle.remote() for _ in range(10)]
    pids = {r.result() for r in responses}
    assert len(pids) == 2

    def check_num_replicas(live: int, dead: int):
        live_actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "ALIVE"),
            ]
        )
        dead_actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "DEAD"),
            ]
        )

        return len(live_actors) == live and len(dead_actors) == dead

    wait_for_condition(
        check_num_replicas, retry_interval_ms=1000, timeout=20, live=2, dead=2
    )
    ray.get(signal.send.remote())

    # re-deploy the application with initial_replicas. This should override the
    # previous number of replicas.
    scaler = scaler.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            initial_replicas=3,
            max_replicas=5,
            downscale_delay_s=600,
            upscale_delay_s=600,
            metrics_interval_s=1,
            look_back_period_s=1,
        )
    )
    handle = serve.run(scaler.bind())
    responses = [handle.remote() for _ in range(15)]
    pids = {r.result() for r in responses}
    assert len(pids) == 3

    wait_for_condition(
        check_num_replicas, retry_interval_ms=1000, timeout=20, live=3, dead=4
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas_rest_api(serve_instance):
    client = serve_instance
    signal = SignalActor.options(name="signal", namespace="serve").remote()

    # Step 1: Prepare the script in a zip file so it can be submitted via REST API.
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_path:
        with zipfile.ZipFile(tmp_path, "w") as zip_obj:
            with zip_obj.open("app.py", "w") as f:
                f.write(
                    """
from ray import serve
import ray
import os

@serve.deployment
def g():
    signal = ray.get_actor("signal", namespace="serve")
    ray.get(signal.wait.remote())
    return os.getpid()


app = g.bind()
""".encode()
                )

    # Step 2: Deploy it with max_replicas=1
    app_config = {
        "import_path": "app:app",
        "runtime_env": {"working_dir": f"file://{tmp_path.name}"},
        "deployments": [
            {
                "name": "g",
                "autoscaling_config": {
                    "min_replicas": 0,
                    "max_replicas": 1,
                    "downscale_delay_s": 600,
                    "upscale_delay_s": 0,
                    "metrics_interval_s": 1,
                    "look_back_period_s": 1,
                },
            }
        ],
    }

    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    dep_id = DeploymentID("g", SERVE_DEFAULT_APP_NAME)
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )

    # Step 3: Verify that it can scale from 0 to 1.
    @ray.remote
    def send_request():
        return requests.get("http://localhost:8000/").text

    ref = send_request.remote()

    def check_num_replicas(num: int):
        actors = state_api.list_actors(
            filters=[
                ("class_name", "=", dep_id.to_replica_actor_class_name()),
                ("state", "=", "ALIVE"),
            ]
        )
        return len(actors) == num

    wait_for_condition(check_num_replicas, retry_interval_ms=1000, timeout=20, num=1)

    signal.send.remote()
    existing_pid = ray.get(ref)

    # Step 4: Change the max replicas to 2
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )
    wait_for_condition(check_num_replicas, retry_interval_ms=1000, timeout=20, num=1)

    # Step 5: Make sure it is the same replica (lightweight change).
    for _ in range(10):
        other_pid = ray.get(send_request.remote())
        assert other_pid == existing_pid

    # Step 6: Make sure initial_replicas overrides previous replicas
    app_config["deployments"][0]["autoscaling_config"]["max_replicas"] = 5
    app_config["deployments"][0]["autoscaling_config"]["initial_replicas"] = 3
    app_config["deployments"][0]["autoscaling_config"]["upscale_delay"] = 600
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status == "RUNNING"
    )
    wait_for_condition(check_num_replicas, retry_interval_ms=1000, timeout=20, num=3)

    # Step 7: Make sure original replica is still running (lightweight change)
    pids = set()
    for _ in range(15):
        pids.add(ray.get(send_request.remote()))
    assert existing_pid in pids


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_autoscaling_status_changes(serve_instance):
    """Test status changes when autoscaling deployments are deployed.

    This test runs an autoscaling deployment and an actor called the
    EventManager. During initialization, each replica creates an asyncio.Event
    in the EventManager, and it waits on the event. Once the event is set, the
    replica can finish initializing. The test uses this EventManager to control
    the number of replicas that should be running at a given time.

    The test does the following:

    1.  Starts an EventManager.
    2.  Deploys an autoscaling deployment with min_replicas 3.
    3.  Releases 2 replicas via the EventManager.
    4.  Checks that the deployment remains in the UPDATING status.
    5.  Redeploys the deployment with min_replicas 4.
    6.  Releases 1 more replica via the EventManager.
    7.  Checks that the deployment remains in the UPDATING status.
    8.  Releases 1 more replica.
    9.  Checks that the deployment enters HEALTHY status.
    10. Redeploys the deployment with min_replicas 5.
    11. Checks that the deployment re-enters and remains in the UPDATING status.
    12. Releases 1 more replica.
    13  Checks that the deployment enters HEALTHY status.
    """

    @ray.remote
    class EventManager:
        """Manages events for each deployment replica.

        This actor uses a goal-state architecture. The test sets a max number
        of replicas to run. Whenever this manager creates or removes an event,
        it checks how many replicas are running and attempts to match the goal
        state.
        """

        def __init__(self):
            self._max_replicas_to_run = 0

            # This dictionary maps replica names -> asyncio.Event.
            self._events: Dict[str, asyncio.Event] = dict()

        def get_num_running_replicas(self):
            running_replicas = [
                actor_name
                for actor_name, event in self._events.items()
                if event.is_set()
            ]
            return len(running_replicas)

        def release_replicas(self):
            """Releases replicas until self._max_replicas_to_run are released."""

            num_replicas_released = 0
            for _, event in self._events.items():
                if self.get_num_running_replicas() < self._max_replicas_to_run:
                    if not event.is_set():
                        event.set()
                        num_replicas_released += 1
                else:
                    break

            if num_replicas_released > 0:
                print(
                    f"Started running {num_replicas_released} replicas. "
                    f"{self.get_waiter_statuses()}"
                )

        async def wait(self, actor_name):
            print(f"Replica {actor_name} started waiting...")
            event = asyncio.Event()
            self._events[actor_name] = event
            self.release_replicas()
            await event.wait()
            print(f"Replica {actor_name} finished waiting.")

        async def set_max_replicas_to_run(self, max_num_replicas: int = 1):
            print(f"Setting _max_replicas_to_run to {max_num_replicas}.")
            self._max_replicas_to_run = max_num_replicas
            self.release_replicas()

        async def get_max_replicas_to_run(self) -> int:
            return self._max_replicas_to_run

        async def num_active_replicas(self) -> int:
            """The number of replicas that are waiting or running."""

            return len(self._events)

        def get_waiter_statuses(self) -> Dict[str, bool]:
            return {
                actor_name: event.is_set() for actor_name, event in self._events.items()
            }

        async def clear_dead_replicas(self):
            """Clears dead replicas from internal _events dictionary."""

            actor_names = list(self._events.keys())
            for name in actor_names:
                try:
                    ray.get_actor(name=name, namespace=SERVE_NAMESPACE)
                except ValueError:
                    print(f"Actor {name} has died. Removing event.")
                    self._events.pop(name)

            self.release_replicas()

    print("Starting EventManager actor...")

    event_manager_actor_name = "event_manager_actor"
    event_manager = EventManager.options(
        name=event_manager_actor_name, namespace=SERVE_NAMESPACE
    ).remote()

    print("Starting Serve app...")

    deployment_name = "autoscaling_app"
    min_replicas = 3
    max_replicas = 15

    @serve.deployment(
        name=deployment_name,
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        ),
        ray_actor_options=dict(num_cpus=0),
        graceful_shutdown_timeout_s=0,
    )
    class AutoscalingDeployment:
        """Deployment that autoscales."""

        async def __init__(self):
            self.name = ray.get_runtime_context().get_actor_name()
            print(f"Replica {self.name} initializing...")
            event_manager = ray.get_actor(
                name=event_manager_actor_name, namespace=SERVE_NAMESPACE
            )
            await event_manager.wait.remote(self.name)
            print(f"Replica {self.name} has initialized.")

    app_name = "autoscaling_app"
    app = AutoscalingDeployment.bind()

    # Start the AutoscalingDeployment.
    serve.run(app, name=app_name, _blocking=False)

    # Active replicas are replicas that are waiting or running.
    expected_num_active_replicas: int = min_replicas

    def check_num_active_replicas(expected: int) -> bool:
        ray.get(event_manager.clear_dead_replicas.remote())
        assert ray.get(event_manager.num_active_replicas.remote()) == expected
        return True

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Releasing some replicas...")

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas - 1))

    # Wait for replicas to start.
    print("Waiting for replicas to run.")

    def replicas_running(expected_num_running_replicas: int) -> bool:
        ray.get(event_manager.clear_dead_replicas.remote())
        status = serve.status()
        app_status = status.applications[app_name]
        deployment_status = app_status.deployments[deployment_name]
        num_running_replicas = deployment_status.replica_states.get(
            ReplicaState.RUNNING, 0
        )
        assert num_running_replicas == expected_num_running_replicas, (
            f"{app_status}, {ray.available_resources()}, "
            f"{ray.get(event_manager.get_waiter_statuses.remote())}, "
            f"{ray.get(event_manager.get_max_replicas_to_run.remote())}"
        )
        return True

    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=(min_replicas - 1),
        timeout=15,
    )

    def check_expected_statuses(
        expected_app_status: ApplicationStatus,
        expected_deployment_status: DeploymentStatus,
        expected_deployment_status_trigger: DeploymentStatusTrigger,
    ) -> bool:
        status = serve.status()

        app_status = status.applications[app_name]
        assert app_status.status == expected_app_status, f"{app_status}"

        deployment_status = app_status.deployments[deployment_name]
        assert (
            deployment_status.status == expected_deployment_status
        ), f"{deployment_status}"
        assert (
            deployment_status.status_trigger == expected_deployment_status_trigger
        ), f"{deployment_status}"

        return True

    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    # Check that these statuses don't change over time.
    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print("Statuses are still as expected. Redeploying...")

    # Check the status after redeploying the deployment.
    min_replicas += 1
    app = AutoscalingDeployment.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        )
    ).bind()
    serve.run(app, name=app_name, _blocking=False)
    expected_num_active_replicas = min_replicas

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Releasing some replicas...")

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas - 1))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=(min_replicas - 1),
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print(
        "Statuses are still as expected. "
        "Releasing some replicas and checking again..."
    )

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)

    # Release enough replicas for deployment to enter autoscaling bounds.
    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=min_replicas,
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.RUNNING,
        DeploymentStatus.HEALTHY,
        DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
    )

    print("Statuses are as expected. Redeploying with higher min_replicas...")
    min_replicas += 1
    app = AutoscalingDeployment.options(
        autoscaling_config=AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        )
    ).bind()
    serve.run(app, name=app_name, _blocking=False)
    expected_num_active_replicas = min_replicas

    wait_for_condition(check_num_active_replicas, expected=expected_num_active_replicas)
    print("Replicas have started waiting. Checking statuses...")

    # DeploymentStatus should return to UPDATING because the
    # autoscaling_config changed.
    wait_for_condition(
        check_expected_statuses,
        expected_app_status=ApplicationStatus.DEPLOYING,
        expected_deployment_status=DeploymentStatus.UPDATING,
        expected_deployment_status_trigger=(
            DeploymentStatusTrigger.CONFIG_UPDATE_STARTED
        ),
    )

    print("Statuses are as expected. Sleeping briefly and checking again...")
    time.sleep(1.5)
    check_expected_statuses(
        ApplicationStatus.DEPLOYING,
        DeploymentStatus.UPDATING,
        DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
    )

    print(
        "Statuses are still as expected. Releasing some replicas and checking again..."
    )

    ray.get(event_manager.set_max_replicas_to_run.remote(min_replicas))
    wait_for_condition(
        replicas_running,
        expected_num_running_replicas=min_replicas,
        timeout=20,
    )

    check_expected_statuses(
        ApplicationStatus.RUNNING,
        DeploymentStatus.HEALTHY,
        DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED,
    )

    print("Statuses are as expected.")


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
