from unittest import mock

from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve.autoscaling_policy import (BasicAutoscalingPolicy,
                                          calculate_desired_num_replicas)
from ray.serve.deployment_state import ReplicaState
from ray.serve.config import AutoscalingConfig
from ray.serve.constants import CONTROL_LOOP_PERIOD_S
from ray.serve.controller import ServeController
from ray.serve.api import Deployment

import ray
from ray import serve


class TestCalculateDesiredNumReplicas:
    def test_bounds_checking(self):
        num_replicas = 10
        max_replicas = 11
        min_replicas = 9
        config = AutoscalingConfig(
            max_replicas=max_replicas,
            min_replicas=min_replicas,
            target_num_ongoing_requests_per_replica=100)

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[150] * num_replicas)
        assert desired_num_replicas == max_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=[50] * num_replicas)
        assert desired_num_replicas == min_replicas

        for i in range(50, 150):
            desired_num_replicas = calculate_desired_num_replicas(
                autoscaling_config=config,
                current_num_ongoing_requests=[i] * num_replicas)
            assert min_replicas <= desired_num_replicas <= max_replicas

    def test_scale_up(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1)
        num_replicas = 10
        num_ongoing_requests = [2.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    def test_scale_down(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1)
        num_replicas = 10
        num_ongoing_requests = [0.5] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 4 <= desired_num_replicas <= 6  # 10 * 0.5 = 5

    def test_smoothing_factor(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            target_num_ongoing_requests_per_replica=1,
            smoothing_factor=0.5)
        num_replicas = 10

        num_ongoing_requests = [4.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 24 <= desired_num_replicas <= 26  # 10 + 0.5 * (40 - 10) = 25

        num_ongoing_requests = [0.25] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config,
            current_num_ongoing_requests=num_ongoing_requests)
        assert 5 <= desired_num_replicas <= 8  # 10 + 0.5 * (2.5 - 10) = 6.25


def get_num_running_replicas(controller: ServeController,
                             deployment: Deployment) -> int:
    """ Get the amount of replicas currently running for given deployment """
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment.name))
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return len(running_replicas)


def get_deployment_start_time(controller: ServeController,
                              deployment: Deployment):
    """ Return start time for given deployment """
    deployments = ray.get(controller.list_deployments.remote())
    deployment_info, _route_prefix = deployments[deployment.name]
    return deployment_info.start_time_ms


def test_e2e_basic_scale_up_down(serve_instance):
    """Send 100 requests and check that we autoscale up, and then back down."""

    signal = SignalActor.remote()

    @serve.deployment(
        _autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 2,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0,
            "upscale_delay_s": 0
        },
        # We will send over a lot of queries. This will make sure replicas are
        # killed quickly during cleanup.
        _graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1")
    class A:
        def __call__(self):
            ray.get(signal.wait.remote())

    A.deploy()

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    handle = A.get_handle()
    [handle.remote() for _ in range(100)]

    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 2)
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 1)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


@mock.patch.object(ServeController, "autoscale")
def test_initial_num_replicas(mock, serve_instance):
    """ assert that the inital amount of replicas a deployment is launched with
    respects the bounds set by autoscaling_config.

    For this test we mock out the autoscaling loop, make sure the number of
    replicas is set correctly before we hit the autoscaling procedure.
    """

    @serve.deployment(
        _autoscaling_config={
            "min_replicas": 2,
            "max_replicas": 4,
        },
        version="v1")
    class A:
        def __call__(self):
            return "ok!"

    A.deploy()

    controller = serve_instance._controller
    assert get_num_running_replicas(controller, A) == 2


def test_upscale_downscale_delay():
    """Unit test for upscale_delay_s and downscale_delay_s."""

    upscale_delay_s = 30.0
    downscale_delay_s = 600.0

    config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=2,
        target_num_ongoing_requests_per_replica=1,
        upscale_delay_s=30.0,
        downscale_delay_s=600.0)

    policy = BasicAutoscalingPolicy(config)

    upscale_wait_periods = int(upscale_delay_s / CONTROL_LOOP_PERIOD_S)
    downscale_wait_periods = int(downscale_delay_s / CONTROL_LOOP_PERIOD_S)

    overload_requests = [100]

    # We should scale up only after enough consecutive scale-up decisions.
    for i in range(upscale_wait_periods):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1)
        assert new_num_replicas == 1, i

    new_num_replicas = policy.get_decision_num_replicas(
        current_num_ongoing_requests=overload_requests,
        curr_target_num_replicas=1)
    assert new_num_replicas == 2

    no_requests = [0, 0]

    # We should scale down only after enough consecutive scale-down decisions.
    for i in range(downscale_wait_periods):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2)
        assert new_num_replicas == 2, i

    new_num_replicas = policy.get_decision_num_replicas(
        current_num_ongoing_requests=no_requests, curr_target_num_replicas=2)
    assert new_num_replicas == 1

    # Get some scale-up decisions, but not enough to trigger a scale up.
    for i in range(int(upscale_wait_periods / 2)):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1)
        assert new_num_replicas == 1, i

    # Interrupt with a scale-down decision.
    policy.get_decision_num_replicas(
        current_num_ongoing_requests=[0], curr_target_num_replicas=1)

    # The counter should be reset, so it should require `upscale_wait_periods`
    # more periods before we actually scale up.
    for i in range(upscale_wait_periods):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=overload_requests,
            curr_target_num_replicas=1)
        assert new_num_replicas == 1, i

    new_num_replicas = policy.get_decision_num_replicas(
        current_num_ongoing_requests=overload_requests,
        curr_target_num_replicas=1)
    assert new_num_replicas == 2

    # Get some scale-down decisions, but not enough to trigger a scale down.
    for i in range(int(downscale_wait_periods / 2)):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2)
        assert new_num_replicas == 2, i

    # Interrupt with a scale-up decision.
    policy.get_decision_num_replicas(
        current_num_ongoing_requests=[100, 100], curr_target_num_replicas=2)

    # The counter should be reset so it should require `downscale_wait_periods`
    # more periods before we actually scale down.
    for i in range(downscale_wait_periods):
        new_num_replicas = policy.get_decision_num_replicas(
            current_num_ongoing_requests=no_requests,
            curr_target_num_replicas=2)
        assert new_num_replicas == 2, i

    new_num_replicas = policy.get_decision_num_replicas(
        current_num_ongoing_requests=no_requests, curr_target_num_replicas=2)
    assert new_num_replicas == 1


def test_replicas_delayed_startup():
    """Unit test simulating replicas taking time to start up."""
    config = AutoscalingConfig(
        min_replicas=1,
        max_replicas=200,
        target_num_ongoing_requests_per_replica=1,
        upscale_delay_s=0,
        downscale_delay_s=100000)

    policy = BasicAutoscalingPolicy(config)

    new_num_replicas = policy.get_decision_num_replicas([100], 1)
    assert new_num_replicas == 100

    # New target is 100, but no new replicas finished spinning up during this
    # timestep.
    new_num_replicas = policy.get_decision_num_replicas([100], 100)
    assert new_num_replicas == 100

    # Two new replicas spun up during this timestep.
    new_num_replicas = policy.get_decision_num_replicas([100, 20, 3], 100)
    assert new_num_replicas == 123

    # A lot of queries got drained and a lot of replicas started up, but
    # new_num_replicas should not decrease, because of the downscale delay.
    new_num_replicas = policy.get_decision_num_replicas([6, 2, 1, 1], 123)
    assert new_num_replicas == 123


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
