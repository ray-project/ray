import os
import sys
import tempfile
import time
from unittest import mock
from typing import List, Iterable
import zipfile

import pytest
import requests

from ray._private.test_utils import SignalActor, wait_for_condition
from ray.serve._private.autoscaling_policy import (
    BasicAutoscalingPolicy,
    calculate_desired_num_replicas,
)
from ray.serve._private.common import DeploymentInfo
from ray.serve._private.deployment_state import ReplicaState
from ray.serve.config import AutoscalingConfig
from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S
from ray.serve.controller import ServeController
from ray.serve.deployment import Deployment
import ray.experimental.state.api as state_api
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient

import ray
from ray import serve
from ray.serve.generated.serve_pb2 import DeploymentRouteList

import numpy as np


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

    def test_scale_up(self):
        config = AutoscalingConfig(
            min_replicas=0, max_replicas=100, target_num_ongoing_requests_per_replica=1
        )
        num_replicas = 10
        num_ongoing_requests = [2.0] * num_replicas
        desired_num_replicas = calculate_desired_num_replicas(
            autoscaling_config=config, current_num_ongoing_requests=num_ongoing_requests
        )
        assert 19 <= desired_num_replicas <= 21  # 10 * 2 = 20

    def test_scale_down(self):
        config = AutoscalingConfig(
            min_replicas=0, max_replicas=100, target_num_ongoing_requests_per_replica=1
        )
        num_replicas = 10
        num_ongoing_requests = [0.5] * num_replicas
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


def get_running_replicas(controller: ServeController, deployment: Deployment) -> List:
    """Get the replicas currently running for given deployment"""
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment.name)
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return running_replicas


def get_running_replica_tags(
    controller: ServeController, deployment: Deployment
) -> List:
    """Get the replica tags of running replicas for given deployment"""
    running_replicas = get_running_replicas(controller, deployment)
    return [replica.replica_tag for replica in running_replicas]


def get_num_running_replicas(
    controller: ServeController, deployment: Deployment
) -> int:
    """Get the amount of replicas currently running for given deployment"""
    running_replicas = get_running_replicas(controller, deployment)
    return len(running_replicas)


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


def get_deployment_start_time(controller: ServeController, deployment: Deployment):
    """Return start time for given deployment"""
    deployment_route_list = DeploymentRouteList.FromString(
        ray.get(controller.list_deployments.remote())
    )
    deployments = {
        deployment_route.deployment_info.name: (
            DeploymentInfo.from_proto(deployment_route.deployment_info),
            deployment_route.route if deployment_route.route != "" else None,
        )
        for deployment_route in deployment_route_list.deployment_routes
    }
    deployment_info, _route_prefix = deployments[deployment.name]
    return deployment_info.start_time_ms


@pytest.mark.parametrize("min_replicas", [1, 2])
def test_e2e_basic_scale_up_down(min_replicas, serve_instance):
    """Send 100 requests and check that we autoscale up, and then back down."""

    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": 3,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0,
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

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    [handle.remote() for _ in range(100)]

    # scale up one more replica from min_replicas
    wait_for_condition(
        lambda: get_num_running_replicas(controller, A) >= min_replicas + 1
    )
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= min_replicas)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_basic_scale_up_down_with_0_replica(serve_instance):
    """Send 100 requests and check that we autoscale up, and then back down."""

    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 0,
            "max_replicas": 2,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0,
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

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    [handle.remote() for _ in range(100)]

    # scale up one more replica from min_replicas
    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 1)
    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 0)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


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
    assert get_num_running_replicas(controller, A) == 2


def test_upscale_downscale_delay():
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


def test_replicas_delayed_startup():
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
def test_fluctuating_ongoing_requests(delay_s):
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
def test_imbalanced_replicas(ongoing_requests):
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
            config.target_num_ongoing_requests_per_replica - np.mean(ongoing_requests)
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
def test_single_replica_receives_all_requests(ongoing_requests):
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


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_bursty(serve_instance):
    """
    Sends 100 requests in bursts. Uses delays for smooth provisioning.
    """

    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 2,
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

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    [handle.remote() for _ in range(100)]

    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 2)
    num_replicas = get_num_running_replicas(controller, A)
    signal.send.remote()

    # Execute a bursty workload that issues 100 requests every 0.05 seconds
    # The SignalActor allows all requests in a burst to be queued before they
    # are all executed, which increases the
    # target_in_flight_requests_per_replica. Then the send method will bring
    # it back to 0. This bursty behavior should be smoothed by the delay
    # parameters.
    for _ in range(5):
        time.sleep(0.05)
        assert get_num_running_replicas(controller, A) == num_replicas
        [handle.remote() for _ in range(100)]
        signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 1)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_intermediate_downscaling(serve_instance):
    """
    Scales up, then down, and up again.
    """

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

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    A.get_handle()
    [handle.remote() for _ in range(50)]

    wait_for_condition(
        lambda: get_num_running_replicas(controller, A) >= 20, timeout=30
    )
    signal.send.remote()

    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 1, timeout=30)
    signal.send.remote(clear=True)

    [handle.remote() for _ in range(50)]
    wait_for_condition(
        lambda: get_num_running_replicas(controller, A) >= 20, timeout=30
    )

    signal.send.remote()
    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) < 1, timeout=30)

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.skip(reason="Currently failing with undefined behavior")
def test_e2e_update_autoscaling_deployment(serve_instance):
    # See https://github.com/ray-project/ray/issues/21017 for details

    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 0,
            "max_replicas": 10,
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
    print("Deployed A with min_replicas 1 and max_replicas 10.")

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    assert get_num_running_replicas(controller, A) == 0

    [handle.remote() for _ in range(400)]
    print("Issued 400 requests.")

    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 10)
    print("Scaled to 10 replicas.")
    first_deployment_replicas = get_running_replica_tags(controller, A)

    assert get_num_running_replicas(controller, A) < 20

    [handle.remote() for _ in range(458)]
    time.sleep(3)
    print("Issued 458 requests. Request routing in-progress.")

    serve.run(
        A.options(
            autoscaling_config={
                "metrics_interval_s": 0.1,
                "min_replicas": 2,
                "max_replicas": 20,
                "look_back_period_s": 0.2,
                "downscale_delay_s": 0.2,
                "upscale_delay_s": 0.2,
            },
            version="v1",
        ).bind()
    )
    print("Redeployed A.")

    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 20)
    print("Scaled up to 20 requests.")
    second_deployment_replicas = get_running_replica_tags(controller, A)

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 2)
    assert get_num_running_replicas(controller, A) > 1

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time

    # scale down to 0
    serve.run(
        A.options(
            autoscaling_config={
                "metrics_interval_s": 0.1,
                "min_replicas": 0,
                "max_replicas": 20,
                "look_back_period_s": 0.2,
                "downscale_delay_s": 0.2,
                "upscale_delay_s": 0.2,
            },
            version="v1",
        ).bind()
    )
    print("Redeployed A.")

    wait_for_condition(lambda: get_num_running_replicas(controller, A) < 1)
    assert get_num_running_replicas(controller, A) == 0

    # scale up
    [handle.remote() for _ in range(400)]
    wait_for_condition(lambda: get_num_running_replicas(controller, A) > 0)
    signal.send.remote()
    wait_for_condition(lambda: get_num_running_replicas(controller, A) < 1)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_raise_min_replicas(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 0,
            "max_replicas": 10,
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

    A.deploy()
    print("Deployed A.")

    controller = serve_instance._controller
    start_time = get_deployment_start_time(controller, A)

    assert get_num_running_replicas(controller, A) == 0

    handle = A.get_handle()
    [handle.remote() for _ in range(1)]
    print("Issued one request.")

    time.sleep(2)
    assert get_num_running_replicas(controller, A) == 1
    print("Scale up to 1 replica.")

    first_deployment_replicas = get_running_replica_tags(controller, A)

    A.options(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 2,
            "max_replicas": 10,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2,
        },
        graceful_shutdown_timeout_s=1,
        max_concurrent_queries=1000,
        version="v1",
    ).deploy()
    print("Redeployed A with min_replicas set to 2.")

    wait_for_condition(lambda: get_num_running_replicas(controller, A) >= 2)
    time.sleep(5)

    # Confirm that autoscaler doesn't scale above 2 even after waiting
    assert get_num_running_replicas(controller, A) == 2
    print("Autoscaled to 2 without issuing any new requests.")

    second_deployment_replicas = get_running_replica_tags(controller, A)

    # Confirm that none of the original replicas were de-provisioned
    assert_no_replicas_deprovisioned(
        first_deployment_replicas, second_deployment_replicas
    )

    signal.send.remote()
    time.sleep(1)
    print("Completed request.")

    # As the queue is drained, we should scale back down.
    wait_for_condition(lambda: get_num_running_replicas(controller, A) <= 2)
    assert get_num_running_replicas(controller, A) > 1
    print("Stayed at 2 replicas.")

    # Make sure start time did not change for the deployment
    assert get_deployment_start_time(controller, A) == start_time


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas(serve_instance):
    signal = SignalActor.remote()

    @serve.deployment(
        max_concurrent_queries=5,
        # The config will trigger scale up really quickly and then
        # wait close to forever to downscale.
        autoscaling_config=AutoscalingConfig(
            min_replicas=1,
            max_replicas=2,
            downscale_delay_s=600,
            upscale_delay_s=0,
            metrics_interval_s=1,
            look_back_period_s=1,
        ),
    )
    def f():
        ray.get(signal.wait.remote())
        time.sleep(0.2)
        return os.getpid()

    handle = serve.run(f.bind())
    refs = [handle.remote() for _ in range(10)]

    def check_two_replicas():
        actors = state_api.list_actors(
            filters=[("class_name", "=", "ServeReplica:f"), ("state", "=", "ALIVE")]
        )
        print(actors)
        return len(actors) == 2

    wait_for_condition(check_two_replicas, retry_interval_ms=1000, timeout=20)

    signal.send.remote()

    old_pids = set(ray.get(refs))
    assert len(old_pids) == 2

    # Now re-deploy the application, make sure it is still 2 replicas and it shouldn't
    # be scaled down.
    handle = serve.run(f.bind())
    new_pids = set(ray.get([handle.remote() for _ in range(10)]))
    assert len(new_pids) == 2

    def check_two_new_replicas_two_old():
        live_actors = state_api.list_actors(
            filters=[("class_name", "=", "ServeReplica:f"), ("state", "=", "ALIVE")]
        )
        dead_actors = state_api.list_actors(
            filters=[("class_name", "=", "ServeReplica:f"), ("state", "=", "DEAD")]
        )

        return len(live_actors) == 2 and len(dead_actors) == 2

    wait_for_condition(
        check_two_new_replicas_two_old, retry_interval_ms=1000, timeout=20
    )


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_e2e_preserve_prev_replicas_rest_api(serve_instance):
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
def f():
    signal = ray.get_actor("signal", namespace="serve")
    ray.get(signal.wait.remote())
    return os.getpid()


app = f.bind()
""".encode()
                )

    # Step 2: Deploy it with max_replicas=1
    payload = {
        "import_path": "app:app",
        "runtime_env": {"working_dir": f"file://{tmp_path.name}"},
        "deployments": [
            {
                "name": "f",
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

    client = ServeSubmissionClient("http://localhost:52365")
    client.deploy_application(payload)
    wait_for_condition(lambda: client.get_status()["app_status"]["status"] == "RUNNING")

    # Step 3: Verify that it can scale from 0 to 1.
    @ray.remote
    def send_request():
        return requests.get("http://localhost:8000/").text

    ref = send_request.remote()

    def check_one_replicas():
        actors = state_api.list_actors(
            filters=[("class_name", "=", "ServeReplica:f"), ("state", "=", "ALIVE")]
        )
        return len(actors) == 1

    wait_for_condition(check_one_replicas, retry_interval_ms=1000, timeout=20)

    signal.send.remote()
    existing_pid = ray.get(ref)

    # Step 4: Change the max replicas to 2
    payload["deployments"][0]["autoscaling_config"]["max_replicas"] = 2
    client.deploy_application(payload)
    wait_for_condition(lambda: client.get_status()["app_status"]["status"] == "RUNNING")
    wait_for_condition(check_one_replicas, retry_interval_ms=1000, timeout=20)

    # Step 5: Make sure it is the same replica (lightweight change).
    for _ in range(10):
        other_pid = ray.get(send_request.remote())
        assert other_pid == existing_pid


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
