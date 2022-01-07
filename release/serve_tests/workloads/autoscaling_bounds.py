#!/usr/bin/env python3
"""
Benchmark test for autoscaling a single deployment at 1k no-op replica scale.

1) Start with a single head node.
2) Autoscale to 1k no-op replicas over N nodes.
3) Launch wrk in each running node to simulate load balanced request
4) Run a 10-minute wrk trial on each node, aggregate results.
5) Autoscale back down to 1 no-op replica
"""

import click
import math
import os

import ray
from ray import serve
from ray.serve.utils import logger
from serve_test_utils import (
    run_wrk_on_all_nodes,
    save_test_results,
)
from serve_test_cluster_utils import (
    setup_local_single_node_cluster,
    setup_anyscale_cluster,
    warm_up_one_cluster,
    NUM_CPU_PER_NODE,
)
from ray.serve.controller import ServeController
from ray.serve.deployment_state import ReplicaState
from ray._private.test_utils import SignalActor, wait_for_condition
from typing import Optional

# Experiment configs
DEFAULT_SMOKE_TEST_MIN_REPLICA = 1
DEFAULT_FULL_TEST_MIN_REPLICA = 1
DEFAULT_SMOKE_TEST_MAX_REPLICA = 4
DEFAULT_FULL_TEST_MAX_REPLICA = 100

# Experiment configs - wrk specific
DEFAULT_SMOKE_TEST_TRIAL_LENGTH = "5s"
DEFAULT_FULL_TEST_TRIAL_LENGTH = "5m"


def get_num_running_replicas(controller: ServeController,
                             deployment_name: str) -> int:
    """
    Get the amount of replicas currently running for given deployment.
    """
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_name))
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return len(running_replicas)


def running_replicas_bounded(controller: ServeController,
                             deployment_name: str,
                             min: int = -float("inf"),
                             max: int = float("inf")) -> bool:

    return min <= get_num_running_replicas(controller, deployment_name) <= max


def convert_duration_to_seconds(duration_string: str) -> float:
    """
    Takes in a string indicated a time duration and converts it to a float
    in seconds.

    duration_string: The duration parameter string for a wrk command. Some
                     examples are "1s", "5m", "3us", "17h".
    return: A float representing the duration in seconds. Some examples are:
            "1s" -> 1.0, "5m" -> 300.0, "3us" -> 0.000003, "17h" -> 61200.0.
    """
    if len(duration_string) < 2:
        raise ValueError("duration_string should be wrk duration formatted "
                         "string with a numeric duration and time units.")

    unit_multipliers = {"us": 0.000001, "ms": 0.001, "s": 1, "m": 60, "h": 360}

    duration, unit = None, None

    if duration_string[-2].isalpha():
        duration, unit = float(duration_string[:-2]), duration_string[-2:]
    else:
        duration, unit = float(duration_string[:-1]), duration_string[-1:]

    return duration * unit_multipliers[unit]


def deploy_replicas(min_replicas: int, max_replicas: int, trial_length: str,
                    signal: SignalActor, deployment_name: str):

    # A long _graceful_shutdown_timeout_s prevents any downscaling until after
    # the wrk simulation ends. This allows the test to check whether the
    # autoscaler can sustain high loads of traffic for long periods of time.
    @serve.deployment(
        name=deployment_name,
        _graceful_shutdown_timeout_s=convert_duration_to_seconds(trial_length),
        _autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "look_back_period_s": 0.2,
            "downscale_delay_s": 0.2,
            "upscale_delay_s": 0.2
        },
        version="v1")
    class Echo:
        def __call__(self):
            ray.get(signal.wait.remote())
            return "echo woke up"

    Echo.deploy()

    return Echo.get_handle()


@click.command()
@click.option("--min-replicas", "-min", type=int)
@click.option("--max-replicas", "-max", type=int)
@click.option("--trial-length", "-tl", type=str)
def main(max_replicas: Optional[int], min_replicas: Optional[int],
         trial_length: Optional[str]):
    # Give default cluster parameter values based on smoke_test config
    # if user provided values explicitly, use them instead.
    # IS_SMOKE_TEST is set by args of releaser's e2e.py
    smoke_test = os.environ.get("IS_SMOKE_TEST", "1")
    if smoke_test == "1":
        min_replicas = min_replicas or DEFAULT_SMOKE_TEST_MIN_REPLICA
        max_replicas = max_replicas or DEFAULT_SMOKE_TEST_MAX_REPLICA
        trial_length = trial_length or DEFAULT_SMOKE_TEST_TRIAL_LENGTH
        logger.info(
            f"Running local / smoke test with a minimum of {min_replicas} "
            f"replicas and a maximum of {max_replicas} replicas.\n")

        # Choose cluster setup based on user config. Local test uses Cluster()
        # to mock actors that requires # of nodes to be specified, but ray
        # client doesn't need to
        num_nodes = int(math.ceil(max_replicas / NUM_CPU_PER_NODE))
        logger.info(f"Setting up local ray cluster with {num_nodes} nodes.\n")
        serve_client = setup_local_single_node_cluster(num_nodes)[0]
    else:
        min_replicas = min_replicas or DEFAULT_FULL_TEST_MIN_REPLICA
        max_replicas = max_replicas or DEFAULT_FULL_TEST_MAX_REPLICA
        trial_length = trial_length or DEFAULT_FULL_TEST_TRIAL_LENGTH
        logger.info(
            f"Running local / smoke test with a minimum of {min_replicas} "
            f"replicas and a maximum of {max_replicas} replicas.\n")
        logger.info("Setting up anyscale ray cluster. \n")
        serve_client = setup_anyscale_cluster()

    signal = SignalActor.remote()

    http_host = str(serve_client._http_config.host)
    http_port = str(serve_client._http_config.port)
    logger.info(f"Ray serve http_host: {http_host}, http_port: {http_port}")

    controller = serve_client._controller
    deployment_name = "echo"

    logger.info(f"Deploying with {min_replicas} replicas ....\n")
    deploy_replicas(min_replicas, max_replicas, trial_length, signal,
                    deployment_name)

    logger.info("Warming up cluster ....\n")
    ray.get(
        warm_up_one_cluster.remote(
            10, http_host, http_port, deployment_name, nonblocking=True))

    ray.get(signal.send.remote())

    # Allow deployments to downscale to min_replicas
    wait_for_condition(lambda: running_replicas_bounded(controller,
                                                        deployment_name,
                                                        max=min_replicas))

    for _ in range(2):

        ray.get(signal.send.remote(clear=True))

        logger.info(
            f"Starting wrk trial on all nodes for {trial_length} ....\n")
        # For detailed discussion, see https://github.com/wg/wrk/issues/205
        # TODO:(jiaodong) What's the best number to use here ?
        all_endpoints = list(serve.list_deployments().keys())
        run_wrk_on_all_nodes(
            trial_length,
            100,
            http_host,
            http_port,
            all_endpoints=all_endpoints)

        # Check that deployments upscaled to max_replicas
        wait_for_condition(lambda: running_replicas_bounded(controller,
                                                            deployment_name,
                                                            min=max_replicas))
        logger.info("Deployments scaled up to max replicas ....\n")

        ray.get(signal.send.remote())
        logger.info("Clearing all requests ....\n")

        # Check that deployments scale back to min_replicas
        wait_for_condition(lambda: running_replicas_bounded(controller,
                                                            deployment_name,
                                                            max=min_replicas))
        logger.info("Deployments scaled down to min replicas ....\n")

    save_test_results(
        {
            "success_message": "autoscaling successful!"
        },
        default_output_file="/tmp/autoscaling_bounds.json")


if __name__ == "__main__":
    main()
    import pytest
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
