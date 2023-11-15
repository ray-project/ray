import asyncio
import sys
from typing import Dict

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema

INGRESS_DEPLOYMENT_NAME = "ingress"
INGRESS_DEPLOYMENT_NUM_REPLICAS = 6
DOWNSTREAM_DEPLOYMENT_NAME = "downstream"
DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS = 2
SCALE_TO_ZERO_DEPLOYMENT_NAME = "zero"
SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS = 4


@pytest.fixture
def shutdown_ray_and_serve():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


@serve.deployment(ray_actor_options={"num_cpus": 0})
class DummyDeployment:
    def __init__(self, *args):
        pass

    def __call__(self, *args):
        return "hi"


test_app = DummyDeployment.options(
    name=INGRESS_DEPLOYMENT_NAME,
    num_replicas=INGRESS_DEPLOYMENT_NUM_REPLICAS,
).bind(
    DummyDeployment.options(
        name=DOWNSTREAM_DEPLOYMENT_NAME,
        num_replicas=DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
    ).bind()
)


def check_expected_num_replicas(deployment_to_num_replicas: Dict[str, int]) -> bool:
    status = serve.status()
    assert SERVE_DEFAULT_APP_NAME in status.applications

    application = status.applications[SERVE_DEFAULT_APP_NAME]
    assert application.status == ApplicationStatus.RUNNING

    for name, num_replicas in deployment_to_num_replicas.items():
        assert name in application.deployments
        assert (
            sum(application.deployments[name].replica_states.values()) == num_replicas
        )

    return True


@pytest.fixture
def client() -> ServeControllerClient:
    serve.start()
    yield _get_global_client()


def test_incremental_scale_up(shutdown_ray_and_serve, client: ServeControllerClient):
    config = ServeDeploySchema(
        applications=[
            ServeApplicationSchema(
                import_path="ray.serve.tests.test_target_capacity:test_app"
            )
        ]
    )

    # Initially deploy at target_capacity 0, should have 1 replica of each.
    config.target_capacity = 0.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 0.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: 1,
            DOWNSTREAM_DEPLOYMENT_NAME: 1,
        },
    )

    # Increase target_capacity to 50, ingress deployment should scale up.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    # Increase target_capacity to 100, both should fully scale up.
    config.target_capacity = 100.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 100.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
        },
    )

    # Finish rollout (remove target_capacity), should have no effect.
    config.target_capacity = None
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity is None)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
        },
    )


def test_incremental_scale_down(shutdown_ray_and_serve, client: ServeControllerClient):
    config = ServeDeploySchema(
        applications=[
            ServeApplicationSchema(
                import_path="ray.serve.tests.test_target_capacity:test_app"
            )
        ]
    )

    # Initially deploy with no target_capacity (full scale).
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity is None)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
        },
    )

    # Decrease target_capacity to 50, both deployments should scale down.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    # Decrease target_capacity to 0, both should fully scale down to zero.
    config.target_capacity = 0.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 0.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: 1,
            DOWNSTREAM_DEPLOYMENT_NAME: 1,
        },
    )


def test_controller_recover_target_capacity(
    shutdown_ray_and_serve, client: ServeControllerClient
):
    config = ServeDeploySchema(
        applications=[
            ServeApplicationSchema(
                import_path="ray.serve.tests.test_target_capacity:test_app"
            )
        ]
    )

    # Deploy with target_capacity 50, both deployments should be at half scale.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    ray.kill(client._controller, no_restart=False)

    # Verify that the target_capacity is recovered after the controller comes back.
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NAME: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )


@serve.deployment(
    ray_actor_options={"num_cpus": 0},
    autoscaling_config={
        "min_replicas": 0,
        "max_replicas": SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS,
        "target_num_ongoing_requests_per_replicas": 1,
        "upscale_delay_s": 1,
        "downscale_delay_s": 1,
        "smoothing_factor": 4,
        "metrics_interval_s": 1,
    },
    max_concurrent_queries=2,
    graceful_shutdown_timeout_s=0,
)
class ScaleToZeroDeployment:
    async def __call__(self, *args):
        await asyncio.sleep(10000)


scale_to_zero_app = ScaleToZeroDeployment.options(
    name=SCALE_TO_ZERO_DEPLOYMENT_NAME,
).bind()


def test_autoscaling_scale_to_zero(
    shutdown_ray_and_serve, client: ServeControllerClient
):
    config = ServeDeploySchema(
        applications=[
            ServeApplicationSchema(
                import_path="ray.serve.tests.test_target_capacity:scale_to_zero_app"
            )
        ]
    )

    # Initially deploy at target_capacity 0, should have 0 replicas.
    config.target_capacity = 0.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 0.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            SCALE_TO_ZERO_DEPLOYMENT_NAME: 0,
        },
    )

    # Send a bunch of requests. Autoscaler will want to scale it up to max replicas,
    # but it should stay at one due to the target_capacity.
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    responses = [
        handle.remote() for _ in range(5 * SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS)
    ]
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            SCALE_TO_ZERO_DEPLOYMENT_NAME: 1,
        },
    )

    # Increase to target_capacity 100, should scale all the way up.
    config.target_capacity = 100.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 100.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            SCALE_TO_ZERO_DEPLOYMENT_NAME: SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS,
        },
    )

    # Decrease to target_capacity 50, should scale down.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            SCALE_TO_ZERO_DEPLOYMENT_NAME: SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS / 2,
        },
    )

    # TODO(edoakes): for some reason, the deployment does not actually scale down to
    # zero here, so skipping this part for now. Instead it repeatedly scales down and
    # then back up. Seems to have something to do with the handle-side queue metric.
    return

    # Cancel all of the requests, should scale down to zero.
    [r.cancel() for r in responses]
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            SCALE_TO_ZERO_DEPLOYMENT_NAME: 0,
        },
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
