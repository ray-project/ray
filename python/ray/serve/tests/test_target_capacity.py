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
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: 1,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: 1,
        },
    )

    # Increase target_capacity to 50, ingress deployment should scale up.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    # Increase target_capacity to 100, both should fully scale up.
    config.target_capacity = 100.0
    client.deploy_apps(config)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
        },
    )

    # Finish rollout (remove target_capacity), should have no effect.
    config.target_capacity = None
    client.deploy_apps(config)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
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
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS,
        },
    )

    # Decrease target_capacity to 50, both deployments should scale down.
    config.target_capacity = 50.0
    client.deploy_apps(config)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    # Decrease target_capacity to 0, both should fully scale down to zero.
    config.target_capacity = 0.0
    client.deploy_apps(config)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: 1,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: 1,
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
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )

    ray.kill(client._controller, no_restart=False)

    # Verify that the target_capacity is recovered after the controller comes back.
    wait_for_condition(lambda: serve.status().target_capacity == 50.0)
    wait_for_condition(
        check_expected_num_replicas,
        deployment_to_num_replicas={
            INGRESS_DEPLOYMENT_NAME: INGRESS_DEPLOYMENT_NUM_REPLICAS / 2,
            DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS: DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS / 2,
        },
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
