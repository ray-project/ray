import asyncio
import sys
from copy import deepcopy
from typing import Dict

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve import Application
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, TargetCapacityScaleDirection
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema

INGRESS_DEPLOYMENT_NAME = "ingress"
INGRESS_DEPLOYMENT_NUM_REPLICAS = 6
DOWNSTREAM_DEPLOYMENT_NAME = "downstream"
DOWNSTREAM_DEPLOYMENT_NUM_REPLICAS = 2
SCALE_TO_ZERO_DEPLOYMENT_NAME = "zero"
SCALE_TO_ZERO_DEPLOYMENT_MAX_REPLICAS = 4
START_AT_10_DEPLOYMENT_NAME = "start_at_ten"
START_AT_10_DEPLOYMENT_INITIAL_REPLICAS = 10
START_AT_10_DEPLOYMENT_MIN_REPLICAS = 0
START_AT_10_DEPLOYMENT_MAX_REPLICAS = 20


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


def check_expected_num_replicas(
    deployment_to_num_replicas: Dict[str, int], app_name: str = SERVE_DEFAULT_APP_NAME
) -> bool:
    """Checks that the expected number of replicas for each deployment is running.

    Looks for the deployments in the app named app_name.
    """

    status = serve.status()
    assert app_name in status.applications

    application = status.applications[app_name]
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
        target_capacity=100,
        applications=[
            ServeApplicationSchema(
                import_path="ray.serve.tests.test_target_capacity:test_app"
            )
        ],
    )

    client.deploy_apps(config)
    wait_for_condition(lambda: serve.status().target_capacity == 100.0)
    assert (
        ray.get(client._controller._get_scale_direction.remote())
        == TargetCapacityScaleDirection.UP
    )

    # Scale down to target_capacity 50, both deployments should be at half scale.
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
    assert (
        ray.get(client._controller._get_scale_direction.remote())
        == TargetCapacityScaleDirection.DOWN
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
    assert (
        ray.get(client._controller._get_scale_direction.remote())
        == TargetCapacityScaleDirection.DOWN
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


@serve.deployment(
    ray_actor_options={"num_cpus": 0},
    max_concurrent_queries=2,
    graceful_shutdown_timeout_s=0,
)
class HangDeployment:
    async def __call__(self, *args):
        await asyncio.sleep(10000)


def create_hang_app(config: Dict) -> Application:
    name: str = config["name"]
    min_replicas: int = config["min_replicas"]
    initial_replicas: int = config["initial_replicas"]
    max_replicas: int = config["max_replicas"]

    return HangDeployment.options(
        name=name,
        autoscaling_config={
            "min_replicas": min_replicas,
            "initial_replicas": initial_replicas,
            "max_replicas": max_replicas,
            "target_num_ongoing_requests_per_replicas": 1,
            "metrics_interval_s": 0.01,
            "downscale_delay_s": 0.01,
        },
        graceful_shutdown_timeout_s=0.01,
    ).bind()


class TestInitialReplicasHandling:
    def deploy_config_and_wait_for_target_capacity(
        self,
        client: ServeControllerClient,
        config: ServeDeploySchema,
        target_capacity: float,
    ):
        config.target_capacity = target_capacity
        client.deploy_apps(config)
        wait_for_condition(lambda: serve.status().target_capacity == target_capacity)

    def test_initial_replicas_scales_down(
        self, shutdown_ray_and_serve, client: ServeControllerClient
    ):
        deployment_name = "start_at_ten"
        min_replicas = 0
        initial_replicas = 10

        config = ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    import_path="ray.serve.tests.test_target_capacity:create_hang_app",
                    args={
                        "name": deployment_name,
                        "min_replicas": 0,
                        "initial_replicas": initial_replicas,
                        "max_replicas": 20,
                    },
                ),
            ],
        )

        client.deploy_apps(config)
        wait_for_condition(lambda: len(serve.status().applications) == 1)
        assert serve.status().target_capacity is None

        # Set a target capacity
        config.target_capacity = 100
        client.deploy_apps(config)
        wait_for_condition(lambda: serve.status().target_capacity == 100)
        # Initial replicas should be respected since setting a target_capacity
        # indicates that a new roll forward has started, and the cluster
        # should start scaling up.
        wait_for_condition(
            check_expected_num_replicas,
            deployment_to_num_replicas={deployment_name: initial_replicas},
        )

        # Kick off downscaling pattern.
        test_target_capacities = [90, 60, 20, 0]
        expected_num_replicas = [min_replicas] * len(test_target_capacities)

        for target_capacity, num_replicas in zip(
            test_target_capacities, expected_num_replicas
        ):
            print(f"target_capacity: {target_capacity}, num_replicas: {num_replicas}")
            config.target_capacity = target_capacity
            client.deploy_apps(config)
            wait_for_condition(
                lambda: serve.status().target_capacity == target_capacity
            )
            wait_for_condition(
                check_expected_num_replicas,
                deployment_to_num_replicas={deployment_name: num_replicas},
            )

    def test_initial_replicas_scales_up_and_down(
        self, shutdown_ray_and_serve, client: ServeControllerClient
    ):
        deployment_name = "start_at_ten"
        min_replicas = 0
        initial_replicas = 10

        config = ServeDeploySchema(
            target_capacity=0,
            applications=[
                ServeApplicationSchema(
                    import_path="ray.serve.tests.test_target_capacity:create_hang_app",
                    args={
                        "name": deployment_name,
                        "min_replicas": min_replicas,
                        "initial_replicas": initial_replicas,
                        "max_replicas": 2 * initial_replicas,
                    },
                ),
            ],
        )

        test_target_capacities = [0, 20, 60, 40, 30, 100, None]
        expected_num_replicas = [
            1,
            2,
            6,
            min_replicas,
            min_replicas,
            initial_replicas,
            min_replicas,
        ]

        for target_capacity, num_replicas in zip(
            test_target_capacities, expected_num_replicas
        ):
            print(f"target_capacity: {target_capacity}, num_replicas: {num_replicas}")
            config.target_capacity = target_capacity
            client.deploy_apps(config)
            wait_for_condition(
                lambda: serve.status().target_capacity == target_capacity
            )
            wait_for_condition(
                check_expected_num_replicas,
                deployment_to_num_replicas={deployment_name: num_replicas},
            )

    def test_initial_replicas_zero(
        self, shutdown_ray_and_serve, client: ServeControllerClient
    ):
        deployment_name = "start_at_zero"
        min_replicas = 0
        initial_replicas = 0

        config = ServeDeploySchema(
            target_capacity=0,
            applications=[
                ServeApplicationSchema(
                    import_path="ray.serve.tests.test_target_capacity:create_hang_app",
                    args={
                        "name": deployment_name,
                        "min_replicas": min_replicas,
                        "initial_replicas": initial_replicas,
                        "max_replicas": 20,
                    },
                ),
            ],
        )

        test_target_capacities = [0, 20, 60, 40, 30, 100, None]
        expected_num_replicas = [0] * len(test_target_capacities)

        for target_capacity, num_replicas in zip(
            test_target_capacities, expected_num_replicas
        ):
            config.target_capacity = target_capacity
            client.deploy_apps(config)
            wait_for_condition(
                lambda: serve.status().target_capacity == target_capacity
            )
            wait_for_condition(
                check_expected_num_replicas,
                deployment_to_num_replicas={deployment_name: num_replicas},
            )

    def test_initial_replicas_new_configs(
        self, shutdown_ray_and_serve, client: ServeControllerClient
    ):
        deployment_name = "start_at_ten"
        min_replicas = 0
        initial_replicas = 20
        config_target_capacity = 40

        config = ServeDeploySchema(
            target_capacity=config_target_capacity,
            applications=[
                ServeApplicationSchema(
                    name="app1",
                    import_path="ray.serve.tests.test_target_capacity:create_hang_app",
                    args={
                        "name": deployment_name,
                        "min_replicas": min_replicas,
                        "initial_replicas": initial_replicas,
                        "max_replicas": initial_replicas * 2,
                    },
                ),
            ],
        )

        # When deploying first config, initial_replicas * target_capacity
        # should be treated as floor
        client.deploy_apps(config)
        wait_for_condition(
            lambda: serve.status().target_capacity == config_target_capacity
        )
        wait_for_condition(
            check_expected_num_replicas,
            deployment_to_num_replicas={
                deployment_name: int(initial_replicas * config_target_capacity / 100)
            },
            app_name="app1",
        )

        # When deploying a new config, initial_replicas * target_capacity
        # should be treated as floor for all deployments, even if
        # target_capacity is lower
        new_deployment_name = f"new_{deployment_name}"
        new_config_target_capacity = config_target_capacity / 2

        new_config = deepcopy(config)
        new_config.applications.append(
            ServeApplicationSchema(
                name="app2",
                import_path="ray.serve.tests.test_target_capacity:create_hang_app",
                args={
                    "name": new_deployment_name,
                    "min_replicas": min_replicas,
                    "initial_replicas": initial_replicas,
                    "max_replicas": initial_replicas * 2,
                },
            ),
        )
        new_config.target_capacity = new_config_target_capacity

        client.deploy_apps(new_config)
        wait_for_condition(
            lambda: serve.status().target_capacity == new_config_target_capacity
        )
        wait_for_condition(
            check_expected_num_replicas,
            deployment_to_num_replicas={
                deployment_name: int(
                    initial_replicas * new_config_target_capacity / 100
                )
            },
            app_name="app1",
        )
        wait_for_condition(
            check_expected_num_replicas,
            deployment_to_num_replicas={
                new_deployment_name: int(
                    initial_replicas * new_config_target_capacity / 100
                )
            },
            app_name="app2",
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
