import os
import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Dict, Set
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import pytest
import requests

import ray
import ray.actor
import ray._private.state
from ray.experimental.state.api import list_actors

from ray import serve
from ray._private.test_utils import (
    wait_for_condition,
    SignalActor,
)
from ray.exceptions import RayActorError
from ray.serve.exceptions import RayServeException
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, DeploymentStatus, ReplicaState
from ray.serve._private.constants import (
    SERVE_NAMESPACE,
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)
from ray.serve.context import get_global_client
from ray.serve.schema import (
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.tests.conftest import call_ray_stop_only  # noqa: F401


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture()
def ray_instance(request):
    """Starts and stops a Ray instance for this test.

    Args:
        request: request.param should contain a dictionary of env vars and
            their values. The Ray instance will be started with these env vars.
    """

    original_env_vars = os.environ.copy()

    try:
        requested_env_vars = request.param
    except AttributeError:
        requested_env_vars = {}

    os.environ.update(requested_env_vars)

    yield ray.init(
        _metrics_export_port=9999,
        _system_config={
            "metrics_report_interval_ms": 1000,
            "task_retry_delay_ms": 50,
        },
    )

    ray.shutdown()

    os.environ.clear()
    os.environ.update(original_env_vars)


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(
        ["ray", "start", "--head"],
    )
    yield
    subprocess.check_output(
        ["ray", "stop", "--force"],
    )


@pytest.fixture(scope="function")
def start_and_shutdown_ray_cli_function():
    with start_and_shutdown_ray_cli():
        yield


@pytest.fixture(scope="class")
def start_and_shutdown_ray_cli_class():
    with start_and_shutdown_ray_cli():
        yield


def _check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


def test_standalone_actor_outside_serve():
    # https://github.com/ray-project/ray/issues/20066

    ray.init(num_cpus=8, namespace="serve")

    @ray.remote
    class MyActor:
        def ready(self):
            return

    a = MyActor.options(name="my_actor").remote()
    ray.get(a.ready.remote())

    serve.start()
    serve.shutdown()

    ray.get(a.ready.remote())
    ray.shutdown()


def test_memory_omitted_option(ray_shutdown):
    """Ensure that omitting memory doesn't break the deployment."""

    @serve.deployment(ray_actor_options={"num_cpus": 1, "num_gpus": 1})
    def hello(*args, **kwargs):
        return "world"

    ray.init(num_gpus=3, namespace="serve")
    handle = serve.run(hello.bind())

    assert ray.get(handle.remote()) == "world"


@pytest.mark.parametrize("detached", [True, False])
@pytest.mark.parametrize("ray_namespace", ["arbitrary", SERVE_NAMESPACE, None])
def test_serve_namespace(shutdown_ray, detached, ray_namespace):
    """Test that Serve starts in SERVE_NAMESPACE regardless of driver namespace."""

    with ray.init(namespace=ray_namespace) as ray_context:

        @serve.deployment
        def f(*args):
            return "got f"

        serve.run(f.bind())

        actors = list_actors(
            address=ray_context.address_info["address"],
            filters=[("state", "=", "ALIVE")],
        )

        assert len(actors) == 3

        # All actors should be in the SERVE_NAMESPACE, so none of these calls
        # should throw an error.
        for actor in actors:
            ray.get_actor(name=actor["name"], namespace=SERVE_NAMESPACE)

        assert requests.get("http://localhost:8000/f").text == "got f"

        serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_update_num_replicas(shutdown_ray, detached):
    """Test updating num_replicas."""

    with ray.init() as ray_context:

        @serve.deployment(num_replicas=2)
        def f(*args):
            return "got f"

        serve.run(f.bind())

        actors = list_actors(
            address=ray_context.address_info["address"],
            filters=[("state", "=", "ALIVE")],
        )

        serve.run(f.options(num_replicas=4).bind())
        updated_actors = list_actors(
            address=ray_context.address_info["address"],
            filters=[("state", "=", "ALIVE")],
        )

        # Check that only 2 new replicas were created
        assert len(updated_actors) == len(actors) + 2

        serve.run(f.options(num_replicas=1).bind())
        updated_actors = list_actors(
            address=ray_context.address_info["address"],
            filters=[("state", "=", "ALIVE")],
        )

        # Check that all but 1 replica has spun down
        assert len(updated_actors) == len(actors) - 1

        serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_refresh_controller_after_death(shutdown_ray, detached):
    """Check if serve.start() refreshes the controller handle if it's dead."""

    ray.init(namespace="ray_namespace")
    serve.shutdown()  # Ensure serve isn't running before beginning the test
    serve.start(detached=detached)

    old_handle = get_global_client()._controller
    ray.kill(old_handle, no_restart=True)

    def controller_died(handle):
        try:
            ray.get(handle.check_alive.remote())
            return False
        except RayActorError:
            return True

    wait_for_condition(controller_died, handle=old_handle, timeout=15)

    # Call start again to refresh handle
    serve.start(detached=detached)

    new_handle = get_global_client()._controller
    assert new_handle is not old_handle

    # Health check should not error
    ray.get(new_handle.check_alive.remote())

    serve.shutdown()
    ray.shutdown()


def test_get_serve_status(shutdown_ray):

    ray.init()

    @serve.deployment
    def f(*args):
        return "Hello world"

    serve.run(f.bind())

    client = get_global_client()
    status_info_1 = client.get_serve_status()
    assert status_info_1.app_status.status == "RUNNING"
    assert (
        status_info_1.deployment_statuses[0].name
        == f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
    )
    assert status_info_1.deployment_statuses[0].status in {"UPDATING", "HEALTHY"}

    serve.shutdown()
    ray.shutdown()


def test_controller_deserialization_deployment_def(start_and_shutdown_ray_cli_function):
    """Ensure controller doesn't deserialize deployment_def or init_args/kwargs."""

    @ray.remote
    def run_graph():
        """Deploys a Serve application to the controller's Ray cluster."""
        from ray import serve
        from ray._private.utils import import_attr
        from ray.serve.api import build

        # Import and build the graph
        graph = import_attr("test_config_files.pizza.serve_dag")
        app = build(graph)

        # Override options for each deployment
        for name in app.deployments:
            app.deployments[name].set_options(ray_actor_options={"num_cpus": 0.1})

        # Run the graph locally on the cluster
        serve.start(detached=True)
        serve.run(graph)

    # Start Serve controller in a directory without access to the graph code
    ray.init(
        address="auto",
        namespace="serve",
        runtime_env={
            "working_dir": os.path.join(os.path.dirname(__file__), "storage_tests")
        },
    )
    serve.start(detached=True)
    serve.context._global_client = None
    ray.shutdown()

    # Run the task in a directory with access to the graph code
    ray.init(
        address="auto",
        namespace="serve",
        runtime_env={"working_dir": os.path.dirname(__file__)},
    )
    ray.get(run_graph.remote())
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )

    serve.shutdown()
    ray.shutdown()


def test_controller_deserialization_args_and_kwargs():
    """Ensures init_args and init_kwargs stay serialized in controller."""

    ray.init()
    client = serve.start()

    class PidBasedString(str):
        pass

    def generate_pid_based_deserializer(pid, raw_deserializer):
        """Cannot be deserialized by the process with specified pid."""

        def deserializer(*args):

            import os

            if os.getpid() == pid:
                raise RuntimeError("Cannot be deserialized by this process!")
            else:
                return raw_deserializer(*args)

        return deserializer

    PidBasedString.__reduce__ = generate_pid_based_deserializer(
        ray.get(client._controller.get_pid.remote()), PidBasedString.__reduce__
    )

    @serve.deployment
    class Echo:
        def __init__(self, arg_str, kwarg_str="failed"):
            self.arg_str = arg_str
            self.kwarg_str = kwarg_str

        def __call__(self, request):
            return self.arg_str + self.kwarg_str

    serve.run(Echo.bind(PidBasedString("hello "), kwarg_str=PidBasedString("world!")))

    assert requests.get("http://localhost:8000/Echo").text == "hello world!"

    serve.shutdown()
    ray.shutdown()


def test_controller_recover_and_delete(shutdown_ray):
    """Ensure that in-progress deletion can finish even after controller dies."""

    ray_context = ray.init()
    client = serve.start()

    @serve.deployment(
        num_replicas=50,
        ray_actor_options={"num_cpus": 0.001},
    )
    def f():
        pass

    serve.run(f.bind())

    actors = list_actors(
        address=ray_context.address_info["address"], filters=[("state", "=", "ALIVE")]
    )

    # Try to delete the application and kill the controller right after
    serve.delete(SERVE_DEFAULT_APP_NAME, _blocking=False)
    ray.kill(client._controller, no_restart=False)

    # All replicas should be removed already or after the controller revives
    wait_for_condition(
        lambda: len(
            list_actors(
                address=ray_context.address_info["address"],
                filters=[("state", "=", "ALIVE")],
            )
        )
        < len(actors)
    )

    wait_for_condition(
        lambda: len(
            list_actors(
                address=ray_context.address_info["address"],
                filters=[("state", "=", "ALIVE")],
            )
        )
        == len(actors) - 50
    )

    # The deployment should be deleted, meaning its state should not be stored
    # in the DeploymentStateManager. This can be checked by attempting to
    # retrieve the deployment's status through the controller.
    assert (
        client.get_serve_status().get_deployment_status(
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        )
        is None
    )

    serve.shutdown()
    ray.shutdown()


class TestDeployApp:
    @pytest.fixture(scope="function")
    def client(self):
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(
            _check_ray_stop,
            timeout=15,
        )
        subprocess.check_output(["ray", "start", "--head"])
        wait_for_condition(
            lambda: requests.get("http://localhost:52365/api/ray/version").status_code
            == 200,
            timeout=15,
        )
        ray.init(address="auto", namespace=SERVE_NAMESPACE)
        yield serve.start(detached=True)
        serve.shutdown()
        ray.shutdown()
        subprocess.check_output(["ray", "stop", "--force"])
        wait_for_condition(
            _check_ray_stop,
            timeout=15,
        )

    def check_deployment_running(self, client: ServeControllerClient, name: str):
        serve_status = client.get_serve_status()
        return (
            serve_status.get_deployment_status(name) is not None
            and serve_status.app_status.status == ApplicationStatus.RUNNING
            and serve_status.get_deployment_status(name).status
            == DeploymentStatus.HEALTHY
        )

    def check_deployments_dead(self, deployment_names):
        actor_names = [
            actor["class_name"]
            for actor in list_actors(
                filters=[("state", "=", "ALIVE")],
            )
        ]
        return all(
            f"ServeReplica:{name}" not in actor_names for name in deployment_names
        )

    def get_num_replicas(self, client: ServeControllerClient, deployment_name: str):
        replicas = ray.get(
            client._controller._dump_replica_states_for_testing.remote(deployment_name)
        )
        running_replicas = replicas.get([ReplicaState.RUNNING])
        return len(running_replicas)

    def get_test_config(self) -> Dict:
        return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}

    def check_single_app(self):
        """Checks the application deployed through the config from get_test_config()"""
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

    def get_test_deploy_config(self) -> Dict:
        return {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
                    "deployments": [
                        {
                            "name": "Adder",
                            "user_config": {
                                "increment": 3,
                            },
                        },
                        {
                            "name": "Multiplier",
                            "user_config": {
                                "factor": 4,
                            },
                        },
                    ],
                },
            ],
        }

    def check_multi_app(self):
        """
        Checks the applications deployed through the config from
        get_test_deploy_config().
        """

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "5 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 3]).json()
            == "12 pizzas please!"
        )

    def test_deploy_app_basic(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

    def test_deploy_multi_app(self, client: ServeControllerClient):
        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        client.deploy_apps(config)
        self.check_multi_app()

    def test_deploy_app_with_overriden_config(self, client: ServeControllerClient):

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Multiplier",
                "user_config": {
                    "factor": 4,
                },
            },
            {
                "name": "Adder",
                "user_config": {
                    "increment": 5,
                },
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).json()
            == "5 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).json()
            == "8 pizzas please!"
        )

    def test_deploy_app_update_config(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": -1,
                },
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "1 pizzas please!"
        )

    def test_deploy_multi_app_update_config(self, client: ServeControllerClient):
        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        self.check_multi_app()

        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": -1,
                },
            },
        ]

        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": 10,
                },
            },
        ]

        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "1 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "12 pizzas please!"
        )

    def test_deploy_app_update_num_replicas(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

        actors = list_actors(filters=[("state", "=", "ALIVE")])

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
                "user_config": {
                    "increment": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 3,
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "2 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "0 pizzas please!"
        )

        wait_for_condition(
            lambda: client.get_serve_status().app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )

        updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
        assert len(updated_actors) == len(actors) + 3

    def test_deploy_multi_app_update_num_replicas(self, client: ServeControllerClient):
        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        self.check_multi_app()

        actors = list_actors(filters=[("state", "=", "ALIVE")])

        # app1
        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,  # +1
                "user_config": {
                    "increment": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 3,  # +2
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        # app2
        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 3,  # +2
                "user_config": {
                    "increment": 100,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 4,  # +3
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "2 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "102 pizzas please!"
        )

        wait_for_condition(
            lambda: client.get_serve_status("app1").app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )
        wait_for_condition(
            lambda: client.get_serve_status("app2").app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )

        updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
        assert len(updated_actors) == len(actors) + 8

    def test_deploy_app_update_timestamp(self, client: ServeControllerClient):
        assert client.get_serve_status().app_status.deployment_timestamp == 0

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)

        assert client.get_serve_status().app_status.deployment_timestamp > 0

        first_deploy_time = client.get_serve_status().app_status.deployment_timestamp
        time.sleep(0.1)

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
            },
        ]
        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        assert (
            client.get_serve_status().app_status.deployment_timestamp
            > first_deploy_time
        )
        assert client.get_serve_status().app_status.status in {
            ApplicationStatus.DEPLOYING,
            ApplicationStatus.RUNNING,
        }

    def test_deploy_multi_app_update_timestamp(self, client: ServeControllerClient):
        assert client.get_serve_status("app1").app_status.deployment_timestamp == 0
        assert client.get_serve_status("app2").app_status.deployment_timestamp == 0

        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))

        first_deploy_time_app1 = client.get_serve_status(
            "app1"
        ).app_status.deployment_timestamp
        first_deploy_time_app2 = client.get_serve_status(
            "app2"
        ).app_status.deployment_timestamp

        assert first_deploy_time_app1 > 0 and first_deploy_time_app2 > 0
        time.sleep(0.1)

        # app1
        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
            },
        ]
        # app2
        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 3,
            },
        ]
        client.deploy_apps(ServeDeploySchema.parse_obj(config))

        assert (
            client.get_serve_status("app1").app_status.deployment_timestamp
            > first_deploy_time_app1
            and client.get_serve_status("app2").app_status.deployment_timestamp
            > first_deploy_time_app2
        )
        assert {
            client.get_serve_status("app1").app_status.status,
            client.get_serve_status("app2").app_status.status,
        } <= {
            ApplicationStatus.DEPLOYING,
            ApplicationStatus.RUNNING,
        }

    def test_deploy_app_overwrite_apps(self, client: ServeControllerClient):
        """Check that overwriting a live app with a new one works."""

        # Launch first graph. Its driver's route_prefix should be "/".
        test_config_1 = ServeApplicationSchema.parse_obj(
            {
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            }
        )
        client.deploy_apps(test_config_1)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/").text == "wonderful world"
        )

        # Launch second graph. Its driver's route_prefix should also be "/".
        # "/" should lead to the new driver.
        test_config_2 = ServeApplicationSchema.parse_obj(
            {
                "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
            }
        )
        client.deploy_apps(test_config_2)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

    def test_deploy_multi_app_overwrite_apps(self, client: ServeControllerClient):
        """Check that redeploying different apps with same names works as expected."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app1",
                        "route_prefix": "/app1",
                        "import_path": world_import_path,
                    },
                    {
                        "name": "app2",
                        "route_prefix": "/app2",
                        "import_path": pizza_import_path,
                    },
                ],
            }
        )
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Switch the two application import paths
        test_config.applications[0].import_path = pizza_import_path
        test_config.applications[1].import_path = world_import_path
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
        )

    def test_deploy_multi_app_overwrite_apps2(self, client: ServeControllerClient):
        """Check that deploying a new set of applications removes old ones."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app1",
                        "route_prefix": "/app1",
                        "import_path": world_import_path,
                    },
                    {
                        "name": "app2",
                        "route_prefix": "/app2",
                        "import_path": pizza_import_path,
                    },
                ],
            }
        )
        # Deploy app1 and app2
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Deploy app3
        new_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app3",
                        "route_prefix": "/app3",
                        "import_path": pizza_import_path,
                        "deployments": [
                            {
                                "name": "Adder",
                                "user_config": {
                                    "increment": 3,
                                },
                            },
                        ],
                    },
                ],
            }
        )
        client.deploy_apps(new_config)

        def check_dead():
            actors = list_actors(
                filters=[
                    ("ray_namespace", "=", SERVE_NAMESPACE),
                    ("state", "=", "ALIVE"),
                ]
            )
            for actor in actors:
                assert (
                    "app1" not in actor["class_name"]
                    and "app2" not in actor["class_name"]
                )
            return True

        # Deployments from app1 and app2 should be deleted
        wait_for_condition(check_dead)

        # App1 and App2 should be gone
        assert requests.get("http://localhost:8000/app1").status_code != 200
        assert (
            requests.post("http://localhost:8000/app2", json=["ADD", 2]).status_code
            != 200
        )

        # App3 should be up and running
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app3", json=["ADD", 2]).json()
            == "5 pizzas please!"
        )

    def test_deploy_app_runtime_env(self, client: ServeControllerClient):
        config_template = {
            "import_path": "conditional_dag.serve_dag",
            "runtime_env": {
                "working_dir": (
                    "https://github.com/ray-project/test_dag/archive/"
                    "41d09119cbdf8450599f993f51318e9e27c59098.zip"
                )
            },
        }

        config1 = ServeApplicationSchema.parse_obj(config_template)
        client.deploy_apps(config1)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "0 pizzas please!"
        )

        # Override the configuration
        config_template["deployments"] = [
            {
                "name": "Adder",
                "ray_actor_options": {
                    "runtime_env": {"env_vars": {"override_increment": "1"}}
                },
            }
        ]
        config2 = ServeApplicationSchema.parse_obj(config_template)
        client.deploy_apps(config2)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "3 pizzas please!"
        )

    def test_deploy_multi_app_deployments_removed(self, client: ServeControllerClient):
        """Test redeploying applications will remove old deployments."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        world_deployments = ["f", "BasicDriver"]
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        pizza_deployments = [
            "Adder",
            "Multiplier",
            "Router",
            "create_order",
            "DAGDriver",
        ]
        test_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app1",
                        "route_prefix": "/app1",
                        "import_path": pizza_import_path,
                    },
                ],
            }
        )
        # Deploy with pizza graph first
        client.deploy_apps(test_config)

        def check_pizza():
            # Check that the live deployments and actors are what we expect: exactly the
            # set of deployments in the pizza graph
            actor_class_names = {
                actor["class_name"]
                for actor in list_actors(filters=[("state", "=", "ALIVE")])
            }
            deployment_replicas = set(
                ray.get(client._controller._all_running_replicas.remote()).keys()
            )
            assert {
                f"app1_{deployment}" for deployment in pizza_deployments
            } == deployment_replicas
            assert {"HTTPProxyActor", "ServeController"}.union(
                {f"ServeReplica:app1_{deployment}" for deployment in pizza_deployments}
            ) == actor_class_names
            return True

        wait_for_condition(check_pizza)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Redeploy with world graph
        test_config.applications[0].import_path = world_import_path
        client.deploy_apps(test_config)

        def check_world():
            # Check that the live deployments and actors are what we expect: exactly the
            # set of deployments in the world graph
            actor_class_names = {
                actor["class_name"]
                for actor in list_actors(filters=[("state", "=", "ALIVE")])
            }
            deployment_replicas = set(
                ray.get(client._controller._all_running_replicas.remote()).keys()
            )
            assert {
                f"app1_{deployment}" for deployment in world_deployments
            } == deployment_replicas
            assert {"HTTPProxyActor", "ServeController"}.union(
                {f"ServeReplica:app1_{deployment}" for deployment in world_deployments}
            ) == actor_class_names
            return True

        wait_for_condition(check_world)
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )

    def test_controller_recover_and_deploy(self, client: ServeControllerClient):
        """Ensure that in-progress deploy can finish even after controller dies."""

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        # Wait for app to deploy
        self.check_single_app()
        deployment_timestamp = client.get_serve_status().app_status.deployment_timestamp

        # Delete all deployments, but don't update config
        deployments = [
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}{name}"
            for name in ["Router", "Multiplier", "Adder", "create_order", "DAGDriver"]
        ]
        client.delete_deployments(deployments)

        ray.kill(client._controller, no_restart=False)

        # When controller restarts, it should redeploy config automatically
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )
        assert (
            deployment_timestamp
            == client.get_serve_status().app_status.deployment_timestamp
        )

        serve.shutdown()
        client = serve.start(detached=True)

        # Ensure config checkpoint has been deleted
        assert client.get_serve_status().app_status.deployment_timestamp == 0

    @pytest.mark.parametrize(
        "field_to_update",
        ["import_path", "runtime_env", "ray_actor_options"],
    )
    def test_deploy_config_update_heavyweight(
        self, client: ServeControllerClient, field_to_update: str
    ):
        """Check that replicas are torn down when code updates are made."""
        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [
                {
                    "name": "f",
                    "autoscaling_config": None,
                    "user_config": {"name": "alice"},
                    "ray_actor_options": {"num_cpus": 0.1},
                },
            ],
        }

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        pid1, _ = requests.get("http://localhost:8000/f").json()

        if field_to_update == "import_path":
            config_template[
                "import_path"
            ] = "ray.serve.tests.test_config_files.pid.dup_node"
        elif field_to_update == "runtime_env":
            config_template["runtime_env"] = {"env_vars": {"test_var": "test_val"}}
        elif field_to_update == "ray_actor_options":
            config_template["deployments"][0]["ray_actor_options"] = {"num_cpus": 0.2}

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        # This assumes that Serve implements round-robin routing for its replicas. As
        # long as that doesn't change, this test shouldn't be flaky; however if that
        # routing ever changes, this test could become mysteriously flaky
        pids = []
        for _ in range(4):
            pids.append(requests.get("http://localhost:8000/f").json()[0])
        assert pid1 not in pids

    def test_update_config_user_config(self, client: ServeControllerClient):
        """Check that replicas stay alive when user config is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [{"name": "f", "user_config": {"name": "alice"}}],
        }

        # Deploy first time
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        # Query
        pid1, res = requests.get("http://localhost:8000/f").json()
        assert res == "alice"

        # Redeploy with updated option
        config_template["deployments"][0]["user_config"] = {"name": "bob"}
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        # This assumes that Serve implements round-robin routing for its replicas. As
        # long as that doesn't change, this test shouldn't be flaky; however if that
        # routing ever changes, this test could become mysteriously flaky
        # Query
        pids = []
        for _ in range(4):
            pid, res = requests.get("http://localhost:8000/f").json()
            assert res == "bob"
            pids.append(pid)
        assert pid1 in pids

    def test_update_config_graceful_shutdown_timeout(
        self, client: ServeControllerClient
    ):
        """Check that replicas stay alive when graceful_shutdown_timeout_s is updated"""
        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [{"name": "f", "graceful_shutdown_timeout_s": 1000}],
        }

        # Deploy first time
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        handle = client.get_handle(name)

        # Start off with signal ready, and send query
        ray.get(handle.send.remote())
        pid1 = ray.get(handle.remote())[0]
        print("PID of replica after first deployment:", pid1)

        # Redeploy with shutdown timeout set to 5 seconds
        config_template["deployments"][0]["graceful_shutdown_timeout_s"] = 5
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2
        print("PID of replica after redeployment:", pid2)

        # Send blocking query
        handle.send.remote(clear=True)
        handle.remote()
        # Try to delete deployment, should be blocked until the timeout at 5 seconds
        client.delete_deployments([name], blocking=False)
        # Replica should be dead within 10 second timeout, which means
        # graceful_shutdown_timeout_s was successfully updated lightweightly
        wait_for_condition(partial(self.check_deployments_dead, ["f"]))

    def test_update_config_max_concurrent_queries(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        url = "http://localhost:8000/f"
        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.async_node",
            "deployments": [{"name": "f", "max_concurrent_queries": 1000}],
        }

        # Deploy first time
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        handle = client.get_handle(name)
        # Block on calls
        ray.get(handle.send.remote(clear=True))

        with ThreadPoolExecutor() as pool:
            # Send 10 queries
            futs = [pool.submit(partial(requests.get, url)) for _ in range(10)]
            wait_for_condition(lambda: 10 == ray.get(handle.get_counter.remote()))

            # Unblock
            ray.get(handle.send.remote())
            pids = [fut.result().json()[0] for fut in futs]
            pid1 = pids[0]
            # Check all returned pids are the same, meaning requests were served by the
            # same replica
            assert all(pid == pid1 for pid in pids)

        # Redeploy with max concurrent queries set to 2
        config_template["deployments"][0]["max_concurrent_queries"] = 2
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        # Re-block
        ray.get(handle.send.remote(clear=True))

        with ThreadPoolExecutor() as pool:
            # Send 3 queries
            futs = [pool.submit(partial(requests.get, url)) for _ in range(3)]
            # Only 2 out of the 3 queries should have been sent to the replica because
            # max concurrent queries is 2
            time.sleep(10)
            assert ray.get(handle.get_counter.remote()) < 103

            # Unblock
            ray.get(handle.send.remote())
            pids = [fut.result().json()[0] for fut in futs]
            pid2 = pids[0]
            assert all(pid == pid2 for pid in pids)

        # Check that it's the same replica, it didn't get teared down
        assert pid1 == pid2

    def test_update_config_health_check_period(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.async_node",
            "deployments": [{"name": "f", "health_check_period_s": 100}],
        }

        # Deploy first time, wait for replica running and deployment healthy
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        handle = client.get_handle(name)
        pid1 = ray.get(handle.remote())[0]
        # Health check counter shouldn't increase beyond any initial health checks done
        # upon replica actor startup
        initial_counter = ray.get(handle.get_counter.remote(health_check=True))
        time.sleep(5)
        assert initial_counter == ray.get(handle.get_counter.remote(health_check=True))

        # Redeploy with health check period reduced to 1 second
        config_template["deployments"][0]["health_check_period_s"] = 0.1
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        # health check counter should now very quickly increase
        wait_for_condition(
            lambda: ray.get(handle.get_counter.remote(health_check=True)) >= 30,
            retry_interval_ms=1000,
            timeout=5,
        )

        # Check that it's the same replica, it didn't get teared down
        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2

    def test_update_config_health_check_timeout(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        # Deploy with a very long initial health_check_timeout_s
        # Also set small health_check_period_s to make test run faster
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.async_node",
            "deployments": [
                {
                    "name": "f",
                    "health_check_period_s": 1,
                    "health_check_timeout_s": 1000,
                }
            ],
        }

        # Deploy first time, wait for replica running and deployment healthy
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        handle = client.get_handle(name)
        pid1 = ray.get(handle.remote())[0]

        # Redeploy with health check timeout reduced to 1 second
        config_template["deployments"][0]["health_check_timeout_s"] = 1
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        # Check that it's the same replica, it didn't get teared down
        # (needs to be done before the tests below because the replica will be marked
        # unhealthy then stopped and restarted)
        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2

        # Block in health check
        ray.get(handle.send.remote(clear=True, health_check=True))
        wait_for_condition(
            lambda: client.get_serve_status().get_deployment_status(name).status
            == DeploymentStatus.UNHEALTHY
        )

    def test_deploy_separate_runtime_envs(self, client: ServeControllerClient):
        """Deploy two applications with separate runtime envs."""

        config_template = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "conditional_dag.serve_dag",
                    "runtime_env": {
                        "working_dir": (
                            "https://github.com/ray-project/test_dag/archive/"
                            "41d09119cbdf8450599f993f51318e9e27c59098.zip"
                        )
                    },
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": "hello_world.app",
                    "runtime_env": {
                        "working_dir": (
                            "https://github.com/zcin/test_runtime_env/archive/"
                            "c96019b6049cd9a2997db5ea0f10432bfeffb844.zip"
                        )
                    },
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**config_template))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "0 pizzas please!"
        )

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2").text == "Hello world!"
        )

    def test_deploy_one_app_failed(self, client: ServeControllerClient):
        """Deploy two applications with separate runtime envs."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        fail_import_path = "ray.serve.tests.test_config_files.fail.node"
        config_template = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": world_import_path,
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": fail_import_path,
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**config_template))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world"
        )

        wait_for_condition(
            lambda: client.get_serve_status("app1").app_status.status
            == ApplicationStatus.RUNNING
            and client.get_serve_status("app2").app_status.status
            == ApplicationStatus.DEPLOY_FAILED
        )

    def test_deploy_with_route_prefix_conflict(self, client: ServeControllerClient):
        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": world_import_path,
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": pizza_import_path,
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**test_config))

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Buffer time
        time.sleep(1)

        test_config["applications"][1] = {
            "name": "app3",
            "route_prefix": "/app2",
            "import_path": world_import_path,
        }

        client.deploy_apps(ServeDeploySchema(**test_config))

        def check():
            serve_details = ServeInstanceDetails(
                **ray.get(client._controller.get_serve_instance_details.remote())
            )
            app1_running = (
                "app1" in serve_details.applications
                and serve_details.applications["app1"].status == "RUNNING"
            )
            app3_running = (
                "app3" in serve_details.applications
                and serve_details.applications["app3"].status == "RUNNING"
            )
            app2_gone = "app2" not in serve_details.applications
            return app1_running and app3_running and app2_gone

        wait_for_condition(check)

        # app1 and app3 should be up and running
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
        )

    def test_deploy_single_then_multi(self, client: ServeControllerClient):
        """Deploying single-app then multi-app config should fail."""

        single_app_config = ServeApplicationSchema.parse_obj(self.get_test_config())
        multi_app_config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())

        # Deploy single app config
        client.deploy_apps(single_app_config)
        self.check_single_app()

        # Deploying multi app config afterwards should fail
        with pytest.raises(RayServeException):
            client.deploy_apps(multi_app_config)
        # The original application should still be up and running
        self.check_single_app()

    def test_deploy_multi_then_single(self, client: ServeControllerClient):
        """Deploying multi-app then single-app config should fail."""

        single_app_config = ServeApplicationSchema.parse_obj(self.get_test_config())
        multi_app_config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())

        # Deploy multi app config
        client.deploy_apps(multi_app_config)
        self.check_multi_app()

        # Deploying single app config afterwards should fail
        with pytest.raises(RayServeException):
            client.deploy_apps(single_app_config)
        # The original applications should still be up and running
        self.check_multi_app()

    def test_deploy_multi_app_deleting(self, client: ServeControllerClient):
        """Test deleting an application by removing from config."""

        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        client.deploy_apps(config)
        self.check_multi_app()

        # Delete app2
        del config.applications[1]
        client.deploy_apps(config)

        # Fetch details immediately afterwards, should parse correctly
        details = ray.get(client._controller.get_serve_instance_details.remote())
        ServeInstanceDetails(**details)
        # We don't enforce that the state is deleting here because that could cause
        # flaky test performance. The app could have been deleted by the time of query
        assert (
            "app2" not in details["applications"]
            or details["applications"]["app2"]["status"] == ApplicationStatus.DELETING
        )

        info_valid = True

        def check_app_status():
            global info_valid
            try:
                # Fetch details, should always parse correctly
                details = ray.get(
                    client._controller.get_serve_instance_details.remote()
                )
                ServeInstanceDetails(**details)
                return (
                    details["applications"]["app1"]["status"]
                    == ApplicationStatus.RUNNING
                )
            except Exception:
                info_valid = False

        wait_for_condition(check_app_status)
        # Check that all all details fetched from controller parsed correctly
        assert info_valid

    def test_deploy_nonexistent_deployment(self, client: ServeControllerClient):
        """Apply a config that lists a deployment that doesn't exist in the application.
        The error message should be descriptive.
        """

        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        # Change names to invalid names that don't contain "deployment" or "application"
        config.applications[1].name = "random1"
        config.applications[1].deployments[0].name = "random2"
        client.deploy_apps(config)

        def check_app_message():
            details = ray.get(client._controller.get_serve_instance_details.remote())
            # The error message should be descriptive
            # e.g. no deployment "x" in application "y"
            return (
                "application" in details["applications"]["random1"]["message"]
                and "deployment" in details["applications"]["random1"]["message"]
            )

        wait_for_condition(check_app_message)

    def test_deploy_with_no_applications(self, client: ServeControllerClient):
        """Deploy an empty list of applications, serve should just be started."""

        config = ServeDeploySchema.parse_obj({"applications": []})
        client.deploy_apps(config)

        def serve_running():
            ServeInstanceDetails.parse_obj(
                ray.get(client._controller.get_serve_instance_details.remote())
            )
            actors = list_actors(
                filters=[
                    ("ray_namespace", "=", SERVE_NAMESPACE),
                    ("state", "=", "ALIVE"),
                ]
            )
            actor_names = [actor["class_name"] for actor in actors]
            return "ServeController" in actor_names and "HTTPProxyActor" in actor_names

        wait_for_condition(serve_running)

    def test_deployments_not_listed_in_config(self, client: ServeControllerClient):
        """Apply a config without the app's deployments listed. The deployments should
        not redeploy.
        """

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config = {"import_path": "ray.serve.tests.test_config_files.pid.node"}
        client.deploy_apps(ServeApplicationSchema(**config))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )
        pid1, _ = requests.get("http://localhost:8000/f").json()

        # Redeploy the same config (with no deployments listed)
        client.deploy_apps(ServeApplicationSchema(**config))
        wait_for_condition(
            partial(self.check_deployment_running, client, name), timeout=15
        )

        # It should be the same replica actor
        pids = []
        for _ in range(4):
            pids.append(requests.get("http://localhost:8000/f").json()[0])
        assert all(pid == pid1 for pid in pids)


class TestServeRequestProcessingTimeoutS:
    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "5"},
            {"SERVE_REQUEST_PROCESSING_TIMEOUT_S": "5"},
            {
                "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "5",
                "SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0",
            },
        ],
        indirect=True,
    )
    def test_normal_operation(self, ray_instance):
        """Checks that a moderate timeout doesn't affect normal operation."""

        @serve.deployment(num_replicas=2)
        def f(*args):
            return "Success!"

        serve.run(f.bind())

        for _ in range(20):
            requests.get("http://localhost:8000").text == "Success!"

        serve.shutdown()

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1"},
            {"SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1"},
            {
                "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1",
                "SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0",
            },
        ],
        indirect=True,
    )
    def test_hanging_request(self, ray_instance):
        """Checks that the env var mitigates the hang."""

        @ray.remote
        class PidTracker:
            def __init__(self):
                self.pids = set()

            def add_pid(self, pid: int) -> None:
                self.pids.add(pid)

            def get_pids(self) -> Set[int]:
                return self.pids

        pid_tracker = PidTracker.remote()
        signal_actor = SignalActor.remote()

        @serve.deployment(num_replicas=2)
        async def waiter(*args):
            import os

            ray.get(pid_tracker.add_pid.remote(os.getpid()))
            await signal_actor.wait.remote()
            return "Success!"

        serve.run(waiter.bind())

        with ThreadPoolExecutor() as pool:
            response_fut = pool.submit(requests.get, "http://localhost:8000")

            # Force request to hang
            time.sleep(0.5)
            ray.get(signal_actor.send.remote())

            wait_for_condition(lambda: response_fut.done())
            assert response_fut.result().text == "Success!"

        # Hanging request should have been retried
        assert len(ray.get(pid_tracker.get_pids.remote())) == 2

        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
