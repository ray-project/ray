import os
import subprocess
import sys
import time
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Dict, Set
from concurrent.futures.thread import ThreadPoolExecutor

import pytest
import requests

import ray
import ray.actor
import ray._private.state
from ray.experimental.state.api import list_actors

from ray import serve
from ray._private.test_utils import (
    run_string_as_driver,
    wait_for_condition,
    SignalActor,
)
from ray._private.ray_constants import gcs_actor_scheduling_enabled
from ray.cluster_utils import AutoscalingCluster
from ray.exceptions import RayActorError
from ray.serve.exceptions import RayServeException
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, DeploymentStatus
from ray.serve._private.constants import (
    SERVE_NAMESPACE,
    SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY,
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
    serve.run(hello.bind())

    assert ray.get(hello.get_handle().remote()) == "world"


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
    assert status_info_1.deployment_statuses[0].name == "f"
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

    f.deploy()

    actors = list_actors(
        address=ray_context.address_info["address"], filters=[("state", "=", "ALIVE")]
    )

    # Try to delete the deployments and kill the controller right after
    client.delete_deployments(["f"], blocking=False)
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
    assert client.get_serve_status().get_deployment_status("f") is None

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

    def test_controller_recover_and_deploy(self, client: ServeControllerClient):
        """Ensure that in-progress deploy can finish even after controller dies."""

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        # Wait for app to deploy
        self.check_single_app()
        deployment_timestamp = client.get_serve_status().app_status.deployment_timestamp

        # Delete all deployments, but don't update config
        client.delete_deployments(
            ["Router", "Multiplier", "Adder", "create_order", "DAGDriver"]
        )

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
        "field_to_update,option_to_update,config_update",
        [
            ("import_path", "", False),
            ("runtime_env", "", False),
            ("deployments", "num_replicas", True),
            ("deployments", "autoscaling_config", True),
            ("deployments", "user_config", True),
            ("deployments", "ray_actor_options", False),
        ],
    )
    def test_deploy_config_update(
        self,
        client: ServeControllerClient,
        field_to_update: str,
        option_to_update: str,
        config_update: bool,
    ):
        """
        Check that replicas stay alive when lightweight config updates are made and
        replicas are torn down when code updates are made.
        """

        def deployment_running():
            serve_status = client.get_serve_status()
            return (
                serve_status.get_deployment_status("f") is not None
                and serve_status.app_status.status == ApplicationStatus.RUNNING
                and serve_status.get_deployment_status("f").status
                == DeploymentStatus.HEALTHY
            )

        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [
                {
                    "name": "f",
                    "autoscaling_config": None,
                    "user_config": None,
                    "ray_actor_options": {"num_cpus": 0.1},
                },
            ],
        }

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(deployment_running, timeout=15)
        pid1 = requests.get("http://localhost:8000/f").text

        if field_to_update == "import_path":
            config_template[
                "import_path"
            ] = "ray.serve.tests.test_config_files.pid.bnode"
        elif field_to_update == "runtime_env":
            config_template["runtime_env"] = {"env_vars": {"test_var": "test_val"}}
        elif field_to_update == "deployments":
            updated_options = {
                "num_replicas": 2,
                "autoscaling_config": {"max_replicas": 2},
                "user_config": {"name": "bob"},
                "ray_actor_options": {"num_cpus": 0.2},
            }
            config_template["deployments"][0][option_to_update] = updated_options[
                option_to_update
            ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(deployment_running, timeout=15)

        # This assumes that Serve implements round-robin routing for its replicas. As
        # long as that doesn't change, this test shouldn't be flaky; however if that
        # routing ever changes, this test could become mysteriously flaky
        pids = []
        for _ in range(4):
            pids.append(requests.get("http://localhost:8000/f").text)
        assert (pid1 in pids) == config_update

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
        """Remove an application from a config, it should reach a deleting state."""

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


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_LOWER_BOUND": "1",
            "LISTEN_FOR_CHANGE_REQUEST_TIMEOUT_S_UPPER_BOUND": "2",
        },
    ],
    indirect=True,
)
def test_long_poll_timeout_with_max_concurrent_queries(ray_instance):
    """Test max_concurrent_queries can be honorded with long poll timeout

    issue: https://github.com/ray-project/ray/issues/32652
    """

    signal_actor = SignalActor.remote()

    @serve.deployment(max_concurrent_queries=1)
    async def f():
        await signal_actor.wait.remote()
        return "hello"

    handle = serve.run(f.bind())
    first_ref = handle.remote()

    # Clear all the internal longpoll client objects within handle
    # long poll client will receive new updates from long poll host,
    # this is to simulate the longpoll timeout
    object_snapshots1 = handle.router.long_poll_client.object_snapshots
    handle.router.long_poll_client._reset()
    wait_for_condition(
        lambda: len(handle.router.long_poll_client.object_snapshots) > 0, timeout=10
    )
    object_snapshots2 = handle.router.long_poll_client.object_snapshots

    # Check object snapshots between timeout interval
    assert object_snapshots1.keys() == object_snapshots2.keys()
    assert len(object_snapshots1.keys()) == 1
    key = list(object_snapshots1.keys())[0]
    assert (
        object_snapshots1[key][0].actor_handle != object_snapshots2[key][0].actor_handle
    )
    assert (
        object_snapshots1[key][0].actor_handle._actor_id
        == object_snapshots2[key][0].actor_handle._actor_id
    )

    # Make sure the inflight queries still one
    assert len(handle.router._replica_set.in_flight_queries) == 1
    key = list(handle.router._replica_set.in_flight_queries.keys())[0]
    assert len(handle.router._replica_set.in_flight_queries[key]) == 1

    # Make sure the first request is being run.
    replicas = list(handle.router._replica_set.in_flight_queries.keys())
    assert len(handle.router._replica_set.in_flight_queries[replicas[0]]) == 1
    # First ref should be still ongoing
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(first_ref, timeout=1)
    # Unblock the first request.
    signal_actor.send.remote()
    assert ray.get(first_ref) == "hello"
    serve.shutdown()


@pytest.mark.parametrize(
    "ray_instance",
    [
        {
            "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S": "0.1",
            "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES": "5",
        },
    ],
    indirect=True,
)
@pytest.mark.parametrize("crash", [True, False])
def test_http_request_number_of_retries(ray_instance, crash):
    """Test HTTP proxy retry requests."""

    signal_actor = SignalActor.remote()

    @serve.deployment
    class Model:
        async def __call__(self):
            if crash:
                # Trigger Actor Error
                os._exit(0)
            await signal_actor.wait.remote()
            return "hello"

    serve.run(Model.bind())
    assert requests.get("http://127.0.0.1:8000/").status_code == 500

    def verify_metrics():
        resp = requests.get("http://127.0.0.1:9999").text
        resp = resp.split("\n")
        # Make sure http proxy retry 5 times
        verfied = False
        for metrics in resp:
            if "# HELP" in metrics or "# TYPE" in metrics:
                continue
            if "serve_num_router_requests" in metrics:
                assert "6.0" in metrics
                verfied = True
        return verfied

    wait_for_condition(verify_metrics, timeout=60, retry_interval_ms=500)
    signal_actor.send.remote()
    serve.shutdown()


def test_shutdown_remote(start_and_shutdown_ray_cli_function):
    """Check that serve.shutdown() works on a remote Ray cluster."""

    deploy_serve_script = (
        "import ray\n"
        "from ray import serve\n"
        "\n"
        'ray.init(address="auto", namespace="x")\n'
        "serve.start(detached=True)\n"
        "\n"
        "@serve.deployment\n"
        "def f(*args):\n"
        '   return "got f"\n'
        "\n"
        "serve.run(f.bind())\n"
    )

    shutdown_serve_script = (
        "import ray\n"
        "from ray import serve\n"
        "\n"
        'ray.init(address="auto", namespace="x")\n'
        "serve.shutdown()\n"
    )

    # Cannot use context manager due to tmp file's delete flag issue in Windows
    # https://stackoverflow.com/a/15590253
    deploy_file = NamedTemporaryFile(mode="w+", delete=False, suffix=".py")
    shutdown_file = NamedTemporaryFile(mode="w+", delete=False, suffix=".py")

    try:
        deploy_file.write(deploy_serve_script)
        deploy_file.close()

        shutdown_file.write(shutdown_serve_script)
        shutdown_file.close()

        # Ensure Serve can be restarted and shutdown with for loop
        for _ in range(2):
            subprocess.check_output(["python", deploy_file.name])
            assert requests.get("http://localhost:8000/f").text == "got f"
            subprocess.check_output(["python", shutdown_file.name])
            with pytest.raises(requests.exceptions.ConnectionError):
                requests.get("http://localhost:8000/f")
    finally:
        os.unlink(deploy_file.name)
        os.unlink(shutdown_file.name)


def test_handle_early_detect_failure(shutdown_ray):
    """Check that handle can be notified about replicas failure.

    It should detect replica raises ActorError and take them out of the replicas set.
    """
    ray.init()
    serve.start(detached=True)

    @serve.deployment(num_replicas=2, max_concurrent_queries=1)
    def f(do_crash: bool = False):
        if do_crash:
            os._exit(1)
        return os.getpid()

    handle = serve.run(f.bind())
    pids = ray.get([handle.remote() for _ in range(2)])
    assert len(set(pids)) == 2
    assert len(handle.router._replica_set.in_flight_queries.keys()) == 2

    client = get_global_client()
    # Kill the controller so that the replicas membership won't be updated
    # through controller health check + long polling.
    ray.kill(client._controller, no_restart=True)

    with pytest.raises(RayActorError):
        ray.get(handle.remote(do_crash=True))

    pids = ray.get([handle.remote() for _ in range(10)])
    assert len(set(pids)) == 1
    assert len(handle.router._replica_set.in_flight_queries.keys()) == 1

    # Restart the controller, and then clean up all the replicas
    serve.start(detached=True)
    serve.shutdown()


@pytest.mark.skipif(
    gcs_actor_scheduling_enabled(),
    reason="Raylet-based scheduler favors (http proxy) actors' owner "
    + "nodes (the head one), so the `EveryNode` option is actually not "
    + "enforced. Besides, the second http proxy does not die with the "
    + "placeholder (happens to both schedulers), so gcs-based scheduler (which "
    + "may collocate the second http proxy and the place holder) "
    + "can not shutdown the worker node.",
)
def test_autoscaler_shutdown_node_http_everynode(
    shutdown_ray, call_ray_stop_only  # noqa: F811
):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 4,
                    "IS_WORKER": 100,
                },
                "node_config": {},
                "max_workers": 1,
            },
        },
        idle_timeout_minutes=0.05,
    )
    cluster.start()
    ray.init(address="auto")

    serve.start(http_options={"location": "EveryNode"})

    @ray.remote
    class Placeholder:
        def ready(self):
            return 1

    a = Placeholder.options(resources={"IS_WORKER": 1}).remote()
    assert ray.get(a.ready.remote()) == 1

    # 2 proxies, 1 controller, and one placeholder.
    wait_for_condition(lambda: len(ray._private.state.actors()) == 4)
    assert len(ray.nodes()) == 2

    # Now make sure the placeholder actor exits.
    ray.kill(a)
    # The http proxy on worker node should exit as well.
    wait_for_condition(
        lambda: len(
            list(
                filter(
                    lambda a: a["State"] == "ALIVE",
                    ray._private.state.actors().values(),
                )
            )
        )
        == 2
    )
    # Only head node should exist now.
    wait_for_condition(
        lambda: len(list(filter(lambda n: n["Alive"], ray.nodes()))) == 1
    )


def test_legacy_sync_handle_env_var(call_ray_stop_only):  # noqa: F811
    script = """
from ray import serve
from ray.serve.dag import InputNode
from ray.serve.drivers import DAGDriver
import ray

@serve.deployment
class A:
    def predict(self, inp):
        return inp

@serve.deployment
class Dispatch:
    def __init__(self, handle):
        self.handle = handle

    def predict(self, inp):
        ref = self.handle.predict.remote(inp)
        assert isinstance(ref, ray.ObjectRef), ref
        return ray.get(ref)

with InputNode() as inp:
    a = A.bind()
    d = Dispatch.bind(a)
    dag = d.predict.bind(inp)

handle = serve.run(DAGDriver.bind(dag))
assert ray.get(handle.predict.remote(1)) == 1
    """

    run_string_as_driver(script, env={SYNC_HANDLE_IN_DAG_FEATURE_FLAG_ENV_KEY: "1"})


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
