from contextlib import contextmanager
import sys
import os
import subprocess
from tempfile import NamedTemporaryFile
import requests
from typing import Dict

import pytest
from ray.cluster_utils import AutoscalingCluster
from ray.exceptions import RayActorError

import ray
import ray.state
from ray import serve
from ray.serve.context import get_global_client
from ray.serve.schema import ServeApplicationSchema
from ray.serve.client import ServeControllerClient
from ray.serve.common import ApplicationStatus
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest import call_ray_stop_only  # noqa: F401


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


@pytest.fixture(scope="function")
def start_and_shutdown_ray_cli_function():
    with start_and_shutdown_ray_cli():
        yield


@pytest.fixture(scope="class")
def start_and_shutdown_ray_cli_class():
    with start_and_shutdown_ray_cli():
        yield


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
    serve.start()
    hello.deploy()

    assert ray.get(hello.get_handle().remote()) == "world"


@pytest.mark.parametrize("detached", [True, False])
def test_override_namespace(shutdown_ray, detached):
    """Test the _override_controller_namespace flag in serve.start()."""

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    controller_name = get_global_client()._controller_name
    ray.get_actor(controller_name, namespace=controller_namespace)

    serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_deploy_with_overriden_namespace(shutdown_ray, detached):
    """Test deployments with overriden namespace."""

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    for iteration in range(2):

        @serve.deployment
        def f(*args):
            return f"{iteration}"

        f.deploy()
        assert requests.get("http://localhost:8000/f").text == f"{iteration}"

    serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_update_num_replicas_anonymous_namespace(shutdown_ray, detached):
    """Test updating num_replicas with anonymous namespace."""

    ray.init()
    serve.start(detached=detached)

    @serve.deployment(num_replicas=1)
    def f(*args):
        return "got f"

    f.deploy()

    num_actors = len(ray.util.list_named_actors(all_namespaces=True))

    for _ in range(5):
        f.deploy()
        assert num_actors == len(ray.util.list_named_actors(all_namespaces=True))

    serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_update_num_replicas_with_overriden_namespace(shutdown_ray, detached):
    """Test updating num_replicas with overriden namespace."""

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    @serve.deployment(num_replicas=2)
    def f(*args):
        return "got f"

    f.deploy()

    actors = ray.util.list_named_actors(all_namespaces=True)

    f.options(num_replicas=4).deploy()
    updated_actors = ray.util.list_named_actors(all_namespaces=True)

    # Check that only 2 new replicas were created
    assert len(updated_actors) == len(actors) + 2

    f.options(num_replicas=1).deploy()
    updated_actors = ray.util.list_named_actors(all_namespaces=True)

    # Check that all but 1 replica has spun down
    assert len(updated_actors) == len(actors) - 1

    serve.shutdown()


@pytest.mark.parametrize("detached", [True, False])
def test_refresh_controller_after_death(shutdown_ray, detached):
    """Check if serve.start() refreshes the controller handle if it's dead."""

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.shutdown()  # Ensure serve isn't running before beginning the test
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

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
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    new_handle = get_global_client()._controller
    assert new_handle is not old_handle

    # Health check should not error
    ray.get(new_handle.check_alive.remote())

    serve.shutdown()
    ray.shutdown()


def test_get_serve_status(shutdown_ray):

    ray.init()
    client = serve.start()

    @serve.deployment
    def f(*args):
        return "Hello world"

    f.deploy()

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
        from ray.serve.api import build
        from ray._private.utils import import_attr

        # Import and build the graph
        graph = import_attr("test_config_files.pizza.serve_dag")
        app = build(graph)

        # Override options for each deployment
        for name in app.deployments:
            app.deployments[name].set_options(ray_actor_options={"num_cpus": 0.1})

        # Run the graph locally on the cluster
        serve.start(detached=True, _override_controller_namespace="serve")
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
        def deserializer(*args):
            """Cannot be deserialized by the process with specified pid."""

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

    Echo.deploy(PidBasedString("hello "), kwarg_str=PidBasedString("world!"))

    assert requests.get("http://localhost:8000/Echo").text == "hello world!"

    serve.shutdown()
    ray.shutdown()


@pytest.mark.usefixtures("start_and_shutdown_ray_cli_class")
class TestDeployApp:
    @pytest.fixture()
    def client(self):
        ray.init(address="auto", namespace="serve")
        client = serve.start(detached=True)
        yield client
        serve.shutdown()
        ray.shutdown()

    def get_test_config(self) -> Dict:
        return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}

    def test_deploy_app_basic(self, client: ServeControllerClient):

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_app(config)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

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

        client.deploy_app(ServeApplicationSchema.parse_obj(config))

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
        client.deploy_app(config)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": -1,
                },
            },
        ]

        client.deploy_app(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "1 pizzas please!"
        )

    def test_deploy_app_update_num_replicas(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_app(config)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

        actors = ray.util.list_named_actors(all_namespaces=True)

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

        client.deploy_app(ServeApplicationSchema.parse_obj(config))

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

        updated_actors = ray.util.list_named_actors(all_namespaces=True)
        assert len(updated_actors) == len(actors) + 3

    def test_deploy_app_update_timestamp(self, client: ServeControllerClient):
        assert client.get_serve_status().app_status.deployment_timestamp == 0

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_app(config)

        wait_for_condition(
            lambda: client.get_serve_status().app_status.deployment_timestamp > 0
        )

        first_deploy_time = client.get_serve_status().app_status.deployment_timestamp

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
            },
        ]
        client.deploy_app(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: client.get_serve_status().app_status.deployment_timestamp
            > first_deploy_time
        )
        assert client.get_serve_status().app_status.status in {
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
        client.deploy_app(test_config_1)

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
        client.deploy_app(test_config_2)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )


def test_controller_recover_and_delete():
    """Ensure that in-progress deletion can finish even after controller dies."""

    ray.init()
    client = serve.start()

    @serve.deployment(
        num_replicas=50,
        ray_actor_options={"num_cpus": 0.001},
    )
    def f():
        pass

    f.deploy()

    actors = ray.util.list_named_actors(all_namespaces=True)
    client.delete_deployments(["f"], blocking=False)

    wait_for_condition(
        lambda: len(ray.util.list_named_actors(all_namespaces=True)) < len(actors)
    )
    ray.kill(client._controller, no_restart=False)

    # There should still be replicas remaining
    assert len(ray.util.list_named_actors(all_namespaces=True)) > 2

    # All replicas should be removed once the controller revives
    wait_for_condition(
        lambda: len(ray.util.list_named_actors(all_namespaces=True)) == len(actors) - 50
    )

    # The deployment should be deleted, meaning its state should not be stored
    # in the DeploymentStateManager. This can be checked by attempting to
    # retrieve the deployment's status through the controller.
    assert client.get_serve_status().get_deployment_status("f") is None

    serve.shutdown()
    ray.shutdown()


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
        "f.deploy()\n"
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
    wait_for_condition(lambda: len(ray.state.actors()) == 4)
    assert len(ray.nodes()) == 2

    # Now make sure the placeholder actor exits.
    ray.kill(a)
    # The http proxy on worker node should exit as well.
    wait_for_condition(
        lambda: len(
            list(filter(lambda a: a["State"] == "ALIVE", ray.state.actors().values()))
        )
        == 2
    )
    # Only head node should exist now.
    wait_for_condition(
        lambda: len(list(filter(lambda n: n["Alive"], ray.nodes()))) == 1
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
