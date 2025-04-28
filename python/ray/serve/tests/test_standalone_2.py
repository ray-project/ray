import os
import subprocess
import sys
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import pytest
import requests

import ray
import ray._private.state
import ray.actor
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.exceptions import RayActorError
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve.context import _get_global_client
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors


@pytest.fixture
def shutdown_ray_and_serve():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)
    subprocess.check_output(["ray", "start", "--head"])

    yield

    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)


@pytest.fixture(scope="function")
def start_and_shutdown_ray_cli_function():
    with start_and_shutdown_ray_cli():
        yield


def _check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


def test_standalone_actor_outside_serve(shutdown_ray_and_serve):
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


def test_memory_omitted_option(shutdown_ray_and_serve):
    """Ensure that omitting memory doesn't break the deployment."""

    @serve.deployment(ray_actor_options={"num_cpus": 1, "num_gpus": 1})
    def hello(*args, **kwargs):
        return "world"

    ray.init(num_gpus=3, namespace="serve")
    handle = serve.run(hello.bind())

    assert handle.remote().result() == "world"


@pytest.mark.parametrize("ray_namespace", ["arbitrary", SERVE_NAMESPACE, None])
def test_serve_namespace(shutdown_ray_and_serve, ray_namespace):
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


def test_update_num_replicas(shutdown_ray_and_serve):
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


def test_refresh_controller_after_death(shutdown_ray_and_serve):
    """Check if serve.start() refreshes the controller handle if it's dead."""

    ray.init(namespace="ray_namespace")
    serve.shutdown()  # Ensure serve isn't running before beginning the test
    serve.start()

    old_handle = _get_global_client()._controller
    ray.kill(old_handle, no_restart=True)

    def controller_died(handle):
        try:
            ray.get(handle.check_alive.remote())
            return False
        except RayActorError:
            return True

    wait_for_condition(controller_died, handle=old_handle, timeout=15)

    # Call start again to refresh handle
    serve.start()

    new_handle = _get_global_client()._controller
    assert new_handle is not old_handle

    # Health check should not error
    ray.get(new_handle.check_alive.remote())


def test_get_serve_status(shutdown_ray_and_serve):
    ray.init()

    @serve.deployment
    def f(*args):
        return "Hello world"

    serve.run(f.bind())

    client = _get_global_client()
    status_info_1 = client.get_serve_status()
    assert status_info_1.app_status.status == "RUNNING"
    assert status_info_1.deployment_statuses[0].name == "f"
    assert status_info_1.deployment_statuses[0].status in {"UPDATING", "HEALTHY"}


def test_controller_deserialization_deployment_def(
    start_and_shutdown_ray_cli_function, shutdown_ray_and_serve
):
    """Ensure controller doesn't deserialize deployment_def or init_args/kwargs."""

    @ray.remote
    def run_graph():
        """Deploys a Serve application to the controller's Ray cluster."""
        from ray import serve
        from ray._common.utils import import_attr

        # Import and build the graph
        graph = import_attr("test_config_files.pizza.serve_dag")

        # Run the graph locally on the cluster
        serve.run(graph)

    # Start Serve controller in a directory without access to the graph code
    ray.init(
        address="auto",
        namespace="serve",
        runtime_env={"working_dir": os.path.join(os.path.dirname(__file__), "common")},
    )
    serve.start()
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
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).text
        == "4 pizzas please!"
    )


def test_controller_deserialization_args_and_kwargs(shutdown_ray_and_serve):
    """Ensures init_args and init_kwargs stay serialized in controller."""
    serve.start()
    client = _get_global_client()

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


def test_controller_recover_and_delete(shutdown_ray_and_serve):
    """Ensure that in-progress deletion can finish even after controller dies."""

    ray_context = ray.init()
    serve.start()
    client = _get_global_client()

    num_replicas = 10

    @serve.deployment(
        num_replicas=num_replicas,
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
        == len(actors) - num_replicas
    )

    # The application should be deleted.
    wait_for_condition(
        lambda: SERVE_DEFAULT_APP_NAME not in serve.status().applications
    )


def test_serve_stream_logs(start_and_shutdown_ray_cli_function):
    """Test that serve logs show up across different drivers."""

    file1 = """from ray import serve
@serve.deployment
class A:
    def __call__(self):
        return "Hello A"
serve.run(A.bind())"""

    file2 = """from ray import serve
@serve.deployment
class B:
    def __call__(self):
        return "Hello B"
serve.run(B.bind())"""

    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        f1.write(file1.encode("utf-8"))
        f1.seek(0)
        # Driver 1 (starts Serve controller)
        output = subprocess.check_output(["python", f1.name], stderr=subprocess.STDOUT)
        assert "Connecting to existing Ray cluster" in output.decode("utf-8")
        assert "Adding 1 replica to Deployment(name='A'" in output.decode("utf-8")

        f2.write(file2.encode("utf-8"))
        f2.seek(0)
        # Driver 2 (reconnects to the same Serve controller)
        output = subprocess.check_output(["python", f2.name], stderr=subprocess.STDOUT)
        assert "Connecting to existing Ray cluster" in output.decode("utf-8")
        assert "Adding 1 replica to Deployment(name='B'" in output.decode("utf-8")


def test_checkpoint_deleted_on_serve_shutdown(start_and_shutdown_ray_cli_function):
    """Test the application target state checkpoint is deleted when Serve is shutdown"""

    file1 = """from ray import serve
@serve.deployment
class A:
    def __call__(self):
        return "Hello A"
serve.run(A.bind())"""

    file2 = """from ray import serve
@serve.deployment
class B:
    def __call__(self):
        return "Hello B"
serve.run(B.bind())"""

    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        f1.write(file1.encode("utf-8"))
        f1.seek(0)
        output = subprocess.check_output(["python", f1.name], stderr=subprocess.STDOUT)
        print(output.decode("utf-8"))
        assert "Connecting to existing Ray cluster" in output.decode("utf-8")
        subprocess.check_output(["serve", "shutdown", "-y"])

        f2.write(file2.encode("utf-8"))
        f2.seek(0)
        output = subprocess.check_output(["python", f2.name], stderr=subprocess.STDOUT)
        print(output.decode("utf-8"))
        assert "Connecting to existing Ray cluster" in output.decode("utf-8")
        assert "Recovering target state for application" not in output.decode("utf-8")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
