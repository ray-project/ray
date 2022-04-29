import sys

import pytest
from ray.exceptions import RayActorError
import requests

import ray
from ray import serve
from ray.serve.api import internal_get_global_client
from ray._private.test_utils import wait_for_condition


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


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


@pytest.mark.parametrize("detached", [True, False])
def test_override_namespace(shutdown_ray, detached):
    """Test the _override_controller_namespace flag in serve.start()."""

    ray_namespace = "ray_namespace"
    controller_namespace = "controller_namespace"

    ray.init(namespace=ray_namespace)
    serve.start(detached=detached, _override_controller_namespace=controller_namespace)

    controller_name = internal_get_global_client()._controller_name
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

    old_handle = internal_get_global_client()._controller
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

    new_handle = internal_get_global_client()._controller
    assert new_handle is not old_handle

    # Health check should not error
    ray.get(new_handle.check_alive.remote())

    serve.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
