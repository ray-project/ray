"""Tests for Serve shutdown behavior (isolated from test_standalone)."""

import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray._raylet import GcsClient
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_DEPLOYMENT_ACTOR_PREFIX,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
)
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.utils import format_actor_name
from ray.serve.config import DeploymentActorConfig
from ray.util.state import list_actors


def test_shutdown(ray_shutdown):
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    actor_names = [
        SERVE_CONTROLLER_NAME,
        format_actor_name(
            SERVE_PROXY_NAME,
            cluster_node_info_cache.get_alive_nodes()[0][0],
        ),
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
            except ValueError:
                alive = False
        return alive

    wait_for_condition(check_alive)

    serve.shutdown()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_shutdown_async(ray_shutdown):
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    actor_names = [
        SERVE_CONTROLLER_NAME,
        format_actor_name(
            SERVE_PROXY_NAME,
            cluster_node_info_cache.get_alive_nodes()[0][0],
        ),
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
            except ValueError:
                alive = False
        return alive

    wait_for_condition(check_alive)

    await serve.shutdown_async()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_single_app_shutdown_actors(ray_shutdown):
    """Tests serve.shutdown() works correctly in single-app case

    Ensures that after deploying a (nameless) app using serve.run(), serve.shutdown()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    serve.shutdown()
    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_single_app_shutdown_actors_async(ray_shutdown):
    """Tests serve.shutdown_async() works correctly in single-app case

    Ensures that after deploying a (nameless) app using serve.run(), serve.shutdown_async()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    await serve.shutdown_async()
    wait_for_condition(check_dead)


def test_multi_app_shutdown_actors(ray_shutdown):
    """Tests serve.shutdown() works correctly in multi-app case.

    Ensures that after deploying multiple distinct applications, serve.shutdown()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app1", route_prefix="/app1")
    serve.run(f.bind(), name="app2", route_prefix="/app2")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app1:f",
        "ServeReplica:app2:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    serve.shutdown()
    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_multi_app_shutdown_actors_async(ray_shutdown):
    """Tests serve.shutdown_async() works correctly in multi-app case.

    Ensures that after deploying multiple distinct applications, serve.shutdown_async()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app1", route_prefix="/app1")
    serve.run(f.bind(), name="app2", route_prefix="/app2")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app1:f",
        "ServeReplica:app2:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    await serve.shutdown_async()
    wait_for_condition(check_dead)


def test_registered_cleanup_actors_killed_on_shutdown(ray_shutdown):
    """Test that actors registered via _register_shutdown_cleanup_actor are killed.

    This tests the internal actor registration API that allows deployments to register
    auxiliary actors (like caches, coordinators, etc.) for cleanup on serve.shutdown().
    """
    ray.init(num_cpus=4)
    serve.start()

    # Create a detached actor that we'll register for cleanup
    @ray.remote
    class DummyActor:
        def ping(self):
            return "pong"

    dummy_actor_name = "test_registered_cleanup_dummy"
    dummy = DummyActor.options(
        name=dummy_actor_name, namespace=SERVE_NAMESPACE, lifetime="detached"
    ).remote()

    # Verify actor is alive
    assert ray.get(dummy.ping.remote()) == "pong"

    # Register the actor with the controller for cleanup
    controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
    ray.get(controller._register_shutdown_cleanup_actor.remote(dummy))

    # Shutdown serve
    serve.shutdown()

    # Verify the registered actor is killed
    def check_actor_dead():
        try:
            ray.get_actor(dummy_actor_name, namespace=SERVE_NAMESPACE)
            return False
        except ValueError:
            return True

    wait_for_condition(check_actor_dead)


def test_serve_shutdown(ray_shutdown):
    ray.init(namespace="serve")
    serve.start()

    @serve.deployment
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    assert len(serve.status().applications) == 1

    serve.shutdown()
    serve.start()

    assert len(serve.status().applications) == 0

    serve.run(A.bind())

    assert len(serve.status().applications) == 1


@pytest.mark.asyncio
async def test_serve_shutdown_async(ray_shutdown):
    ray.init(namespace="serve")
    serve.start()

    @serve.deployment
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    assert len(serve.status().applications) == 1

    await serve.shutdown_async()
    serve.start()

    assert len(serve.status().applications) == 0

    serve.run(A.bind())

    assert len(serve.status().applications) == 1


def test_shutdown_after_driver_reconnect(ray_shutdown):
    """serve.shutdown() should work after the driver disconnects and reconnects.

    Regression test for https://github.com/ray-project/ray/issues/61608.
    When using `with ray.init()` as a context manager, ray.shutdown() is called
    on exit but the module-level _global_client in ray.serve.context is not
    cleared. On the next ray.init(), serve.shutdown() must health-check the
    cached controller handle so it can reconnect instead of using a stale
    handle from the previous session.
    """

    @serve.deployment
    class Greeter:
        def __call__(self, *args):
            return "hi"

    # Session 1: start Serve and deploy. The context manager calls
    # ray.shutdown() on exit, but the detached controller survives.
    with ray.init(namespace="serve"):
        serve.start()
        serve.run(Greeter.bind())
        assert len(serve.status().applications) == 1

    # Session 2: reconnect and shut down Serve. This must not fail due to
    # a stale _global_client from session 1.
    with ray.init(namespace="serve"):
        serve.shutdown()

    # Session 3: verify Serve is actually shut down — starting fresh should
    # show zero applications.
    with ray.init(namespace="serve"):
        serve.start()
        assert len(serve.status().applications) == 0
        serve.shutdown()


@pytest.mark.asyncio
async def test_shutdown_async_after_driver_reconnect(ray_shutdown):
    """Async variant: serve.shutdown_async() should work after driver reconnect.

    Regression test for https://github.com/ray-project/ray/issues/61608.
    """

    @serve.deployment
    class Greeter:
        def __call__(self, *args):
            return "hi"

    with ray.init(namespace="serve"):
        serve.start()
        serve.run(Greeter.bind())
        assert len(serve.status().applications) == 1

    with ray.init(namespace="serve"):
        await serve.shutdown_async()

    with ray.init(namespace="serve"):
        serve.start()
        assert len(serve.status().applications) == 0
        await serve.shutdown_async()


def test_serve_shutdown_cleans_up_deployment_actors(ray_shutdown):
    """serve.shutdown() kills all deployment actors.

    Deployment actors are detached, so they must be explicitly killed during
    shutdown.
    """

    ray_context = ray.init(num_cpus=4, namespace="default_test_namespace")
    address = ray_context.address_info["address"]
    serve.start()

    @ray.remote
    class SharedCounter:
        def __init__(self, start: int = 0):
            self.count = start

        def get(self):
            return self.count

    @serve.deployment(
        deployment_actors=[
            DeploymentActorConfig(
                name="counter",
                actor_class=SharedCounter,
                init_kwargs={"start": 0},
            ),
        ],
    )
    class MyDeployment:
        def __call__(self):
            counter = serve.get_deployment_actor("counter")
            return str(ray.get(counter.get.remote()))

    serve.run(MyDeployment.bind())

    def _check_deployment_actor_count(expected: int):
        actors = list_actors(address=address, filters=[("state", "=", "ALIVE")])
        names = [
            a["name"]
            for a in actors
            if a["name"] and a["name"].startswith(SERVE_DEPLOYMENT_ACTOR_PREFIX)
        ]
        return len(names) == expected

    wait_for_condition(lambda: _check_deployment_actor_count(1))

    serve.shutdown()

    wait_for_condition(lambda: _check_deployment_actor_count(0), timeout=15)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
