import pytest
import sys
import time

import ray
from ray import ray_constants
from ray._private.test_utils import get_error_message, init_error_pubsub, \
    run_string_as_driver
from ray.cluster_utils import Cluster


def test_isolation(shutdown_only):
    info = ray.init(namespace="namespace")

    address = info["address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}", namespace="{}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    # Start a detached actor in a different namespace.
    run_string_as_driver(driver_template.format(address, "different"))

    @ray.remote
    class Actor:
        def ping(self):
            return "pong"

    # Create an actor. This should succeed because the other actor is in a
    # different namespace.
    probe = Actor.options(name="Pinger").remote()
    assert ray.get(probe.ping.remote()) == "pong"
    del probe

    # Wait for actor removal
    actor_removed = False
    for _ in range(50):  # Timeout after 5s
        try:
            ray.get_actor("Pinger")
        except ValueError:
            actor_removed = True
            # This means the actor was removed.
            break
        else:
            time.sleep(0.1)

    assert actor_removed, "This is an anti-flakey test measure"

    with pytest.raises(ValueError, match="Failed to look up actor with name"):
        ray.get_actor("Pinger")

    # Now make the actor in this namespace, from a different job.
    run_string_as_driver(driver_template.format(address, "namespace"))

    detached_actor = ray.get_actor("Pinger")
    assert ray.get(detached_actor.ping.remote()) == "pong from other job"

    with pytest.raises(ValueError, match="The name .* is already taken"):
        Actor.options(name="Pinger", lifetime="detached").remote()


def test_placement_groups(shutdown_only):
    info = ray.init(namespace="namespace")

    address = info["address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}", namespace="{}")

pg = ray.util.placement_group(bundles=[dict(CPU=1)], name="hello",
    lifetime="detached")
ray.get(pg.ready())
    """

    # Start a detached placement group in a different namespace.
    run_string_as_driver(driver_template.format(address, "different"))

    # Create an actor. This should succeed because the other actor is in a
    # different namespace.
    probe = ray.util.placement_group(bundles=[{"CPU": 1}], name="hello")
    ray.get(probe.ready())
    ray.util.remove_placement_group(probe)

    removed = False
    for _ in range(50):  # Timeout after 5s
        try:
            ray.util.get_placement_group("hello")
        except ValueError:
            removed = True
            # This means the actor was removed.
            break
        else:
            time.sleep(0.1)

    assert removed, "This is an anti-flakey test measure"

    # Now make the actor in this namespace, from a different job.
    run_string_as_driver(driver_template.format(address, "namespace"))


def test_default_namespace(shutdown_only):
    info = ray.init(namespace="namespace")

    address = info["address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    # Each run of this script will create a detached actor. Since the drivers
    # are in different namespaces, both scripts should succeed. If they were
    # placed in the same namespace, the second call will throw an exception.
    run_string_as_driver(driver_template.format(address))
    run_string_as_driver(driver_template.format(address))


def test_namespace_in_job_config(shutdown_only):
    # JobConfig isn't a public API, but we'll set it directly, instead of
    # using the param in code paths like the ray client.
    job_config = ray.job_config.JobConfig(ray_namespace="namespace")
    info = ray.init(job_config=job_config)

    address = info["address"]

    # First param of template is the namespace. Second is the redis address.
    driver_template = """
import ray

ray.init(address="{}", namespace="namespace")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
    """

    run_string_as_driver(driver_template.format(address))

    act = ray.get_actor("Pinger")
    assert ray.get(act.ping.remote()) == "pong from other job"


def test_detached_warning(shutdown_only):
    ray.init()

    @ray.remote
    class DetachedActor:
        def ping(self):
            return "pong"

    error_pubsub = init_error_pubsub()
    actor = DetachedActor.options(  # noqa: F841
        name="Pinger", lifetime="detached").remote()
    errors = get_error_message(error_pubsub, 1, None)
    error = errors.pop()
    assert error.type == ray_constants.DETACHED_ACTOR_ANONYMOUS_NAMESPACE_ERROR


def test_namespace_client():
    cluster = Cluster()
    cluster.add_node(num_cpus=4, ray_client_server_port=8080)
    cluster.wait_for_nodes(1)

    template = """
import ray
ray.util.connect("{address}", namespace="{namespace}")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong from other job"

actor = DetachedActor.options(name="Pinger", lifetime="detached").remote()
ray.get(actor.ping.remote())
print("Done!!!")
    """

    print(
        run_string_as_driver(
            template.format(address="localhost:8080", namespace="test")))

    ray.util.connect("localhost:8080", namespace="test")

    pinger = ray.get_actor("Pinger")
    assert ray.get(pinger.ping.remote()) == "pong from other job"

    ray.util.disconnect()
    cluster.shutdown()
    # This piece of cleanup doesn't seem to happen automatically.
    ray._private.client_mode_hook._explicitly_disable_client_mode()


def test_runtime_context(shutdown_only):
    ray.init(namespace="abc")
    namespace = ray.get_runtime_context().namespace
    assert namespace == "abc"
    assert namespace == ray.get_runtime_context().get()["namespace"]


def test_namespace_validation(shutdown_only):
    with pytest.raises(TypeError):
        ray.init(namespace=123)

    ray.shutdown()

    with pytest.raises(ValueError):
        ray.init(namespace="")

    ray.shutdown()

    ray.init(namespace="abc")

    @ray.remote
    class A:
        pass

    with pytest.raises(TypeError):
        A.options(namespace=123).remote()

    with pytest.raises(ValueError):
        A.options(namespace="").remote()

    A.options(name="a", namespace="test", lifetime="detached").remote()

    with pytest.raises(TypeError):
        ray.get_actor("a", namespace=123)

    with pytest.raises(ValueError):
        ray.get_actor("a", namespace="")

    ray.get_actor("a", namespace="test")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
