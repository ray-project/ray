import pytest

from ray.tests.test_experimental_client import ray_start_client_server
from ray.test_utils import wait_for_condition
import ray as real_ray
from ray.experimental.client import _get_server_instance


def server_object_ref_count(n):
    server = _get_server_instance()
    assert server is not None

    def test_cond():
        if len(server.object_refs) == 0:
            # No open clients
            return n == 0
        client_id = list(server.object_refs.keys())[0]
        return len(server.object_refs[client_id]) == n

    return test_cond


def server_actor_ref_count(n):
    server = _get_server_instance()
    assert server is not None

    def test_cond():
        if len(server.actor_refs) == 0:
            # No running actors
            return n == 0
        return len(server.actor_refs) == n

    return test_cond


def test_delete_refs_on_disconnect(ray_start_regular):
    with ray_start_client_server() as ray:

        @ray.remote
        def f(x):
            return x + 2

        thing1 = f.remote(6)  # noqa
        thing2 = ray.put("Hello World")  # noqa

        # One put, one result, one function
        assert len(real_ray.objects()) == 3
        assert server_object_ref_count(3)()

        # Close the client
        ray.close()

        wait_for_condition(server_object_ref_count(0), timeout=5)

        def test_cond():
            return len(real_ray.objects()) == 0

        # TODO(barakmich): https://github.com/ray-project/ray/issues/12863
        # This test will succeed when the server can serialize an ObjectRef
        # without pinning it. Until then, the objects still live, according to
        # the raylet.
        with pytest.raises(RuntimeError):
            wait_for_condition(test_cond, timeout=5)


def test_delete_ref_on_object_deletion(ray_start_regular):
    with ray_start_client_server() as ray:
        vals = {
            "ref": ray.put("Hello World"),
            "ref2": ray.put("This value stays"),
        }

        del vals["ref"]

        wait_for_condition(server_object_ref_count(1), timeout=5)


def test_delete_actor_on_disconnect(ray_start_regular):
    with ray_start_client_server() as ray:
        @ray.remote
        class Accumulator:
            def __init__(self):
                self.acc = 0

            def inc(self):
                self.acc += 1

        actor = Accumulator.remote()
        actor.inc.remote()

        assert server_actor_ref_count(1)()

        ray.close()

        wait_for_condition(server_actor_ref_count(0), timeout=5)

        def test_cond():
            return len(real_ray.actors()) == 0

        # Ditto the TODO above
        with pytest.raises(RuntimeError):
            wait_for_condition(test_cond, timeout=5)


def test_delete_actor(ray_start_regular):
    with ray_start_client_server() as ray:
        @ray.remote
        class Accumulator:
            def __init__(self):
                self.acc = 0

            def inc(self):
                self.acc += 1

        actor = Accumulator.remote()
        actor.inc.remote()
        actor2 = Accumulator.remote()
        actor2.inc.remote()

        assert server_actor_ref_count(2)()

        del actor

        wait_for_condition(server_actor_ref_count(1), timeout=5)
