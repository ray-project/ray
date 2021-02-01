from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.client.ray_client_helpers import ray_start_client_server_pair
from ray.test_utils import wait_for_condition
import ray as real_ray
from ray.core.generated.gcs_pb2 import ActorTableData


def server_object_ref_count(server, n):
    assert server is not None

    def test_cond():
        if len(server.task_servicer.object_refs) == 0:
            # No open clients
            return n == 0
        client_id = list(server.task_servicer.object_refs.keys())[0]
        return len(server.task_servicer.object_refs[client_id]) == n

    return test_cond


def server_actor_ref_count(server, n):
    assert server is not None

    def test_cond():
        if len(server.task_servicer.actor_refs) == 0:
            # No running actors
            return n == 0
        return len(server.task_servicer.actor_refs) == n

    return test_cond


def test_delete_refs_on_disconnect(ray_start_regular):
    with ray_start_client_server_pair() as pair:
        ray, server = pair

        @ray.remote
        def f(x):
            return x + 2

        thing1 = f.remote(6)  # noqa
        thing2 = ray.put("Hello World")  # noqa

        # One put, one function -- the function result thing1 is
        # in a different category, according to the raylet.
        assert len(real_ray.objects()) == 2
        # But we're maintaining the reference
        assert server_object_ref_count(server, 3)()
        # And can get the data
        assert ray.get(thing1) == 8

        # Close the client
        ray.close()

        wait_for_condition(server_object_ref_count(server, 0), timeout=5)

        def test_cond():
            return len(real_ray.objects()) == 0

        wait_for_condition(test_cond, timeout=5)


def test_delete_ref_on_object_deletion(ray_start_regular):
    with ray_start_client_server_pair() as pair:
        ray, server = pair
        vals = {
            "ref": ray.put("Hello World"),
            "ref2": ray.put("This value stays"),
        }

        del vals["ref"]

        wait_for_condition(server_object_ref_count(server, 1), timeout=5)


def test_delete_actor_on_disconnect(ray_start_regular):
    with ray_start_client_server_pair() as pair:
        ray, server = pair

        @ray.remote
        class Accumulator:
            def __init__(self):
                self.acc = 0

            def inc(self):
                self.acc += 1

            def get(self):
                return self.acc

        actor = Accumulator.remote()
        actor.inc.remote()

        assert server_actor_ref_count(server, 1)()

        assert ray.get(actor.get.remote()) == 1

        ray.close()

        wait_for_condition(server_actor_ref_count(server, 0), timeout=5)

        def test_cond():
            alive_actors = [
                v for v in real_ray.actors().values()
                if v["State"] != ActorTableData.DEAD
            ]
            return len(alive_actors) == 0

        wait_for_condition(test_cond, timeout=10)


def test_delete_actor(ray_start_regular):
    with ray_start_client_server_pair() as pair:
        ray, server = pair

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

        assert server_actor_ref_count(server, 2)()

        del actor

        wait_for_condition(server_actor_ref_count(server, 1), timeout=5)


def test_simple_multiple_references(ray_start_regular):
    with ray_start_client_server() as ray:

        @ray.remote
        class A:
            def __init__(self):
                self.x = ray.put("hi")

            def get(self):
                return [self.x]

        a = A.remote()
        ref1 = ray.get(a.get.remote())[0]
        ref2 = ray.get(a.get.remote())[0]
        del a
        assert ray.get(ref1) == "hi"
        del ref1
        assert ray.get(ref2) == "hi"
        del ref2
