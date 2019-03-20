from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import random
import numpy as np
import os
import pytest
import signal
import sys
import time

import ray
import ray.ray_constants as ray_constants
import ray.tests.utils
import ray.tests.cluster_utils
from ray.tests.conftest import generate_internal_config_map
from ray.tests.utils import (
    relevant_errors,
    wait_for_errors,
)


@pytest.fixture
def ray_checkpointable_actor_cls(request):
    checkpoint_dir = "/tmp/ray_temp_checkpoint_dir/"
    if not os.path.isdir(checkpoint_dir):
        os.mkdir(checkpoint_dir)

    class CheckpointableActor(ray.actor.Checkpointable):
        def __init__(self):
            self.value = 0
            self.resumed_from_checkpoint = False
            self.checkpoint_dir = checkpoint_dir

        def local_plasma(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

        def increase(self):
            self.value += 1
            return self.value

        def get(self):
            return self.value

        def was_resumed_from_checkpoint(self):
            return self.resumed_from_checkpoint

        def get_pid(self):
            return os.getpid()

        def should_checkpoint(self, checkpoint_context):
            # Checkpoint the actor when value is increased to 3.
            should_checkpoint = self.value == 3
            return should_checkpoint

        def save_checkpoint(self, actor_id, checkpoint_id):
            actor_id, checkpoint_id = actor_id.hex(), checkpoint_id.hex()
            # Save checkpoint into a file.
            with open(self.checkpoint_dir + actor_id, "a+") as f:
                print(checkpoint_id, self.value, file=f)

        def load_checkpoint(self, actor_id, available_checkpoints):
            actor_id = actor_id.hex()
            filename = self.checkpoint_dir + actor_id
            # Load checkpoint from the file.
            if not os.path.isfile(filename):
                return None

            with open(filename, "r") as f:
                lines = f.readlines()
                checkpoint_id, value = lines[-1].split(" ")
                self.value = int(value)
                self.resumed_from_checkpoint = True
                checkpoint_id = ray.ActorCheckpointID(
                    ray.utils.hex_to_binary(checkpoint_id))
                assert any(checkpoint_id == checkpoint.checkpoint_id
                           for checkpoint in available_checkpoints)
                return checkpoint_id

        def checkpoint_expired(self, actor_id, checkpoint_id):
            pass

    return CheckpointableActor


def test_actor_init_error_propagated(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self, error=False):
            if error:
                raise Exception("oops")

        def foo(self):
            return "OK"

    actor = Actor.remote(error=False)
    ray.get(actor.foo.remote())

    actor = Actor.remote(error=True)
    with pytest.raises(Exception, match=".*oops.*"):
        ray.get(actor.foo.remote())


@pytest.mark.skipif(
    sys.version_info >= (3, 0), reason="This test requires Python 2.")
def test_old_style_error(ray_start_regular):
    with pytest.raises(TypeError):

        @ray.remote
        class Actor:
            pass


def test_keyword_args(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self, arg0, arg1=1, arg2="a"):
            self.arg0 = arg0
            self.arg1 = arg1
            self.arg2 = arg2

        def get_values(self, arg0, arg1=2, arg2="b"):
            return self.arg0 + arg0, self.arg1 + arg1, self.arg2 + arg2

    actor = Actor.remote(0)
    assert ray.get(actor.get_values.remote(1)) == (1, 3, "ab")

    actor = Actor.remote(1, 2)
    assert ray.get(actor.get_values.remote(2, 3)) == (3, 5, "ab")

    actor = Actor.remote(1, 2, "c")
    assert ray.get(actor.get_values.remote(2, 3, "d")) == (3, 5, "cd")

    actor = Actor.remote(1, arg2="c")
    assert ray.get(actor.get_values.remote(0, arg2="d")) == (1, 3, "cd")
    assert ray.get(actor.get_values.remote(0, arg2="d", arg1=0)) == (1, 1,
                                                                     "cd")

    actor = Actor.remote(1, arg2="c", arg1=2)
    assert ray.get(actor.get_values.remote(0, arg2="d")) == (1, 4, "cd")
    assert ray.get(actor.get_values.remote(0, arg2="d", arg1=0)) == (1, 2,
                                                                     "cd")
    assert ray.get(actor.get_values.remote(arg2="d", arg1=0, arg0=2)) == (3, 2,
                                                                          "cd")

    # Make sure we get an exception if the constructor is called
    # incorrectly.
    with pytest.raises(Exception):
        actor = Actor.remote()

    with pytest.raises(Exception):
        actor = Actor.remote(0, 1, 2, arg3=3)

    with pytest.raises(Exception):
        actor = Actor.remote(0, arg0=1)

    # Make sure we get an exception if the method is called incorrectly.
    actor = Actor.remote(1)
    with pytest.raises(Exception):
        ray.get(actor.get_values.remote())


def test_variable_number_of_args(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self, arg0, arg1=1, *args):
            self.arg0 = arg0
            self.arg1 = arg1
            self.args = args

        def get_values(self, arg0, arg1=2, *args):
            return self.arg0 + arg0, self.arg1 + arg1, self.args, args

    actor = Actor.remote(0)
    assert ray.get(actor.get_values.remote(1)) == (1, 3, (), ())

    actor = Actor.remote(1, 2)
    assert ray.get(actor.get_values.remote(2, 3)) == (3, 5, (), ())

    actor = Actor.remote(1, 2, "c")
    assert ray.get(actor.get_values.remote(2, 3, "d")) == (3, 5, ("c", ),
                                                           ("d", ))

    actor = Actor.remote(1, 2, "a", "b", "c", "d")
    assert ray.get(actor.get_values.remote(
        2, 3, 1, 2, 3, 4)) == (3, 5, ("a", "b", "c", "d"), (1, 2, 3, 4))

    @ray.remote
    class Actor(object):
        def __init__(self, *args):
            self.args = args

        def get_values(self, *args):
            return self.args, args

    a = Actor.remote()
    assert ray.get(a.get_values.remote()) == ((), ())
    a = Actor.remote(1)
    assert ray.get(a.get_values.remote(2)) == ((1, ), (2, ))
    a = Actor.remote(1, 2)
    assert ray.get(a.get_values.remote(3, 4)) == ((1, 2), (3, 4))


def test_no_args(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def get_values(self):
            pass

    actor = Actor.remote()
    assert ray.get(actor.get_values.remote()) is None


def test_no_constructor(ray_start_regular):
    # If no __init__ method is provided, that should not be a problem.
    @ray.remote
    class Actor(object):
        def get_values(self):
            pass

    actor = Actor.remote()
    assert ray.get(actor.get_values.remote()) is None


def test_custom_classes(ray_start_regular):
    class Foo(object):
        def __init__(self, x):
            self.x = x

    @ray.remote
    class Actor(object):
        def __init__(self, f2):
            self.f1 = Foo(1)
            self.f2 = f2

        def get_values1(self):
            return self.f1, self.f2

        def get_values2(self, f3):
            return self.f1, self.f2, f3

    actor = Actor.remote(Foo(2))
    results1 = ray.get(actor.get_values1.remote())
    assert results1[0].x == 1
    assert results1[1].x == 2
    results2 = ray.get(actor.get_values2.remote(Foo(3)))
    assert results2[0].x == 1
    assert results2[1].x == 2
    assert results2[2].x == 3


def test_caching_actors(shutdown_only):
    # Test defining actors before ray.init() has been called.

    @ray.remote
    class Foo(object):
        def __init__(self):
            pass

        def get_val(self):
            return 3

    # Check that we can't actually create actors before ray.init() has been
    # called.
    with pytest.raises(Exception):
        f = Foo.remote()

    ray.init(num_cpus=1)

    f = Foo.remote()

    assert ray.get(f.get_val.remote()) == 3


def test_decorator_args(ray_start_regular):
    # This is an invalid way of using the actor decorator.
    with pytest.raises(Exception):

        @ray.remote()
        class Actor(object):
            def __init__(self):
                pass

    # This is an invalid way of using the actor decorator.
    with pytest.raises(Exception):

        @ray.remote(invalid_kwarg=0)  # noqa: F811
        class Actor(object):
            def __init__(self):
                pass

    # This is an invalid way of using the actor decorator.
    with pytest.raises(Exception):

        @ray.remote(num_cpus=0, invalid_kwarg=0)  # noqa: F811
        class Actor(object):
            def __init__(self):
                pass

    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1)  # noqa: F811
    class Actor(object):
        def __init__(self):
            pass

    # This is a valid way of using the decorator.
    @ray.remote(num_gpus=1)  # noqa: F811
    class Actor(object):
        def __init__(self):
            pass

    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1, num_gpus=1)  # noqa: F811
    class Actor(object):
        def __init__(self):
            pass


def test_random_id_generation(ray_start_regular):
    @ray.remote
    class Foo(object):
        def __init__(self):
            pass

    # Make sure that seeding numpy does not interfere with the generation
    # of actor IDs.
    np.random.seed(1234)
    random.seed(1234)
    f1 = Foo.remote()
    np.random.seed(1234)
    random.seed(1234)
    f2 = Foo.remote()

    assert f1._ray_actor_id != f2._ray_actor_id


def test_actor_class_name(ray_start_regular):
    @ray.remote
    class Foo(object):
        def __init__(self):
            pass

    Foo.remote()

    r = ray.worker.global_worker.redis_client
    actor_keys = r.keys("ActorClass*")
    assert len(actor_keys) == 1
    actor_class_info = r.hgetall(actor_keys[0])
    assert actor_class_info[b"class_name"] == b"Foo"
    assert actor_class_info[b"module"] == b"ray.tests.test_actor"


def test_multiple_return_values(ray_start_regular):
    @ray.remote
    class Foo(object):
        def method0(self):
            return 1

        @ray.method(num_return_vals=1)
        def method1(self):
            return 1

        @ray.method(num_return_vals=2)
        def method2(self):
            return 1, 2

        @ray.method(num_return_vals=3)
        def method3(self):
            return 1, 2, 3

    f = Foo.remote()

    id0 = f.method0.remote()
    assert ray.get(id0) == 1

    id1 = f.method1.remote()
    assert ray.get(id1) == 1

    id2a, id2b = f.method2.remote()
    assert ray.get([id2a, id2b]) == [1, 2]

    id3a, id3b, id3c = f.method3.remote()
    assert ray.get([id3a, id3b, id3c]) == [1, 2, 3]


def test_define_actor(ray_start_regular):
    @ray.remote
    class Test(object):
        def __init__(self, x):
            self.x = x

        def f(self, y):
            return self.x + y

    t = Test.remote(2)
    assert ray.get(t.f.remote(1)) == 3

    # Make sure that calling an actor method directly raises an exception.
    with pytest.raises(Exception):
        t.f(1)


def test_actor_deletion(ray_start_regular):
    # Make sure that when an actor handles goes out of scope, the actor
    # destructor is called.

    @ray.remote
    class Actor(object):
        def getpid(self):
            return os.getpid()

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    a = None
    ray.tests.utils.wait_for_pid_to_exit(pid)

    actors = [Actor.remote() for _ in range(10)]
    pids = ray.get([a.getpid.remote() for a in actors])
    a = None
    actors = None
    [ray.tests.utils.wait_for_pid_to_exit(pid) for pid in pids]

    @ray.remote
    class Actor(object):
        def method(self):
            return 1

    # Make sure that if we create an actor and call a method on it
    # immediately, the actor doesn't get killed before the method is
    # called.
    assert ray.get(Actor.remote().method.remote()) == 1


def test_actor_deletion_with_gpus(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1)

    # When an actor that uses a GPU exits, make sure that the GPU resources
    # are released.

    @ray.remote(num_gpus=1)
    class Actor(object):
        def getpid(self):
            return os.getpid()

    for _ in range(5):
        # If we can successfully create an actor, that means that enough
        # GPU resources are available.
        a = Actor.remote()
        ray.get(a.getpid.remote())


def test_actor_state(ray_start_regular):
    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1

        def value(self):
            return self.value

    c1 = Counter.remote()
    c1.increase.remote()
    assert ray.get(c1.value.remote()) == 1

    c2 = Counter.remote()
    c2.increase.remote()
    c2.increase.remote()
    assert ray.get(c2.value.remote()) == 2


def test_actor_class_methods(ray_start_regular):
    class Foo(object):
        x = 2

        @classmethod
        def as_remote(cls):
            return ray.remote(cls)

        @classmethod
        def f(cls):
            return cls.x

        @classmethod
        def g(cls, y):
            return cls.x + y

        def echo(self, value):
            return value

    a = Foo.as_remote().remote()
    assert ray.get(a.echo.remote(2)) == 2
    assert ray.get(a.f.remote()) == 2
    assert ray.get(a.g.remote(2)) == 4


def test_multiple_actors(ray_start_regular):
    @ray.remote
    class Counter(object):
        def __init__(self, value):
            self.value = value

        def increase(self):
            self.value += 1
            return self.value

        def reset(self):
            self.value = 0

    num_actors = 20
    num_increases = 50
    # Create multiple actors.
    actors = [Counter.remote(i) for i in range(num_actors)]
    results = []
    # Call each actor's method a bunch of times.
    for i in range(num_actors):
        results += [actors[i].increase.remote() for _ in range(num_increases)]
    result_values = ray.get(results)
    for i in range(num_actors):
        v = result_values[(num_increases * i):(num_increases * (i + 1))]
        assert v == list(range(i + 1, num_increases + i + 1))

    # Reset the actor values.
    [actor.reset.remote() for actor in actors]

    # Interweave the method calls on the different actors.
    results = []
    for j in range(num_increases):
        results += [actor.increase.remote() for actor in actors]
    result_values = ray.get(results)
    for j in range(num_increases):
        v = result_values[(num_actors * j):(num_actors * (j + 1))]
        assert v == num_actors * [j + 1]


def test_remote_function_within_actor(ray_start_10_cpus):
    # Make sure we can use remote funtions within actors.

    # Create some values to close over.
    val1 = 1
    val2 = 2

    @ray.remote
    def f(x):
        return val1 + x

    @ray.remote
    def g(x):
        return ray.get(f.remote(x))

    @ray.remote
    class Actor(object):
        def __init__(self, x):
            self.x = x
            self.y = val2
            self.object_ids = [f.remote(i) for i in range(5)]
            self.values2 = ray.get([f.remote(i) for i in range(5)])

        def get_values(self):
            return self.x, self.y, self.object_ids, self.values2

        def f(self):
            return [f.remote(i) for i in range(5)]

        def g(self):
            return ray.get([g.remote(i) for i in range(5)])

        def h(self, object_ids):
            return ray.get(object_ids)

    actor = Actor.remote(1)
    values = ray.get(actor.get_values.remote())
    assert values[0] == 1
    assert values[1] == val2
    assert ray.get(values[2]) == list(range(1, 6))
    assert values[3] == list(range(1, 6))

    assert ray.get(ray.get(actor.f.remote())) == list(range(1, 6))
    assert ray.get(actor.g.remote()) == list(range(1, 6))
    assert ray.get(actor.h.remote([f.remote(i) for i in range(5)])) == list(
        range(1, 6))


def test_define_actor_within_actor(ray_start_10_cpus):
    # Make sure we can use remote funtions within actors.

    @ray.remote
    class Actor1(object):
        def __init__(self, x):
            self.x = x

        def new_actor(self, z):
            @ray.remote
            class Actor2(object):
                def __init__(self, x):
                    self.x = x

                def get_value(self):
                    return self.x

            self.actor2 = Actor2.remote(z)

        def get_values(self, z):
            self.new_actor(z)
            return self.x, ray.get(self.actor2.get_value.remote())

    actor1 = Actor1.remote(3)
    assert ray.get(actor1.get_values.remote(5)) == (3, 5)


def test_use_actor_within_actor(ray_start_10_cpus):
    # Make sure we can use actors within actors.

    @ray.remote
    class Actor1(object):
        def __init__(self, x):
            self.x = x

        def get_val(self):
            return self.x

    @ray.remote
    class Actor2(object):
        def __init__(self, x, y):
            self.x = x
            self.actor1 = Actor1.remote(y)

        def get_values(self, z):
            return self.x, ray.get(self.actor1.get_val.remote())

    actor2 = Actor2.remote(3, 4)
    assert ray.get(actor2.get_values.remote(5)) == (3, 4)


def test_define_actor_within_remote_function(ray_start_10_cpus):
    # Make sure we can define and actors within remote funtions.

    @ray.remote
    def f(x, n):
        @ray.remote
        class Actor1(object):
            def __init__(self, x):
                self.x = x

            def get_value(self):
                return self.x

        actor = Actor1.remote(x)
        return ray.get([actor.get_value.remote() for _ in range(n)])

    assert ray.get(f.remote(3, 1)) == [3]
    assert ray.get(
        [f.remote(i, 20) for i in range(10)]) == [20 * [i] for i in range(10)]


def test_use_actor_within_remote_function(ray_start_10_cpus):
    # Make sure we can create and use actors within remote funtions.

    @ray.remote
    class Actor1(object):
        def __init__(self, x):
            self.x = x

        def get_values(self):
            return self.x

    @ray.remote
    def f(x):
        actor = Actor1.remote(x)
        return ray.get(actor.get_values.remote())

    assert ray.get(f.remote(3)) == 3


def test_actor_import_counter(ray_start_10_cpus):
    # This is mostly a test of the export counters to make sure that when
    # an actor is imported, all of the necessary remote functions have been
    # imported.

    # Export a bunch of remote functions.
    num_remote_functions = 50
    for i in range(num_remote_functions):

        @ray.remote
        def f():
            return i

    @ray.remote
    def g():
        @ray.remote
        class Actor(object):
            def __init__(self):
                # This should use the last version of f.
                self.x = ray.get(f.remote())

            def get_val(self):
                return self.x

        actor = Actor.remote()
        return ray.get(actor.get_val.remote())

    assert ray.get(g.remote()) == num_remote_functions - 1


def test_inherit_actor_from_class(ray_start_regular):
    # Make sure we can define an actor by inheriting from a regular class.
    # Note that actors cannot inherit from other actors.

    class Foo(object):
        def __init__(self, x):
            self.x = x

        def f(self):
            return self.x

        def g(self, y):
            return self.x + y

    @ray.remote
    class Actor(Foo):
        def __init__(self, x):
            Foo.__init__(self, x)

        def get_value(self):
            return self.f()

    actor = Actor.remote(1)
    assert ray.get(actor.get_value.remote()) == 1
    assert ray.get(actor.g.remote(5)) == 6


def test_remote_functions_not_scheduled_on_actors(ray_start_regular):
    # Make sure that regular remote functions are not scheduled on actors.

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def get_id(self):
            return ray.worker.global_worker.worker_id

    a = Actor.remote()
    actor_id = ray.get(a.get_id.remote())

    @ray.remote
    def f():
        return ray.worker.global_worker.worker_id

    resulting_ids = ray.get([f.remote() for _ in range(100)])
    assert actor_id not in resulting_ids


def test_actors_on_nodes_with_no_cpus(ray_start_regular):
    @ray.remote
    class Foo(object):
        def method(self):
            pass

    f = Foo.remote()
    ready_ids, _ = ray.wait([f.method.remote()], timeout=0.1)
    assert ready_ids == []


def test_actor_load_balancing(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    class Actor1(object):
        def __init__(self):
            pass

        def get_location(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    # Create a bunch of actors.
    num_actors = 30
    num_attempts = 20
    minimum_count = 5

    # Make sure that actors are spread between the local schedulers.
    attempts = 0
    while attempts < num_attempts:
        actors = [Actor1.remote() for _ in range(num_actors)]
        locations = ray.get([actor.get_location.remote() for actor in actors])
        names = set(locations)
        counts = [locations.count(name) for name in names]
        print("Counts are {}.".format(counts))
        if (len(names) == num_nodes
                and all(count >= minimum_count for count in counts)):
            break
        attempts += 1
    assert attempts < num_attempts

    # Make sure we can get the results of a bunch of tasks.
    results = []
    for _ in range(1000):
        index = np.random.randint(num_actors)
        results.append(actors[index].get_location.remote())
    ray.get(results)


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Failing with new GCS API on Linux.")
def test_actor_gpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 4
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=10 * num_gpus_per_raylet, num_gpus=num_gpus_per_raylet)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_gpus=1)
    class Actor1(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    # Create one actor per GPU.
    actors = [Actor1.remote() for _ in range(num_nodes * num_gpus_per_raylet)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors])
    node_names = {location for location, gpu_id in locations_and_ids}
    assert len(node_names) == num_nodes
    location_actor_combinations = []
    for node_name in node_names:
        for gpu_id in range(num_gpus_per_raylet):
            location_actor_combinations.append((node_name, (gpu_id, )))
    assert set(locations_and_ids) == set(location_actor_combinations)

    # Creating a new actor should fail because all of the GPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actor_multiple_gpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 5
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=10 * num_gpus_per_raylet, num_gpus=num_gpus_per_raylet)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_gpus=2)
    class Actor1(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    # Create some actors.
    actors1 = [Actor1.remote() for _ in range(num_nodes * 2)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors1])
    node_names = {location for location, gpu_id in locations_and_ids}
    assert len(node_names) == num_nodes

    # Keep track of which GPU IDs are being used for each location.
    gpus_in_use = {node_name: [] for node_name in node_names}
    for location, gpu_ids in locations_and_ids:
        gpus_in_use[location].extend(gpu_ids)
    for node_name in node_names:
        assert len(set(gpus_in_use[node_name])) == 4

    # Creating a new actor should fail because all of the GPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []

    # We should be able to create more actors that use only a single GPU.
    @ray.remote(num_gpus=1)
    class Actor2(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    # Create some actors.
    actors2 = [Actor2.remote() for _ in range(num_nodes)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors2])
    names = {location for location, gpu_id in locations_and_ids}
    assert node_names == names
    for location, gpu_ids in locations_and_ids:
        gpus_in_use[location].extend(gpu_ids)
    for node_name in node_names:
        assert len(gpus_in_use[node_name]) == 5
        assert set(gpus_in_use[node_name]) == set(range(5))

    # Creating a new actor should fail because all of the GPUs are being
    # used.
    a = Actor2.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actor_different_numbers_of_gpus(ray_start_cluster):
    # Test that we can create actors on two nodes that have different
    # numbers of GPUs.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=10, num_gpus=0)
    cluster.add_node(num_cpus=10, num_gpus=5)
    cluster.add_node(num_cpus=10, num_gpus=10)
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(num_gpus=1)
    class Actor1(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    # Create some actors.
    actors = [Actor1.remote() for _ in range(0 + 5 + 10)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors])
    node_names = {location for location, gpu_id in locations_and_ids}
    assert len(node_names) == 2
    for node_name in node_names:
        node_gpu_ids = [
            gpu_id for location, gpu_id in locations_and_ids
            if location == node_name
        ]
        assert len(node_gpu_ids) in [5, 10]
        assert set(node_gpu_ids) == {(i, ) for i in range(len(node_gpu_ids))}

    # Creating a new actor should fail because all of the GPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actor_multiple_gpus_from_multiple_tasks(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 5
    num_gpus_per_raylet = 5
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=10 * num_gpus_per_raylet,
            num_gpus=num_gpus_per_raylet,
            _internal_config=json.dumps({
                "num_heartbeats_timeout": 1000
            }))
    ray.init(redis_address=cluster.redis_address)

    @ray.remote
    def create_actors(i, n):
        @ray.remote(num_gpus=1)
        class Actor(object):
            def __init__(self, i, j):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return ((
                    ray.worker.global_worker.plasma_client.store_socket_name),
                        tuple(self.gpu_ids))

            def sleep(self):
                time.sleep(100)

        # Create n actors.
        actors = []
        for j in range(n):
            actors.append(Actor.remote(i, j))

        locations = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors])

        # Put each actor to sleep for a long time to prevent them from getting
        # terminated.
        for actor in actors:
            actor.sleep.remote()

        return locations

    all_locations = ray.get([
        create_actors.remote(i, num_gpus_per_raylet) for i in range(num_nodes)
    ])

    # Make sure that no two actors are assigned to the same GPU.
    node_names = {
        location
        for locations in all_locations for location, gpu_id in locations
    }
    assert len(node_names) == num_nodes

    # Keep track of which GPU IDs are being used for each location.
    gpus_in_use = {node_name: [] for node_name in node_names}
    for locations in all_locations:
        for location, gpu_ids in locations:
            gpus_in_use[location].extend(gpu_ids)
    for node_name in node_names:
        assert len(set(gpus_in_use[node_name])) == num_gpus_per_raylet

    @ray.remote(num_gpus=1)
    class Actor(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    # All the GPUs should be used up now.
    a = Actor.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
def test_actors_and_tasks_with_gpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 6
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=num_gpus_per_raylet, num_gpus=num_gpus_per_raylet)
    ray.init(redis_address=cluster.redis_address)

    def check_intervals_non_overlapping(list_of_intervals):
        for i in range(len(list_of_intervals)):
            for j in range(i):
                first_interval = list_of_intervals[i]
                second_interval = list_of_intervals[j]
                # Check that list_of_intervals[i] and list_of_intervals[j]
                # don't overlap.
                assert first_interval[0] < first_interval[1]
                assert second_interval[0] < second_interval[1]
                intervals_nonoverlapping = (
                    first_interval[1] <= second_interval[0]
                    or second_interval[1] <= first_interval[0])
                assert intervals_nonoverlapping, (
                    "Intervals {} and {} are overlapping.".format(
                        first_interval, second_interval))

    @ray.remote(num_gpus=1)
    def f1():
        t1 = time.monotonic()
        time.sleep(0.4)
        t2 = time.monotonic()
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert gpu_ids[0] in range(num_gpus_per_raylet)
        return (ray.worker.global_worker.plasma_client.store_socket_name,
                tuple(gpu_ids), [t1, t2])

    @ray.remote(num_gpus=2)
    def f2():
        t1 = time.monotonic()
        time.sleep(0.4)
        t2 = time.monotonic()
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert gpu_ids[0] in range(num_gpus_per_raylet)
        assert gpu_ids[1] in range(num_gpus_per_raylet)
        return (ray.worker.global_worker.plasma_client.store_socket_name,
                tuple(gpu_ids), [t1, t2])

    @ray.remote(num_gpus=1)
    class Actor1(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()
            assert len(self.gpu_ids) == 1
            assert self.gpu_ids[0] in range(num_gpus_per_raylet)

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

    def locations_to_intervals_for_many_tasks():
        # Launch a bunch of GPU tasks.
        locations_ids_and_intervals = ray.get(
            [f1.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)] +
            [f2.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)] +
            [f1.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)])

        locations_to_intervals = collections.defaultdict(lambda: [])
        for location, gpu_ids, interval in locations_ids_and_intervals:
            for gpu_id in gpu_ids:
                locations_to_intervals[(location, gpu_id)].append(interval)
        return locations_to_intervals

    # Run a bunch of GPU tasks.
    locations_to_intervals = locations_to_intervals_for_many_tasks()
    # Make sure that all GPUs were used.
    assert (len(locations_to_intervals) == num_nodes * num_gpus_per_raylet)
    # For each GPU, verify that the set of tasks that used this specific
    # GPU did not overlap in time.
    for locations in locations_to_intervals:
        check_intervals_non_overlapping(locations_to_intervals[locations])

    # Create an actor that uses a GPU.
    a = Actor1.remote()
    actor_location = ray.get(a.get_location_and_ids.remote())
    actor_location = (actor_location[0], actor_location[1][0])
    # This check makes sure that actor_location is formatted the same way
    # that the keys of locations_to_intervals are formatted.
    assert actor_location in locations_to_intervals

    # Run a bunch of GPU tasks.
    locations_to_intervals = locations_to_intervals_for_many_tasks()
    # Make sure that all but one of the GPUs were used.
    assert (len(locations_to_intervals) == num_nodes * num_gpus_per_raylet - 1)
    # For each GPU, verify that the set of tasks that used this specific
    # GPU did not overlap in time.
    for locations in locations_to_intervals:
        check_intervals_non_overlapping(locations_to_intervals[locations])
    # Make sure that the actor's GPU was not used.
    assert actor_location not in locations_to_intervals

    # Create several more actors that use GPUs.
    actors = [Actor1.remote() for _ in range(3)]
    actor_locations = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors])

    # Run a bunch of GPU tasks.
    locations_to_intervals = locations_to_intervals_for_many_tasks()
    # Make sure that all but 11 of the GPUs were used.
    assert (
        len(locations_to_intervals) == num_nodes * num_gpus_per_raylet - 1 - 3)
    # For each GPU, verify that the set of tasks that used this specific
    # GPU did not overlap in time.
    for locations in locations_to_intervals:
        check_intervals_non_overlapping(locations_to_intervals[locations])
    # Make sure that the GPUs were not used.
    assert actor_location not in locations_to_intervals
    for location in actor_locations:
        assert location not in locations_to_intervals

    # Create more actors to fill up all the GPUs.
    more_actors = [
        Actor1.remote() for _ in range(num_nodes * num_gpus_per_raylet - 1 - 3)
    ]
    # Wait for the actors to finish being created.
    ray.get([actor.get_location_and_ids.remote() for actor in more_actors])

    # Now if we run some GPU tasks, they should not be scheduled.
    results = [f1.remote() for _ in range(30)]
    ready_ids, remaining_ids = ray.wait(results, timeout=1.0)
    assert len(ready_ids) == 0


def test_actors_and_tasks_with_gpus_version_two(shutdown_only):
    # Create tasks and actors that both use GPUs and make sure that they
    # are given different GPUs
    ray.init(num_cpus=10, num_gpus=10)

    @ray.remote(num_gpus=1)
    def f():
        time.sleep(5)
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        return gpu_ids[0]

    @ray.remote(num_gpus=1)
    class Actor(object):
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()
            assert len(self.gpu_ids) == 1

        def get_gpu_id(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return self.gpu_ids[0]

    results = []
    actors = []
    for _ in range(5):
        results.append(f.remote())
        a = Actor.remote()
        results.append(a.get_gpu_id.remote())
        # Prevent the actor handle from going out of scope so that its GPU
        # resources don't get released.
        actors.append(a)

    gpu_ids = ray.get(results)
    assert set(gpu_ids) == set(range(10))


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
def test_actors_and_task_resource_bookkeeping(ray_start_regular):
    @ray.remote
    class Foo(object):
        def __init__(self):
            start = time.monotonic()
            time.sleep(0.1)
            end = time.monotonic()
            self.interval = (start, end)

        def get_interval(self):
            return self.interval

        def sleep(self):
            start = time.monotonic()
            time.sleep(0.01)
            end = time.monotonic()
            return start, end

    # First make sure that we do not have more actor methods running at a
    # time than we have CPUs.
    actors = [Foo.remote() for _ in range(4)]
    interval_ids = []
    interval_ids += [actor.get_interval.remote() for actor in actors]
    for _ in range(4):
        interval_ids += [actor.sleep.remote() for actor in actors]

    # Make sure that the intervals don't overlap.
    intervals = ray.get(interval_ids)
    intervals.sort(key=lambda x: x[0])
    for interval1, interval2 in zip(intervals[:-1], intervals[1:]):
        assert interval1[0] < interval1[1]
        assert interval1[1] < interval2[0]
        assert interval2[0] < interval2[1]


def test_blocking_actor_task(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1)

    @ray.remote(num_gpus=1)
    def f():
        return 1

    @ray.remote
    class Foo(object):
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure we can execute a blocking actor method even if there is
    # only one CPU.
    actor = Foo.remote()
    ray.get(actor.blocking_method.remote())

    @ray.remote(num_cpus=1)
    class CPUFoo(object):
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure that lifetime CPU resources are not released when actors
    # block.
    actor = CPUFoo.remote()
    x_id = actor.blocking_method.remote()
    ready_ids, remaining_ids = ray.wait([x_id], timeout=1.0)
    assert ready_ids == []
    assert remaining_ids == [x_id]

    @ray.remote(num_gpus=1)
    class GPUFoo(object):
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure that GPU resources are not released when actors block.
    actor = GPUFoo.remote()
    x_id = actor.blocking_method.remote()
    ready_ids, remaining_ids = ray.wait([x_id], timeout=1.0)
    assert ready_ids == []
    assert remaining_ids == [x_id]


def test_exception_raised_when_actor_node_dies(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.x = 0

        def local_plasma(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

        def inc(self):
            self.x += 1
            return self.x

    # Create an actor that is not on the local scheduler.
    actor = Counter.remote()
    while (ray.get(actor.local_plasma.remote()) !=
           remote_node.plasma_store_socket_name):
        actor = Counter.remote()

    # Kill the second node.
    cluster.remove_node(remote_node)

    # Submit some new actor tasks both before and after the node failure is
    # detected. Make sure that getting the result raises an exception.
    for _ in range(10):
        # Submit some new actor tasks.
        x_ids = [actor.inc.remote() for _ in range(5)]
        for x_id in x_ids:
            with pytest.raises(ray.exceptions.RayActorError):
                # There is some small chance that ray.get will actually
                # succeed (if the object is transferred before the raylet
                # dies).
                ray.get(x_id)


@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_actor_init_fails(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote(max_reconstructions=1)
    class Counter(object):
        def __init__(self):
            self.x = 0

        def inc(self):
            self.x += 1
            return self.x

    # Create many actors. It should take a while to finish initializing them.
    actors = [Counter.remote() for _ in range(100)]
    # Allow some time to forward the actor creation tasks to the other node.
    time.sleep(0.1)
    # Kill the second node.
    cluster.remove_node(remote_node)

    # Get all of the results
    results = ray.get([actor.inc.remote() for actor in actors])
    assert results == [1 for actor in actors]


def test_reconstruction_suppression(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    num_nodes = 10
    worker_nodes = [cluster.add_node() for _ in range(num_nodes)]

    @ray.remote(max_reconstructions=1)
    class Counter(object):
        def __init__(self):
            self.x = 0

        def inc(self):
            self.x += 1
            return self.x

    @ray.remote
    def inc(actor_handle):
        return ray.get(actor_handle.inc.remote())

    # Make sure all of the actors have started.
    actors = [Counter.remote() for _ in range(20)]
    ray.get([actor.inc.remote() for actor in actors])

    # Kill a node.
    cluster.remove_node(worker_nodes[0])

    # Submit several tasks per actor. These should be randomly scheduled to the
    # nodes, so that multiple nodes will detect and try to reconstruct the
    # actor that died, but only one should succeed.
    results = []
    for _ in range(10):
        results += [inc.remote(actor) for actor in actors]
    # Make sure that we can get the results from the reconstructed actor.
    results = ray.get(results)


def setup_counter_actor(test_checkpoint=False,
                        save_exception=False,
                        resume_exception=False):
    # Only set the checkpoint interval if we're testing with checkpointing.
    checkpoint_interval = -1
    if test_checkpoint:
        checkpoint_interval = 5

    @ray.remote(checkpoint_interval=checkpoint_interval)
    class Counter(object):
        _resume_exception = resume_exception

        def __init__(self, save_exception):
            self.x = 0
            self.num_inc_calls = 0
            self.save_exception = save_exception
            self.restored = False

        def local_plasma(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

        def inc(self, *xs):
            self.x += 1
            self.num_inc_calls += 1
            return self.x

        def get_num_inc_calls(self):
            return self.num_inc_calls

        def test_restore(self):
            # This method will only return True if __ray_restore__ has been
            # called.
            return self.restored

        def __ray_save__(self):
            if self.save_exception:
                raise Exception("Exception raised in checkpoint save")
            return self.x, self.save_exception

        def __ray_restore__(self, checkpoint):
            if self._resume_exception:
                raise Exception("Exception raised in checkpoint resume")
            self.x, self.save_exception = checkpoint
            self.num_inc_calls = 0
            self.restored = True

    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

    # Create an actor that is not on the local scheduler.
    actor = Counter.remote(save_exception)
    while ray.get(actor.local_plasma.remote()) == local_plasma:
        actor = Counter.remote(save_exception)

    args = [ray.put(0) for _ in range(100)]
    ids = [actor.inc.remote(*args[i:]) for i in range(100)]

    return actor, ids


@pytest.mark.skip("Fork/join consistency not yet implemented.")
def test_distributed_handle(ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    counter, ids = setup_counter_actor(test_checkpoint=False)

    @ray.remote
    def fork_many_incs(counter, num_incs):
        x = None
        for _ in range(num_incs):
            x = counter.inc.remote()
        # Only call ray.get() on the last task submitted.
        return ray.get(x)

    # Fork num_iters times.
    count = ray.get(ids[-1])
    num_incs = 100
    num_iters = 10
    forks = [
        fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)
    ]
    ray.wait(forks, num_returns=len(forks))
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding local scheduler to exit.
    cluster.list_all_nodes()[1].kill_plasma_store(wait=True)

    # Check that the actor did not restore from a checkpoint.
    assert not ray.get(counter.test_restore.remote())
    # Check that we can submit another call on the actor and get the
    # correct counter result.
    x = ray.get(counter.inc.remote())
    assert x == count + 1


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Hanging with new GCS API.")
def test_remote_checkpoint_distributed_handle(ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    counter, ids = setup_counter_actor(test_checkpoint=True)

    @ray.remote
    def fork_many_incs(counter, num_incs):
        x = None
        for _ in range(num_incs):
            x = counter.inc.remote()
        # Only call ray.get() on the last task submitted.
        return ray.get(x)

    # Fork num_iters times.
    count = ray.get(ids[-1])
    num_incs = 100
    num_iters = 10
    forks = [
        fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)
    ]
    ray.wait(forks, num_returns=len(forks))
    ray.wait([counter.__ray_checkpoint__.remote()])
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding local scheduler to exit.
    cluster.list_all_nodes()[1].kill_plasma_store(wait=True)

    # Check that the actor restored from a checkpoint.
    assert ray.get(counter.test_restore.remote())
    # Check that the number of inc calls since actor initialization is
    # exactly zero, since there could not have been another inc call since
    # the remote checkpoint.
    num_inc_calls = ray.get(counter.get_num_inc_calls.remote())
    assert num_inc_calls == 0
    # Check that we can submit another call on the actor and get the
    # correct counter result.
    x = ray.get(counter.inc.remote())
    assert x == count + 1


@pytest.mark.skip("Fork/join consistency not yet implemented.")
def test_checkpoint_distributed_handle(ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    counter, ids = setup_counter_actor(test_checkpoint=True)

    @ray.remote
    def fork_many_incs(counter, num_incs):
        x = None
        for _ in range(num_incs):
            x = counter.inc.remote()
        # Only call ray.get() on the last task submitted.
        return ray.get(x)

    # Fork num_iters times.
    count = ray.get(ids[-1])
    num_incs = 100
    num_iters = 10
    forks = [
        fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)
    ]
    ray.wait(forks, num_returns=len(forks))
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding local scheduler to exit.
    cluster.list_all_nodes()[1].kill_plasma_store(wait=True)

    # Check that the actor restored from a checkpoint.
    assert ray.get(counter.test_restore.remote())
    # Check that we can submit another call on the actor and get the
    # correct counter result.
    x = ray.get(counter.inc.remote())
    assert x == count + 1


def _test_nondeterministic_reconstruction(
        cluster, num_forks, num_items_per_fork, num_forks_to_wait):
    # Make a shared queue.
    @ray.remote
    class Queue(object):
        def __init__(self):
            self.queue = []

        def local_plasma(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

        def push(self, item):
            self.queue.append(item)

        def read(self):
            return self.queue

    # Schedule the shared queue onto the remote local scheduler.
    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name
    actor = Queue.remote()
    while ray.get(actor.local_plasma.remote()) == local_plasma:
        actor = Queue.remote()

    # A task that takes in the shared queue and a list of items to enqueue,
    # one by one.
    @ray.remote
    def enqueue(queue, items):
        done = None
        for item in items:
            done = queue.push.remote(item)
        # TODO(swang): Return the object ID returned by the last method
        # called on the shared queue, so that the caller of enqueue can
        # wait for all of the queue methods to complete. This can be
        # removed once join consistency is implemented.
        return [done]

    # Call the enqueue task num_forks times, each with num_items_per_fork
    # unique objects to push onto the shared queue.
    enqueue_tasks = []
    for fork in range(num_forks):
        enqueue_tasks.append(
            enqueue.remote(actor,
                           [(fork, i) for i in range(num_items_per_fork)]))
    # Wait for the forks to complete their tasks.
    enqueue_tasks = ray.get(enqueue_tasks)
    enqueue_tasks = [fork_ids[0] for fork_ids in enqueue_tasks]
    ray.wait(enqueue_tasks, num_returns=num_forks_to_wait)

    # Read the queue to get the initial order of execution.
    queue = ray.get(actor.read.remote())

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding local scheduler to exit.
    cluster.list_all_nodes()[1].kill_plasma_store(wait=True)

    # Read the queue again and check for deterministic reconstruction.
    ray.get(enqueue_tasks)
    reconstructed_queue = ray.get(actor.read.remote())
    # Make sure the final queue has all items from all forks.
    assert len(reconstructed_queue) == num_forks * num_items_per_fork
    # Make sure that the prefix of the final queue matches the queue from
    # the initial execution.
    assert queue == reconstructed_queue[:len(queue)]


@pytest.mark.skip("This test does not work yet.")
@pytest.mark.skipif(
    os.environ.get("RAY_USE_NEW_GCS") == "on",
    reason="Currently doesn't work with the new GCS.")
def test_nondeterministic_reconstruction(ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    _test_nondeterministic_reconstruction(cluster, 10, 100, 10)


@pytest.mark.skip("Nondeterministic reconstruction currently not supported "
                  "when there are concurrent forks that didn't finish "
                  "initial execution.")
def test_nondeterministic_reconstruction_concurrent_forks(
        ray_start_cluster_2_nodes):
    cluster = ray_start_cluster_2_nodes
    _test_nondeterministic_reconstruction(cluster, 10, 100, 1)


@pytest.fixture
def setup_queue_actor():
    ray.init(num_cpus=1)

    @ray.remote
    class Queue(object):
        def __init__(self):
            self.queue = []

        def enqueue(self, key, item):
            self.queue.append((key, item))

        def read(self):
            return self.queue

    yield Queue.remote()

    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_fork(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(queue, key, item):
        return ray.get(queue.enqueue.remote(key, item))

    # Fork num_iters times.
    num_iters = 100
    ray.get([fork.remote(queue, i, 0) for i in range(num_iters)])
    items = ray.get(queue.read.remote())
    for i in range(num_iters):
        filtered_items = [item[1] for item in items if item[0] == i]
        assert filtered_items == list(range(1))


def test_fork_consistency(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(queue, key, num_items):
        x = None
        for item in range(num_items):
            x = queue.enqueue.remote(key, item)
        return ray.get(x)

    # Fork num_iters times.
    num_forks = 10
    num_items_per_fork = 100

    # Submit some tasks on new actor handles.
    forks = [
        fork.remote(queue, i, num_items_per_fork) for i in range(num_forks)
    ]
    # Submit some more tasks on the original actor handle.
    for item in range(num_items_per_fork):
        local_fork = queue.enqueue.remote(num_forks, item)
    forks.append(local_fork)
    # Wait for tasks from all handles to complete.
    ray.get(forks)
    # Check that all tasks from all handles have completed.
    items = ray.get(queue.read.remote())
    for i in range(num_forks + 1):
        filtered_items = [item[1] for item in items if item[0] == i]
        assert filtered_items == list(range(num_items_per_fork))


def test_pickled_handle_consistency(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(pickled_queue, key, num_items):
        queue = ray.worker.pickle.loads(pickled_queue)
        x = None
        for item in range(num_items):
            x = queue.enqueue.remote(key, item)
        return ray.get(x)

    # Fork num_iters times.
    num_forks = 10
    num_items_per_fork = 100

    # Submit some tasks on the pickled actor handle.
    new_queue = ray.worker.pickle.dumps(queue)
    forks = [
        fork.remote(new_queue, i, num_items_per_fork) for i in range(num_forks)
    ]
    # Submit some more tasks on the original actor handle.
    for item in range(num_items_per_fork):
        local_fork = queue.enqueue.remote(num_forks, item)
    forks.append(local_fork)
    # Wait for tasks from all handles to complete.
    ray.get(forks)
    # Check that all tasks from all handles have completed.
    items = ray.get(queue.read.remote())
    for i in range(num_forks + 1):
        filtered_items = [item[1] for item in items if item[0] == i]
        assert filtered_items == list(range(num_items_per_fork))


def test_nested_fork(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(queue, key, num_items):
        x = None
        for item in range(num_items):
            x = queue.enqueue.remote(key, item)
        return ray.get(x)

    @ray.remote
    def nested_fork(queue, key, num_items):
        # Pass the actor into a nested task.
        ray.get(fork.remote(queue, key + 1, num_items))
        x = None
        for item in range(num_items):
            x = queue.enqueue.remote(key, item)
        return ray.get(x)

    # Fork num_iters times.
    num_forks = 10
    num_items_per_fork = 100

    # Submit some tasks on new actor handles.
    forks = [
        nested_fork.remote(queue, i, num_items_per_fork)
        for i in range(0, num_forks, 2)
    ]
    ray.get(forks)
    # Check that all tasks from all handles have completed.
    items = ray.get(queue.read.remote())
    for i in range(num_forks):
        filtered_items = [item[1] for item in items if item[0] == i]
        assert filtered_items == list(range(num_items_per_fork))


@pytest.mark.skip("Garbage collection for distributed actor handles not "
                  "implemented.")
def test_garbage_collection(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(queue):
        for i in range(10):
            x = queue.enqueue.remote(0, i)
            time.sleep(0.1)
        return ray.get(x)

    x = fork.remote(queue)
    ray.get(queue.read.remote())
    del queue

    print(ray.get(x))


def test_calling_put_on_actor_handle(ray_start_regular):
    @ray.remote
    class Counter(object):
        def __init__(self):
            self.x = 0

        def inc(self):
            self.x += 1
            return self.x

    @ray.remote
    def f():
        return Counter.remote()

    @ray.remote
    def g():
        return [Counter.remote()]

    # Currently, calling ray.put on an actor handle is allowed, but is
    # there a good use case?
    counter = Counter.remote()
    counter_id = ray.put(counter)
    new_counter = ray.get(counter_id)
    assert ray.get(new_counter.inc.remote()) == 1
    assert ray.get(counter.inc.remote()) == 2
    assert ray.get(new_counter.inc.remote()) == 3

    with pytest.raises(Exception):
        ray.get(f.remote())

    # The below test works, but do we want to disallow this usage?
    ray.get(g.remote())


def test_pickling_actor_handle(ray_start_regular):
    @ray.remote
    class Foo(object):
        def method(self):
            pass

    f = Foo.remote()
    new_f = ray.worker.pickle.loads(ray.worker.pickle.dumps(f))
    # Verify that we can call a method on the unpickled handle. TODO(rkn):
    # we should also test this from a different driver.
    ray.get(new_f.method.remote())


def test_pickled_actor_handle_call_in_method_twice(ray_start_regular):
    @ray.remote
    class Actor1(object):
        def f(self):
            return 1

    @ray.remote
    class Actor2(object):
        def __init__(self, constructor):
            self.actor = constructor()

        def step(self):
            ray.get(self.actor.f.remote())

    a = Actor1.remote()

    b = Actor2.remote(lambda: a)

    ray.get(b.step.remote())
    ray.get(b.step.remote())


def test_register_and_get_named_actors(ray_start_regular):
    # TODO(heyucongtom): We should test this from another driver.

    @ray.remote
    class Foo(object):
        def __init__(self):
            self.x = 0

        def method(self):
            self.x += 1
            return self.x

    f1 = Foo.remote()
    # Test saving f.
    ray.experimental.register_actor("f1", f1)
    # Test getting f.
    f2 = ray.experimental.get_actor("f1")
    assert f1._actor_id == f2._actor_id

    # Test same name register shall raise error.
    with pytest.raises(ValueError):
        ray.experimental.register_actor("f1", f2)

    # Test register with wrong object type.
    with pytest.raises(TypeError):
        ray.experimental.register_actor("f3", 1)

    # Test getting a nonexistent actor.
    with pytest.raises(ValueError):
        ray.experimental.get_actor("nonexistent")

    # Test method
    assert ray.get(f1.method.remote()) == 1
    assert ray.get(f2.method.remote()) == 2
    assert ray.get(f1.method.remote()) == 3
    assert ray.get(f2.method.remote()) == 4


@pytest.mark.skipif(
    sys.version_info < (3, 0),
    reason="This test is currently failing on Python 2.7.")
def test_lifetime_and_transient_resources(ray_start_regular):
    # This actor acquires resources only when running methods.
    @ray.remote
    class Actor1(object):
        def method(self):
            pass

    # This actor acquires resources for its lifetime.
    @ray.remote(num_cpus=1)
    class Actor2(object):
        def method(self):
            pass

    actor1s = [Actor1.remote() for _ in range(10)]
    ray.get([a.method.remote() for a in actor1s])

    actor2s = [Actor2.remote() for _ in range(2)]
    results = [a.method.remote() for a in actor2s]
    ready_ids, remaining_ids = ray.wait(
        results, num_returns=len(results), timeout=1.0)
    assert len(ready_ids) == 1


def test_custom_label_placement(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, resources={"CustomResource1": 2})
    cluster.add_node(num_cpus=2, resources={"CustomResource2": 2})
    ray.init(redis_address=cluster.redis_address)

    @ray.remote(resources={"CustomResource1": 1})
    class ResourceActor1(object):
        def get_location(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    @ray.remote(resources={"CustomResource2": 1})
    class ResourceActor2(object):
        def get_location(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

    # Create some actors.
    actors1 = [ResourceActor1.remote() for _ in range(2)]
    actors2 = [ResourceActor2.remote() for _ in range(2)]
    locations1 = ray.get([a.get_location.remote() for a in actors1])
    locations2 = ray.get([a.get_location.remote() for a in actors2])
    for location in locations1:
        assert location == local_plasma
    for location in locations2:
        assert location != local_plasma


def test_creating_more_actors_than_resources(shutdown_only):
    ray.init(num_cpus=10, num_gpus=2, resources={"CustomResource1": 1})

    @ray.remote(num_gpus=1)
    class ResourceActor1(object):
        def method(self):
            return ray.get_gpu_ids()[0]

    @ray.remote(resources={"CustomResource1": 1})
    class ResourceActor2(object):
        def method(self):
            pass

    # Make sure the first two actors get created and the third one does
    # not.
    actor1 = ResourceActor1.remote()
    result1 = actor1.method.remote()
    ray.wait([result1])
    actor2 = ResourceActor1.remote()
    result2 = actor2.method.remote()
    ray.wait([result2])
    actor3 = ResourceActor1.remote()
    result3 = actor3.method.remote()
    ready_ids, _ = ray.wait([result3], timeout=0.2)
    assert len(ready_ids) == 0

    # By deleting actor1, we free up resources to create actor3.
    del actor1

    results = ray.get([result1, result2, result3])
    assert results[0] == results[2]
    assert set(results) == {0, 1}

    # Make sure that when one actor goes out of scope a new actor is
    # created because some resources have been freed up.
    results = []
    for _ in range(3):
        actor = ResourceActor2.remote()
        object_id = actor.method.remote()
        results.append(object_id)
        # Wait for the task to execute. We do this because otherwise it may
        # be possible for the __ray_terminate__ task to execute before the
        # method.
        ray.wait([object_id])

    ray.get(results)


@pytest.mark.parametrize(
    "ray_start_object_store_memory", [10**8], indirect=True)
def test_actor_eviction(ray_start_object_store_memory):
    object_store_memory = ray_start_object_store_memory

    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def create_object(self, size):
            return np.random.rand(size)

    a = Actor.remote()
    # Submit enough methods on the actor so that they exceed the size of the
    # object store.
    objects = []
    num_objects = 20
    for _ in range(num_objects):
        obj = a.create_object.remote(object_store_memory // num_objects)
        objects.append(obj)
        # Get each object once to make sure each object gets created.
        ray.get(obj)

    # Get each object again. At this point, the earlier objects should have
    # been evicted.
    num_evicted, num_success = 0, 0
    for obj in objects:
        try:
            ray.get(obj)
            num_success += 1
        except ray.exceptions.UnreconstructableError:
            num_evicted += 1
    # Some objects should have been evicted, and some should still be in the
    # object store.
    assert num_evicted > 0
    assert num_success > 0


def test_actor_reconstruction(ray_start_regular):
    """Test actor reconstruction when actor process is killed."""

    @ray.remote(max_reconstructions=1)
    class ReconstructableActor(object):
        """An actor that will be reconstructed at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, delay=0):
            time.sleep(delay)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = ReconstructableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    # Call increase 3 times
    for _ in range(3):
        ray.get(actor.increase.remote())
    # Call increase again with some delay.
    result = actor.increase.remote(delay=0.5)
    # Sleep some time to wait for the above task to start execution.
    time.sleep(0.2)
    # Kill actor process, while the above task is still being executed.
    os.kill(pid, signal.SIGKILL)
    # Check that the above task didn't fail and the actor is reconstructed.
    assert ray.get(result) == 4
    # Check that we can still call the actor.
    assert ray.get(actor.increase.remote()) == 5
    # kill actor process one more time.
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    # The actor has exceeded max reconstructions, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())

    # Create another actor.
    actor = ReconstructableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be reconstructed.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())


def test_actor_reconstruction_on_node_failure(ray_start_cluster_head):
    """Test actor reconstruction when node dies unexpectedly."""
    cluster = ray_start_cluster_head
    max_reconstructions = 3
    # Add a few nodes to the cluster.
    # Use custom resource to make sure the actor is only created on worker
    # nodes, not on the head node.
    for _ in range(max_reconstructions + 2):
        cluster.add_node(
            resources={"a": 1},
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 200,
                "num_heartbeats_timeout": 10,
            }),
        )

    def kill_node(object_store_socket):
        node_to_remove = None
        for node in cluster.worker_nodes:
            if object_store_socket == node.plasma_store_socket_name:
                node_to_remove = node
        cluster.remove_node(node_to_remove)

    @ray.remote(max_reconstructions=max_reconstructions, resources={"a": 1})
    class MyActor(object):
        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

        def get_object_store_socket(self):
            return ray.worker.global_worker.plasma_client.store_socket_name

    actor = MyActor.remote()
    # Call increase 3 times.
    for _ in range(3):
        ray.get(actor.increase.remote())

    for i in range(max_reconstructions):
        object_store_socket = ray.get(actor.get_object_store_socket.remote())
        # Kill actor's node and the actor should be reconstructed
        # on a different node.
        kill_node(object_store_socket)
        # Call increase again.
        # Check that the actor is reconstructed and value is correct.
        assert ray.get(actor.increase.remote()) == 4 + i
        # Check that the actor is now on a different node.
        assert object_store_socket != ray.get(
            actor.get_object_store_socket.remote())

    # kill the node again.
    object_store_socket = ray.get(actor.get_object_store_socket.remote())
    kill_node(object_store_socket)
    # The actor has exceeded max reconstructions, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())


# NOTE(hchen): we set initial_reconstruction_timeout_milliseconds to 1s for
# this test. Because if this value is too small, suprious task reconstruction
# may happen and cause the test fauilure. If the value is too large, this test
# could be very slow. We can remove this once we support dynamic timeout.
@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_internal_config_map(
            initial_reconstruction_timeout_milliseconds=1000)
    ],
    indirect=True)
def test_multiple_actor_reconstruction(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    # This test can be made more stressful by increasing the numbers below.
    # The total number of actors created will be
    # num_actors_at_a_time * num_nodes.
    num_nodes = 5
    num_actors_at_a_time = 3
    num_function_calls_at_a_time = 10

    worker_nodes = [
        cluster.add_node(
            num_cpus=3,
            _internal_config=json.dumps({
                "initial_reconstruction_timeout_milliseconds": 200,
                "num_heartbeats_timeout": 10,
            })) for _ in range(num_nodes)
    ]

    @ray.remote(max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION)
    class SlowCounter(object):
        def __init__(self):
            self.x = 0

        def inc(self, duration):
            time.sleep(duration)
            self.x += 1
            return self.x

    # Create some initial actors.
    actors = [SlowCounter.remote() for _ in range(num_actors_at_a_time)]

    # Wait for the actors to start up.
    time.sleep(1)

    # This is a mapping from actor handles to object IDs returned by
    # methods on that actor.
    result_ids = collections.defaultdict(lambda: [])

    # In a loop we are going to create some actors, run some methods, kill
    # a local scheduler, and run some more methods.
    for node in worker_nodes:
        # Create some actors.
        actors.extend(
            [SlowCounter.remote() for _ in range(num_actors_at_a_time)])
        # Run some methods.
        for j in range(len(actors)):
            actor = actors[j]
            for _ in range(num_function_calls_at_a_time):
                result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))
        # Kill a node.
        cluster.remove_node(node)

        # Run some more methods.
        for j in range(len(actors)):
            actor = actors[j]
            for _ in range(num_function_calls_at_a_time):
                result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))

    # Get the results and check that they have the correct values.
    for _, result_id_list in result_ids.items():
        results = list(range(1, len(result_id_list) + 1))
        assert ray.get(result_id_list) == results


def kill_actor(actor):
    """A helper function that kills an actor process."""
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)


def test_checkpointing(ray_start_regular, ray_checkpointable_actor_cls):
    """Test actor checkpointing and restoring from a checkpoint."""
    actor = ray.remote(
        max_reconstructions=2)(ray_checkpointable_actor_cls).remote()
    # Call increase 3 times.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again and check that reconstruction still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


def test_remote_checkpointing(ray_start_regular, ray_checkpointable_actor_cls):
    """Test checkpointing of a remote actor through method invocation."""

    # Define a class that exposes a method to save checkpoints.
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def __init__(self):
            super(RemoteCheckpointableActor, self).__init__()
            self._should_checkpoint = False

        def checkpoint(self):
            self._should_checkpoint = True

        def should_checkpoint(self, checkpoint_context):
            should_checkpoint = self._should_checkpoint
            self._should_checkpoint = False
            return should_checkpoint

    cls = ray.remote(max_reconstructions=2)(RemoteCheckpointableActor)
    actor = cls.remote()
    # Call increase 3 times.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Call a checkpoint task.
    actor.checkpoint.remote()
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again and check that reconstruction still works after the
    # actor resuming from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


def test_checkpointing_on_node_failure(ray_start_cluster_2_nodes,
                                       ray_checkpointable_actor_cls):
    """Test actor checkpointing on a remote node."""
    # Place the actor on the remote node.
    cluster = ray_start_cluster_2_nodes
    remote_node = [node for node in cluster.worker_nodes]
    actor_cls = ray.remote(max_reconstructions=1)(ray_checkpointable_actor_cls)
    actor = actor_cls.remote()
    while (ray.get(actor.local_plasma.remote()) !=
           remote_node[0].plasma_store_socket_name):
        actor = actor_cls.remote()

    # Call increase several times.
    expected = 0
    for _ in range(6):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    cluster.remove_node(remote_node[0])
    # Assert that the actor was resumed from a checkpoint and its value is
    # still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is True


def test_checkpointing_save_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to complete."""

    @ray.remote(max_reconstructions=2)
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def save_checkpoint(self, actor_id, checkpoint_context):
            raise Exception("Error during save")

    actor = RemoteCheckpointableActor.remote()
    # Call increase 3 times.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor still wasn't resumed from a checkpoint and its
    # value is still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again, and check that reconstruction still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that checkpointing errors were pushed to the driver.
    errors = ray.error_info()
    assert len(errors) > 0
    for error in errors:
        # An error for the actor process dying may also get pushed.
        assert (error["type"] == ray_constants.CHECKPOINT_PUSH_ERROR
                or error["type"] == ray_constants.WORKER_DIED_PUSH_ERROR)


def test_checkpointing_load_exception(ray_start_regular,
                                      ray_checkpointable_actor_cls):
    """Test actor can still be recovered if checkpoints fail to load."""

    @ray.remote(max_reconstructions=2)
    class RemoteCheckpointableActor(ray_checkpointable_actor_cls):
        def load_checkpoint(self, actor_id, checkpoints):
            raise Exception("Error during load")

    actor = RemoteCheckpointableActor.remote()
    # Call increase 3 times.
    expected = 0
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Assert that the actor wasn't resumed from a checkpoint.
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False
    # Kill actor process.
    kill_actor(actor)
    # Assert that the actor still wasn't resumed from a checkpoint and its
    # value is still correct.
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Submit some more tasks. These should get replayed since they happen after
    # the checkpoint.
    for _ in range(3):
        ray.get(actor.increase.remote())
        expected += 1
    # Kill actor again, and check that reconstruction still works and the actor
    # wasn't resumed from a checkpoint.
    kill_actor(actor)
    assert ray.get(actor.get.remote()) == expected
    assert ray.get(actor.was_resumed_from_checkpoint.remote()) is False

    # Check that checkpointing errors were pushed to the driver.
    errors = ray.error_info()
    assert len(errors) > 0
    for error in errors:
        # An error for the actor process dying may also get pushed.
        assert (error["type"] == ray_constants.CHECKPOINT_PUSH_ERROR
                or error["type"] == ray_constants.WORKER_DIED_PUSH_ERROR)


@pytest.mark.parametrize(
    "ray_start_regular",
    # This overwrite currently isn't effective,
    # see https://github.com/ray-project/ray/issues/3926.
    [generate_internal_config_map(num_actor_checkpoints_to_keep=20)],
    indirect=True,
)
def test_deleting_actor_checkpoint(ray_start_regular):
    """Test deleting old actor checkpoints."""

    @ray.remote
    class CheckpointableActor(ray.actor.Checkpointable):
        def __init__(self):
            self.checkpoint_ids = []

        def get_checkpoint_ids(self):
            return self.checkpoint_ids

        def should_checkpoint(self, checkpoint_context):
            # Save checkpoints after every task
            return True

        def save_checkpoint(self, actor_id, checkpoint_id):
            self.checkpoint_ids.append(checkpoint_id)
            pass

        def load_checkpoint(self, actor_id, available_checkpoints):
            pass

        def checkpoint_expired(self, actor_id, checkpoint_id):
            assert checkpoint_id == self.checkpoint_ids[0]
            del self.checkpoint_ids[0]

    actor = CheckpointableActor.remote()
    for i in range(19):
        assert len(ray.get(actor.get_checkpoint_ids.remote())) == i + 1
    for _ in range(20):
        assert len(ray.get(actor.get_checkpoint_ids.remote())) == 20


def test_bad_checkpointable_actor_class():
    """Test error raised if an actor class doesn't implement all abstract
    methods in the Checkpointable interface."""

    with pytest.raises(TypeError):

        @ray.remote
        class BadCheckpointableActor(ray.actor.Checkpointable):
            def should_checkpoint(self, checkpoint_context):
                return True


def test_init_exception_in_checkpointable_actor(ray_start_regular,
                                                ray_checkpointable_actor_cls):
    # This test is similar to test_failure.py::test_failed_actor_init.
    # This test is used to guarantee that checkpointable actor does not
    # break the same logic.
    error_message1 = "actor constructor failed"
    error_message2 = "actor method failed"

    @ray.remote
    class CheckpointableFailedActor(ray_checkpointable_actor_cls):
        def __init__(self):
            raise Exception(error_message1)

        def fail_method(self):
            raise Exception(error_message2)

        def should_checkpoint(self, checkpoint_context):
            return True

    a = CheckpointableFailedActor.remote()

    # Make sure that we get errors from a failed constructor.
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 1, timeout=2)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 1
    assert error_message1 in errors[0]["message"]

    # Make sure that we get errors from a failed method.
    a.fail_method.remote()
    wait_for_errors(ray_constants.TASK_PUSH_ERROR, 2, timeout=2)
    errors = relevant_errors(ray_constants.TASK_PUSH_ERROR)
    assert len(errors) == 2
    assert error_message1 in errors[1]["message"]
