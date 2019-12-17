from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import numpy as np
import os
import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys
import time

import ray
import ray.test_utils
import ray.cluster_utils
from ray import ray_constants
from ray.test_utils import run_string_as_driver

RAY_FORCE_DIRECT = ray_constants.direct_call_enabled()


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


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
def test_actor_class_attributes(ray_start_regular):
    class Grandparent(object):
        GRANDPARENT = 2

    class Parent1(Grandparent):
        PARENT1 = 6

    class Parent2(object):
        PARENT2 = 7

    @ray.remote
    class TestActor(Parent1, Parent2):
        X = 3

        @classmethod
        def f(cls):
            assert TestActor.GRANDPARENT == 2
            assert TestActor.PARENT1 == 6
            assert TestActor.PARENT2 == 7
            assert TestActor.X == 3
            return 4

        def g(self):
            assert TestActor.GRANDPARENT == 2
            assert TestActor.PARENT1 == 6
            assert TestActor.PARENT2 == 7
            assert TestActor.f() == 4
            return TestActor.X

    t = TestActor.remote()
    assert ray.get(t.g.remote()) == 3


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

    assert f1._actor_id != f2._actor_id


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
    assert b"test_actor" in actor_class_info[b"module"]


def test_actor_inheritance(ray_start_regular):
    class NonActorBase(object):
        def __init__(self):
            pass

    # Test that an actor class can inherit from a non-actor class.
    @ray.remote
    class ActorBase(NonActorBase):
        def __init__(self):
            pass

    # Test that you can't instantiate an actor class directly.
    with pytest.raises(
            Exception, match="Actors cannot be instantiated directly."):
        ActorBase()

    # Test that you can't inherit from an actor class.
    with pytest.raises(
            TypeError,
            match="Inheriting from actor classes is not "
            "currently supported."):

        class Derived(ActorBase):
            def __init__(self):
                pass


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
    ray.test_utils.wait_for_pid_to_exit(pid)

    actors = [Actor.remote() for _ in range(10)]
    pids = ray.get([a.getpid.remote() for a in actors])
    a = None
    actors = None
    [ray.test_utils.wait_for_pid_to_exit(pid) for pid in pids]


@pytest.mark.skipif(
    sys.version_info < (3, 0), reason="This test requires Python 3.")
def test_actor_method_deletion(ray_start_regular):
    @ray.remote
    class Actor(object):
        def method(self):
            return 1

    # TODO(ekl) this doesn't work in Python 2 after the weak ref method change.
    # Make sure that if we create an actor and call a method on it
    # immediately, the actor doesn't get killed before the method is
    # called.
    assert ray.get(Actor.remote().method.remote()) == 1


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

    num_actors = 5
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


def test_actors_on_nodes_with_no_cpus(ray_start_no_cpu):
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
    ray.init(address=cluster.address)

    @ray.remote
    class Actor1(object):
        def __init__(self):
            pass

        def get_location(self):
            return ray.worker.global_worker.node.unique_id

    # Create a bunch of actors.
    num_actors = 30
    num_attempts = 20
    minimum_count = 5

    # Make sure that actors are spread between the raylets.
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


def test_actor_lifetime_load_balancing(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    num_nodes = 3
    for i in range(num_nodes):
        cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=1)
    class Actor(object):
        def __init__(self):
            pass

        def ping(self):
            return

    actors = [Actor.remote() for _ in range(num_nodes)]
    ray.get([actor.ping.remote() for actor in actors])


def test_exception_raised_when_actor_node_dies(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote(max_reconstructions=0)
    class Counter(object):
        def __init__(self):
            self.x = 0

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

        def inc(self):
            self.x += 1
            return self.x

    # Create an actor that is not on the raylet.
    actor = Counter.remote()
    while (ray.get(actor.node_id.remote()) != remote_node.unique_id):
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
@pytest.mark.skipif(RAY_FORCE_DIRECT, reason="no ft yet")
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
    actors = [Counter.remote() for _ in range(15)]
    # Allow some time to forward the actor creation tasks to the other node.
    time.sleep(0.1)
    # Kill the second node.
    cluster.remove_node(remote_node)

    # Get all of the results.
    results = ray.get([actor.inc.remote() for actor in actors])
    assert results == [1 for actor in actors]


@pytest.mark.skipif(RAY_FORCE_DIRECT, reason="no ft yet")
def test_reconstruction_suppression(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    num_nodes = 5
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
    actors = [Counter.remote() for _ in range(10)]
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

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

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

    node_id = ray.worker.global_worker.node.unique_id

    # Create an actor that is not on the raylet.
    actor = Counter.remote(save_exception)
    while ray.get(actor.node_id.remote()) == node_id:
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
    # trigger the corresponding raylet to exit.
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
    # trigger the corresponding raylet to exit.
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
    # trigger the corresponding raylet to exit.
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

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

        def push(self, item):
            self.queue.append(item)

        def read(self):
            return self.queue

    # Schedule the shared queue onto the remote raylet.
    node_id = ray.worker.global_worker.node.unique_id
    actor = Queue.remote()
    while ray.get(actor.node_id.remote()) == node_id:
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
    # trigger the corresponding raylet to exit.
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
    ray.init(num_cpus=1, object_store_memory=int(150 * 1024 * 1024))

    @ray.remote
    class Queue(object):
        def __init__(self):
            self.queue = []

        def enqueue(self, key, item):
            self.queue.append((key, item))

        def read(self):
            return self.queue

    queue = Queue.remote()
    # Make sure queue actor is initialized.
    ray.get(queue.read.remote())

    yield queue

    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_fork(setup_queue_actor):
    queue = setup_queue_actor

    @ray.remote
    def fork(queue, key, item):
        # ray.get here could be blocked and cause ray to start
        # a lot of python workers.
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
    num_forks = 5
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


def test_detached_actor(ray_start_regular):
    @ray.remote
    class DetachedActor(object):
        def ping(self):
            return "pong"

    with pytest.raises(Exception, match="Detached actors must be named"):
        DetachedActor._remote(detached=True)

    with pytest.raises(ValueError, match="Please use a different name"):
        _ = DetachedActor._remote(name="d_actor")
        DetachedActor._remote(name="d_actor")

    redis_address = ray_start_regular["redis_address"]

    actor_name = "DetachedActor"
    driver_script = """
import ray
ray.init(address="{}")

@ray.remote
class DetachedActor(object):
    def ping(self):
        return "pong"

actor = DetachedActor._remote(name="{}", detached=True)
ray.get(actor.ping.remote())
""".format(redis_address, actor_name)

    run_string_as_driver(driver_script)
    detached_actor = ray.experimental.get_actor(actor_name)
    assert ray.get(detached_actor.ping.remote()) == "pong"


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
