import random
import pytest
import numpy as np
import os
import pickle
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.test_utils
import ray.cluster_utils


def test_actor_exit_from_task(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            print("Actor created")

        def f(self):
            return 0

    @ray.remote
    def f():
        a = Actor.remote()
        x_id = a.f.remote()
        return [x_id]

    x_id = ray.get(f.remote())[0]
    print(ray.get(x_id))  # This should not hang.


def test_actor_init_error_propagated(ray_start_regular):
    @ray.remote
    class Actor:
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


def test_keyword_args(ray_start_regular):
    @ray.remote
    class Actor:
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


def test_actor_method_metadata_cache(ray_start_regular):
    class Actor(object):
        pass

    # The cache of ActorClassMethodMetadata.
    cache = ray.actor.ActorClassMethodMetadata._cache
    cache.clear()

    # Check cache hit during ActorHandle deserialization.
    A1 = ray.remote(Actor)
    a = A1.remote()
    assert len(cache) == 1
    cached_data_id = [id(x) for x in list(cache.items())[0]]
    for x in range(10):
        a = pickle.loads(pickle.dumps(a))
    assert len(ray.actor.ActorClassMethodMetadata._cache) == 1
    assert [id(x) for x in list(cache.items())[0]] == cached_data_id

    # Check cache hit when @ray.remote
    A2 = ray.remote(Actor)
    assert id(A1.__ray_metadata__) != id(A2.__ray_metadata__)
    assert id(A1.__ray_metadata__.method_meta) == id(
        A2.__ray_metadata__.method_meta)


def test_actor_name_conflict(ray_start_regular):
    @ray.remote
    class A(object):
        def foo(self):
            return 100000

    a = A.remote()
    r = a.foo.remote()

    results = [r]
    for x in range(10):

        @ray.remote
        class A(object):
            def foo(self):
                return x

        a = A.remote()
        r = a.foo.remote()
        results.append(r)

    assert ray.get(results) == [100000, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_variable_number_of_args(ray_start_regular):
    @ray.remote
    class Actor:
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
    class Actor:
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
    class Actor:
        def __init__(self):
            pass

        def get_values(self):
            pass

    actor = Actor.remote()
    assert ray.get(actor.get_values.remote()) is None


def test_no_constructor(ray_start_regular):
    @ray.remote
    class Actor:
        def get_values(self):
            pass

    actor = Actor.remote()
    assert ray.get(actor.get_values.remote()) is None


def test_custom_classes(ray_start_regular):
    class Foo:
        def __init__(self, x):
            self.x = x

    @ray.remote
    class Actor:
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


def test_actor_class_attributes(ray_start_regular):
    class Grandparent:
        GRANDPARENT = 2

    class Parent1(Grandparent):
        PARENT1 = 6

    class Parent2:
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


def test_actor_static_attributes(ray_start_regular):
    class Grandparent:
        GRANDPARENT = 2

        @staticmethod
        def grandparent_static():
            assert Grandparent.GRANDPARENT == 2
            return 1

    class Parent1(Grandparent):
        PARENT1 = 6

        @staticmethod
        def parent1_static():
            assert Parent1.PARENT1 == 6
            return 2

        def parent1(self):
            assert Parent1.PARENT1 == 6

    class Parent2:
        PARENT2 = 7

        def parent2(self):
            assert Parent2.PARENT2 == 7

    @ray.remote
    class TestActor(Parent1, Parent2):
        X = 3

        @staticmethod
        def f():
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
    class Foo:
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
        class Actor:
            def __init__(self):
                pass

    # This is an invalid way of using the actor decorator.
    with pytest.raises(Exception):

        @ray.remote(invalid_kwarg=0)  # noqa: F811
        class Actor:
            def __init__(self):
                pass

    # This is an invalid way of using the actor decorator.
    with pytest.raises(Exception):

        @ray.remote(num_cpus=0, invalid_kwarg=0)  # noqa: F811
        class Actor:
            def __init__(self):
                pass

    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1)  # noqa: F811
    class Actor:
        def __init__(self):
            pass

    # This is a valid way of using the decorator.
    @ray.remote(num_gpus=1)  # noqa: F811
    class Actor:
        def __init__(self):
            pass

    # This is a valid way of using the decorator.
    @ray.remote(num_cpus=1, num_gpus=1)  # noqa: F811
    class Actor:
        def __init__(self):
            pass


def test_random_id_generation(ray_start_regular):
    @ray.remote
    class Foo:
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
    class Foo:
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
    class NonActorBase:
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
    class Foo:
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
    class Test:
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
    class Actor:
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


def test_actor_method_deletion(ray_start_regular):
    @ray.remote
    class Actor:
        def method(self):
            return 1

    # Make sure that if we create an actor and call a method on it
    # immediately, the actor doesn't get killed before the method is
    # called.
    assert ray.get(Actor.remote().method.remote()) == 1


def test_distributed_actor_handle_deletion(ray_start_regular):
    @ray.remote
    class Actor:
        def method(self):
            return 1

        def getpid(self):
            return os.getpid()

    @ray.remote
    def f(actor, signal):
        ray.get(signal.wait.remote())
        return ray.get(actor.method.remote())

    signal = ray.test_utils.SignalActor.remote()
    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    # Pass the handle to another task that cannot run yet.
    x_id = f.remote(a, signal)
    # Delete the original handle. The actor should not get killed yet.
    del a

    # Once the task finishes, the actor process should get killed.
    ray.get(signal.send.remote())
    assert ray.get(x_id) == 1
    ray.test_utils.wait_for_pid_to_exit(pid)


def test_multiple_actors(ray_start_regular):
    @ray.remote
    class Counter:
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
    class Actor:
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
    class Actor1:
        def __init__(self, x):
            self.x = x

        def new_actor(self, z):
            @ray.remote
            class Actor2:
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
    class Actor1:
        def __init__(self, x):
            self.x = x

        def get_val(self):
            return self.x

    @ray.remote
    class Actor2:
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
        class Actor1:
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
    class Actor1:
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
        class Actor:
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

    class Foo:
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
