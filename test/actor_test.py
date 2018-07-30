from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import random
import numpy as np
import os
import pytest
import sys
import time
import unittest

import ray
import ray.ray_constants as ray_constants
import ray.test.test_utils


class ActorAPI(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testKeywordArgs(self):
        ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

        @ray.remote
        class Actor(object):
            def __init__(self, arg0, arg1=1, arg2="a"):
                self.arg0 = arg0
                self.arg1 = arg1
                self.arg2 = arg2

            def get_values(self, arg0, arg1=2, arg2="b"):
                return self.arg0 + arg0, self.arg1 + arg1, self.arg2 + arg2

        actor = Actor.remote(0)
        self.assertEqual(ray.get(actor.get_values.remote(1)), (1, 3, "ab"))

        actor = Actor.remote(1, 2)
        self.assertEqual(ray.get(actor.get_values.remote(2, 3)), (3, 5, "ab"))

        actor = Actor.remote(1, 2, "c")
        self.assertEqual(
            ray.get(actor.get_values.remote(2, 3, "d")), (3, 5, "cd"))

        actor = Actor.remote(1, arg2="c")
        self.assertEqual(
            ray.get(actor.get_values.remote(0, arg2="d")), (1, 3, "cd"))
        self.assertEqual(
            ray.get(actor.get_values.remote(0, arg2="d", arg1=0)),
            (1, 1, "cd"))

        actor = Actor.remote(1, arg2="c", arg1=2)
        self.assertEqual(
            ray.get(actor.get_values.remote(0, arg2="d")), (1, 4, "cd"))
        self.assertEqual(
            ray.get(actor.get_values.remote(0, arg2="d", arg1=0)),
            (1, 2, "cd"))
        self.assertEqual(
            ray.get(actor.get_values.remote(arg2="d", arg1=0, arg0=2)),
            (3, 2, "cd"))

        # Make sure we get an exception if the constructor is called
        # incorrectly.
        with self.assertRaises(Exception):
            actor = Actor.remote()

        with self.assertRaises(Exception):
            actor = Actor.remote(0, 1, 2, arg3=3)

        with self.assertRaises(Exception):
            actor = Actor.remote(0, arg0=1)

        # Make sure we get an exception if the method is called incorrectly.
        actor = Actor.remote(1)
        with self.assertRaises(Exception):
            ray.get(actor.get_values.remote())

    def testVariableNumberOfArgs(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def __init__(self, arg0, arg1=1, *args):
                self.arg0 = arg0
                self.arg1 = arg1
                self.args = args

            def get_values(self, arg0, arg1=2, *args):
                return self.arg0 + arg0, self.arg1 + arg1, self.args, args

        actor = Actor.remote(0)
        self.assertEqual(ray.get(actor.get_values.remote(1)), (1, 3, (), ()))

        actor = Actor.remote(1, 2)
        self.assertEqual(
            ray.get(actor.get_values.remote(2, 3)), (3, 5, (), ()))

        actor = Actor.remote(1, 2, "c")
        self.assertEqual(
            ray.get(actor.get_values.remote(2, 3, "d")), (3, 5, ("c", ),
                                                          ("d", )))

        actor = Actor.remote(1, 2, "a", "b", "c", "d")
        self.assertEqual(
            ray.get(actor.get_values.remote(2, 3, 1, 2, 3, 4)),
            (3, 5, ("a", "b", "c", "d"), (1, 2, 3, 4)))

        @ray.remote
        class Actor(object):
            def __init__(self, *args):
                self.args = args

            def get_values(self, *args):
                return self.args, args

        a = Actor.remote()
        self.assertEqual(ray.get(a.get_values.remote()), ((), ()))
        a = Actor.remote(1)
        self.assertEqual(ray.get(a.get_values.remote(2)), ((1, ), (2, )))
        a = Actor.remote(1, 2)
        self.assertEqual(ray.get(a.get_values.remote(3, 4)), ((1, 2), (3, 4)))

    def testNoArgs(self):
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def __init__(self):
                pass

            def get_values(self):
                pass

        actor = Actor.remote()
        self.assertEqual(ray.get(actor.get_values.remote()), None)

    def testNoConstructor(self):
        # If no __init__ method is provided, that should not be a problem.
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def get_values(self):
                pass

        actor = Actor.remote()
        self.assertEqual(ray.get(actor.get_values.remote()), None)

    def testCustomClasses(self):
        ray.init(num_workers=0)

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
        self.assertEqual(results1[0].x, 1)
        self.assertEqual(results1[1].x, 2)
        results2 = ray.get(actor.get_values2.remote(Foo(3)))
        self.assertEqual(results2[0].x, 1)
        self.assertEqual(results2[1].x, 2)
        self.assertEqual(results2[2].x, 3)

    def testCachingActors(self):
        # Test defining actors before ray.init() has been called.

        @ray.remote
        class Foo(object):
            def __init__(self):
                pass

            def get_val(self):
                return 3

        # Check that we can't actually create actors before ray.init() has been
        # called.
        with self.assertRaises(Exception):
            f = Foo.remote()

        ray.init(num_workers=0)

        f = Foo.remote()

        self.assertEqual(ray.get(f.get_val.remote()), 3)

    def testDecoratorArgs(self):
        ray.init(num_workers=0, driver_mode=ray.SILENT_MODE)

        # This is an invalid way of using the actor decorator.
        with self.assertRaises(Exception):

            @ray.remote()
            class Actor(object):
                def __init__(self):
                    pass

        # This is an invalid way of using the actor decorator.
        with self.assertRaises(Exception):

            @ray.remote(invalid_kwarg=0)  # noqa: F811
            class Actor(object):
                def __init__(self):
                    pass

        # This is an invalid way of using the actor decorator.
        with self.assertRaises(Exception):

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

    def testRandomIDGeneration(self):
        ray.init(num_workers=0)

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

        self.assertNotEqual(f1._ray_actor_id.id(), f2._ray_actor_id.id())

    def testActorClassName(self):
        ray.init(num_workers=0)

        @ray.remote
        class Foo(object):
            def __init__(self):
                pass

        Foo.remote()

        r = ray.worker.global_worker.redis_client
        actor_keys = r.keys("ActorClass*")
        self.assertEqual(len(actor_keys), 1)
        actor_class_info = r.hgetall(actor_keys[0])
        self.assertEqual(actor_class_info[b"class_name"], b"Foo")
        self.assertEqual(actor_class_info[b"module"], b"actor_test")

    def testMultipleReturnValues(self):
        ray.init(num_workers=0)

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
        self.assertEqual(ray.get(id0), 1)

        id1 = f.method1.remote()
        self.assertEqual(ray.get(id1), 1)

        id2a, id2b = f.method2.remote()
        self.assertEqual(ray.get([id2a, id2b]), [1, 2])

        id3a, id3b, id3c = f.method3.remote()
        self.assertEqual(ray.get([id3a, id3b, id3c]), [1, 2, 3])


class ActorMethods(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testDefineActor(self):
        ray.init()

        @ray.remote
        class Test(object):
            def __init__(self, x):
                self.x = x

            def f(self, y):
                return self.x + y

        t = Test.remote(2)
        self.assertEqual(ray.get(t.f.remote(1)), 3)

        # Make sure that calling an actor method directly raises an exception.
        with self.assertRaises(Exception):
            t.f(1)

    def testActorDeletion(self):
        ray.init(num_workers=0)

        # Make sure that when an actor handles goes out of scope, the actor
        # destructor is called.

        @ray.remote
        class Actor(object):
            def getpid(self):
                return os.getpid()

        a = Actor.remote()
        pid = ray.get(a.getpid.remote())
        a = None
        ray.test.test_utils.wait_for_pid_to_exit(pid)

        actors = [Actor.remote() for _ in range(10)]
        pids = ray.get([a.getpid.remote() for a in actors])
        a = None
        actors = None
        [ray.test.test_utils.wait_for_pid_to_exit(pid) for pid in pids]

        @ray.remote
        class Actor(object):
            def method(self):
                return 1

        # Make sure that if we create an actor and call a method on it
        # immediately, the actor doesn't get killed before the method is
        # called.
        self.assertEqual(ray.get(Actor.remote().method.remote()), 1)

    def testActorDeletionWithGPUs(self):
        ray.init(num_workers=0, num_gpus=1)

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

    def testActorState(self):
        ray.init()

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
        self.assertEqual(ray.get(c1.value.remote()), 1)

        c2 = Counter.remote()
        c2.increase.remote()
        c2.increase.remote()
        self.assertEqual(ray.get(c2.value.remote()), 2)

    def testActorClassMethods(self):
        ray.init()

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
        self.assertEqual(ray.get(a.echo.remote(2)), 2)
        self.assertEqual(ray.get(a.f.remote()), 2)
        self.assertEqual(ray.get(a.g.remote(2)), 4)

    def testMultipleActors(self):
        # Create a bunch of actors and call a bunch of methods on all of them.
        ray.init(num_workers=0)

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
            results += [
                actors[i].increase.remote() for _ in range(num_increases)
            ]
        result_values = ray.get(results)
        for i in range(num_actors):
            self.assertEqual(
                result_values[(num_increases * i):(num_increases * (i + 1))],
                list(range(i + 1, num_increases + i + 1)))

        # Reset the actor values.
        [actor.reset.remote() for actor in actors]

        # Interweave the method calls on the different actors.
        results = []
        for j in range(num_increases):
            results += [actor.increase.remote() for actor in actors]
        result_values = ray.get(results)
        for j in range(num_increases):
            self.assertEqual(
                result_values[(num_actors * j):(num_actors * (j + 1))],
                num_actors * [j + 1])


class ActorNesting(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testRemoteFunctionWithinActor(self):
        # Make sure we can use remote funtions within actors.
        ray.init(num_cpus=10)

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
        self.assertEqual(values[0], 1)
        self.assertEqual(values[1], val2)
        self.assertEqual(ray.get(values[2]), list(range(1, 6)))
        self.assertEqual(values[3], list(range(1, 6)))

        self.assertEqual(ray.get(ray.get(actor.f.remote())), list(range(1, 6)))
        self.assertEqual(ray.get(actor.g.remote()), list(range(1, 6)))
        self.assertEqual(
            ray.get(actor.h.remote([f.remote(i) for i in range(5)])),
            list(range(1, 6)))

    def testDefineActorWithinActor(self):
        # Make sure we can use remote funtions within actors.
        ray.init(num_cpus=10)

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
        self.assertEqual(ray.get(actor1.get_values.remote(5)), (3, 5))

    def testUseActorWithinActor(self):
        # Make sure we can use actors within actors.
        ray.init(num_cpus=10)

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
        self.assertEqual(ray.get(actor2.get_values.remote(5)), (3, 4))

    def testDefineActorWithinRemoteFunction(self):
        # Make sure we can define and actors within remote funtions.
        ray.init(num_cpus=10)

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

        self.assertEqual(ray.get(f.remote(3, 1)), [3])
        self.assertEqual(
            ray.get([f.remote(i, 20) for i in range(10)]),
            [20 * [i] for i in range(10)])

    def testUseActorWithinRemoteFunction(self):
        # Make sure we can create and use actors within remote funtions.
        ray.init(num_cpus=10)

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

        self.assertEqual(ray.get(f.remote(3)), 3)

    def testActorImportCounter(self):
        # This is mostly a test of the export counters to make sure that when
        # an actor is imported, all of the necessary remote functions have been
        # imported.
        ray.init(num_cpus=10)

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

        self.assertEqual(ray.get(g.remote()), num_remote_functions - 1)


class ActorInheritance(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testInheritActorFromClass(self):
        # Make sure we can define an actor by inheriting from a regular class.
        # Note that actors cannot inherit from other actors.
        ray.init()

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
        self.assertEqual(ray.get(actor.get_value.remote()), 1)
        self.assertEqual(ray.get(actor.g.remote(5)), 6)


class ActorSchedulingProperties(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testRemoteFunctionsNotScheduledOnActors(self):
        # Make sure that regular remote functions are not scheduled on actors.
        ray.init(num_workers=0)

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
        self.assertNotIn(actor_id, resulting_ids)


class ActorsOnMultipleNodes(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testActorsOnNodesWithNoCPUs(self):
        ray.init(num_cpus=0)

        @ray.remote
        class Foo(object):
            def method(self):
                pass

        f = Foo.remote()
        ready_ids, _ = ray.wait([f.method.remote()], timeout=100)
        self.assertEquals(ready_ids, [])

    def testActorLoadBalancing(self):
        num_local_schedulers = 3
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=num_local_schedulers)

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
            locations = ray.get(
                [actor.get_location.remote() for actor in actors])
            names = set(locations)
            counts = [locations.count(name) for name in names]
            print("Counts are {}.".format(counts))
            if (len(names) == num_local_schedulers
                    and all(count >= minimum_count for count in counts)):
                break
            attempts += 1
        self.assertLess(attempts, num_attempts)

        # Make sure we can get the results of a bunch of tasks.
        results = []
        for _ in range(1000):
            index = np.random.randint(num_actors)
            results.append(actors[index].get_location.remote())
        ray.get(results)


class ActorsWithGPUs(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Crashing with new GCS API.")
    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testActorGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 4
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=num_local_schedulers,
            num_cpus=(num_local_schedulers * [10 * num_gpus_per_scheduler]),
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        @ray.remote(num_gpus=1)
        class Actor1(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                assert ray.get_gpu_ids() == self.gpu_ids
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # Create one actor per GPU.
        actors = [
            Actor1.remote()
            for _ in range(num_local_schedulers * num_gpus_per_scheduler)
        ]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors])
        node_names = {location for location, gpu_id in locations_and_ids}
        self.assertEqual(len(node_names), num_local_schedulers)
        location_actor_combinations = []
        for node_name in node_names:
            for gpu_id in range(num_gpus_per_scheduler):
                location_actor_combinations.append((node_name, (gpu_id, )))
        self.assertEqual(
            set(locations_and_ids), set(location_actor_combinations))

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        a = Actor1.remote()
        ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=10)
        self.assertEqual(ready_ids, [])

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testActorMultipleGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 5
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=num_local_schedulers,
            num_cpus=(num_local_schedulers * [10 * num_gpus_per_scheduler]),
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        @ray.remote(num_gpus=2)
        class Actor1(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                assert ray.get_gpu_ids() == self.gpu_ids
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # Create some actors.
        actors1 = [Actor1.remote() for _ in range(num_local_schedulers * 2)]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors1])
        node_names = {location for location, gpu_id in locations_and_ids}
        self.assertEqual(len(node_names), num_local_schedulers)

        # Keep track of which GPU IDs are being used for each location.
        gpus_in_use = {node_name: [] for node_name in node_names}
        for location, gpu_ids in locations_and_ids:
            gpus_in_use[location].extend(gpu_ids)
        for node_name in node_names:
            self.assertEqual(len(set(gpus_in_use[node_name])), 4)

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        a = Actor1.remote()
        ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=10)
        self.assertEqual(ready_ids, [])

        # We should be able to create more actors that use only a single GPU.
        @ray.remote(num_gpus=1)
        class Actor2(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # Create some actors.
        actors2 = [Actor2.remote() for _ in range(num_local_schedulers)]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors2])
        self.assertEqual(node_names,
                         {location
                          for location, gpu_id in locations_and_ids})
        for location, gpu_ids in locations_and_ids:
            gpus_in_use[location].extend(gpu_ids)
        for node_name in node_names:
            self.assertEqual(len(gpus_in_use[node_name]), 5)
            self.assertEqual(set(gpus_in_use[node_name]), set(range(5)))

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        a = Actor2.remote()
        ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=10)
        self.assertEqual(ready_ids, [])

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testActorDifferentNumbersOfGPUs(self):
        # Test that we can create actors on two nodes that have different
        # numbers of GPUs.
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=3,
            num_cpus=[10, 10, 10],
            num_gpus=[0, 5, 10])

        @ray.remote(num_gpus=1)
        class Actor1(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # Create some actors.
        actors = [Actor1.remote() for _ in range(0 + 5 + 10)]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors])
        node_names = {location for location, gpu_id in locations_and_ids}
        self.assertEqual(len(node_names), 2)
        for node_name in node_names:
            node_gpu_ids = [
                gpu_id for location, gpu_id in locations_and_ids
                if location == node_name
            ]
            self.assertIn(len(node_gpu_ids), [5, 10])
            self.assertEqual(
                set(node_gpu_ids), {(i, )
                                    for i in range(len(node_gpu_ids))})

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        a = Actor1.remote()
        ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=10)
        self.assertEqual(ready_ids, [])

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testActorMultipleGPUsFromMultipleTasks(self):
        num_local_schedulers = 10
        num_gpus_per_scheduler = 10
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=num_local_schedulers,
            redirect_output=True,
            num_cpus=(num_local_schedulers * [10 * num_gpus_per_scheduler]),
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        @ray.remote
        def create_actors(n):
            @ray.remote(num_gpus=1)
            class Actor(object):
                def __init__(self):
                    self.gpu_ids = ray.get_gpu_ids()

                def get_location_and_ids(self):
                    return ((ray.worker.global_worker.plasma_client.
                             store_socket_name), tuple(self.gpu_ids))

            # Create n actors.
            for _ in range(n):
                Actor.remote()

        ray.get([
            create_actors.remote(num_gpus_per_scheduler)
            for _ in range(num_local_schedulers)
        ])

        @ray.remote(num_gpus=1)
        class Actor(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # All the GPUs should be used up now.
        a = Actor.remote()
        ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=10)
        self.assertEqual(ready_ids, [])

    @unittest.skipIf(sys.version_info < (3, 0), "This test requires Python 3.")
    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testActorsAndTasksWithGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 6
        ray.worker._init(
            start_ray_local=True,
            num_workers=0,
            num_local_schedulers=num_local_schedulers,
            num_cpus=num_gpus_per_scheduler,
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        def check_intervals_non_overlapping(list_of_intervals):
            for i in range(len(list_of_intervals)):
                for j in range(i):
                    first_interval = list_of_intervals[i]
                    second_interval = list_of_intervals[j]
                    # Check that list_of_intervals[i] and list_of_intervals[j]
                    # don't overlap.
                    self.assertLess(first_interval[0], first_interval[1])
                    self.assertLess(second_interval[0], second_interval[1])
                    intervals_nonoverlapping = (
                        first_interval[1] <= second_interval[0]
                        or second_interval[1] <= first_interval[0])
                    assert intervals_nonoverlapping, (
                        "Intervals {} and {} are overlapping.".format(
                            first_interval, second_interval))

        @ray.remote(num_gpus=1)
        def f1():
            t1 = time.monotonic()
            time.sleep(0.1)
            t2 = time.monotonic()
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 1
            assert gpu_ids[0] in range(num_gpus_per_scheduler)
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(gpu_ids), [t1, t2])

        @ray.remote(num_gpus=2)
        def f2():
            t1 = time.monotonic()
            time.sleep(0.1)
            t2 = time.monotonic()
            gpu_ids = ray.get_gpu_ids()
            assert len(gpu_ids) == 2
            assert gpu_ids[0] in range(num_gpus_per_scheduler)
            assert gpu_ids[1] in range(num_gpus_per_scheduler)
            return (ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(gpu_ids), [t1, t2])

        @ray.remote(num_gpus=1)
        class Actor1(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()
                assert len(self.gpu_ids) == 1
                assert self.gpu_ids[0] in range(num_gpus_per_scheduler)

            def get_location_and_ids(self):
                assert ray.get_gpu_ids() == self.gpu_ids
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        def locations_to_intervals_for_many_tasks():
            # Launch a bunch of GPU tasks.
            locations_ids_and_intervals = ray.get([
                f1.remote() for _ in range(5 * num_local_schedulers *
                                           num_gpus_per_scheduler)
            ] + [
                f2.remote() for _ in range(5 * num_local_schedulers *
                                           num_gpus_per_scheduler)
            ] + [
                f1.remote() for _ in range(5 * num_local_schedulers *
                                           num_gpus_per_scheduler)
            ])

            locations_to_intervals = collections.defaultdict(lambda: [])
            for location, gpu_ids, interval in locations_ids_and_intervals:
                for gpu_id in gpu_ids:
                    locations_to_intervals[(location, gpu_id)].append(interval)
            return locations_to_intervals

        # Run a bunch of GPU tasks.
        locations_to_intervals = locations_to_intervals_for_many_tasks()
        # Make sure that all GPUs were used.
        self.assertEqual(
            len(locations_to_intervals),
            num_local_schedulers * num_gpus_per_scheduler)
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
        self.assertIn(actor_location, locations_to_intervals)

        # Run a bunch of GPU tasks.
        locations_to_intervals = locations_to_intervals_for_many_tasks()
        # Make sure that all but one of the GPUs were used.
        self.assertEqual(
            len(locations_to_intervals),
            num_local_schedulers * num_gpus_per_scheduler - 1)
        # For each GPU, verify that the set of tasks that used this specific
        # GPU did not overlap in time.
        for locations in locations_to_intervals:
            check_intervals_non_overlapping(locations_to_intervals[locations])
        # Make sure that the actor's GPU was not used.
        self.assertNotIn(actor_location, locations_to_intervals)

        # Create several more actors that use GPUs.
        actors = [Actor1.remote() for _ in range(3)]
        actor_locations = ray.get(
            [actor.get_location_and_ids.remote() for actor in actors])

        # Run a bunch of GPU tasks.
        locations_to_intervals = locations_to_intervals_for_many_tasks()
        # Make sure that all but 11 of the GPUs were used.
        self.assertEqual(
            len(locations_to_intervals),
            num_local_schedulers * num_gpus_per_scheduler - 1 - 3)
        # For each GPU, verify that the set of tasks that used this specific
        # GPU did not overlap in time.
        for locations in locations_to_intervals:
            check_intervals_non_overlapping(locations_to_intervals[locations])
        # Make sure that the GPUs were not used.
        self.assertNotIn(actor_location, locations_to_intervals)
        for location in actor_locations:
            self.assertNotIn(location, locations_to_intervals)

        # Create more actors to fill up all the GPUs.
        more_actors = [
            Actor1.remote()
            for _ in range(num_local_schedulers * num_gpus_per_scheduler - 1 -
                           3)
        ]
        # Wait for the actors to finish being created.
        ray.get([actor.get_location_and_ids.remote() for actor in more_actors])

        # Now if we run some GPU tasks, they should not be scheduled.
        results = [f1.remote() for _ in range(30)]
        ready_ids, remaining_ids = ray.wait(results, timeout=1000)
        self.assertEqual(len(ready_ids), 0)

    def testActorsAndTasksWithGPUsVersionTwo(self):
        # Create tasks and actors that both use GPUs and make sure that they
        # are given different GPUs
        ray.init(num_cpus=10, num_gpus=10)

        @ray.remote(num_gpus=1)
        def f():
            time.sleep(4)
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
        self.assertEqual(set(gpu_ids), set(range(10)))

    @unittest.skipIf(sys.version_info < (3, 0), "This test requires Python 3.")
    def testActorsAndTaskResourceBookkeeping(self):
        ray.init(num_cpus=1)

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
            self.assertLess(interval1[0], interval1[1])
            self.assertLess(interval1[1], interval2[0])
            self.assertLess(interval2[0], interval2[1])

    def testBlockingActorTask(self):
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
        ready_ids, remaining_ids = ray.wait([x_id], timeout=1000)
        self.assertEqual(ready_ids, [])
        self.assertEqual(remaining_ids, [x_id])

        @ray.remote(num_gpus=1)
        class GPUFoo(object):
            def __init__(self):
                pass

            def blocking_method(self):
                ray.get(f.remote())

        # Make sure that GPU resources are not released when actors block.
        actor = GPUFoo.remote()
        x_id = actor.blocking_method.remote()
        ready_ids, remaining_ids = ray.wait([x_id], timeout=1000)
        self.assertEqual(ready_ids, [])
        self.assertEqual(remaining_ids, [x_id])


@unittest.skipIf(
    os.environ.get("RAY_USE_XRAY") == "1",
    "This test does not work with xray yet.")
class ActorReconstruction(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testLocalSchedulerDying(self):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=2,
            num_workers=0,
            redirect_output=True)

        @ray.remote
        class Counter(object):
            def __init__(self):
                self.x = 0

            def local_plasma(self):
                return ray.worker.global_worker.plasma_client.store_socket_name

            def inc(self):
                self.x += 1
                return self.x

        local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

        # Create an actor that is not on the local scheduler.
        actor = Counter.remote()
        while ray.get(actor.local_plasma.remote()) == local_plasma:
            actor = Counter.remote()

        ids = [actor.inc.remote() for _ in range(100)]

        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the second plasma store to get rid of the cached objects and
        # trigger the corresponding local scheduler to exit.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Get all of the results
        results = ray.get(ids)

        self.assertEqual(results, list(range(1, 1 + len(results))))

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testManyLocalSchedulersDying(self):
        # This test can be made more stressful by increasing the numbers below.
        # The total number of actors created will be
        # num_actors_at_a_time * num_local_schedulers.
        num_local_schedulers = 5
        num_actors_at_a_time = 3
        num_function_calls_at_a_time = 10

        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=num_local_schedulers,
            num_workers=0,
            redirect_output=True)

        @ray.remote
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
        for i in range(num_local_schedulers - 1):
            # Create some actors.
            actors.extend(
                [SlowCounter.remote() for _ in range(num_actors_at_a_time)])
            # Run some methods.
            for j in range(len(actors)):
                actor = actors[j]
                for _ in range(num_function_calls_at_a_time):
                    result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))
            # Kill a plasma store to get rid of the cached objects and trigger
            # exit of the corresponding local scheduler. Don't kill the first
            # local scheduler since that is the one that the driver is
            # connected to.
            process = ray.services.all_processes[
                ray.services.PROCESS_TYPE_PLASMA_STORE][i + 1]
            process.kill()
            process.wait()

            # Run some more methods.
            for j in range(len(actors)):
                actor = actors[j]
                for _ in range(num_function_calls_at_a_time):
                    result_ids[actor].append(actor.inc.remote(j**2 * 0.000001))

        # Get the results and check that they have the correct values.
        for _, result_id_list in result_ids.items():
            self.assertEqual(
                ray.get(result_id_list), list(
                    range(1,
                          len(result_id_list) + 1)))

    def setup_counter_actor(self,
                            test_checkpoint=False,
                            save_exception=False,
                            resume_exception=False):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=2,
            num_workers=0,
            redirect_output=True)

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

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testCheckpointing(self):
        actor, ids = self.setup_counter_actor(test_checkpoint=True)
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor restored from a checkpoint.
        self.assertTrue(ray.get(actor.test_restore.remote()))
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(actor.inc.remote())
        self.assertEqual(x, 101)
        # Check that the number of inc calls since actor initialization is less
        # than the counter value, since the actor initialized from a
        # checkpoint.
        num_inc_calls = ray.get(actor.get_num_inc_calls.remote())
        self.assertLess(num_inc_calls, x)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testRemoteCheckpoint(self):
        actor, ids = self.setup_counter_actor(test_checkpoint=True)

        # Do a remote checkpoint call and wait for it to finish.
        ray.get(actor.__ray_checkpoint__.remote())

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor restored from a checkpoint.
        self.assertTrue(ray.get(actor.test_restore.remote()))
        # Check that the number of inc calls since actor initialization is
        # exactly zero, since there could not have been another inc call since
        # the remote checkpoint.
        num_inc_calls = ray.get(actor.get_num_inc_calls.remote())
        self.assertEqual(num_inc_calls, 0)
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(actor.inc.remote())
        self.assertEqual(x, 101)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testLostCheckpoint(self):
        actor, ids = self.setup_counter_actor(test_checkpoint=True)
        # Wait for the first fraction of tasks to finish running.
        ray.get(ids[len(ids) // 10])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor restored from a checkpoint.
        self.assertTrue(ray.get(actor.test_restore.remote()))
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(actor.inc.remote())
        self.assertEqual(x, 101)
        # Check that the number of inc calls since actor initialization is less
        # than the counter value, since the actor initialized from a
        # checkpoint.
        num_inc_calls = ray.get(actor.get_num_inc_calls.remote())
        self.assertLess(num_inc_calls, x)
        self.assertLess(5, num_inc_calls)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testCheckpointException(self):
        actor, ids = self.setup_counter_actor(
            test_checkpoint=True, save_exception=True)
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(actor.inc.remote())
        self.assertEqual(x, 101)
        # Check that the number of inc calls since actor initialization is
        # equal to the counter value, since the actor did not initialize from a
        # checkpoint.
        num_inc_calls = ray.get(actor.get_num_inc_calls.remote())
        self.assertEqual(num_inc_calls, x)
        # Check that errors were raised when trying to save the checkpoint.
        errors = ray.error_info()
        self.assertLess(0, len(errors))
        for error in errors:
            self.assertEqual(error["type"],
                             ray_constants.CHECKPOINT_PUSH_ERROR)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testCheckpointResumeException(self):
        actor, ids = self.setup_counter_actor(
            test_checkpoint=True, resume_exception=True)
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(actor.inc.remote())
        self.assertEqual(x, 101)
        # Check that the number of inc calls since actor initialization is
        # equal to the counter value, since the actor did not initialize from a
        # checkpoint.
        num_inc_calls = ray.get(actor.get_num_inc_calls.remote())
        self.assertEqual(num_inc_calls, x)
        # Check that an error was raised when trying to resume from the
        # checkpoint.
        errors = ray.error_info()
        self.assertEqual(len(errors), 1)
        for error in errors:
            self.assertEqual(error["type"],
                             ray_constants.CHECKPOINT_PUSH_ERROR)

    @unittest.skip("Fork/join consistency not yet implemented.")
    def testDistributedHandle(self):
        counter, ids = self.setup_counter_actor(test_checkpoint=False)

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
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor did not restore from a checkpoint.
        self.assertFalse(ray.get(counter.test_restore.remote()))
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(counter.inc.remote())
        self.assertEqual(x, count + 1)

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False), "Hanging with new GCS API.")
    def testRemoteCheckpointDistributedHandle(self):
        counter, ids = self.setup_counter_actor(test_checkpoint=True)

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
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor restored from a checkpoint.
        self.assertTrue(ray.get(counter.test_restore.remote()))
        # Check that the number of inc calls since actor initialization is
        # exactly zero, since there could not have been another inc call since
        # the remote checkpoint.
        num_inc_calls = ray.get(counter.get_num_inc_calls.remote())
        self.assertEqual(num_inc_calls, 0)
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(counter.inc.remote())
        self.assertEqual(x, count + 1)

    @unittest.skip("Fork/join consistency not yet implemented.")
    def testCheckpointDistributedHandle(self):
        counter, ids = self.setup_counter_actor(test_checkpoint=True)

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
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Check that the actor restored from a checkpoint.
        self.assertTrue(ray.get(counter.test_restore.remote()))
        # Check that we can submit another call on the actor and get the
        # correct counter result.
        x = ray.get(counter.inc.remote())
        self.assertEqual(x, count + 1)

    def _testNondeterministicReconstruction(
            self, num_forks, num_items_per_fork, num_forks_to_wait):
        ray.worker._init(
            start_ray_local=True,
            num_local_schedulers=2,
            num_workers=0,
            redirect_output=True)

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
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Read the queue again and check for deterministic reconstruction.
        ray.get(enqueue_tasks)
        reconstructed_queue = ray.get(actor.read.remote())
        # Make sure the final queue has all items from all forks.
        self.assertEqual(
            len(reconstructed_queue), num_forks * num_items_per_fork)
        # Make sure that the prefix of the final queue matches the queue from
        # the initial execution.
        self.assertEqual(queue, reconstructed_queue[:len(queue)])

    @unittest.skipIf(
        os.environ.get('RAY_USE_NEW_GCS', False),
        "Currently doesn't work with the new GCS.")
    def testNondeterministicReconstruction(self):
        self._testNondeterministicReconstruction(10, 100, 10)

    @unittest.skip("Nondeterministic reconstruction currently not supported "
                   "when there are concurrent forks that didn't finish "
                   "initial execution.")
    def testNondeterministicReconstructionConcurrentForks(self):
        self._testNondeterministicReconstruction(10, 100, 1)


class DistributedActorHandles(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def setup_queue_actor(self):
        ray.init()

        @ray.remote
        class Queue(object):
            def __init__(self):
                self.queue = []

            def enqueue(self, key, item):
                self.queue.append((key, item))

            def read(self):
                return self.queue

        return Queue.remote()

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testFork(self):
        queue = self.setup_queue_actor()

        @ray.remote
        def fork(queue, key, item):
            return ray.get(queue.enqueue.remote(key, item))

        # Fork num_iters times.
        num_iters = 100
        ray.get([fork.remote(queue, i, 0) for i in range(num_iters)])
        items = ray.get(queue.read.remote())
        for i in range(num_iters):
            filtered_items = [item[1] for item in items if item[0] == i]
            self.assertEqual(filtered_items, list(range(1)))

    @unittest.skipIf(
        os.environ.get("RAY_USE_XRAY") == "1",
        "This test does not work with xray yet.")
    def testForkConsistency(self):
        queue = self.setup_queue_actor()

        @ray.remote
        def fork(queue, key, num_items):
            x = None
            for item in range(num_items):
                x = queue.enqueue.remote(key, item)
            return ray.get(x)

        # Fork num_iters times.
        num_forks = 10
        num_items_per_fork = 100
        ray.get([
            fork.remote(queue, i, num_items_per_fork) for i in range(num_forks)
        ])
        items = ray.get(queue.read.remote())
        for i in range(num_forks):
            filtered_items = [item[1] for item in items if item[0] == i]
            self.assertEqual(filtered_items, list(range(num_items_per_fork)))

    @unittest.skip("Garbage collection for distributed actor handles not "
                   "implemented.")
    def testGarbageCollection(self):
        queue = self.setup_queue_actor()

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

    def testCallingPutOnActorHandle(self):
        ray.worker.init(num_workers=1)

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

        with self.assertRaises(Exception):
            ray.get(f.remote())

        # The below test works, but do we want to disallow this usage?
        ray.get(g.remote())

    def testPicklingActorHandle(self):
        ray.worker.init(num_workers=1)

        @ray.remote
        class Foo(object):
            def method(self):
                pass

        f = Foo.remote()
        new_f = ray.worker.pickle.loads(ray.worker.pickle.dumps(f))
        # Verify that we can call a method on the unpickled handle. TODO(rkn):
        # we should also test this from a different driver.
        ray.get(new_f.method.remote())

    def testRegisterAndGetNamedActors(self):
        # TODO(heyucongtom): We should test this from another driver.
        ray.worker.init(num_workers=1)

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
        self.assertEqual(f1._actor_id, f2._actor_id)

        # Test same name register shall raise error.
        with self.assertRaises(ValueError):
            ray.experimental.register_actor("f1", f2)

        # Test register with wrong object type.
        with self.assertRaises(TypeError):
            ray.experimental.register_actor("f3", 1)

        # Test getting a nonexistent actor.
        with self.assertRaises(ValueError):
            ray.experimental.get_actor("nonexistent")

        # Test method
        self.assertEqual(ray.get(f1.method.remote()), 1)
        self.assertEqual(ray.get(f2.method.remote()), 2)
        self.assertEqual(ray.get(f1.method.remote()), 3)
        self.assertEqual(ray.get(f2.method.remote()), 4)


@pytest.fixture
def ray_stop():
    # The initialization code depends on the test that is run.
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


@unittest.skipIf(
    os.environ.get("RAY_USE_XRAY") == "1" or sys.version_info < (3, 0),
    "This test does not work with xray yet"
    " and is currently failing on Python 2.7.")
def testLifetimeAndTransientResources(ray_stop):
    ray.init(num_cpus=1)

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
        results, num_returns=len(results), timeout=1000)
    assert len(ready_ids) == 1


def testCustomLabelPlacement(ray_stop):
    ray.worker._init(
        start_ray_local=True,
        num_local_schedulers=2,
        num_workers=0,
        resources=[{
            "CustomResource1": 2
        }, {
            "CustomResource2": 2
        }])

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


def testCreatingMoreActorsThanResources(ray_stop):
    ray.init(
        num_workers=0,
        num_cpus=10,
        num_gpus=2,
        resources={"CustomResource1": 1})

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
    ready_ids, _ = ray.wait([result3], timeout=200)
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
