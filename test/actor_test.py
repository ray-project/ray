from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import random
import numpy as np
import os
import sys
import time
import unittest

import ray
import ray.test.test_utils


class ActorAPI(unittest.TestCase):

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
        self.assertEqual(ray.get(actor.get_values.remote(2, 3, "d")),
                         (3, 5, "cd"))

        actor = Actor.remote(1, arg2="c")
        self.assertEqual(ray.get(actor.get_values.remote(0, arg2="d")),
                         (1, 3, "cd"))
        self.assertEqual(ray.get(actor.get_values.remote(0, arg2="d", arg1=0)),
                         (1, 1, "cd"))

        actor = Actor.remote(1, arg2="c", arg1=2)
        self.assertEqual(ray.get(actor.get_values.remote(0, arg2="d")),
                         (1, 4, "cd"))
        self.assertEqual(ray.get(actor.get_values.remote(0, arg2="d", arg1=0)),
                         (1, 2, "cd"))

        # Make sure we get an exception if the constructor is called
        # incorrectly.
        with self.assertRaises(Exception):
            actor = Actor.remote()

        with self.assertRaises(Exception):
            actor = Actor.remote(0, 1, 2, arg3=3)

        # Make sure we get an exception if the method is called incorrectly.
        actor = Actor.remote(1)
        with self.assertRaises(Exception):
            ray.get(actor.get_values.remote())

        ray.worker.cleanup()

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
        self.assertEqual(ray.get(actor.get_values.remote(2, 3)),
                         (3, 5, (), ()))

        actor = Actor.remote(1, 2, "c")
        self.assertEqual(ray.get(actor.get_values.remote(2, 3, "d")),
                         (3, 5, ("c",), ("d",)))

        actor = Actor.remote(1, 2, "a", "b", "c", "d")
        self.assertEqual(ray.get(actor.get_values.remote(2, 3, 1, 2, 3, 4)),
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
        self.assertEqual(ray.get(a.get_values.remote(2)), ((1,), (2,)))
        a = Actor.remote(1, 2)
        self.assertEqual(ray.get(a.get_values.remote(3, 4)), ((1, 2), (3, 4)))

        ray.worker.cleanup()

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

        ray.worker.cleanup()

    def testNoConstructor(self):
        # If no __init__ method is provided, that should not be a problem.
        ray.init(num_workers=0)

        @ray.remote
        class Actor(object):
            def get_values(self):
                pass

        actor = Actor.remote()
        self.assertEqual(ray.get(actor.get_values.remote()), None)

        ray.worker.cleanup()

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

        ray.worker.cleanup()

    # def testCachingActors(self):
    #   # TODO(rkn): Implement this.
    #   pass

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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
        self.assertEqual(actor_class_info[b"module"], b"__main__")

        ray.worker.cleanup()


class ActorMethods(unittest.TestCase):

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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
            pid = ray.get(a.getpid.remote())

            # Make sure that we can't create another actor.
            with self.assertRaises(Exception):
                Actor.remote()

            # Let the actor go out of scope, and wait for it to exit.
            a = None
            ray.test.test_utils.wait_for_pid_to_exit(pid)

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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
            results += [actors[i].increase.remote()
                        for _ in range(num_increases)]
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

        ray.worker.cleanup()


class ActorNesting(unittest.TestCase):

    def testRemoteFunctionWithinActor(self):
        # Make sure we can use remote funtions within actors.
        ray.init(num_cpus=100)

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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
        self.assertEqual(ray.get([f.remote(i, 20) for i in range(10)]),
                         [20 * [i] for i in range(10)])

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()


class ActorInheritance(unittest.TestCase):

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

        ray.worker.cleanup()


class ActorSchedulingProperties(unittest.TestCase):

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

        ray.worker.cleanup()


class ActorsOnMultipleNodes(unittest.TestCase):

    def testActorsOnNodesWithNoCPUs(self):
        ray.init(num_cpus=0)

        @ray.remote
        class Foo(object):
            def __init__(self):
                pass

        with self.assertRaises(Exception):
            Foo.remote()

        ray.worker.cleanup()

    def testActorLoadBalancing(self):
        num_local_schedulers = 3
        ray.worker._init(start_ray_local=True, num_workers=0,
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
            locations = ray.get([actor.get_location.remote()
                                 for actor in actors])
            names = set(locations)
            counts = [locations.count(name) for name in names]
            print("Counts are {}.".format(counts))
            if (len(names) == num_local_schedulers and
                    all([count >= minimum_count for count in counts])):
                break
            attempts += 1
        self.assertLess(attempts, num_attempts)

        # Make sure we can get the results of a bunch of tasks.
        results = []
        for _ in range(1000):
            index = np.random.randint(num_actors)
            results.append(actors[index].get_location.remote())
        ray.get(results)

        ray.worker.cleanup()


class ActorsWithGPUs(unittest.TestCase):

    def testActorGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 4
        ray.worker._init(
            start_ray_local=True, num_workers=0,
            num_local_schedulers=num_local_schedulers,
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
        actors = [Actor1.remote() for _
                  in range(num_local_schedulers * num_gpus_per_scheduler)]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get([actor.get_location_and_ids.remote()
                                     for actor in actors])
        node_names = set([location for location, gpu_id in locations_and_ids])
        self.assertEqual(len(node_names), num_local_schedulers)
        location_actor_combinations = []
        for node_name in node_names:
            for gpu_id in range(num_gpus_per_scheduler):
                location_actor_combinations.append((node_name, (gpu_id,)))
        self.assertEqual(set(locations_and_ids),
                         set(location_actor_combinations))

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        with self.assertRaises(Exception):
            Actor1.remote()

        ray.worker.cleanup()

    def testActorMultipleGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 5
        ray.worker._init(
            start_ray_local=True, num_workers=0,
            num_local_schedulers=num_local_schedulers,
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        @ray.remote(num_gpus=2)
        class Actor1(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # Create some actors.
        actors1 = [Actor1.remote() for _ in range(num_local_schedulers * 2)]
        # Make sure that no two actors are assigned to the same GPU.
        locations_and_ids = ray.get([actor.get_location_and_ids.remote()
                                     for actor in actors1])
        node_names = set([location for location, gpu_id in locations_and_ids])
        self.assertEqual(len(node_names), num_local_schedulers)

        # Keep track of which GPU IDs are being used for each location.
        gpus_in_use = {node_name: [] for node_name in node_names}
        for location, gpu_ids in locations_and_ids:
            gpus_in_use[location].extend(gpu_ids)
        for node_name in node_names:
            self.assertEqual(len(set(gpus_in_use[node_name])), 4)

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        with self.assertRaises(Exception):
            Actor1.remote()

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
        locations_and_ids = ray.get([actor.get_location_and_ids.remote()
                                     for actor in actors2])
        self.assertEqual(node_names,
                         set([location for location, gpu_id
                              in locations_and_ids]))
        for location, gpu_ids in locations_and_ids:
            gpus_in_use[location].extend(gpu_ids)
        for node_name in node_names:
            self.assertEqual(len(gpus_in_use[node_name]), 5)
            self.assertEqual(set(gpus_in_use[node_name]), set(range(5)))

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        with self.assertRaises(Exception):
            Actor2.remote()

        ray.worker.cleanup()

    def testActorDifferentNumbersOfGPUs(self):
        # Test that we can create actors on two nodes that have different
        # numbers of GPUs.
        ray.worker._init(start_ray_local=True, num_workers=0,
                         num_local_schedulers=3, num_gpus=[0, 5, 10])

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
        locations_and_ids = ray.get([actor.get_location_and_ids.remote()
                                    for actor in actors])
        node_names = set([location for location, gpu_id in locations_and_ids])
        self.assertEqual(len(node_names), 2)
        for node_name in node_names:
            node_gpu_ids = [gpu_id for location, gpu_id in locations_and_ids
                            if location == node_name]
            self.assertIn(len(node_gpu_ids), [5, 10])
            self.assertEqual(set(node_gpu_ids),
                             set([(i,) for i in range(len(node_gpu_ids))]))

        # Creating a new actor should fail because all of the GPUs are being
        # used.
        with self.assertRaises(Exception):
            Actor1.remote()

        ray.worker.cleanup()

    def testActorMultipleGPUsFromMultipleTasks(self):
        num_local_schedulers = 10
        num_gpus_per_scheduler = 10
        ray.worker._init(
            start_ray_local=True, num_workers=0,
            num_local_schedulers=num_local_schedulers, redirect_output=True,
            num_gpus=(num_local_schedulers * [num_gpus_per_scheduler]))

        @ray.remote
        def create_actors(n):
            @ray.remote(num_gpus=1)
            class Actor(object):
                def __init__(self):
                    self.gpu_ids = ray.get_gpu_ids()

                def get_location_and_ids(self):
                    return ((ray.worker.global_worker.plasma_client
                                .store_socket_name),
                            tuple(self.gpu_ids))
            # Create n actors.
            for _ in range(n):
                Actor.remote()

        ray.get([create_actors.remote(num_gpus_per_scheduler)
                 for _ in range(num_local_schedulers)])

        @ray.remote(num_gpus=1)
        class Actor(object):
            def __init__(self):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    ray.worker.global_worker.plasma_client.store_socket_name,
                    tuple(self.gpu_ids))

        # All the GPUs should be used up now.
        with self.assertRaises(Exception):
            Actor.remote()

        ray.worker.cleanup()

    @unittest.skipIf(sys.version_info < (3, 0), "This test requires Python 3.")
    def testActorsAndTasksWithGPUs(self):
        num_local_schedulers = 3
        num_gpus_per_scheduler = 6
        ray.worker._init(
            start_ray_local=True, num_workers=0,
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
                        first_interval[1] <= second_interval[0] or
                        second_interval[1] <= first_interval[0])
                    assert intervals_nonoverlapping, (
                        "Intervals {} and {} are overlapping."
                        .format(first_interval, second_interval))

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
            locations_ids_and_intervals = ray.get(
                [f1.remote() for _
                 in range(5 * num_local_schedulers * num_gpus_per_scheduler)] +
                [f2.remote() for _
                 in range(5 * num_local_schedulers * num_gpus_per_scheduler)] +
                [f1.remote() for _
                 in range(5 * num_local_schedulers * num_gpus_per_scheduler)])

            locations_to_intervals = collections.defaultdict(lambda: [])
            for location, gpu_ids, interval in locations_ids_and_intervals:
                for gpu_id in gpu_ids:
                    locations_to_intervals[(location, gpu_id)].append(interval)
            return locations_to_intervals

        # Run a bunch of GPU tasks.
        locations_to_intervals = locations_to_intervals_for_many_tasks()
        # Make sure that all GPUs were used.
        self.assertEqual(len(locations_to_intervals),
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
        self.assertEqual(len(locations_to_intervals),
                         num_local_schedulers * num_gpus_per_scheduler - 1)
        # For each GPU, verify that the set of tasks that used this specific
        # GPU did not overlap in time.
        for locations in locations_to_intervals:
            check_intervals_non_overlapping(locations_to_intervals[locations])
        # Make sure that the actor's GPU was not used.
        self.assertNotIn(actor_location, locations_to_intervals)

        # Create several more actors that use GPUs.
        actors = [Actor1.remote() for _ in range(3)]
        actor_locations = ray.get([actor.get_location_and_ids.remote()
                                   for actor in actors])

        # Run a bunch of GPU tasks.
        locations_to_intervals = locations_to_intervals_for_many_tasks()
        # Make sure that all but 11 of the GPUs were used.
        self.assertEqual(len(locations_to_intervals),
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
        more_actors = [Actor1.remote() for _ in
                       range(num_local_schedulers *
                             num_gpus_per_scheduler - 1 - 3)]
        # Wait for the actors to finish being created.
        ray.get([actor.get_location_and_ids.remote() for actor in more_actors])

        # Now if we run some GPU tasks, they should not be scheduled.
        results = [f1.remote() for _ in range(30)]
        ready_ids, remaining_ids = ray.wait(results, timeout=1000)
        self.assertEqual(len(ready_ids), 0)

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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

        ray.worker.cleanup()

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

        @ray.remote(num_gpus=1)
        class GPUFoo(object):
            def __init__(self):
                pass

            def blocking_method(self):
                ray.get(f.remote())

        # Make sure that we GPU resources are not released when actors block.
        actor = GPUFoo.remote()
        x_id = actor.blocking_method.remote()
        ready_ids, remaining_ids = ray.wait([x_id], timeout=500)
        self.assertEqual(ready_ids, [])
        self.assertEqual(remaining_ids, [x_id])

        ray.worker.cleanup()


class ActorReconstruction(unittest.TestCase):

    def testLocalSchedulerDying(self):
        ray.worker._init(start_ray_local=True, num_local_schedulers=2,
                         num_workers=0, redirect_output=True)

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

        ray.worker.cleanup()

    def testManyLocalSchedulersDying(self):
        # This test can be made more stressful by increasing the numbers below.
        # The total number of actors created will be
        # num_actors_at_a_time * num_local_schedulers.
        num_local_schedulers = 5
        num_actors_at_a_time = 3
        num_function_calls_at_a_time = 10

        ray.worker._init(start_ray_local=True,
                         num_local_schedulers=num_local_schedulers,
                         num_workers=0, redirect_output=True)

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
            actors.extend([SlowCounter.remote()
                           for _ in range(num_actors_at_a_time)])
            # Run some methods.
            for j in range(len(actors)):
                actor = actors[j]
                for _ in range(num_function_calls_at_a_time):
                    result_ids[actor].append(
                        actor.inc.remote(j ** 2 * 0.000001))
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
                    result_ids[actor].append(
                        actor.inc.remote(j ** 2 * 0.000001))

        # Get the results and check that they have the correct values.
        for _, result_id_list in result_ids.items():
            self.assertEqual(ray.get(result_id_list),
                             list(range(1, len(result_id_list) + 1)))

        ray.worker.cleanup()

    def setup_test_checkpointing(self, save_exception=False,
                                 resume_exception=False):
        ray.worker._init(start_ray_local=True, num_local_schedulers=2,
                         num_workers=0, redirect_output=True)

        @ray.remote(checkpoint_interval=5)
        class Counter(object):
            _resume_exception = resume_exception

            def __init__(self, save_exception):
                self.x = 0
                # The number of times that inc has been called. We won't bother
                # restoring this in the checkpoint
                self.num_inc_calls = 0
                self.save_exception = save_exception

            def local_plasma(self):
                return ray.worker.global_worker.plasma_client.store_socket_name

            def inc(self, *xs):
                self.num_inc_calls += 1
                self.x += 1
                return self.x

            def get_num_inc_calls(self):
                return self.num_inc_calls

            def test_restore(self):
                # This method will only work if __ray_restore__ has been run.
                return self.y

            def __ray_save__(self):
                if self.save_exception:
                    raise Exception("Exception raised in checkpoint save")
                return self.x, -1

            def __ray_restore__(self, checkpoint):
                if self._resume_exception:
                    raise Exception("Exception raised in checkpoint resume")
                self.x, val = checkpoint
                self.num_inc_calls = 0
                # Test that __ray_save__ has been run.
                assert val == -1
                self.y = self.x

        local_plasma = ray.worker.global_worker.plasma_client.store_socket_name

        # Create an actor that is not on the local scheduler.
        actor = Counter.remote(save_exception)
        while ray.get(actor.local_plasma.remote()) == local_plasma:
            actor = Counter.remote(save_exception)

        args = [ray.put(0) for _ in range(100)]
        ids = [actor.inc.remote(*args[i:]) for i in range(100)]

        return actor, ids

    def testCheckpointing(self):
        actor, ids = self.setup_test_checkpointing()
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        # Get all of the results. TODO(rkn): This currently doesn't work.
        # results = ray.get(ids)
        # self.assertEqual(results, list(range(1, 1 + len(results))))

        self.assertEqual(ray.get(actor.test_restore.remote()), 99)

        # The inc method should only have executed once on the new actor (for
        # the one method call since the most recent checkpoint).
        self.assertEqual(ray.get(actor.get_num_inc_calls.remote()), 1)

        ray.worker.cleanup()

    def testLostCheckpoint(self):
        actor, ids = self.setup_test_checkpointing()
        # Wait for the first fraction of tasks to finish running.
        ray.get(ids[len(ids) // 10])

        actor_key = b"Actor:" + actor._ray_actor_id.id()
        for index in ray.actor.get_checkpoint_indices(
                ray.worker.global_worker, actor._ray_actor_id.id()):
            ray.worker.global_worker.redis_client.hdel(
                actor_key, "checkpoint_{}".format(index))

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        self.assertEqual(ray.get(actor.inc.remote()), 101)

        # Each inc method has been reexecuted once on the new actor.
        self.assertEqual(ray.get(actor.get_num_inc_calls.remote()), 101)
        # Get all of the results that were previously lost. Because the
        # checkpoints were lost, all methods should be reconstructed.
        results = ray.get(ids)
        self.assertEqual(results, list(range(1, 1 + len(results))))

        ray.worker.cleanup()

    def testCheckpointException(self):
        actor, ids = self.setup_test_checkpointing(save_exception=True)
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        self.assertEqual(ray.get(actor.inc.remote()), 101)
        # Each inc method has been reexecuted once on the new actor, since all
        # checkpoint saves failed.
        self.assertEqual(ray.get(actor.get_num_inc_calls.remote()), 101)
        # Get all of the results that were previously lost. Because the
        # checkpoints were lost, all methods should be reconstructed.
        results = ray.get(ids)
        self.assertEqual(results, list(range(1, 1 + len(results))))

        errors = ray.error_info()
        # We submitted 101 tasks with a checkpoint interval of 5.
        num_checkpoints = 101 // 5
        # Each checkpoint task throws an exception when saving during initial
        # execution, and then again during re-execution.
        self.assertEqual(len([error for error in errors if error[b"type"] ==
                              b"task"]), num_checkpoints * 2)

        ray.worker.cleanup()

    def testCheckpointResumeException(self):
        actor, ids = self.setup_test_checkpointing(resume_exception=True)
        # Wait for the last task to finish running.
        ray.get(ids[-1])

        # Kill the corresponding plasma store to get rid of the cached objects.
        process = ray.services.all_processes[
            ray.services.PROCESS_TYPE_PLASMA_STORE][1]
        process.kill()
        process.wait()

        self.assertEqual(ray.get(actor.inc.remote()), 101)
        # Each inc method has been reexecuted once on the new actor, since all
        # checkpoint resumes failed.
        self.assertEqual(ray.get(actor.get_num_inc_calls.remote()), 101)
        # Get all of the results that were previously lost. Because the
        # checkpoints were lost, all methods should be reconstructed.
        results = ray.get(ids)
        self.assertEqual(results, list(range(1, 1 + len(results))))

        errors = ray.error_info()
        # The most recently executed checkpoint task should throw an exception
        # when trying to resume. All other checkpoint tasks should reconstruct
        # the previous task but throw no errors.
        self.assertTrue(len([error for error in errors if error[b"type"] ==
                             b"task"]) > 0)

        ray.worker.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
