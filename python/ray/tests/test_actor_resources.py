import collections
import os
import sys
import time

import pytest

import ray
import ray.cluster_utils

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None


def test_actor_deletion_with_gpus(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1, object_store_memory=int(150 * 1024 * 1024))

    # When an actor that uses a GPU exits, make sure that the GPU resources
    # are released.

    @ray.remote(num_gpus=1)
    class Actor:
        def getpid(self):
            return os.getpid()

    for _ in range(5):
        # If we can successfully create an actor, that means that enough
        # GPU resources are available.
        a = Actor.remote()
        ray.get(a.getpid.remote())


def test_actor_state(ray_start_regular):
    @ray.remote
    class Counter:
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
    class Foo:
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


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_actor_gpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 4
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=10 * num_gpus_per_raylet, num_gpus=num_gpus_per_raylet
        )
    ray.init(address=cluster.address)

    @ray.remote(num_gpus=1)
    class Actor1:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    # Create one actor per GPU.
    actors = [Actor1.remote() for _ in range(num_nodes * num_gpus_per_raylet)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors]
    )
    node_names = {location for location, gpu_id in locations_and_ids}
    assert len(node_names) == num_nodes
    location_actor_combinations = []
    for node_name in node_names:
        for gpu_id in range(num_gpus_per_raylet):
            location_actor_combinations.append((node_name, (gpu_id,)))
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
            num_cpus=10 * num_gpus_per_raylet, num_gpus=num_gpus_per_raylet
        )
    ray.init(address=cluster.address)

    @ray.remote(num_gpus=2)
    class Actor1:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    # Create some actors.
    actors1 = [Actor1.remote() for _ in range(num_nodes * 2)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors1]
    )
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
    class Actor2:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    # Create some actors.
    actors2 = [Actor2.remote() for _ in range(num_nodes)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors2]
    )
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


@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky.")
def test_actor_different_numbers_of_gpus(ray_start_cluster):
    # Test that we can create actors on two nodes that have different
    # numbers of GPUs.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=10, num_gpus=0)
    cluster.add_node(num_cpus=10, num_gpus=5)
    cluster.add_node(num_cpus=10, num_gpus=10)
    ray.init(address=cluster.address)

    @ray.remote(num_gpus=1)
    class Actor1:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    # Create some actors.
    actors = [Actor1.remote() for _ in range(0 + 5 + 10)]
    # Make sure that no two actors are assigned to the same GPU.
    locations_and_ids = ray.get(
        [actor.get_location_and_ids.remote() for actor in actors]
    )
    node_names = {location for location, gpu_id in locations_and_ids}
    assert len(node_names) == 2
    for node_name in node_names:
        node_gpu_ids = [
            gpu_id for location, gpu_id in locations_and_ids if location == node_name
        ]
        assert len(node_gpu_ids) in [5, 10]
        assert set(node_gpu_ids) == {(i,) for i in range(len(node_gpu_ids))}

    # Creating a new actor should fail because all of the GPUs are being
    # used.
    a = Actor1.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actor_multiple_gpus_from_multiple_tasks(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 2
    for i in range(num_nodes):
        cluster.add_node(
            num_cpus=4 * num_gpus_per_raylet,
            num_gpus=num_gpus_per_raylet,
            _system_config={"num_heartbeats_timeout": 100} if i == 0 else {},
        )
    ray.init(address=cluster.address)

    @ray.remote
    def create_actors(i, n):
        @ray.remote(num_gpus=1)
        class Actor:
            def __init__(self, i, j):
                self.gpu_ids = ray.get_gpu_ids()

            def get_location_and_ids(self):
                return (
                    (ray._private.worker.global_worker.node.unique_id),
                    tuple(self.gpu_ids),
                )

            def sleep(self):
                time.sleep(100)

        # Create n actors.
        actors = []
        for j in range(n):
            actors.append(Actor.remote(i, j))

        locations = ray.get([actor.get_location_and_ids.remote() for actor in actors])

        # Put each actor to sleep for a long time to prevent them from getting
        # terminated.
        for actor in actors:
            actor.sleep.remote()

        return locations

    all_locations = ray.get(
        [create_actors.remote(i, num_gpus_per_raylet) for i in range(num_nodes)]
    )

    # Make sure that no two actors are assigned to the same GPU.
    node_names = {
        location for locations in all_locations for location, gpu_id in locations
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
    class Actor:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()

        def get_location_and_ids(self):
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    # All the GPUs should be used up now.
    a = Actor.remote()
    ready_ids, _ = ray.wait([a.get_location_and_ids.remote()], timeout=0.01)
    assert ready_ids == []


def test_actors_and_tasks_with_gpus(ray_start_cluster):
    cluster = ray_start_cluster
    num_nodes = 3
    num_gpus_per_raylet = 2
    for i in range(num_nodes):
        cluster.add_node(num_cpus=num_gpus_per_raylet, num_gpus=num_gpus_per_raylet)
    ray.init(address=cluster.address)

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
                    or second_interval[1] <= first_interval[0]
                )
                assert (
                    intervals_nonoverlapping
                ), "Intervals {} and {} are overlapping.".format(
                    first_interval, second_interval
                )

    @ray.remote(num_gpus=1)
    def f1():
        t1 = time.monotonic()
        time.sleep(0.1)
        t2 = time.monotonic()
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        assert gpu_ids[0] in range(num_gpus_per_raylet)
        return (
            ray._private.worker.global_worker.node.unique_id,
            tuple(gpu_ids),
            [t1, t2],
        )

    @ray.remote(num_gpus=2)
    def f2():
        t1 = time.monotonic()
        time.sleep(0.1)
        t2 = time.monotonic()
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 2
        assert gpu_ids[0] in range(num_gpus_per_raylet)
        assert gpu_ids[1] in range(num_gpus_per_raylet)
        return (
            ray._private.worker.global_worker.node.unique_id,
            tuple(gpu_ids),
            [t1, t2],
        )

    @ray.remote(num_gpus=1)
    class Actor1:
        def __init__(self):
            self.gpu_ids = ray.get_gpu_ids()
            assert len(self.gpu_ids) == 1
            assert self.gpu_ids[0] in range(num_gpus_per_raylet)

        def get_location_and_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids
            return (
                ray._private.worker.global_worker.node.unique_id,
                tuple(self.gpu_ids),
            )

    def locations_to_intervals_for_many_tasks():
        # Launch a bunch of GPU tasks.
        locations_ids_and_intervals = ray.get(
            [f1.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)]
            + [f2.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)]
            + [f1.remote() for _ in range(5 * num_nodes * num_gpus_per_raylet)]
        )

        locations_to_intervals = collections.defaultdict(lambda: [])
        for location, gpu_ids, interval in locations_ids_and_intervals:
            for gpu_id in gpu_ids:
                locations_to_intervals[(location, gpu_id)].append(interval)
        return locations_to_intervals

    # Run a bunch of GPU tasks.
    locations_to_intervals = locations_to_intervals_for_many_tasks()
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
    # For each GPU, verify that the set of tasks that used this specific
    # GPU did not overlap in time.
    for locations in locations_to_intervals:
        check_intervals_non_overlapping(locations_to_intervals[locations])
    # Make sure that the actor's GPU was not used.
    assert actor_location not in locations_to_intervals

    # Create more actors to fill up all the GPUs.
    more_actors = [Actor1.remote() for _ in range(num_nodes * num_gpus_per_raylet - 1)]
    # Wait for the actors to finish being created.
    ray.get([actor.get_location_and_ids.remote() for actor in more_actors])

    # Now if we run some GPU tasks, they should not be scheduled.
    results = [f1.remote() for _ in range(30)]
    ready_ids, remaining_ids = ray.wait(results, timeout=1.0)
    assert len(ready_ids) == 0


def test_actors_and_tasks_with_gpus_version_two(shutdown_only):
    # Create tasks and actors that both use GPUs and make sure that they
    # are given different GPUs
    num_gpus = 4

    ray.init(
        num_cpus=(num_gpus + 1),
        num_gpus=num_gpus,
        object_store_memory=int(150 * 1024 * 1024),
    )

    # The point of this actor is to record which GPU IDs have been seen. We
    # can't just return them from the tasks, because the tasks don't return
    # for a long time in order to make sure the GPU is not released
    # prematurely.
    @ray.remote
    class RecordGPUs:
        def __init__(self):
            self.gpu_ids_seen = []
            self.num_calls = 0

        def add_ids(self, gpu_ids):
            self.gpu_ids_seen += gpu_ids
            self.num_calls += 1

        def get_gpu_ids_and_calls(self):
            return self.gpu_ids_seen, self.num_calls

    @ray.remote(num_gpus=1)
    def f(record_gpu_actor):
        gpu_ids = ray.get_gpu_ids()
        assert len(gpu_ids) == 1
        record_gpu_actor.add_ids.remote(gpu_ids)
        # Sleep for a long time so that the GPU never gets released. This task
        # will be killed by ray.shutdown() before it actually finishes.
        time.sleep(1000)

    @ray.remote(num_gpus=1)
    class Actor:
        def __init__(self, record_gpu_actor):
            self.gpu_ids = ray.get_gpu_ids()
            assert len(self.gpu_ids) == 1
            record_gpu_actor.add_ids.remote(self.gpu_ids)

        def check_gpu_ids(self):
            assert ray.get_gpu_ids() == self.gpu_ids

    record_gpu_actor = RecordGPUs.remote()

    actors = []
    actor_results = []
    for _ in range(num_gpus // 2):
        f.remote(record_gpu_actor)
        a = Actor.remote(record_gpu_actor)
        actor_results.append(a.check_gpu_ids.remote())
        # Prevent the actor handle from going out of scope so that its GPU
        # resources don't get released.
        actors.append(a)

    # Make sure that the actor method calls succeeded.
    ray.get(actor_results)

    start_time = time.time()
    while time.time() - start_time < 30:
        seen_gpu_ids, num_calls = ray.get(
            record_gpu_actor.get_gpu_ids_and_calls.remote()
        )
        if num_calls == num_gpus:
            break
    assert set(seen_gpu_ids) == set(range(num_gpus))


def test_blocking_actor_task(shutdown_only):
    ray.init(num_cpus=1, num_gpus=1, object_store_memory=int(150 * 1024 * 1024))

    @ray.remote(num_gpus=1)
    def f():
        return 1

    @ray.remote
    class Foo:
        def __init__(self):
            pass

        def blocking_method(self):
            ray.get(f.remote())

    # Make sure we can execute a blocking actor method even if there is
    # only one CPU.
    actor = Foo.remote()
    ray.get(actor.blocking_method.remote())

    @ray.remote(num_cpus=1)
    class CPUFoo:
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
    class GPUFoo:
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


def test_lifetime_and_transient_resources(ray_start_regular):
    # This actor acquires resources only when running methods.
    @ray.remote
    class Actor1:
        def method(self):
            pass

    # This actor acquires resources for its lifetime.
    @ray.remote(num_cpus=1)
    class Actor2:
        def method(self):
            pass

    actor1s = [Actor1.remote() for _ in range(10)]
    ray.get([a.method.remote() for a in actor1s])

    actor2s = [Actor2.remote() for _ in range(2)]
    results = [a.method.remote() for a in actor2s]
    ready_ids, remaining_ids = ray.wait(results, num_returns=len(results), timeout=5.0)
    assert len(ready_ids) == 1


def test_custom_label_placement(ray_start_cluster):
    cluster = ray_start_cluster
    custom_resource1_node = cluster.add_node(
        num_cpus=2, resources={"CustomResource1": 2}
    )
    custom_resource2_node = cluster.add_node(
        num_cpus=2, resources={"CustomResource2": 2}
    )
    ray.init(address=cluster.address)

    @ray.remote(resources={"CustomResource1": 1})
    class ResourceActor1:
        def get_location(self):
            return ray._private.worker.global_worker.node.unique_id

    @ray.remote(resources={"CustomResource2": 1})
    class ResourceActor2:
        def get_location(self):
            return ray._private.worker.global_worker.node.unique_id

    # Create some actors.
    actors1 = [ResourceActor1.remote() for _ in range(2)]
    actors2 = [ResourceActor2.remote() for _ in range(2)]
    locations1 = ray.get([a.get_location.remote() for a in actors1])
    locations2 = ray.get([a.get_location.remote() for a in actors2])
    for location in locations1:
        assert location == custom_resource1_node.unique_id
    for location in locations2:
        assert location == custom_resource2_node.unique_id


def test_creating_more_actors_than_resources(shutdown_only):
    ray.init(num_cpus=10, num_gpus=2, resources={"CustomResource1": 1})

    @ray.remote(num_gpus=1)
    class ResourceActor1:
        def method(self):
            return ray.get_gpu_ids()[0]

    @ray.remote(resources={"CustomResource1": 1})
    class ResourceActor2:
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
        object_ref = actor.method.remote()
        results.append(object_ref)
        # Wait for the task to execute. We do this because otherwise it may
        # be possible for the __ray_terminate__ task to execute before the
        # method.
        ray.wait([object_ref])

    ray.get(results)


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
