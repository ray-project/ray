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
from ray.test_utils import run_string_as_driver
from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_put


def test_remote_functions_not_scheduled_on_actors(ray_start_regular):
    # Make sure that regular remote functions are not scheduled on actors.

    @ray.remote
    class Actor:
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
    class Foo:
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
    class Actor1:
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
    class Actor:
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
    class Counter:
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
def test_actor_init_fails(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote(max_reconstructions=1)
    class Counter:
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


def test_reconstruction_suppression(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    num_nodes = 5
    worker_nodes = [cluster.add_node() for _ in range(num_nodes)]

    @ray.remote(max_reconstructions=1)
    class Counter:
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
    class Counter:
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


@pytest.fixture
def setup_queue_actor():
    ray.init(num_cpus=1, object_store_memory=int(150 * 1024 * 1024))

    @ray.remote
    class Queue:
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
    class Counter:
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
    class Foo:
        def method(self):
            pass

    f = Foo.remote()
    new_f = ray.worker.pickle.loads(ray.worker.pickle.dumps(f))
    # Verify that we can call a method on the unpickled handle. TODO(rkn):
    # we should also test this from a different driver.
    ray.get(new_f.method.remote())


def test_pickled_actor_handle_call_in_method_twice(ray_start_regular):
    @ray.remote
    class Actor1:
        def f(self):
            return 1

    @ray.remote
    class Actor2:
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
    class Foo:
        def __init__(self):
            self.x = 0

        def method(self):
            self.x += 1
            return self.x

    f1 = Foo.remote()
    # Test saving f.
    ray.util.register_actor("f1", f1)
    # Test getting f.
    f2 = ray.util.get_actor("f1")
    assert f1._actor_id == f2._actor_id

    # Test same name register shall raise error.
    with pytest.raises(ValueError):
        ray.util.register_actor("f1", f2)

    # Test register with wrong object type.
    with pytest.raises(TypeError):
        ray.util.register_actor("f3", 1)

    # Test getting a nonexistent actor.
    with pytest.raises(ValueError):
        ray.util.get_actor("nonexistent")

    # Test method
    assert ray.get(f1.method.remote()) == 1
    assert ray.get(f2.method.remote()) == 2
    assert ray.get(f1.method.remote()) == 3
    assert ray.get(f2.method.remote()) == 4


def test_detached_actor(ray_start_regular):
    @ray.remote
    class DetachedActor:
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
class DetachedActor:
    def ping(self):
        return "pong"

actor = DetachedActor._remote(name="{}", detached=True)
ray.get(actor.ping.remote())
""".format(redis_address, actor_name)

    run_string_as_driver(driver_script)
    detached_actor = ray.util.get_actor(actor_name)
    assert ray.get(detached_actor.ping.remote()) == "pong"


@pytest.mark.parametrize("deprecated_codepath", [False, True])
def test_kill(ray_start_regular, deprecated_codepath):
    @ray.remote
    class Actor:
        def hang(self):
            while True:
                time.sleep(1)

    actor = Actor.remote()
    result = actor.hang.remote()
    ready, _ = ray.wait([result], timeout=0.5)
    assert len(ready) == 0
    if deprecated_codepath:
        actor.__ray_kill__()
    else:
        ray.kill(actor)

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(result)

    if not deprecated_codepath:
        with pytest.raises(ValueError):
            ray.kill("not_an_actor_handle")


# This test verifies actor creation task failure will not
# hang the caller.
def test_actor_creation_task_crash(ray_start_regular):
    # Test actor death in constructor.
    @ray.remote(max_reconstructions=0)
    class Actor:
        def __init__(self):
            print("crash")
            os._exit(0)

        def f(self):
            return "ACTOR OK"

    # Verify an exception is thrown.
    a = Actor.remote()
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.f.remote())

    # Test an actor can be reconstructed successfully
    # afte it dies in its constructor.
    @ray.remote(max_reconstructions=3)
    class ReconstructableActor:
        def __init__(self):
            count = self.get_count()
            count += 1
            # Make it die for the first 2 times.
            if count < 3:
                self.set_count(count)
                print("crash: " + str(count))
                os._exit(0)
            else:
                print("no crash")

        def f(self):
            return "ACTOR OK"

        def get_count(self):
            value = _internal_kv_get("count")
            if value is None:
                count = 0
            else:
                count = int(value)
            return count

        def set_count(self, count):
            _internal_kv_put("count", count, True)

    # Verify we can get the object successfully.
    ra = ReconstructableActor.remote()
    ray.get(ra.f.remote())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
