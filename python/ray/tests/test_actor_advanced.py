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
import ray.cluster_utils
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    run_string_as_driver,
    get_non_head_nodes,
    kill_actor_and_wait_for_failure,
    make_global_state_accessor,
    SignalActor,
    wait_for_condition,
    wait_for_pid_to_exit,
    convert_actor_state,
)
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
        if len(names) == num_nodes and all(count >= minimum_count for count in counts):
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


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "resources": {"actor": 1},
            "num_cpus": 2,
        }
    ],
    indirect=True,
)
def test_deleted_actor_no_restart(ray_start_regular):
    @ray.remote(resources={"actor": 1}, max_restarts=3)
    class Actor:
        def method(self):
            return 1

        def getpid(self):
            return os.getpid()

    @ray.remote
    def f(actor, signal):
        ray.get(signal.wait.remote())
        return ray.get(actor.method.remote())

    signal = SignalActor.remote()
    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    # Pass the handle to another task that cannot run yet.
    x_id = f.remote(a, signal)
    # Delete the original handle. The actor should not get killed yet.
    del a

    # Once the task finishes, the actor process should get killed.
    ray.get(signal.send.remote())
    assert ray.get(x_id) == 1
    wait_for_pid_to_exit(pid)

    # Create another actor with the same resource requirement to make sure the
    # old one was not restarted.
    a = Actor.remote()
    pid = ray.get(a.getpid.remote())


def test_exception_raised_when_actor_node_dies(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote(max_restarts=0)
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
    while ray.get(actor.node_id.remote()) != remote_node.unique_id:
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


def test_actor_init_fails(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    remote_node = cluster.add_node()

    @ray.remote(max_restarts=1, max_task_retries=-1)
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

    @ray.remote(max_restarts=1)
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
    # Make sure that we can get the results from the restarted actor.
    results = ray.get(results)


def setup_counter_actor(
    test_checkpoint=False, save_exception=False, resume_exception=False
):
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
    forks = [fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)]
    ray.wait(forks, num_returns=len(forks))
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding raylet to exit.
    # TODO: kill raylet instead once this test is not skipped.
    get_non_head_nodes(cluster)[0].kill_plasma_store(wait=True)

    # Check that the actor did not restore from a checkpoint.
    assert not ray.get(counter.test_restore.remote())
    # Check that we can submit another call on the actor and get the
    # correct counter result.
    x = ray.get(counter.inc.remote())
    assert x == count + 1


@pytest.mark.skip("This test does not work yet.")
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
    forks = [fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)]
    ray.wait(forks, num_returns=len(forks))
    ray.wait([counter.__ray_checkpoint__.remote()])
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding raylet to exit.
    # TODO: kill raylet instead once this test is not skipped.
    get_non_head_nodes(cluster)[0].kill_plasma_store(wait=True)

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
    forks = [fork_many_incs.remote(counter, num_incs) for _ in range(num_iters)]
    ray.wait(forks, num_returns=len(forks))
    count += num_incs * num_iters

    # Kill the second plasma store to get rid of the cached objects and
    # trigger the corresponding raylet to exit.
    # TODO: kill raylet instead once this test is not skipped.
    get_non_head_nodes(cluster)[0].kill_plasma_store(wait=True)

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
    forks = [fork.remote(queue, i, num_items_per_fork) for i in range(num_forks)]
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
    forks = [fork.remote(new_queue, i, num_items_per_fork) for i in range(num_forks)]
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
        nested_fork.remote(queue, i, num_items_per_fork) for i in range(0, num_forks, 2)
    ]
    ray.get(forks)
    # Check that all tasks from all handles have completed.
    items = ray.get(queue.read.remote())
    for i in range(num_forks):
        filtered_items = [item[1] for item in items if item[0] == i]
        assert filtered_items == list(range(num_items_per_fork))


@pytest.mark.skip("Garbage collection for distributed actor handles not implemented.")
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

    # Currently, calling ray.put on an actor handle is allowed, but is
    # there a good use case?
    counter = Counter.remote()
    counter_id = ray.put(counter)
    new_counter = ray.get(counter_id)
    assert ray.get(new_counter.inc.remote()) == 1
    assert ray.get(counter.inc.remote()) == 2
    assert ray.get(new_counter.inc.remote()) == 3

    ray.get(f.remote())


def test_named_but_not_detached(ray_start_regular):
    address = ray_start_regular["address"]

    driver_script = """
import ray
ray.init(address="{}")

@ray.remote
class NotDetached:
    def ping(self):
        return "pong"

actor = NotDetached.options(name="actor").remote()
assert ray.get(actor.ping.remote()) == "pong"
handle = ray.get_actor("actor")
assert ray.util.list_named_actors() == ["actor"]
assert ray.get(handle.ping.remote()) == "pong"
""".format(
        address
    )

    # Creates and kills actor once the driver exits.
    run_string_as_driver(driver_script)

    # Must raise an exception since lifetime is not detached.
    with pytest.raises(Exception):
        assert not ray.util.list_named_actors()
        detached_actor = ray.get_actor("actor")
        ray.get(detached_actor.ping.remote())

    # Check that the names are reclaimed after actors die.

    def check_name_available(name):
        try:
            ray.get_actor(name)
            return False
        except ValueError:
            return True

    @ray.remote
    class A:
        pass

    a = A.options(name="my_actor_1").remote()
    ray.kill(a, no_restart=True)
    wait_for_condition(lambda: check_name_available("my_actor_1"))

    b = A.options(name="my_actor_2").remote()
    del b
    wait_for_condition(lambda: check_name_available("my_actor_2"))


def test_detached_actor(ray_start_regular):
    @ray.remote
    class DetachedActor:
        def ping(self):
            return "pong"

    with pytest.raises(TypeError):
        DetachedActor._remote(lifetime="detached", name=1)

    with pytest.raises(ValueError, match="Actor name cannot be an empty string"):
        DetachedActor._remote(lifetime="detached", name="")

    with pytest.raises(ValueError):
        DetachedActor._remote(lifetime="detached", name="hi", namespace="")

    with pytest.raises(TypeError):
        DetachedActor._remote(lifetime="detached", name="hi", namespace=2)

    d = DetachedActor._remote(lifetime="detached", name="d_actor")
    assert ray.get(d.ping.remote()) == "pong"

    with pytest.raises(ValueError, match="Please use a different name"):
        DetachedActor._remote(lifetime="detached", name="d_actor")

    address = ray_start_regular["address"]

    get_actor_name = "d_actor"
    create_actor_name = "DetachedActor"
    driver_script = """
import ray
ray.init(address="{}", namespace="default_test_namespace")

name = "{}"
assert ray.util.list_named_actors() == [name]
existing_actor = ray.get_actor(name)
assert ray.get(existing_actor.ping.remote()) == "pong"

@ray.remote
def foo():
    return "bar"

@ray.remote
class NonDetachedActor:
    def foo(self):
        return "bar"

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong"

    def foobar(self):
        actor = NonDetachedActor.remote()
        return ray.get([foo.remote(), actor.foo.remote()])

actor = DetachedActor._remote(lifetime="detached", name="{}")
ray.get(actor.ping.remote())
""".format(
        address, get_actor_name, create_actor_name
    )

    run_string_as_driver(driver_script)
    assert len(ray.util.list_named_actors()) == 2
    assert get_actor_name in ray.util.list_named_actors()
    assert create_actor_name in ray.util.list_named_actors()
    detached_actor = ray.get_actor(create_actor_name)
    assert ray.get(detached_actor.ping.remote()) == "pong"
    # Verify that a detached actor is able to create tasks/actors
    # even if the driver of the detached actor has exited.
    assert ray.get(detached_actor.foobar.remote()) == ["bar", "bar"]


def test_detached_actor_cleanup(ray_start_regular):
    @ray.remote
    class DetachedActor:
        def ping(self):
            return "pong"

    dup_actor_name = "actor"

    def create_and_kill_actor(actor_name):
        # Make sure same name is creatable after killing it.
        detached_actor = DetachedActor.options(
            lifetime="detached", name=actor_name
        ).remote()
        # Wait for detached actor creation.
        assert ray.get(detached_actor.ping.remote()) == "pong"
        del detached_actor
        assert ray.util.list_named_actors() == [dup_actor_name]
        detached_actor = ray.get_actor(dup_actor_name)
        ray.kill(detached_actor)
        # Wait until actor dies.
        actor_status = ray.state.actors(actor_id=detached_actor._actor_id.hex())
        max_wait_time = 10
        wait_time = 0
        while actor_status["State"] != convert_actor_state(
            gcs_utils.ActorTableData.DEAD
        ):
            actor_status = ray.state.actors(actor_id=detached_actor._actor_id.hex())
            time.sleep(1.0)
            wait_time += 1
            if wait_time >= max_wait_time:
                assert None, "It took too much time to kill an actor: {}".format(
                    detached_actor._actor_id
                )

    create_and_kill_actor(dup_actor_name)

    # This shouldn't be broken because actor
    # name should have been cleaned up from GCS.
    create_and_kill_actor(dup_actor_name)

    address = ray_start_regular["address"]
    driver_script = """
import ray
import ray._private.gcs_utils as gcs_utils
import time
from ray._private.test_utils import convert_actor_state
ray.init(address="{}", namespace="default_test_namespace")

@ray.remote
class DetachedActor:
    def ping(self):
        return "pong"

# Make sure same name is creatable after killing it.
detached_actor = DetachedActor.options(lifetime="detached", name="{}").remote()
assert ray.get(detached_actor.ping.remote()) == "pong"
ray.kill(detached_actor)
# Wait until actor dies.
actor_status = ray.state.actors(actor_id=detached_actor._actor_id.hex())
max_wait_time = 10
wait_time = 0
while actor_status["State"] != convert_actor_state(gcs_utils.ActorTableData.DEAD): # noqa
    actor_status = ray.state.actors(actor_id=detached_actor._actor_id.hex())
    time.sleep(1.0)
    wait_time += 1
    if wait_time >= max_wait_time:
        assert None, (
            "It took too much time to kill an actor")
""".format(
        address, dup_actor_name
    )

    run_string_as_driver(driver_script)
    # Make sure we can create a detached actor created/killed
    # at other scripts.
    create_and_kill_actor(dup_actor_name)


@pytest.mark.parametrize("ray_start_regular", [{"local_mode": True}], indirect=True)
def test_detached_actor_local_mode(ray_start_regular):
    RETURN_VALUE = 3

    @ray.remote
    class Y:
        def f(self):
            return RETURN_VALUE

    Y.options(lifetime="detached", name="test").remote()
    assert ray.util.list_named_actors() == ["test"]
    y = ray.get_actor("test")
    assert ray.get(y.f.remote()) == RETURN_VALUE

    ray.kill(y)
    assert not ray.util.list_named_actors()
    with pytest.raises(ValueError):
        ray.get_actor("test")


@pytest.mark.parametrize("ray_start_regular", [{"local_mode": True}], indirect=True)
def test_get_actor_local_mode(ray_start_regular):
    @ray.remote
    class A:
        def hi(self):
            return "hi"

    a = A.options(name="hi").remote()  # noqa: F841
    b = ray.get_actor("hi")
    assert ray.get(b.hi.remote()) == "hi"


@pytest.mark.parametrize(
    "ray_start_cluster",
    [{"num_cpus": 3, "num_nodes": 1, "resources": {"first_node": 5}}],
    indirect=True,
)
def test_detached_actor_cleanup_due_to_failure(ray_start_cluster):
    cluster = ray_start_cluster
    node = cluster.add_node(resources={"second_node": 1})
    cluster.wait_for_nodes()

    @ray.remote
    class DetachedActor:
        def ping(self):
            return "pong"

        def kill_itself(self):
            # kill itself.
            os._exit(0)

    worker_failure_actor_name = "worker_failure_actor_name"
    node_failure_actor_name = "node_failure_actor_name"

    def wait_until_actor_dead(handle):
        actor_status = ray.state.actors(actor_id=handle._actor_id.hex())
        max_wait_time = 10
        wait_time = 0
        while actor_status["State"] != convert_actor_state(
            gcs_utils.ActorTableData.DEAD
        ):
            actor_status = ray.state.actors(actor_id=handle._actor_id.hex())
            time.sleep(1.0)
            wait_time += 1
            if wait_time >= max_wait_time:
                assert None, "It took too much time to kill an actor: {}".format(
                    handle._actor_id
                )

    def create_detached_actor_blocking(actor_name, schedule_in_second_node=False):
        resources = {"second_node": 1} if schedule_in_second_node else {"first_node": 1}
        actor_handle = DetachedActor.options(
            lifetime="detached", name=actor_name, resources=resources
        ).remote()
        # Wait for detached actor creation.
        assert ray.get(actor_handle.ping.remote()) == "pong"
        return actor_handle

    # Name should be cleaned when workers fail
    deatched_actor = create_detached_actor_blocking(worker_failure_actor_name)
    deatched_actor.kill_itself.remote()
    wait_until_actor_dead(deatched_actor)
    # Name should be available now.
    deatched_actor = create_detached_actor_blocking(worker_failure_actor_name)
    assert ray.get(deatched_actor.ping.remote()) == "pong"

    # Name should be cleaned when nodes fail.
    deatched_actor = create_detached_actor_blocking(
        node_failure_actor_name, schedule_in_second_node=True
    )
    cluster.remove_node(node)
    wait_until_actor_dead(deatched_actor)
    # Name should be available now.
    deatched_actor = create_detached_actor_blocking(node_failure_actor_name)
    assert ray.get(deatched_actor.ping.remote()) == "pong"


# This test verifies actor creation task failure will not
# hang the caller.
def test_actor_creation_task_crash(ray_start_regular):
    # Test actor death in constructor.
    @ray.remote(max_restarts=0)
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

    # Test an actor can be restarted successfully
    # afte it dies in its constructor.
    @ray.remote(max_restarts=3)
    class RestartableActor:
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
            _internal_kv_put("count", str(count), True)

    # Verify we can get the object successfully.
    ra = RestartableActor.remote()
    ray.get(ra.f.remote())


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 2, "resources": {"a": 1}}], indirect=True
)
def test_pending_actor_removed_by_owner(ray_start_regular):
    # Verify when an owner of pending actors is killed, the actor resources
    # are correctly returned.

    @ray.remote(num_cpus=1, resources={"a": 1})
    class A:
        def __init__(self):
            self.actors = []

        def create_actors(self):
            self.actors = [B.remote() for _ in range(2)]

    @ray.remote(resources={"a": 1})
    class B:
        def ping(self):
            return True

    @ray.remote(resources={"a": 1})
    def f():
        return True

    a = A.remote()
    # Create pending actors
    ray.get(a.create_actors.remote())

    # Owner is dead. pending actors should be killed
    # and raylet should return workers correctly.
    del a
    a = B.remote()
    assert ray.get(a.ping.remote())
    ray.kill(a)
    assert ray.get(f.remote())


def test_pickling_actor_handle(ray_start_regular_shared):
    @ray.remote
    class Foo:
        def method(self):
            pass

    f = Foo.remote()
    new_f = ray.worker.pickle.loads(ray.worker.pickle.dumps(f))
    # Verify that we can call a method on the unpickled handle. TODO(rkn):
    # we should also test this from a different driver.
    ray.get(new_f.method.remote())


def test_pickled_actor_handle_call_in_method_twice(ray_start_regular_shared):
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


def test_kill(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def hang(self):
            while True:
                time.sleep(1)

    actor = Actor.remote()
    result = actor.hang.remote()
    ready, _ = ray.wait([result], timeout=0.5)
    assert len(ready) == 0
    kill_actor_and_wait_for_failure(actor)

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(result)

    with pytest.raises(ValueError):
        ray.kill("not_an_actor_handle")


def test_get_actor_no_input(ray_start_regular_shared):
    for bad_name in [None, "", "    "]:
        with pytest.raises(ValueError):
            ray.get_actor(bad_name)


def test_actor_resource_demand(shutdown_only):
    ray.shutdown()
    cluster = ray.init(num_cpus=3)
    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote(num_cpus=2)
    class Actor:
        def foo(self):
            return "ok"

    a = Actor.remote()
    ray.get(a.foo.remote())
    time.sleep(1)

    message = global_state_accessor.get_all_resource_usage()
    resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)

    # The actor is scheduled so there should be no more demands left.
    assert len(resource_usages.resource_load_by_shape.resource_demands) == 0

    @ray.remote(num_cpus=80)
    class Actor2:
        pass

    actors = []
    actors.append(Actor2.remote())
    time.sleep(1)

    # This actor cannot be scheduled.
    message = global_state_accessor.get_all_resource_usage()
    resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)
    assert len(resource_usages.resource_load_by_shape.resource_demands) == 1
    assert resource_usages.resource_load_by_shape.resource_demands[0].shape == {
        "CPU": 80.0
    }
    assert (
        resource_usages.resource_load_by_shape.resource_demands[
            0
        ].num_infeasible_requests_queued
        == 1
    )

    actors.append(Actor2.remote())
    time.sleep(1)

    # Two actors cannot be scheduled.
    message = global_state_accessor.get_all_resource_usage()
    resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)
    assert len(resource_usages.resource_load_by_shape.resource_demands) == 1
    assert (
        resource_usages.resource_load_by_shape.resource_demands[
            0
        ].num_infeasible_requests_queued
        == 2
    )

    global_state_accessor.disconnect()


def test_kill_pending_actor_with_no_restart_true():
    cluster = ray.init()
    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote(resources={"WORKER": 1.0})
    class PendingActor:
        pass

    # Kill actor with `no_restart=True`.
    actor = PendingActor.remote()
    # TODO(ffbin): The raylet doesn't guarantee the order when dealing with
    # RequestWorkerLease and CancelWorkerLease. If we kill the actor
    # immediately after creating the actor, we may not be able to clean up
    # the request cached by the raylet.
    # See https://github.com/ray-project/ray/issues/13545 for details.
    time.sleep(1)
    ray.kill(actor, no_restart=True)

    def condition1():
        message = global_state_accessor.get_all_resource_usage()
        resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)
        if len(resource_usages.resource_load_by_shape.resource_demands) == 0:
            return True
        return False

    # Actor is dead, so the infeasible task queue length is 0.
    wait_for_condition(condition1, timeout=10)

    global_state_accessor.disconnect()
    ray.shutdown()


def test_kill_pending_actor_with_no_restart_false():
    cluster = ray.init()
    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote(resources={"WORKER": 1.0}, max_restarts=1)
    class PendingActor:
        pass

    # Kill actor with `no_restart=False`.
    actor = PendingActor.remote()
    # TODO(ffbin): The raylet doesn't guarantee the order when dealing with
    # RequestWorkerLease and CancelWorkerLease. If we kill the actor
    # immediately after creating the actor, we may not be able to clean up
    # the request cached by the raylet.
    # See https://github.com/ray-project/ray/issues/13545 for details.
    time.sleep(1)
    ray.kill(actor, no_restart=False)

    def condition1():
        message = global_state_accessor.get_all_resource_usage()
        resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)
        if len(resource_usages.resource_load_by_shape.resource_demands) == 0:
            return False
        return True

    # Actor restarts, so the infeasible task queue length is 1.
    wait_for_condition(condition1, timeout=10)

    # Kill actor again and actor is dead,
    # so the infeasible task queue length is 0.
    ray.kill(actor, no_restart=False)

    def condition2():
        message = global_state_accessor.get_all_resource_usage()
        resource_usages = gcs_utils.ResourceUsageBatchData.FromString(message)
        if len(resource_usages.resource_load_by_shape.resource_demands) == 0:
            return True
        return False

    wait_for_condition(condition2, timeout=10)

    global_state_accessor.disconnect()
    ray.shutdown()


def test_actor_timestamps(ray_start_regular):
    @ray.remote
    class Foo:
        def get_id(self):
            return ray.get_runtime_context().actor_id.hex()

        def kill_self(self):
            sys.exit(1)

    def graceful_exit():
        actor = Foo.remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray.state.actors()[actor_id]
        time.sleep(1)
        del actor
        time.sleep(1)
        state_after_ending = ray.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]

        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        lapsed = end_time - start_time

        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"
        assert 500 < lapsed < 1500, f"Start: {start_time}, End: {end_time}"

    def not_graceful_exit():
        actor = Foo.remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray.state.actors()[actor_id]
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        state_after_ending = ray.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]

        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        lapsed = end_time - start_time

        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"
        assert 500 < lapsed < 1500, f"Start: {start_time}, End: {end_time}"

    def restarted():
        actor = Foo.options(max_restarts=1, max_task_retries=-1).remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray.state.actors()[actor_id]
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        state_after_ending = ray.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]

        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        lapsed = end_time - start_time

        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"
        assert 1500 < lapsed < 2500, f"Start: {start_time}, End: {end_time}"

    graceful_exit()
    not_graceful_exit()
    restarted()


def test_actor_namespace_access(ray_start_regular):
    @ray.remote
    class A:
        def hi(self):
            return "hi"

    A.options(name="actor_in_current_namespace", lifetime="detached").remote()
    A.options(name="actor_name", namespace="namespace", lifetime="detached").remote()
    ray.get_actor("actor_in_current_namespace")  # => works
    ray.get_actor("actor_name", namespace="namespace")  # => works
    match_str = r"Failed to look up actor with name.*"
    with pytest.raises(ValueError, match=match_str):
        ray.get_actor("actor_name")  # => errors


def test_get_actor_after_killed(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote
    class A:
        def ready(self):
            return True

    actor = A.options(name="actor", namespace="namespace", lifetime="detached").remote()
    ray.kill(actor)
    with pytest.raises(ValueError):
        ray.get_actor("actor", namespace="namespace")

    actor = A.options(
        name="actor_2",
        namespace="namespace",
        lifetime="detached",
        max_restarts=1,
        max_task_retries=-1,
    ).remote()
    ray.kill(actor, no_restart=False)
    assert ray.get(ray.get_actor("actor_2", namespace="namespace").ready.remote())


def test_get_actor_race_condition(shutdown_only):
    @ray.remote
    class Actor:
        def ping(self):
            return "ok"

    @ray.remote
    def getter(name):
        try:
            try:
                actor = ray.get_actor(name)
            except Exception:
                print("Get failed, trying to create", name)
                actor = Actor.options(name=name, lifetime="detached").remote()
        except Exception:
            print("Someone else created it, trying to get")
            actor = ray.get_actor(name)
        result = ray.get(actor.ping.remote())
        return result

    def do_run(name, concurrency=4):
        name = "actor_" + str(name)
        tasks = [getter.remote(name) for _ in range(concurrency)]
        result = ray.get(tasks)
        ray.kill(ray.get_actor(name))  # Cleanup
        return result

    for i in range(50):
        CONCURRENCY = 8
        results = do_run(i, concurrency=CONCURRENCY)
        assert ["ok"] * CONCURRENCY == results


def test_get_actor_in_remote_workers(ray_start_cluster):
    """Make sure we can get and create actors without
    race condition in a remote worker.

    Check https://github.com/ray-project/ray/issues/20092. # noqa
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address, namespace="xxx")

    @ray.remote(num_cpus=0)
    class RemoteProc:
        def __init__(self):
            pass

        def procTask(self, a, b):
            print("[%s]-> %s" % (a, b))
            return a, b

    @ray.remote
    def submit_named_actors():
        RemoteProc.options(
            name="test", lifetime="detached", max_concurrency=10, namespace="xxx"
        ).remote()
        proc = ray.get_actor("test", namespace="xxx")
        ray.get(proc.procTask.remote(1, 2))
        # Should be able to create an actor with the same name
        # immediately after killing it.
        ray.kill(proc)
        RemoteProc.options(
            name="test", lifetime="detached", max_concurrency=10, namespace="xxx"
        ).remote()
        proc = ray.get_actor("test", namespace="xxx")
        return ray.get(proc.procTask.remote(1, 2))

    assert (1, 2) == ray.get(submit_named_actors.remote())


if __name__ == "__main__":
    import pytest

    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
