import os
import sys
import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

import pytest

import ray
import ray._private.gcs_utils as gcs_utils
from ray.util.state import list_actors
import ray.cluster_utils
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import (
    convert_actor_state,
    kill_actor_and_wait_for_failure,
    make_global_state_accessor,
    run_string_as_driver,
    wait_for_pid_to_exit,
)
from ray._private.ray_constants import gcs_actor_scheduling_enabled
from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_put


def test_actors_on_nodes_with_no_cpus(ray_start_no_cpu):
    @ray.remote
    class Foo:
        def method(self):
            pass

    f = Foo.remote()
    ready_ids, _ = ray.wait([f.method.remote()], timeout=0.1)
    assert ready_ids == []


@pytest.mark.skipif(
    gcs_actor_scheduling_enabled(),
    reason="This test relies on gcs server randomly choosing raylets "
    + "for actors without required resources, which is only supported by "
    + "raylet-based actor scheduler. The same test logic for gcs-based "
    + "actor scheduler can be found at `test_actor_distribution_balance`.",
)
def test_actor_load_balancing(ray_start_cluster):
    """Check that actor scheduling is load balanced across worker nodes."""
    cluster = ray_start_cluster
    worker_node_ids = set()
    for i in range(2):
        worker_node_ids.add(cluster.add_node(num_cpus=1).node_id)

    ray.init(address=cluster.address)

    @ray.remote
    class Actor:
        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

    # Schedule a group of actors, ensure that the actors are spread between all nodes.
    node_ids = ray.get([Actor.remote().get_node_id.remote() for _ in range(10)])
    assert set(node_ids) == worker_node_ids


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

    @ray.remote(max_restarts=0, scheduling_strategy="SPREAD")
    class Counter:
        def __init__(self):
            self.x = 0

        def node_id(self):
            return ray._private.worker.global_worker.node.unique_id

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


def test_actor_fail_during_constructor_restart(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    worker_nodes = {
        node.node_id: node for node in [cluster.add_node() for _ in range(2)]
    }

    @ray.remote
    class ReportNodeIDActor:
        def __init__(self):
            self._reported_node_id = None

        def report(self, node_id: str):
            self._reported_node_id = node_id

        def get(self) -> Optional[str]:
            return self._reported_node_id

    # Pin these actors to the head node so they don't crash.
    # Occupy the 1 CPU on the head node so the actor below is forced to a worker node.
    pin_head_resources = {"node:__internal_head__": 0.1}
    report_node_id_actor = ReportNodeIDActor.options(
        num_cpus=0.5, resources=pin_head_resources
    ).remote()
    signal = SignalActor.options(
        num_cpus=0.5,
        resources=pin_head_resources,
    ).remote()

    @ray.remote(max_restarts=1, max_task_retries=-1)
    class Actor:
        def __init__(self):
            ray.get(
                report_node_id_actor.report.remote(
                    ray.get_runtime_context().get_node_id()
                )
            )
            ray.get(signal.wait.remote())

    # Create the actor and wait for it to start initializing.
    actor = Actor.remote()
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)
    actor_node_id = ray.get(report_node_id_actor.get.remote())
    assert actor_node_id is not None

    # Kill the worker node.
    cluster.remove_node(worker_nodes[actor_node_id])

    # Verify that the actor was restarted on the other node.
    ray.get(signal.send.remote())
    ray.get(actor.__ray_ready__.remote())
    assert ray.get(report_node_id_actor.get.remote()) != actor_node_id


def test_actor_restart_multiple_callers(ray_start_cluster):
    cluster = ray_start_cluster
    _ = cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)

    _ = cluster.add_node(num_cpus=4)
    actor_worker_node = cluster.add_node(num_cpus=0, resources={"actor": 1})
    cluster.wait_for_nodes()

    @ray.remote(
        num_cpus=0,
        # Only one of the callers should successfully restart the actor.
        max_restarts=1,
        # Retry transient ActorUnavailableErrors.
        max_task_retries=-1,
        # Schedule the actor on actor_worker_node.
        resources={"actor": 1},
    )
    class A:
        def get_node_id(self) -> str:
            return ray.get_runtime_context().get_node_id()

    a = A.remote()

    @ray.remote
    def call_a() -> str:
        return ray.get(a.get_node_id.remote())

    # Run caller tasks in parallel across the other two nodes.
    results = ray.get([call_a.remote() for _ in range(8)])
    assert all(r == actor_worker_node.node_id for r in results), results

    # Kill the node that the actor is running on.
    cluster.remove_node(actor_worker_node)

    # Run caller tasks in parallel again.
    refs = [call_a.remote() for _ in range(8)]
    ready, _ = ray.wait(refs, timeout=0.1)
    assert len(ready) == 0

    # The actor should be restarted once the node becomes available.
    new_actor_worker_node = cluster.add_node(num_cpus=0, resources={"actor": 1})
    results = ray.get(refs)
    assert all(r == new_actor_worker_node.node_id for r in results), results


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
        queue = ray._private.worker.pickle.loads(pickled_queue)
        x = None
        for item in range(num_items):
            x = queue.enqueue.remote(key, item)
        return ray.get(x)

    # Fork num_iters times.
    num_forks = 10
    num_items_per_fork = 100

    # Submit some tasks on the pickled actor handle.
    new_queue = ray._private.worker.pickle.dumps(queue)
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
        actor_status = ray._private.state.actors(
            actor_id=detached_actor._actor_id.hex()
        )
        max_wait_time = 10
        wait_time = 0
        while actor_status["State"] != convert_actor_state(
            gcs_utils.ActorTableData.DEAD
        ):
            actor_status = ray._private.state.actors(
                actor_id=detached_actor._actor_id.hex()
            )
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
actor_status = ray._private.state.actors(actor_id=detached_actor._actor_id.hex())
max_wait_time = 10
wait_time = 0
while actor_status["State"] != convert_actor_state(gcs_utils.ActorTableData.DEAD): # noqa
    actor_status = ray._private.state.actors(actor_id=detached_actor._actor_id.hex())
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
        actor_status = ray._private.state.actors(actor_id=handle._actor_id.hex())
        max_wait_time = 10
        wait_time = 0
        while actor_status["State"] != convert_actor_state(
            gcs_utils.ActorTableData.DEAD
        ):
            actor_status = ray._private.state.actors(actor_id=handle._actor_id.hex())
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
    with pytest.raises(ray.exceptions.RayActorError) as excinfo:
        ray.get(a.f.remote())
    assert excinfo.value.actor_id == a._actor_id.hex()

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
    new_f = ray._private.worker.pickle.loads(ray._private.worker.pickle.dumps(f))
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
            return ray.get_runtime_context().get_actor_id()

        def kill_self(self):
            sys.exit(1)

    def graceful_exit():
        actor = Foo.remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray._private.state.actors()[actor_id]
        time.sleep(1)
        del actor
        time.sleep(1)
        state_after_ending = ray._private.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]
        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"

    def not_graceful_exit():
        actor = Foo.remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray._private.state.actors()[actor_id]
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        state_after_ending = ray._private.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]

        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"

    def restarted():
        actor = Foo.options(max_restarts=1, max_task_retries=-1).remote()
        actor_id = ray.get(actor.get_id.remote())

        state_after_starting = ray._private.state.actors()[actor_id]
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        actor.kill_self.remote()
        time.sleep(1)
        state_after_ending = ray._private.state.actors()[actor_id]

        assert state_after_starting["StartTime"] == state_after_ending["StartTime"]

        start_time = state_after_ending["StartTime"]
        end_time = state_after_ending["EndTime"]
        assert end_time > start_time > 0, f"Start: {start_time}, End: {end_time}"

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

    actor = A.options(name="actor", namespace="namespace").remote()
    ray.kill(actor)
    with pytest.raises(ValueError):
        ray.get_actor("actor", namespace="namespace")

    actor = A.options(
        name="actor_2",
        namespace="namespace",
        max_restarts=1,
        max_task_retries=-1,
    ).remote()
    ray.kill(actor, no_restart=False)
    assert ray.get(ray.get_actor("actor_2", namespace="namespace").ready.remote())


def test_get_actor_from_concurrent_tasks(shutdown_only):
    @ray.remote
    class Actor:
        def get_actor_id(self) -> str:
            return ray.get_runtime_context().get_actor_id()

    actor_name = "test_actor"

    @ray.remote(num_cpus=0)
    def get_or_create_actor():
        try:
            # The first task will try to get the actor but fail (doesn't exist).
            try:
                actor = ray.get_actor(actor_name)
            except Exception:
                print("Get failed, trying to create")
                # Actor must be detached so it outlives this task and other tasks can
                # get a handle to it.
                actor = Actor.options(name=actor_name, lifetime="detached").remote()
        except Exception:
            # Multiple tasks may have reached the creation block above.
            # Only one will succeed and the others will get an error, in which case
            # they fall here and should be able to get the actor handle.
            print("Someone else created it, trying to get")
            actor = ray.get_actor(actor_name)

        return ray.get(actor.get_actor_id.remote())

    # Run 10 concurrent tasks to get or create the same actor.
    # Only one task should succeed at creating it, and all the others should get it.
    assert len(set(ray.get([get_or_create_actor.remote() for _ in range(10)]))) == 1


def test_get_or_create_actor_from_multiple_threads(shutdown_only):
    """Make sure we can create actors in multiple threads without
    race conditions.

    Check https://github.com/ray-project/ray/issues/41324
    """

    @ray.remote
    class Counter:
        def __init__(self):
            self._count = 0

        def inc(self):
            self._count += 1

        def get(self) -> int:
            return self._count

    counter = Counter.remote()

    @ray.remote
    class Actor:
        def __init__(self):
            ray.get(counter.inc.remote())

        def get_actor_id(self) -> str:
            return ray.get_runtime_context().get_actor_id()

    def _create_or_get_actor(*args):
        a = Actor.options(
            name="test_actor",
            get_if_exists=True,
            # Actor must be detached so it outlives this function and other threads
            # can get a handle to it.
            lifetime="detached",
        ).remote()

        return ray.get(a.get_actor_id.remote())

    # Concurrently submit 100 calls to create or get the actor from 10 threads.
    # Ensure that exactly one call actually creates the actor and the other 99 get it.
    with ThreadPoolExecutor(max_workers=10) as tp:
        assert len(set(tp.map(_create_or_get_actor, range(100)))) == 1
        assert ray.get(counter.get.remote()) == 1


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


def test_resource_leak_when_cancel_actor_in_phase_of_creating(ray_start_cluster):
    """Make sure there is no resource leak when cancel an actor in phase of
    creating.

    Check https://github.com/ray-project/ray/issues/27743. # noqa
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self, signal_1, signal_2):
            signal_1.send.remote()
            ray.get(signal_2.wait.remote())
            pass

    signal_1 = SignalActor.remote()
    signal_2 = SignalActor.remote()
    actor = Actor.remote(signal_1, signal_2)

    wait_for_condition(lambda: ray.available_resources()["CPU"] != 2)

    # Checking that the constructor of `Actor`` is invoked.
    ready_ids, _ = ray.wait([signal_1.wait.remote()], timeout=3.0)
    assert len(ready_ids) == 1

    # Kill the actor which is in the phase of creating.
    ray.kill(actor)

    # Ensure there is no resource leak.
    wait_for_condition(lambda: ray.available_resources()["CPU"] == 2)


def test_actor_gc(monkeypatch, shutdown_only):
    MAX_DEAD_ACTOR_CNT = 5
    with monkeypatch.context() as m:
        m.setenv("RAY_maximum_gcs_destroyed_actor_cached_count", MAX_DEAD_ACTOR_CNT)
        ray.init()

        @ray.remote
        class Actor:
            def ready(self):
                pass

        actors = [Actor.remote() for _ in range(10)]
        ray.get([actor.ready.remote() for actor in actors])
        alive_actors = 0
        for a in list_actors():
            if a["state"] == "ALIVE":
                alive_actors += 1
        assert alive_actors == 10
        # Kill actors
        del actors

        def verify_cached_dead_actor_cleaned():
            return len(list_actors()) == MAX_DEAD_ACTOR_CNT  # noqa

        wait_for_condition(verify_cached_dead_actor_cleaned)

        # Test detached actors
        actors = [Actor.options(lifetime="detached").remote() for _ in range(10)]
        ray.get([actor.ready.remote() for actor in actors])
        alive_actors = 0
        for a in list_actors():
            if a["state"] == "ALIVE":
                alive_actors += 1
        assert alive_actors == 10
        # Kill actors
        for actor in actors:
            ray.kill(actor)

        wait_for_condition(verify_cached_dead_actor_cleaned)

        # Test actors created by a driver.

        driver = """
import ray
from ray.util.state import list_actors
ray.init("auto")

@ray.remote
class A:
    def ready(self):
        pass

actors = [A.remote() for _ in range(10)]
ray.get([actor.ready.remote() for actor in actors])
alive_actors = 0
for a in list_actors():
    if a["state"] == "ALIVE":
        alive_actors += 1
assert alive_actors == 10
"""

        run_string_as_driver(driver)
        # Driver exits, so dead actors must be cleaned.
        wait_for_condition(verify_cached_dead_actor_cleaned)
        print(list_actors())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
