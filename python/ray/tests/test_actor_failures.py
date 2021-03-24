import asyncio
import collections
import numpy as np
import os
import pytest
import signal
import sys
import time

import ray
import ray.test_utils
import ray.cluster_utils
from ray.test_utils import (
    wait_for_condition,
    wait_for_pid_to_exit,
    generate_system_config_map,
    get_other_nodes,
    new_scheduler_enabled,
    SignalActor,
)

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.fixture
def ray_init_with_task_retry_delay():
    address = ray.init(_system_config={"task_retry_delay_ms": 100})
    yield address
    ray.shutdown()


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "object_store_memory": 150 * 1024 * 1024,
    }],
    indirect=True)
def test_actor_spilled(ray_start_regular):
    object_store_memory = 150 * 1024 * 1024

    @ray.remote
    class Actor:
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
    # been spilled.
    num_success = 0
    for obj in objects:
        val = ray.get(obj)
        assert isinstance(val, np.ndarray), val
        num_success += 1
    # All of objects should've been spilled, so all of them should succeed.
    assert num_success == len(objects)


@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky on Windows.")
def test_actor_restart(ray_init_with_task_retry_delay):
    """Test actor restart when actor process is killed."""

    @ray.remote(max_restarts=1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, exit=False):
            if exit:
                os._exit(-1)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    # Submit some tasks and kill on a task midway through.
    results = [actor.increase.remote(exit=(i == 100)) for i in range(200)]
    # Make sure that all tasks were executed in order before the actor's death.
    i = 1
    while results:
        res = results[0]
        try:
            r = ray.get(res)
            if r != i:
                # Actor restarted at this task without any failed tasks in
                # between.
                break
            results.pop(0)
            i += 1
        except ray.exceptions.RayActorError:
            break
    # Skip any tasks that errored.
    while results:
        try:
            ray.get(results[0])
        except ray.exceptions.RayActorError:
            results.pop(0)
        else:
            break
    # Check all tasks that executed after the restart.
    if results:
        # The actor executed some tasks after the restart.
        i = 1
        while results:
            r = ray.get(results.pop(0))
            assert r == i
            i += 1

        # Check that we can still call the actor.
        result = actor.increase.remote()
        assert ray.get(result) == r + 1
    else:
        # Wait for the actor to restart.
        def ping():
            try:
                ray.get(actor.increase.remote())
                return True
            except ray.exceptions.RayActorError:
                return False

        wait_for_condition(ping)

    # The actor has restarted. Kill actor process one more time.
    actor.increase.remote(exit=True)
    # The actor has exceeded max restarts. All tasks should fail.
    for _ in range(100):
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.increase.remote())

    # Create another actor.
    actor = RestartableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be restarted.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())


def test_actor_restart_with_retry(ray_init_with_task_retry_delay):
    """Test actor restart when actor process is killed."""

    @ray.remote(max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self, delay=0):
            time.sleep(delay)
            self.value += 1
            return self.value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    results = [actor.increase.remote() for _ in range(100)]
    # Kill actor process, while the above task is still being executed.
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)
    # Check that none of the tasks failed and the actor is restarted.
    seq = list(range(1, 101))
    results = ray.get(results)
    failed_task_index = None
    # Make sure that all tasks were executed in order before and after the
    # actor's death.
    for i, res in enumerate(results):
        if res != seq[0]:
            if failed_task_index is None:
                failed_task_index = i
            assert res + failed_task_index == seq[0]
        seq.pop(0)
    # Check that we can still call the actor.
    result = actor.increase.remote()
    assert ray.get(result) == results[-1] + 1

    # kill actor process one more time.
    results = [actor.increase.remote() for _ in range(100)]
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)
    # The actor has exceeded max restarts, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())

    # Create another actor.
    actor = RestartableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be restarted.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.increase.remote())


def test_named_actor_max_task_retries(ray_init_with_task_retry_delay):
    @ray.remote(num_cpus=0)
    class Counter:
        def __init__(self):
            self.count = 0
            self.event = asyncio.Event()

        def increment(self):
            self.count += 1
            self.event.set()

        async def wait_for_count(self, count):
            while True:
                if self.count >= count:
                    return
                await self.event.wait()
                self.event.clear()

    @ray.remote
    class ActorToKill:
        def __init__(self, counter):
            counter.increment.remote()

        def run(self, counter, signal):
            counter.increment.remote()
            ray.get(signal.wait.remote())

    @ray.remote
    class CallingActor:
        def __init__(self):
            self.actor = ray.get_actor("a")

        def call_other(self, counter, signal):
            return ray.get(self.actor.run.remote(counter, signal))

    init_counter = Counter.remote()
    run_counter = Counter.remote()
    signal = SignalActor.remote()

    # Start the two actors, wait for ActorToKill's constructor to run.
    a = ActorToKill.options(
        name="a", max_restarts=-1, max_task_retries=-1).remote(init_counter)
    c = CallingActor.remote()
    ray.get(init_counter.wait_for_count.remote(1), timeout=30)

    # Signal the CallingActor to call ActorToKill, wait for it to be running,
    # then kill ActorToKill.
    # Verify that this causes ActorToKill's constructor to run a second time
    # and the run method to begin a second time.
    ref = c.call_other.remote(run_counter, signal)
    ray.get(run_counter.wait_for_count.remote(1), timeout=30)
    ray.kill(a, no_restart=False)
    ray.get(init_counter.wait_for_count.remote(2), timeout=30)
    ray.get(run_counter.wait_for_count.remote(2), timeout=30)

    # Signal the run method to finish, verify that the CallingActor returns.
    signal.send.remote()
    ray.get(ref, timeout=30)


def test_actor_restart_on_node_failure(ray_start_cluster):
    config = {
        "num_heartbeats_timeout": 10,
        "raylet_heartbeat_period_milliseconds": 100,
        "object_timeout_milliseconds": 1000,
        "task_retry_delay_ms": 100,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Node to place the actor.
    actor_node = cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be reconstructed at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

        def ready(self):
            return

    actor = RestartableActor.options(lifetime="detached").remote()
    ray.get(actor.ready.remote())
    results = [actor.increase.remote() for _ in range(100)]
    # Kill actor node, while the above task is still being executed.
    cluster.remove_node(actor_node)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    # Check that none of the tasks failed and the actor is restarted.
    seq = list(range(1, 101))
    results = ray.get(results)
    failed_task_index = None
    # Make sure that all tasks were executed in order before and after the
    # actor's death.
    for i, res in enumerate(results):
        elm = seq.pop(0)
        if res != elm:
            if failed_task_index is None:
                failed_task_index = i
            assert res + failed_task_index == elm
    # Check that we can still call the actor.
    result = ray.get(actor.increase.remote())
    assert result == 1 or result == results[-1] + 1


@pytest.mark.skipif(new_scheduler_enabled(), reason="dynamic resources todo")
def test_actor_restart_without_task(ray_start_regular):
    """Test a dead actor can be restarted without sending task to it."""

    @ray.remote(max_restarts=1, resources={"actor": 1})
    class RestartableActor:
        def __init__(self):
            pass

        def get_pid(self):
            return os.getpid()

    @ray.remote(resources={"actor": 1})
    def probe():
        return

    # Returns whether the "actor" resource is available.
    def actor_resource_available():
        p = probe.remote()
        ready, _ = ray.wait([p], timeout=1)
        return len(ready) > 0

    ray.experimental.set_resource("actor", 1)
    actor = RestartableActor.remote()
    wait_for_condition(lambda: not actor_resource_available())
    # Kill the actor.
    pid = ray.get(actor.get_pid.remote())

    p = probe.remote()
    os.kill(pid, SIGKILL)
    ray.get(p)
    wait_for_condition(lambda: not actor_resource_available())


def test_caller_actor_restart(ray_start_regular):
    """Test tasks from a restarted actor can be correctly processed
       by the receiving actor."""

    @ray.remote(max_restarts=1)
    class RestartableActor:
        """An actor that will be restarted at most once."""

        def __init__(self, actor):
            self.actor = actor

        def increase(self):
            return ray.get(self.actor.increase.remote())

        def get_pid(self):
            return os.getpid()

    @ray.remote(max_restarts=1)
    class Actor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

    remote_actor = Actor.remote()
    actor = RestartableActor.remote(remote_actor)
    # Call increase 3 times
    for _ in range(3):
        ray.get(actor.increase.remote())

    # kill the actor.
    # TODO(zhijunfu): use ray.kill instead.
    kill_actor(actor)

    # Check that we can still call the actor.
    assert ray.get(actor.increase.remote()) == 4


def test_caller_task_reconstruction(ray_start_regular):
    """Test a retried task from a dead worker can be correctly processed
       by the receiving actor."""

    @ray.remote(max_retries=5)
    def RetryableTask(actor):
        value = ray.get(actor.increase.remote())
        if value > 2:
            return value
        else:
            os._exit(0)

    @ray.remote(max_restarts=1)
    class Actor:
        """An actor that will be restarted at most once."""

        def __init__(self):
            self.value = 0

        def increase(self):
            self.value += 1
            return self.value

    remote_actor = Actor.remote()

    assert ray.get(RetryableTask.remote(remote_actor)) == 3


@pytest.mark.skipif(sys.platform == "win32", reason="Very flaky on Windows.")
# NOTE(hchen): we set object_timeout_milliseconds to 1s for
# this test. Because if this value is too small, suprious task reconstruction
# may happen and cause the test fauilure. If the value is too large, this test
# could be very slow. We can remove this once we support dynamic timeout.
@pytest.mark.parametrize(
    "ray_start_cluster_head", [
        generate_system_config_map(
            object_timeout_milliseconds=1000, num_heartbeats_timeout=10)
    ],
    indirect=True)
def test_multiple_actor_restart(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    # This test can be made more stressful by increasing the numbers below.
    # The total number of actors created will be
    # num_actors_at_a_time * num_nodes.
    num_nodes = 5
    num_actors_at_a_time = 3
    num_function_calls_at_a_time = 10

    worker_nodes = [cluster.add_node(num_cpus=3) for _ in range(num_nodes)]

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class SlowCounter:
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

    # This is a mapping from actor handles to object refs returned by
    # methods on that actor.
    result_ids = collections.defaultdict(lambda: [])

    # In a loop we are going to create some actors, run some methods, kill
    # a raylet, and run some more methods.
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
        results = ray.get(result_id_list)
        for i, result in enumerate(results):
            if i == 0:
                assert result == 1
            else:
                assert result == results[i - 1] + 1 or result == 1


def kill_actor(actor):
    """A helper function that kills an actor process."""
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)


def test_decorated_method(ray_start_regular):
    def method_invocation_decorator(f):
        def new_f_invocation(args, kwargs):
            # Split one argument into two. Return th kwargs without passing
            # them into the actor.
            return f([args[0], args[0]], {}), kwargs

        return new_f_invocation

    def method_execution_decorator(f):
        def new_f_execution(self, b, c):
            # Turn two arguments into one.
            return f(self, b + c)

        new_f_execution.__ray_invocation_decorator__ = (
            method_invocation_decorator)
        return new_f_execution

    @ray.remote
    class Actor:
        @method_execution_decorator
        def decorated_method(self, x):
            return x + 1

    a = Actor.remote()

    object_ref, extra = a.decorated_method.remote(3, kwarg=3)
    assert isinstance(object_ref, ray.ObjectRef)
    assert extra == {"kwarg": 3}
    assert ray.get(object_ref) == 7  # 2 * 3 + 1


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 1,
        "num_nodes": 3,
    }], indirect=True)
@pytest.mark.skipif(new_scheduler_enabled(), reason="dynamic resources todo")
def test_ray_wait_dead_actor(ray_start_cluster):
    """Tests that methods completed by dead actors are returned as ready"""
    cluster = ray_start_cluster

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def node_id(self):
            return ray.worker.global_worker.node.unique_id

        def ping(self):
            time.sleep(1)

    # Create some actors and wait for them to initialize.
    num_nodes = len(cluster.list_all_nodes())
    actors = [Actor.remote() for _ in range(num_nodes)]
    ray.get([actor.ping.remote() for actor in actors])

    def actor_dead():
        # Ping the actors and make sure the tasks complete.
        ping_ids = [actor.ping.remote() for actor in actors]
        unready = ping_ids[:]
        while unready:
            _, unready = ray.wait(unready, timeout=0)
            time.sleep(1)

        try:
            ray.get(ping_ids)
            return False
        except ray.exceptions.RayActorError:
            return True

    # Kill a node that must not be driver node or head node.
    cluster.remove_node(get_other_nodes(cluster, exclude_head=True)[-1])
    # Repeatedly submit tasks and call ray.wait until the exception for the
    # dead actor is received.
    wait_for_condition(actor_dead)

    # Create an actor on the local node that will call ray.wait in a loop.
    head_node_resource = "HEAD_NODE"
    ray.experimental.set_resource(head_node_resource, 1)

    @ray.remote(num_cpus=0, resources={head_node_resource: 1})
    class ParentActor:
        def __init__(self):
            pass

        def wait(self):
            return actor_dead()

        def ping(self):
            return

    # Repeatedly call ray.wait through the local actor until the exception for
    # the dead actor is received.
    parent_actor = ParentActor.remote()
    wait_for_condition(lambda: ray.get(parent_actor.wait.remote()))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 1,
        "num_nodes": 1,
    }], indirect=True)
def test_actor_owner_worker_dies_before_dependency_ready(ray_start_cluster):
    """Test actor owner worker dies before local dependencies are resolved.
    This test verifies the scenario where owner worker
    has failed before actor dependencies are resolved.
    Reference: https://github.com/ray-project/ray/pull/8045
    """

    @ray.remote
    class Actor:
        def __init__(self, dependency):
            print("actor: {}".format(os.getpid()))
            self.dependency = dependency

        def f(self):
            return self.dependency

    @ray.remote
    class Owner:
        def get_pid(self):
            return os.getpid()

        def create_actor(self, caller_handle):
            s = SignalActor.remote()
            # Create an actor which depends on an object that can never be
            # resolved.
            actor_handle = Actor.remote(s.wait.remote())

            pid = os.getpid()
            signal_handle = SignalActor.remote()
            caller_handle.call.remote(pid, signal_handle, actor_handle)
            # Wait until the `Caller` start executing the remote `call` method.
            ray.get(signal_handle.wait.remote())
            # exit
            os._exit(0)

    @ray.remote
    class Caller:
        def call(self, owner_pid, signal_handle, actor_handle):
            # Notify the `Owner` that the `Caller` is executing the remote
            # `call` method.
            ray.get(signal_handle.send.remote())
            # Wait for the `Owner` to exit.
            wait_for_pid_to_exit(owner_pid)
            oid = actor_handle.f.remote()
            # It will hang without location resolution protocol.
            ray.get(oid)

        def hang(self):
            return True

    owner = Owner.remote()
    owner_pid = ray.get(owner.get_pid.remote())

    caller = Caller.remote()
    owner.create_actor.remote(caller)
    # Wait for the `Owner` to exit.
    wait_for_pid_to_exit(owner_pid)
    # It will hang here if location is not properly resolved.
    wait_for_condition(lambda: ray.get(caller.hang.remote()))


@pytest.mark.parametrize(
    "ray_start_cluster", [{
        "num_cpus": 3,
        "num_nodes": 1,
    }], indirect=True)
def test_actor_owner_node_dies_before_dependency_ready(ray_start_cluster):
    """Test actor owner node dies before local dependencies are resolved.
    This test verifies the scenario where owner node
    has failed before actor dependencies are resolved.
    Reference: https://github.com/ray-project/ray/pull/8045
    """

    @ray.remote
    class Actor:
        def __init__(self, dependency):
            print("actor: {}".format(os.getpid()))
            self.dependency = dependency

        def f(self):
            return self.dependency

    # Make sure it is scheduled in the second node.
    @ray.remote(resources={"node": 1})
    class Owner:
        def get_pid(self):
            return os.getpid()

        def create_actor(self, caller_handle):
            s = SignalActor.remote()
            # Create an actor which depends on an object that can never be
            # resolved.
            actor_handle = Actor.remote(s.wait.remote())

            pid = os.getpid()
            signal_handle = SignalActor.remote()
            caller_handle.call.remote(pid, signal_handle, actor_handle)
            # Wait until the `Caller` start executing the remote `call` method.
            ray.get(signal_handle.wait.remote())

    @ray.remote(resources={"caller": 1})
    class Caller:
        def call(self, owner_pid, signal_handle, actor_handle):
            # Notify the `Owner` that the `Caller` is executing the remote
            # `call` method.
            ray.get(signal_handle.send.remote())
            # Wait for the `Owner` to exit.
            wait_for_pid_to_exit(owner_pid)
            oid = actor_handle.f.remote()
            # It will hang without location resolution protocol.
            ray.get(oid)

        def hang(self):
            return True

    cluster = ray_start_cluster
    node_to_be_broken = cluster.add_node(resources={"node": 1})
    cluster.add_node(resources={"caller": 1})

    owner = Owner.remote()
    owner_pid = ray.get(owner.get_pid.remote())

    caller = Caller.remote()
    ray.get(owner.create_actor.remote(caller))
    cluster.remove_node(node_to_be_broken)
    wait_for_pid_to_exit(owner_pid)

    # It will hang here if location is not properly resolved.
    wait_for_condition(lambda: ray.get(caller.hang.remote()))


def test_recreate_child_actor(ray_start_cluster):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def ready(self):
            return

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class Parent:
        def __init__(self):
            self.child = Actor.remote()

        def ready(self):
            return ray.get(self.child.ready.remote())

        def pid(self):
            return os.getpid()

    ray.init(address=ray_start_cluster.address)
    p = Parent.remote()
    pid = ray.get(p.pid.remote())
    os.kill(pid, 9)
    ray.get(p.ready.remote())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
