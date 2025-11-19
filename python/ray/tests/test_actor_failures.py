import asyncio
import atexit
import collections
import os
import signal
import sys
import tempfile
import time
from typing import Callable, Generator, List

import numpy as np
import pytest

import ray
import ray.cluster_utils
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import (
    generate_system_config_map,
    wait_for_pid_to_exit,
)
from ray.actor import exit_actor
from ray.exceptions import AsyncioActorExit

SIGKILL = signal.SIGKILL if sys.platform != "win32" else signal.SIGTERM


@pytest.fixture
def ray_init_with_task_retry_delay():
    address = ray.init(_system_config={"task_retry_delay_ms": 100})
    yield address
    ray.shutdown()


@pytest.fixture
def ray_init_with_actor_graceful_shutdown_timeout():
    ray.shutdown()
    address = ray.init(_system_config={"actor_graceful_shutdown_timeout_ms": 1000})
    yield address
    ray.shutdown()


@pytest.fixture
def tempfile_factory() -> Generator[Callable[[], str], None, None]:
    """Yields a factory function to generate tempfiles that will be deleted after the test run."""
    files = []

    def create_temp_file():
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.close()
        files.append(temp_file.name)
        return temp_file.name

    yield create_temp_file

    # Cleanup all created files
    for file_path in files:
        try:
            os.unlink(file_path)
        except Exception:
            pass


def check_file_exists_and_not_empty(file_path):
    """Helper to check if file exists and has content."""
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0


@pytest.mark.parametrize(
    "ray_start_regular",
    [
        {
            "object_store_memory": 150 * 1024 * 1024,
        }
    ],
    indirect=True,
)
@pytest.mark.skipif(sys.platform == "win32", reason="Segfaults on CI")
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
    num_objects = 40
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


def test_async_generator_crash_restart(ray_start_cluster):
    """
    Timeline:

    1. On a worker node, run a generator task that generates 2 objects in total and run
       it to completion.
    2. Kill the worker node so the objects are lost but the object refs exist.
    3. Submit a consumer task that depends on the generated object refs.
    4. Add a new worker node that the generator and the consumer can be run on
    5. Verify that the generator outputs are reconstructed and the consumer succeeds.
    """
    cluster = ray_start_cluster
    head_node_id = cluster.add_node(
        _system_config={
            "health_check_timeout_ms": 1000,
            "health_check_failure_threshold": 1,
        }
    ).node_id
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Used to pause the generator task and kill it after it generates the first object.
    signal = SignalActor.remote()

    @ray.remote(
        label_selector={"ray.io/node-id": f"!{head_node_id}"},
        max_restarts=-1,
        max_task_retries=-1,
    )
    class Generator:
        async def generate(self):
            print("Generate first object.")
            yield np.ones(1024**2, dtype=np.uint8)
            print("Wait for SignalActor.")
            ray.get(signal.wait.remote())
            print("Generate second object.")
            yield np.ones(1024**2, dtype=np.uint8)

    @ray.remote(label_selector={"ray.io/node-id": f"!{head_node_id}"})
    def consumer(object_refs: List[ray.ObjectRef]):
        assert len(object_refs) == 2
        print("Calling `ray.get`.")
        ray.get(object_refs)
        print("`ray.get` succeeded.")

    worker_node = cluster.add_node(num_cpus=2, resources={"worker": 2})
    cluster.wait_for_nodes()

    generator = Generator.remote()

    # First run, let the generator run to completion.
    obj_ref_gen_ref = generator.generate.remote()
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)
    ray.get(signal.send.remote(clear=True))
    object_refs = list(ray.get(obj_ref_gen_ref))
    assert len(object_refs) == 2

    # Kill the worker node that holds the objects.
    cluster.remove_node(worker_node, allow_graceful=False)

    # Submit a consumer task that requires the objects from the generator.
    consumer = consumer.remote(object_refs)

    # Start a new worker node that the generator can be rerun on and the consumer can
    # run on.
    worker_node = cluster.add_node(num_cpus=2, resources={"worker": 2})
    cluster.wait_for_nodes()

    # Kill the generator after it generates a single object.
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)
    ray.kill(generator, no_restart=False)

    # Now let the generator complete and check that the consumer succeeds.
    ray.get(signal.send.remote())
    ray.get(consumer)


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

        def sleep_and_echo(self, value, delay=0):
            time.sleep(delay)
            return value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    results = [actor.sleep_and_echo.remote(i) for i in range(100)]
    # Kill actor process, while the above task is still being executed.
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)
    # All tasks should be executed successfully.
    results = ray.get(results)
    assert results == list(range(100))

    # kill actor process one more time.
    results = [actor.sleep_and_echo.remote(i) for i in range(100)]
    pid = ray.get(actor.get_pid.remote())
    os.kill(pid, SIGKILL)
    wait_for_pid_to_exit(pid)
    # The actor has exceeded max restarts, and this task should fail.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.sleep_and_echo.remote(0))

    # Create another actor.
    actor = RestartableActor.remote()
    # Intentionlly exit the actor
    actor.__ray_terminate__.remote()
    # Check that the actor won't be restarted.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.sleep_and_echo.remote(0))


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
    a = ActorToKill.options(name="a", max_restarts=-1, max_task_retries=-1).remote(
        init_counter
    )
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
        "health_check_failure_threshold": 10,
        "health_check_period_ms": 100,
        "health_check_initial_delay_ms": 0,
        "object_timeout_milliseconds": 1000,
        "task_retry_delay_ms": 100,
    }
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(num_cpus=0, _system_config=config)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Node to place the signal actor.
    cluster.add_node(num_cpus=1, resources={"signal": 1})
    # Node to place the actor.
    actor_node = cluster.add_node(num_cpus=1, resources={"actor": 1})
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=1, max_restarts=1, max_task_retries=-1)
    class RestartableActor:
        """An actor that will be reconstructed at most once."""

        def __init__(self, signal):
            self._signal = signal

        def echo(self, value):
            if value >= 50:
                ray.get(self._signal.wait.remote())
            return value

    signal = SignalActor.options(resources={"signal": 1}).remote()
    actor = RestartableActor.options(
        lifetime="detached", resources={"actor": 1}
    ).remote(signal)
    ray.get(actor.__ray_ready__.remote())
    results = [actor.echo.remote(i) for i in range(100)]
    # Kill actor node, while the above task is still being executed.
    cluster.remove_node(actor_node)
    ray.get(signal.send.remote())
    cluster.add_node(num_cpus=1, resources={"actor": 1})
    cluster.wait_for_nodes()
    # All tasks should be executed successfully.
    results = ray.get(results)
    assert results == list(range(100))


def test_caller_actor_restart(ray_start_regular):
    """Test tasks from a restarted actor can be correctly processed
    by the receiving actor."""

    @ray.remote(max_restarts=1, max_task_retries=-1)
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
    "ray_start_cluster_head",
    [
        generate_system_config_map(
            object_timeout_milliseconds=1000,
            health_check_initial_delay_ms=0,
            health_check_period_ms=1000,
            health_check_failure_threshold=10,
        )
    ],
    indirect=True,
)
def test_multiple_actor_restart(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    num_nodes = 5
    num_actors = 15
    num_function_calls_at_a_time = 10

    worker_nodes = [cluster.add_node(num_cpus=3) for _ in range(num_nodes)]

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class SlowCounter:
        def __init__(self):
            pass

        def echo(self, duration, value):
            time.sleep(duration)
            return value

    # Create some initial actors.
    actors = [SlowCounter.remote() for _ in range(num_actors)]
    ray.get([actor.__ray_ready__.remote() for actor in actors])

    # This is a mapping from actor handles to object refs returned by
    # methods on that actor.
    result_ids = collections.defaultdict(lambda: [])

    for i in range(len(actors)):
        actor = actors[i]
        for value in range(num_function_calls_at_a_time):
            result_ids[actor].append(actor.echo.remote(i**2 * 0.000001, value))

    # Kill nodes
    for node in worker_nodes:
        cluster.remove_node(node)

    for i in range(len(actors)):
        actor = actors[i]
        for value in range(
            num_function_calls_at_a_time, 2 * num_function_calls_at_a_time
        ):
            result_ids[actor].append(actor.echo.remote(i**2 * 0.000001, value))

    # Get the results and check that they have the correct values.
    for actor, result_id_list in result_ids.items():
        results = ray.get(result_id_list)
        expected = list(range(num_function_calls_at_a_time * 2))
        assert results == expected


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

        new_f_execution.__ray_invocation_decorator__ = method_invocation_decorator
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
    "ray_start_cluster",
    [
        {
            "num_cpus": 1,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
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
    "ray_start_cluster",
    [
        {
            "num_cpus": 3,
            "num_nodes": 1,
        }
    ],
    indirect=True,
)
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


def test_actor_failure_per_type(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(address="auto")

    @ray.remote
    class Actor:
        def check_alive(self):
            return os.getpid()

        def create_actor(self):
            self.a = Actor.remote()
            return self.a

    # Test actor killed by ray.kill
    a = Actor.remote()
    ray.kill(a)
    with pytest.raises(
        ray.exceptions.RayActorError, match="it was killed by `ray.kill"
    ) as exc_info:
        ray.get(a.check_alive.remote())
    assert exc_info.value.actor_id == a._actor_id.hex()
    print(exc_info._excinfo[1])

    # Test actor killed because of worker failure.
    a = Actor.remote()
    pid = ray.get(a.check_alive.remote())
    os.kill(pid, 9)
    with pytest.raises(
        ray.exceptions.RayActorError,
        match=("The actor is dead because its worker process has died"),
    ) as exc_info:
        ray.get(a.check_alive.remote())
    assert exc_info.value.actor_id == a._actor_id.hex()
    print(exc_info._excinfo[1])

    # Test acator killed because of owner failure.
    owner = Actor.remote()
    a = ray.get(owner.create_actor.remote())
    ray.kill(owner)
    with pytest.raises(
        ray.exceptions.RayActorError,
        match="The actor is dead because its owner has died",
    ) as exc_info:
        ray.get(a.check_alive.remote())
    assert exc_info.value.actor_id == a._actor_id.hex()
    print(exc_info._excinfo[1])

    # Test actor killed because the node is dead.
    node_to_kill = cluster.add_node(resources={"worker": 1})
    a = Actor.options(resources={"worker": 1}).remote()
    ray.get(a.check_alive.remote())
    cluster.remove_node(node_to_kill)
    with pytest.raises(
        ray.exceptions.RayActorError,
        match="The actor died because its node has died.",
    ) as exc_info:
        ray.get(a.check_alive.remote())
    assert exc_info.value.actor_id == a._actor_id.hex()
    print(exc_info._excinfo[1])


def test_utf8_actor_exception(ray_start_regular):
    @ray.remote
    class FlakyActor:
        def __init__(self):
            raise RuntimeError("你好呀，祝你有个好心情！")

        def ping(self):
            return True

    actor = FlakyActor.remote()
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(actor.ping.remote())


# https://github.com/ray-project/ray/issues/18908.
def test_failure_during_dependency_resolution(ray_start_regular):
    @ray.remote
    class Actor:
        def dep(self):
            while True:
                time.sleep(1)

        def foo(self, x):
            return x

    @ray.remote
    def foo():
        time.sleep(3)
        return 1

    a = Actor.remote()
    # Check that the actor is alive.
    ray.get(a.foo.remote(1))

    ray.kill(a, no_restart=False)
    dep = a.dep.remote()
    ref = a.foo.remote(dep)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(ref)


def test_exit_actor_invalid_usage_error(shutdown_only):
    """
    Verify TypeError is raised when exit_actor is not used
    inside an actor.
    """
    with pytest.raises(
        TypeError, match="exit_actor API is called on a non-actor worker"
    ):
        exit_actor()

    @ray.remote
    def f():
        exit_actor()

    with pytest.raises(
        TypeError, match="exit_actor API is called on a non-actor worker"
    ):
        ray.get(f.remote())


def test_exit_actor_normal_actor_raise_immediately(shutdown_only, tmp_path):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    @ray.remote
    class Actor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)

        def exit(self):
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = Actor.remote()
    ray.get(a.__ray_ready__.remote())
    with pytest.raises(ray.exceptions.RayActorError) as exc_info:
        ray.get(a.exit.remote())
    assert "exit_actor()" in str(exc_info.value)

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_normal_actor_in_constructor_should_exit(shutdown_only, tmp_path):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    @ray.remote
    class Actor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = Actor.remote()  # noqa: F841 # Need to preserve the reference.

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_normal_actor_user_catch_err_should_still_exit(
    shutdown_only, tmp_path
):
    temp_file = tmp_path / "actor.log"
    assert not temp_file.exists()

    @ray.remote
    class Actor:
        def exit(self):
            try:
                exit_actor()
            except SystemExit:
                pass

        def create(self):
            temp_file.touch()

    a = Actor.remote()
    ray.get(a.__ray_ready__.remote())
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.exit.remote())

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.create.remote())

    assert not temp_file.exists()


def test_exit_actor_async_actor_raise_immediately(shutdown_only, tmp_path):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    @ray.remote
    class AsyncActor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)

        async def exit(self):
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = AsyncActor.remote()
    ray.get(a.__ray_ready__.remote())

    try:
        ray.get(a.exit.remote())
    except Exception:
        pass

    with pytest.raises(ray.exceptions.RayActorError) as exc_info:
        ray.get(a.exit.remote())
    assert (
        # Exited when task execution returns
        "exit_actor()" in str(exc_info.value)
        # Exited during periodical check in worker
        or "User requested to exit the actor" in str(exc_info.value)
    )

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_async_actor_in_constructor_should_exit(shutdown_only, tmp_path):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    @ray.remote
    class AsyncActor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = AsyncActor.remote()  # noqa: F841 # Need to preserve the reference.

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_async_actor_user_catch_err_should_still_exit(
    shutdown_only, tmp_path
):
    temp_file = tmp_path / "actor.log"
    assert not temp_file.exists()

    @ray.remote
    class AsyncActor:
        async def exit(self):
            try:
                exit_actor()
            except AsyncioActorExit:
                pass

        async def create(self):
            temp_file.touch()

    a = AsyncActor.remote()
    ray.get(a.__ray_ready__.remote())
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.exit.remote())

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.create.remote())
    assert not temp_file.exists()


def test_exit_actor_async_actor_nested_task(shutdown_only, tmp_path):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    signal = SignalActor.remote()

    @ray.remote
    class AsyncActor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)

        async def start_exit_task(self, signal):
            asyncio.create_task(self.exit(signal))

        async def exit(self, signal):
            await signal.wait.remote()
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = AsyncActor.remote()
    ray.get(a.__ray_ready__.remote())
    ray.get(a.start_exit_task.remote(signal))
    ray.get(signal.send.remote())

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_async_actor_nested_task_in_constructor_should_exit(
    shutdown_only, tmp_path
):
    temp_file_atexit = tmp_path / "atexit.log"
    temp_file_after_exit_actor = tmp_path / "after_exit_actor.log"
    assert not temp_file_atexit.exists()
    assert not temp_file_after_exit_actor.exists()

    @ray.remote
    class AsyncActor:
        def __init__(self):
            def f():
                temp_file_atexit.touch()

            atexit.register(f)
            asyncio.create_task(self.exit())

        async def exit(self):
            exit_actor()
            # The following code should not be executed.
            temp_file_after_exit_actor.touch()

    a = AsyncActor.remote()  # noqa: F841 # Need to preserve the reference.

    def verify():
        return temp_file_atexit.exists()

    wait_for_condition(verify)
    time.sleep(3)
    assert not temp_file_after_exit_actor.exists()


def test_exit_actor_queued(shutdown_only):
    """Verify after exit_actor is called the queued tasks won't execute."""

    @ray.remote
    class RegressionSync:
        def f(self):
            import time

            time.sleep(1)
            exit_actor()

        def ping(self):
            pass

    @ray.remote
    class RegressionAsync:
        async def f(self):
            await asyncio.sleep(1)
            exit_actor()

        def ping(self):
            pass

    # Test async case.
    # https://github.com/ray-project/ray/issues/32376
    # If we didn't fix this issue, this will segfault.
    a = RegressionAsync.remote()
    a.f.remote()
    refs = [a.ping.remote() for _ in range(10000)]
    with pytest.raises(ray.exceptions.RayActorError) as exc_info:
        ray.get(refs)
    assert " Worker unexpectedly exits" not in str(exc_info.value)

    # Test a sync case.
    a = RegressionSync.remote()
    a.f.remote()
    with pytest.raises(ray.exceptions.RayActorError) as exc_info:
        ray.get([a.ping.remote() for _ in range(10000)])
    assert " Worker unexpectedly exits" not in str(exc_info.value)


@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL not supported on windows")
def test_actor_restart_and_actor_received_task(shutdown_only):
    # Create an actor with max_restarts=1 and max_task_retries=1.
    # Submit a task to the actor and kill the actor after it receives
    # the task. Then, the actor should restart in another core worker
    # process, and the driver should resubmit the task to the new process.
    # The task should be executed successfully.
    @ray.remote(max_restarts=1, max_task_retries=1)
    class RestartableActor:
        def __init__(self):
            self.counter = 0

        def increment(self, signal_actor_1, signal_actor_2):
            ray.get(signal_actor_1.send.remote())
            ray.get(signal_actor_2.wait.remote())
            self.counter += 1
            return self.counter

        def fail(self):
            os._exit(1)

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())

    signal_actor_1 = SignalActor.remote()
    signal_actor_2 = SignalActor.remote()
    ref = actor.increment.remote(signal_actor_1, signal_actor_2)
    # Wait for the actor to execute the task `increment`
    ray.get(signal_actor_1.wait.remote())
    os.kill(pid, signal.SIGKILL)

    ray.get(signal_actor_2.send.remote())
    assert ray.get(ref) == 1


@pytest.mark.skipif(sys.platform == "win32", reason="SIGKILL not supported on windows")
def test_actor_restart_and_partial_task_not_completed(shutdown_only):
    # Create an actor with max_restarts=1 and max_task_retries=1.
    # Submit 3 tasks to the actor and wait for them to complete.
    # Then, submit 3 more tasks to the actor and kill the actor.
    # The driver will resubmit the last 3 tasks to the new core worker
    # process, and the tasks will be executed successfully.
    @ray.remote(max_restarts=1, max_task_retries=1)
    class RestartableActor:
        def __init__(self):
            pass

        def echo(self, value):
            return value

        def wait_and_echo(self, value, signal_actor_1, signal_actor_2):
            ray.get(signal_actor_1.send.remote())
            ray.get(signal_actor_2.wait.remote())
            return value

        def get_pid(self):
            return os.getpid()

    actor = RestartableActor.remote()
    pid = ray.get(actor.get_pid.remote())
    refs = []
    for i in range(3):
        refs.append(actor.echo.remote(i))
    assert ray.get(refs) == [0, 1, 2]

    refs = []
    signal_actor_1 = SignalActor.remote()
    signal_actor_2 = SignalActor.remote()
    refs.append(actor.wait_and_echo.remote(3, signal_actor_1, signal_actor_2))
    ray.get(signal_actor_1.wait.remote())
    refs.append(actor.echo.remote(4))
    refs.append(actor.echo.remote(5))

    os.kill(pid, signal.SIGKILL)
    ray.get(signal_actor_2.send.remote())
    assert ray.get(refs) == [3, 4, 5]


def test_actor_user_shutdown_method(ray_start_regular_shared, tempfile_factory):
    """Test that __ray_shutdown__ method is called during actor termination."""
    shutdown_file = tempfile_factory()

    @ray.remote
    class UserShutdownActor:
        def __init__(self):
            pass

        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("ray_shutdown_called")
                f.flush()

        def get_ready(self):
            return "ready"

    actor = UserShutdownActor.remote()
    ray.get(actor.get_ready.remote())
    actor.__ray_terminate__.remote()

    wait_for_condition(lambda: check_file_exists_and_not_empty(shutdown_file))

    with open(shutdown_file, "r") as f:
        assert f.read() == "ray_shutdown_called"


def test_actor_ray_shutdown_handles_exceptions(
    ray_start_regular_shared, tempfile_factory
):
    """Test that Ray handles unhandled exceptions in __ray_shutdown__ gracefully."""
    shutdown_file = tempfile_factory()

    @ray.remote
    class ExceptionActor:
        def __ray_shutdown__(self):
            # Write to file before raising exception
            with open(shutdown_file, "w") as f:
                f.write("cleanup_started")
                f.flush()

            # Let exception propagate to Ray's machinery
            raise ValueError("Unhandled exception in __ray_shutdown__")

        def get_ready(self):
            return "ready"

    actor = ExceptionActor.remote()
    ray.get(actor.get_ready.remote())
    actor.__ray_terminate__.remote()

    # Verify that despite the exception:
    # 1. File was written (cleanup started)
    # 2. Actor shuts down properly (no system crash)
    wait_for_condition(lambda: check_file_exists_and_not_empty(shutdown_file))

    with open(shutdown_file, "r") as f:
        assert f.read() == "cleanup_started"


def test_actor_atexit_handler_dont_conflict_with_ray_shutdown(
    ray_start_regular_shared, tempfile_factory
):
    """Test that atexit handler methods don't conflict with __ray_shutdown__ and both run."""
    shutdown_file = tempfile_factory()
    atexit_file = tempfile_factory()

    @ray.remote
    class CleanupActor:
        def __init__(self):
            atexit.register(self.cleanup)

        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("ray_shutdown_called")
                f.flush()

        def cleanup(self):
            with open(atexit_file, "w") as f:
                f.write("atexit_cleanup_called")
                f.flush()

        def get_ready(self):
            return "ready"

    actor = CleanupActor.remote()
    ray.get(actor.get_ready.remote())
    actor.__ray_terminate__.remote()

    wait_for_condition(lambda: check_file_exists_and_not_empty(shutdown_file))

    with open(shutdown_file, "r") as f:
        assert f.read() == "ray_shutdown_called"
    wait_for_condition(lambda: check_file_exists_and_not_empty(atexit_file))
    with open(atexit_file, "r") as f:
        assert f.read() == "atexit_cleanup_called"


def test_actor_ray_shutdown_dont_interfere_with_kill(
    ray_start_regular_shared, tempfile_factory
):
    """Test __ray_shutdown__ is not called when actor is killed with ray.kill()."""
    shutdown_file = tempfile_factory()

    @ray.remote
    class KillableActor:
        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("shutdown_called_kill")
                f.flush()

        def get_ready(self):
            return "ready"

        def sleep_forever(self):
            time.sleep(3600)

    actor = KillableActor.remote()
    ray.get(actor.get_ready.remote())
    _ = actor.sleep_forever.remote()
    ray.kill(actor)

    wait_for_condition(lambda: not check_file_exists_and_not_empty(shutdown_file))


def test_actor_ray_shutdown_called_on_del(ray_start_regular_shared, tempfile_factory):
    """Test that __ray_shutdown__ is called when actor goes out of scope via del."""
    shutdown_file = tempfile_factory()

    @ray.remote
    class DelTestActor:
        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("shutdown_called_on_del")
                f.flush()

        def ready(self):
            return "ready"

    actor = DelTestActor.remote()
    ray.get(actor.ready.remote())
    del actor

    wait_for_condition(
        lambda: check_file_exists_and_not_empty(shutdown_file), timeout=10
    )

    with open(shutdown_file, "r") as f:
        assert f.read() == "shutdown_called_on_del", (
            "Expected __ray_shutdown__ to be called within actor_graceful_shutdown_timeout_ms "
            "after actor handle was deleted with del"
        )


def test_actor_del_with_atexit(ray_start_regular_shared, tempfile_factory):
    """Test that both __ray_shutdown__ and atexit handlers run on del actor."""
    shutdown_file = tempfile_factory()
    atexit_file = tempfile_factory()
    order_file = tempfile_factory()

    @ray.remote
    class BothHandlersActor:
        def __init__(self):
            atexit.register(self.cleanup)

        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("ray_shutdown_del")
                f.flush()
            with open(order_file, "a") as f:
                f.write(f"shutdown:{time.time()}\n")
                f.flush()

        def cleanup(self):
            with open(atexit_file, "w") as f:
                f.write("atexit_del")
                f.flush()

            with open(order_file, "a") as f:
                f.write(f"atexit:{time.time()}\n")
                f.flush()

        def ready(self):
            return "ready"

    actor = BothHandlersActor.remote()
    ray.get(actor.ready.remote())
    del actor

    wait_for_condition(
        lambda: check_file_exists_and_not_empty(shutdown_file), timeout=10
    )
    with open(shutdown_file, "r") as f:
        assert (
            f.read() == "ray_shutdown_del"
        ), "Expected __ray_shutdown__ to be called when actor deleted"

    wait_for_condition(lambda: check_file_exists_and_not_empty(atexit_file), timeout=10)
    with open(atexit_file, "r") as f:
        assert f.read() == "atexit_del", "Expected atexit handler to be called"

    # Verify execution order: __ray_shutdown__ should run before atexit
    wait_for_condition(lambda: check_file_exists_and_not_empty(order_file), timeout=10)
    with open(order_file, "r") as f:
        order = f.read()
        lines = order.strip().split("\n")
        assert len(lines) == 2, f"Expected 2 entries, got: {lines}"
        assert lines[0].startswith(
            "shutdown:"
        ), f"Expected __ray_shutdown__ first, got order: {lines}"
        assert lines[1].startswith(
            "atexit:"
        ), f"Expected atexit second, got order: {lines}"


def test_actor_ray_shutdown_called_on_scope_exit(
    ray_start_regular_shared, tempfile_factory
):
    """Test that __ray_shutdown__ is called when actor goes out of scope."""
    shutdown_file = tempfile_factory()

    @ray.remote
    class ScopeTestActor:
        def __ray_shutdown__(self):
            with open(shutdown_file, "w") as f:
                f.write("shutdown_called_on_scope_exit")
                f.flush()

        def ready(self):
            return "ready"

    def create_and_use_actor():
        actor = ScopeTestActor.remote()
        ray.get(actor.ready.remote())
        # Actor goes out of scope at end of function

    create_and_use_actor()

    wait_for_condition(
        lambda: check_file_exists_and_not_empty(shutdown_file), timeout=10
    )

    with open(shutdown_file, "r") as f:
        assert f.read() == "shutdown_called_on_scope_exit"


def test_actor_graceful_shutdown_timeout_fallback(
    ray_init_with_actor_graceful_shutdown_timeout, tempfile_factory
):
    """Test that actor is force killed if __ray_shutdown__ exceeds timeout."""
    shutdown_started_file = tempfile_factory()
    shutdown_completed_file = tempfile_factory()

    @ray.remote
    class HangingShutdownActor:
        def __ray_shutdown__(self):
            with open(shutdown_started_file, "w") as f:
                f.write("shutdown_started")
                f.flush()

            # Hang indefinitely - simulating buggy cleanup code
            time.sleep(5)

            # This should never be reached due to force kill fallback
            with open(shutdown_completed_file, "w") as f:
                f.write("should_not_reach")
                f.flush()

        def ready(self):
            return "ready"

    actor = HangingShutdownActor.remote()
    ray.get(actor.ready.remote())
    del actor

    # Verify that shutdown started
    wait_for_condition(
        lambda: check_file_exists_and_not_empty(shutdown_started_file), timeout=5
    )
    with open(shutdown_started_file, "r") as f:
        assert (
            f.read() == "shutdown_started"
        ), "Expected __ray_shutdown__ to start execution"

    # Verify that shutdown did NOT complete (force killed before completion)
    assert not check_file_exists_and_not_empty(shutdown_completed_file), (
        "Expected actor to be force-killed before __ray_shutdown__ completed, "
        "but completion file exists. This means force kill fallback did not work."
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
