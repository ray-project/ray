import sys
import threading
import time

import numpy as np
import pytest

import ray
import ray._common.test_utils
import ray._private.test_utils as test_utils
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.state import available_resources


def ensure_cpu_returned(expected_cpus):
    ray._common.test_utils.wait_for_condition(
        lambda: (available_resources().get("CPU", 0) == expected_cpus)
    )


def test_threaded_actor_basic(ray_start_cluster):
    """Test the basic threaded actor."""
    ray.init(num_cpus=1)

    @ray.remote(num_cpus=1)
    class ThreadedActor:
        def __init__(self):
            self.received = []
            self.lock = threading.Lock()

        def add(self, seqno):
            with self.lock:
                self.received.append(seqno)

        def get_all(self):
            with self.lock:
                return self.received

    a = ThreadedActor.options(max_concurrency=10).remote()
    max_seq = 50
    ray.get([a.add.remote(seqno) for seqno in range(max_seq)])
    seqnos = ray.get(a.get_all.remote())
    # Currently, the caller submission order is not guaranteed
    # when the threaded actor is used.
    assert sorted(seqnos) == list(range(max_seq))
    ray.kill(a)
    ensure_cpu_returned(1)


def test_threaded_actor_api_thread_safe(ray_start_cluster):
    """Test if Ray APIs are thread safe
    when they are used within threaded actor.
    """
    ray.init(
        num_cpus=8,
        # from 1024 bytes, the return obj will go to the plasma store.
        _system_config={"max_direct_call_object_size": 1024},
    )

    @ray.remote
    def in_memory_return(i):
        return i

    @ray.remote
    def plasma_return(i):
        arr = np.zeros(8 * 1024 * i, dtype=np.uint8)  # 8 * i KB
        return arr

    @ray.remote(num_cpus=1)
    class ThreadedActor:
        def __init__(self):
            self.received = []
            self.lock = threading.Lock()

        def in_memory_return_test(self, i):
            self._add(i)
            return ray.get(in_memory_return.remote(i))

        def plasma_return_test(self, i):
            self._add(i)
            return ray.get(plasma_return.remote(i))

        def _add(self, seqno):
            with self.lock:
                self.received.append(seqno)

        def get_all(self):
            with self.lock:
                return self.received

    a = ThreadedActor.options(max_concurrency=10).remote()
    max_seq = 50

    # Test in-memory return obj
    seqnos = ray.get(
        [a.in_memory_return_test.remote(seqno) for seqno in range(max_seq)]
    )
    assert sorted(seqnos) == list(range(max_seq))

    # Test plasma return obj
    real = ray.get([a.plasma_return_test.remote(seqno) for seqno in range(max_seq)])
    expected = [np.zeros(8 * 1024 * i, dtype=np.uint8) for i in range(max_seq)]
    for r, e in zip(real, expected):
        assert np.array_equal(r, e)

    ray.kill(a)
    ensure_cpu_returned(8)


def test_threaded_actor_creation_and_kill(ray_start_cluster):
    """Test the scenario where the threaded actors are created and killed."""
    cluster = ray_start_cluster
    NUM_CPUS_PER_NODE = 3
    NUM_NODES = 2
    for _ in range(NUM_NODES):
        cluster.add_node(num_cpus=NUM_CPUS_PER_NODE)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0)
    class ThreadedActor:
        def __init__(self):
            self.received = []
            self.lock = threading.Lock()

        def add(self, seqno):
            time.sleep(1)
            with self.lock:
                self.received.append(seqno)

        def get_all(self):
            with self.lock:
                return self.received

        def ready(self):
            pass

        def terminate(self):
            ray.actor.exit_actor()

    # - Create threaded actors
    # - Submit many tasks.
    # - Ungracefully kill them in the middle.
    for _ in range(10):
        actors = [
            ThreadedActor.options(max_concurrency=10).remote()
            for _ in range(NUM_NODES * NUM_CPUS_PER_NODE)
        ]
        ray.get([actor.ready.remote() for actor in actors])

        for _ in range(10):
            for actor in actors:
                actor.add.remote(1)
        time.sleep(0.5)
        for actor in actors:
            ray.kill(actor)
    ensure_cpu_returned(NUM_NODES * NUM_CPUS_PER_NODE)

    # - Create threaded actors
    # - Submit many tasks.
    # - Gracefully kill them in the middle.
    for _ in range(10):
        actors = [
            ThreadedActor.options(max_concurrency=10).remote()
            for _ in range(NUM_NODES * NUM_CPUS_PER_NODE)
        ]
        ray.get([actor.ready.remote() for actor in actors])
        for _ in range(10):
            for actor in actors:
                actor.add.remote(1)

        time.sleep(0.5)
        for actor in actors:
            actor.terminate.remote()
    ensure_cpu_returned(NUM_NODES * NUM_CPUS_PER_NODE)


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
@pytest.mark.parametrize("ray_start_cluster_head", [{"num_cpus": 2}], indirect=True)
def test_threaded_actor_integration_test_stress(
    ray_start_cluster_head, log_pubsub, error_pubsub
):
    """This is a sanity test that checks threaded actors are
    working with the nightly stress test.
    """
    cluster = ray_start_cluster_head
    p = log_pubsub
    e = error_pubsub

    # Prepare the config
    num_remote_nodes = 4
    num_parents = 6
    num_children = 6
    death_probability = 0.95
    max_concurrency = 10

    for _ in range(num_remote_nodes):
        cluster.add_node(num_cpus=2)

    @ray.remote
    class Child(object):
        def __init__(self, death_probability):
            self.death_probability = death_probability

        def ping(self):
            # Exit process with some probability.
            exit_chance = np.random.rand()
            if exit_chance > self.death_probability:
                sys.exit(-1)

    @ray.remote
    class Parent(object):
        def __init__(self, num_children, death_probability=0.95):
            self.death_probability = death_probability
            self.children = [
                Child.options(max_concurrency=max_concurrency).remote(death_probability)
                for _ in range(num_children)
            ]

        def ping(self, num_pings):
            children_outputs = []
            for _ in range(num_pings):
                children_outputs += [child.ping.remote() for child in self.children]
            try:
                ray.get(children_outputs)
            except Exception:
                # Replace the children if one of them died.
                self.__init__(len(self.children), self.death_probability)

        def kill(self):
            # Clean up children.
            ray.get([child.__ray_terminate__.remote() for child in self.children])

    parents = [
        Parent.options(max_concurrency=max_concurrency).remote(
            num_children, death_probability
        )
        for _ in range(num_parents)
    ]

    start = time.time()
    loop_times = []
    for _ in range(10):
        loop_start = time.time()
        ray.get([parent.ping.remote(10) for parent in parents])

        # Kill a parent actor with some probability.
        exit_chance = np.random.rand()
        if exit_chance > death_probability:
            parent_index = np.random.randint(len(parents))
            parents[parent_index].kill.remote()
            parents[parent_index] = Parent.options(
                max_concurrency=max_concurrency
            ).remote(num_children, death_probability)
        loop_times.append(time.time() - loop_start)
    result = {}
    print("Finished in: {}s".format(time.time() - start))
    print("Average iteration time: {}s".format(np.mean(loop_times)))
    print("Max iteration time: {}s".format(max(loop_times)))
    print("Min iteration time: {}s".format(min(loop_times)))
    result["total_time"] = time.time() - start
    result["avg_iteration_time"] = np.mean(loop_times)
    result["max_iteration_time"] = max(loop_times)
    result["min_iteration_time"] = min(loop_times)
    result["success"] = 1
    print(result)
    ensure_cpu_returned(10)
    del parents

    # Make sure parents are still scheduleable.
    parents = [
        Parent.options(max_concurrency=max_concurrency).remote(
            num_children, death_probability
        )
        for _ in range(num_parents)
    ]
    ray.get([parent.ping.remote(10) for parent in parents])
    """
    Make sure there are not SIGSEGV, SIGBART, or other odd check failures.
    """
    # Get all logs for 20 seconds.
    logs = test_utils.get_log_message(p, timeout=20)
    for log in logs:
        assert "SIG" not in log, "There's the segfault or SIGBART reported."
        assert "Check failed" not in log, "There's the check failure reported."

    # Get error messages for 10 seconds.
    errors = test_utils.get_error_message(e, timeout=10)
    for error in errors:
        print(error)
        assert (
            "You can ignore this message if" not in error["error_message"]
        ), "Resource deadlock warning shouldn't be printed, but it did."


# Simple single-threaded counter actor used to observe how many actor tasks
# have started executing. Single-threaded, so it needs no locks.
@ray.remote(num_cpus=0)
class _StartCounter:
    def __init__(self):
        self._count = 0

    def incr(self):
        self._count += 1

    def get(self):
        return self._count


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_exit_actor_delivers_inflight_task_results(ray_start_regular):
    """Tasks that are already executing when exit_actor() is called from a
    concurrent thread must run to completion and deliver their results,
    instead of failing with ActorDiedError.
    """
    num_tasks = 3
    started = _StartCounter.remote()

    # One extra slot beyond the blocking tasks so the exit() task can run.
    @ray.remote(max_concurrency=num_tasks + 1)
    class ThreadedActor:
        def wait_for_exit_then_return(self, value):
            ray.get(started.incr.remote())
            # Block until exit_actor() has been called from another thread.
            core_worker = ray._private.worker.global_worker.core_worker
            wait_for_condition(lambda: core_worker.get_current_actor_should_exit())
            return value

        def exit(self):
            ray.actor.exit_actor()

    a = ThreadedActor.remote()
    refs = [a.wait_for_exit_then_return.remote(i) for i in range(num_tasks)]
    # Wait until all tasks are executing before requesting the exit.
    wait_for_condition(lambda: ray.get(started.get.remote()) == num_tasks)
    exit_ref = a.exit.remote()

    assert ray.get(refs, timeout=30) == list(range(num_tasks))
    # The exit task itself completes by exiting the actor, so its ref
    # resolves to the actor death.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(exit_ref, timeout=30)
    # Tasks submitted after the exit fail with ActorDiedError.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.wait_for_exit_then_return.remote(99), timeout=30)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_exit_actor_delivers_inflight_task_errors(ray_start_regular):
    """An in-flight task that raises an application exception while the actor
    is gracefully exiting must deliver that exception, not ActorDiedError."""
    started = SignalActor.remote()

    @ray.remote(max_concurrency=2)
    class ThreadedActor:
        def wait_for_exit_then_raise(self):
            ray.get(started.send.remote())
            # Block until exit_actor() has been called from another thread.
            core_worker = ray._private.worker.global_worker.core_worker
            wait_for_condition(lambda: core_worker.get_current_actor_should_exit())
            raise ValueError("application error")

        def exit(self):
            ray.actor.exit_actor()

    a = ThreadedActor.remote()
    ref = a.wait_for_exit_then_raise.remote()
    ray.get(started.wait.remote())
    exit_ref = a.exit.remote()

    with pytest.raises(ValueError, match="application error"):
        ray.get(ref, timeout=30)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(exit_ref, timeout=30)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 2}], indirect=True)
def test_exit_actor_fails_queued_tasks(ray_start_regular):
    """Methods that are queued (submitted but not yet started executing) when
    exit_actor() is called fail with ActorDiedError."""
    max_concurrency = 2
    started = _StartCounter.remote()
    release = SignalActor.remote()

    @ray.remote(max_concurrency=max_concurrency)
    class ThreadedActor:
        def block_then_exit(self):
            ray.get(started.incr.remote())
            # Hold the concurrency slot until released, then exit the actor.
            ray.get(release.wait.remote())
            ray.actor.exit_actor()

        def work(self, value):
            return value

    a = ThreadedActor.remote()
    # Fill every concurrency slot with a blocking task so later calls can't
    # start and remain queued.
    blocking_refs = [a.block_then_exit.remote() for _ in range(max_concurrency)]
    wait_for_condition(lambda: ray.get(started.get.remote()) == max_concurrency)
    # These can't start (all slots busy) and are queued when the actor exits.
    queued_refs = [a.work.remote(i) for i in range(3)]

    # Release the blocking tasks; they call exit_actor() and the actor exits.
    ray.get(release.send.remote())

    # The tasks that called exit_actor() resolve to the actor death.
    for ref in blocking_refs:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(ref, timeout=30)
    # Queued (never-started) tasks fail with ActorDiedError.
    for ref in queued_refs:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(ref, timeout=30)


if __name__ == "__main__":

    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-sv", __file__]))
