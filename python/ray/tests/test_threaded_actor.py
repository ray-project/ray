import sys
import threading
import time

import numpy as np
import ray

from ray.state import available_resources
import ray._private.test_utils as test_utils


def ensure_cpu_returned(expected_cpus):
    test_utils.wait_for_condition(
        lambda: (available_resources().get("CPU", 0) == expected_cpus))


def test_threaded_actor_basic(shutdown_only):
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


def test_threaded_actor_api_thread_safe(shutdown_only):
    """Test if Ray APIs are thread safe
        when they are used within threaded actor.
    """
    ray.init(
        num_cpus=8,
        # from 1024 bytes, the return obj will go to the plasma store.
        _system_config={"max_direct_call_object_size": 1024})

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
        [a.in_memory_return_test.remote(seqno) for seqno in range(max_seq)])
    assert sorted(seqnos) == list(range(max_seq))

    # Test plasma return obj
    real = ray.get(
        [a.plasma_return_test.remote(seqno) for seqno in range(max_seq)])
    expected = [np.zeros(8 * 1024 * i, dtype=np.uint8) for i in range(max_seq)]
    for r, e in zip(real, expected):
        assert np.array_equal(r, e)

    ray.kill(a)
    ensure_cpu_returned(8)


def test_threaded_actor_creation_and_kill(ray_start_cluster):
    """Test the scenario where the threaded actors are created and killed.
    """
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


if __name__ == "__main__":
    import pytest
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
