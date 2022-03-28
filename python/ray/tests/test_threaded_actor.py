import sys
import threading

import pytest
import numpy as np
import ray

from ray.state import available_resources
import ray._private.test_utils as test_utils


def ensure_cpu_returned(expected_cpus):
    test_utils.wait_for_condition(
        lambda: (available_resources().get("CPU", 0) == expected_cpus)
    )


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


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
