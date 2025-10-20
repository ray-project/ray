import sys
import threading
import time

import numpy as np
import pytest

import ray


def test_multithreaded_ray_get(ray_start_cluster):
    # This will make the object transfer slower and allow the test to
    # interleave Get requests.
    slow_object_transfer_system_config = {
        "object_manager_max_bytes_in_flight": str(1024**2)
    }

    # This test tries to get a large object from the head node to the worker node
    # while making many concurrent ray.get requests for a local object in plasma.
    # TODO(57923): Make this not rely on timing if possible.
    ray_cluster = ray_start_cluster
    ray_cluster.add_node(_system_config=slow_object_transfer_system_config)
    ray.init(address=ray_cluster.address)
    ray_cluster.add_node(
        resources={"worker": 1}, _system_config=slow_object_transfer_system_config
    )

    @ray.remote(resources={"worker": 1}, max_concurrency=2)
    class Actor:
        def __init__(self):
            # ray.put will ensure that the object is in plasma
            # even if it's small.
            self._local_small_ref = ray.put("1")
            self._small_gets_started = threading.Event()
            self._stop_small_gets = threading.Event()

        def small_gets_started(self):
            self._small_gets_started.wait()

        def stop_small_gets(self):
            self._stop_small_gets.set()

        def do_small_gets(self):
            while not self._stop_small_gets.is_set():
                ray.get(self._local_small_ref)
                time.sleep(0.1)

        def do_large_get(self, refs_to_get):
            remote_large_ref = refs_to_get[0]
            ray.get(remote_large_ref)

    actor = Actor.remote()

    # Start a task on one thread that will repeatedly call `ray.get` on small
    # plasma objects.
    small_gets_ref = actor.do_small_gets.remote()
    ray.get(actor.small_gets_started.remote())

    # Start a second task on another thread that will call `ray.get` on a large object.
    # The transfer will be slow due to the system config set above.
    large_ref = ray.put(np.ones(1024**3, dtype=np.int8))
    ray.get(actor.do_large_get.remote([large_ref]))

    # Check that all `ray.get` calls succeeded.
    ray.get(actor.stop_small_gets.remote())
    ray.get(small_gets_ref)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
