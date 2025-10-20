import os
import sys
import threading
import time

import numpy as np
import pytest

import ray


@pytest.fixture(autouse=True)
def cap_object_transfer_throughput():
    # This will make the object transfer slower and allow the test to
    # interleave Get requests.
    # TODO(57923): Make this not autouse when a second test is added to this file.
    os.environ["RAY_object_manager_max_bytes_in_flight"] = str(1024**2)


def test_multithreaded_ray_get(ray_start_cluster):
    # This test tries to get a large object from the head node to the worker node
    # while making many concurrent ray.get requests for a local object in plasma.
    # TODO(57923): Make this not rely on timing if possible.
    ray_cluster = ray_start_cluster
    ray_cluster.add_node()
    ray.init()
    ray_cluster.add_node(resources={"worker": 1})

    @ray.remote(resources={"worker": 1})
    class Actor:
        def ready(self):
            return

        def run(self, refs_to_get):
            remote_large_ref = refs_to_get[0]
            # ray.put will ensure that the object is in plasma
            # even if it's small.
            local_small_ref = ray.put("1")

            def get_small_ref_forever():
                while True:
                    ray.get(local_small_ref)
                    time.sleep(0.1)

            background_thread = threading.Thread(
                target=get_small_ref_forever, daemon=True
            )
            background_thread.start()
            ray.get(remote_large_ref)

    large_ref = ray.put(np.ones(1024**3, dtype=np.int8))
    actor = Actor.remote()
    ray.get(actor.ready.remote())

    ray.get(actor.run.remote([large_ref]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
