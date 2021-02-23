import gc
import numpy as np
import time
import unittest

import ray

MB = 1024 * 1024


@ray.remote(memory=100 * MB)
class Actor:
    def __init__(self):
        pass

    def ping(self):
        return "ok"


def test_memory_request():
    try:
        ray.init(num_cpus=1, _memory=200 * MB)
        # fits first 2
        a = Actor.remote()
        b = Actor.remote()
        ok, _ = ray.wait(
            [a.ping.remote(), b.ping.remote()],
            timeout=60.0,
            num_returns=2)
        assert len(ok) == 2
        # does not fit
        c = Actor.remote()
        ok, _ = ray.wait([c.ping.remote()], timeout=5.0)
        assert len(ok) == 0
    finally:
        ray.shutdown()


def test_object_store_memory_reporting():
    try:
        ray.init(num_cpus=1, object_store_memory=500 * MB)
        time.sleep(0.5)
        assert ray.available_resources()["object_store_memory"] == 10.0
        x1 = ray.put(np.zeros(150 * 1024 * 1024, dtype=np.uint8))
        time.sleep(0.5)
        assert ray.available_resources()["object_store_memory"] == 7.0
        x2 = ray.put(np.zeros(75 * 1024 * 1024, dtype=np.uint8))
        time.sleep(0.5)
        assert ray.available_resources()["object_store_memory"] == 5.5
        del x1
        del x2
        gc.collect()
        time.sleep(1.0)
        assert ray.available_resources()["object_store_memory"] == 10.0
    finally:
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
