import numpy as np
import unittest

import ray
from ray.test_utils import put_unpinned_object

MB = 1024 * 1024

OBJECT_EVICTED = ray.exceptions.ObjectLostError
OBJECT_TOO_LARGE = ray.exceptions.ObjectStoreFullError


@ray.remote
class LightActor:
    def __init__(self):
        pass

    def sample(self):
        return np.zeros(5 * MB, dtype=np.uint8)


@ray.remote
class GreedyActor:
    def __init__(self):
        pass

    def sample(self):
        return np.zeros(20 * MB, dtype=np.uint8)


class TestMemoryLimits(unittest.TestCase):
    def testWithoutQuota(self):
        self._run(100 * MB, None, None)
        self.assertRaises(OBJECT_EVICTED, lambda: self._run(None, None, None))
        self.assertRaises(OBJECT_EVICTED,
                          lambda: self._run(None, 100 * MB, None))

    def testQuotasProtectSelf(self):
        self._run(100 * MB, 100 * MB, None)

    def testQuotasProtectOthers(self):
        self._run(None, None, 100 * MB)

    def testQuotaTooLarge(self):
        self.assertRaisesRegexp(ray.memory_monitor.RayOutOfMemoryError,
                                ".*Failed to set object_store_memory.*",
                                lambda: self._run(300 * MB, None, None))

    def testTooLargeAllocation(self):
        try:
            ray.init(num_cpus=1, _driver_object_store_memory=100 * MB)
            ray.worker.global_worker.put_object(
                np.zeros(50 * MB, dtype=np.uint8))
            self.assertRaises(
                OBJECT_TOO_LARGE,
                lambda: ray.put(np.zeros(200 * MB, dtype=np.uint8)))
        finally:
            ray.shutdown()

    def _run(self, driver_quota, a_quota, b_quota):
        print("*** Testing ***", driver_quota, a_quota, b_quota)
        try:
            ray.init(
                num_cpus=1,
                object_store_memory=300 * MB,
                _driver_object_store_memory=driver_quota)
            obj = np.ones(200 * 1024, dtype=np.uint8)
            z = put_unpinned_object(obj)
            a = LightActor._remote(object_store_memory=a_quota)
            b = GreedyActor._remote(object_store_memory=b_quota)
            for _ in range(5):
                r_a = a.sample.remote()
                for _ in range(20):
                    new_oid = b.sample.remote()
                    ray.get(new_oid)
                ray.get(r_a)
            ray.get(z)
        except Exception as e:
            print("Raised exception", type(e), e)
            raise e
        finally:
            print(ray.worker.global_worker.core_worker.
                  dump_object_store_memory_usage())
            ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
