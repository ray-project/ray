import ray
import numpy as np
import pyarrow
import unittest

MB = 1024 * 1024

OBJECT_EVICTED = ray.exceptions.UnreconstructableError
OBJECT_TOO_LARGE = pyarrow._plasma.PlasmaStoreFull


@ray.remote
class LightActor(object):
    def __init__(self):
        pass

    def sample(self):
        return "tiny_return_value"


@ray.remote
class GreedyActor(object):
    def __init__(self):
        pass

    def sample(self):
        return np.zeros(10 * MB, dtype=np.uint8)


class TestMemoryLimits(unittest.TestCase):
    def testWithoutQuota(self):
        self.assertRaises(OBJECT_EVICTED, lambda: self._run(None, None, None))
        self.assertRaises(OBJECT_EVICTED,
                          lambda: self._run(1 * MB, None, None))
        self.assertRaises(OBJECT_EVICTED,
                          lambda: self._run(None, 1 * MB, None))

    def testQuotasProtectSelf(self):
        self._run(1 * MB, 1 * MB, None)

    def testQuotasProtectOthers(self):
        self._run(None, None, 50 * MB)

    def testQuotaTooLarge(self):
        self.assertRaisesRegexp(ray.exceptions.RayTaskError,
                                ".*Failed to set object_store_memory.*",
                                lambda: self._run(100 * MB, None, None))
        self.assertRaisesRegexp(ray.memory_monitor.RayOutOfMemoryError,
                                ".*Failed to set object_store_memory.*",
                                lambda: self._run(None, None, 100 * MB))

    def testTooLargeAllocation(self):
        try:
            ray.init(driver_object_store_memory=10 * MB)
            ray.put(np.zeros(5 * MB, dtype=np.uint8))
            self.assertRaises(
                OBJECT_TOO_LARGE,
                lambda: ray.put(np.zeros(20 * MB, dtype=np.uint8)))
        finally:
            ray.shutdown()

    def _run(self, driver_quota, a_quota, b_quota):
        print("*** Testing ***", driver_quota, a_quota, b_quota)
        try:
            ray.init(
                object_store_memory=100 * MB,
                driver_object_store_memory=driver_quota)
            z = ray.put("hi")
            a = LightActor._remote(object_store_memory=a_quota)
            b = GreedyActor._remote(object_store_memory=b_quota)
            for _ in range(5):
                r_a = a.sample.remote()
                for _ in range(20):
                    ray.get(b.sample.remote())
                ray.get(r_a)
            ray.get(z)
        except Exception as e:
            print("Raised exception", type(e), e)
            raise e
        finally:
            print(ray.worker.global_worker.plasma_client.debug_string())
            ray.shutdown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
