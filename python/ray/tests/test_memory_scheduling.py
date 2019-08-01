import ray
import unittest

MB = 1024 * 1024


@ray.remote(memory=100 * MB)
class Actor(object):
    def __init__(self):
        pass

    def ping(self):
        return "ok"


@ray.remote(object_store_memory=100 * MB)
class Actor2(object):
    def __init__(self):
        pass

    def ping(self):
        return "ok"


class TestMemoryScheduling(unittest.TestCase):
    def testMemoryRequest(self):
        try:
            ray.init(memory=200 * MB)
            # fits first 2
            a = Actor.remote()
            b = Actor.remote()
            ok, _ = ray.wait(
                [a.ping.remote(), b.ping.remote()],
                timeout=60.0,
                num_returns=2)
            self.assertEqual(len(ok), 2)
            # does not fit
            c = Actor.remote()
            ok, _ = ray.wait([c.ping.remote()], timeout=5.0)
            self.assertEqual(len(ok), 0)
        finally:
            ray.shutdown()

    def testObjectStoreMemoryRequest(self):
        try:
            ray.init(object_store_memory=300 * MB)
            # fits first 2 (70% allowed)
            a = Actor2.remote()
            b = Actor2.remote()
            ok, _ = ray.wait(
                [a.ping.remote(), b.ping.remote()],
                timeout=60.0,
                num_returns=2)
            self.assertEqual(len(ok), 2)
            # does not fit
            c = Actor2.remote()
            ok, _ = ray.wait([c.ping.remote()], timeout=5.0)
            self.assertEqual(len(ok), 0)
        finally:
            ray.shutdown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
