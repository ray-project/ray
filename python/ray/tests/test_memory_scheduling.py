import numpy as np
import unittest

import ray
from ray import tune
from ray.rllib import _register_all

MB = 1024 * 1024


@ray.remote(memory=100 * MB)
class Actor:
    def __init__(self):
        pass

    def ping(self):
        return "ok"


@ray.remote(object_store_memory=100 * MB)
class Actor2:
    def __init__(self):
        pass

    def ping(self):
        return "ok"


def train_oom(config, reporter):
    ray.put(np.zeros(200 * 1024 * 1024))
    reporter(result=123)


class TestMemoryScheduling(unittest.TestCase):
    def testMemoryRequest(self):
        try:
            ray.init(num_cpus=1, _memory=200 * MB)
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
            ray.init(num_cpus=1, object_store_memory=300 * MB)
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

    def testTuneDriverStoreLimit(self):
        try:
            ray.init(
                num_cpus=4,
                _memory=100 * MB,
                object_store_memory=100 * MB,
            )
            _register_all()
            self.assertRaisesRegexp(
                ray.tune.error.TuneError,
                ".*Insufficient cluster resources.*",
                lambda: tune.run(
                    "PG",
                    stop={"timesteps_total": 10000},
                    config={
                        "env": "CartPole-v0",
                        # too large
                        "object_store_memory": 10000 * 1024 * 1024,
                        "framework": "tf",
                    }))
        finally:
            ray.shutdown()

    def testTuneWorkerStoreLimit(self):
        try:
            ray.init(
                num_cpus=4,
                _memory=100 * MB,
                object_store_memory=100 * MB,
            )
            _register_all()
            self.assertRaisesRegexp(
                ray.tune.error.TuneError,
                ".*Insufficient cluster resources.*",
                lambda:
                tune.run("PG", stop={"timesteps_total": 0}, config={
                    "env": "CartPole-v0",
                    "num_workers": 1,
                    # too large
                    "object_store_memory_per_worker": 10000 * 1024 * 1024,
                    "framework": "tf",
                }))
        finally:
            ray.shutdown()

    def testTuneObjectLimitApplied(self):
        try:
            ray.init(num_cpus=2, object_store_memory=500 * MB)
            result = tune.run(
                train_oom,
                resources_per_trial={"object_store_memory": 150 * 1024 * 1024},
                raise_on_failed_trial=False)
            self.assertTrue(result.trials[0].status, "ERROR")
            self.assertTrue("ObjectStoreFullError: Failed to put" in
                            result.trials[0].error_msg)
        finally:
            ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
