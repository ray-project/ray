import numpy as np
import unittest

import ray
from ray import tune
from ray.rllib import _register_all

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


def train_oom(config, reporter):
    ray.put(np.zeros(200 * 1024 * 1024))
    reporter(result=123)


class TestMemoryScheduling(unittest.TestCase):
    def testMemoryRequest(self):
        try:
            ray.init(num_cpus=1, memory=200 * MB)
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

    def testTuneDriverHeapLimit(self):
        try:
            _register_all()
            result = tune.run(
                "PG",
                stop={"timesteps_total": 10000},
                config={
                    "env": "CartPole-v0",
                    "memory": 100 * 1024 * 1024,  # too little
                },
                raise_on_failed_trial=False)
            self.assertEqual(result.trials[0].status, "ERROR")
            self.assertTrue(
                "RayOutOfMemoryError: Heap memory usage for ray_PG_" in
                result.trials[0].error_msg)
        finally:
            ray.shutdown()

    def testTuneDriverStoreLimit(self):
        try:
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
                    }))
        finally:
            ray.shutdown()

    def testTuneWorkerHeapLimit(self):
        try:
            _register_all()
            result = tune.run(
                "PG",
                stop={"timesteps_total": 10000},
                config={
                    "env": "CartPole-v0",
                    "num_workers": 1,
                    "memory_per_worker": 100 * 1024 * 1024,  # too little
                },
                raise_on_failed_trial=False)
            self.assertEqual(result.trials[0].status, "ERROR")
            self.assertTrue(
                "RayOutOfMemoryError: Heap memory usage for ray_Rollout" in
                result.trials[0].error_msg)
        finally:
            ray.shutdown()

    def testTuneWorkerStoreLimit(self):
        try:
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
                }))
        finally:
            ray.shutdown()

    def testTuneObjectLimitApplied(self):
        try:
            result = tune.run(
                train_oom,
                resources_per_trial={"object_store_memory": 150 * 1024 * 1024},
                raise_on_failed_trial=False)
            self.assertTrue(result.trials[0].status, "ERROR")
            self.assertTrue("PlasmaStoreFull: object does not fit" in
                            result.trials[0].error_msg)
        finally:
            ray.shutdown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
