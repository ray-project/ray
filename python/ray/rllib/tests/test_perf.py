from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import time
import unittest

import ray
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.tests.test_rollout_worker import MockPolicy


class TestPerf(unittest.TestCase):
    # Tested on Intel(R) Core(TM) i7-4600U CPU @ 2.10GHz
    # 11/23/18: Samples per second 8501.125113727468
    # 03/01/19: Samples per second 8610.164353268685
    def testBaselinePerformance(self):
        for _ in range(20):
            ev = RolloutWorker(
                env_creator=lambda _: gym.make("CartPole-v0"),
                policy=MockPolicy,
                batch_steps=100)
            start = time.time()
            count = 0
            while time.time() - start < 1:
                count += ev.sample().count
            print()
            print("Samples per second {}".format(
                count / (time.time() - start)))
            print()


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
