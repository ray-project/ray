from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.tune import Trainable, run_experiments


class MyResettableClass(Trainable):
    def _setup(self, config):
        self.config = config
        self.num_resets = 0

    def _train(self):
        return {"num_resets": self.num_resets, "done": True}

    def reset_config(self, new_config, reset_state):
        if reset_state:
            self.num_resets += 1
        return True


class ActorReuseTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0)

    def tearDown(self):
        ray.shutdown()

    def testTrialReuseDisabled(self):
        trials = run_experiments({
            "foo": {
                "run": MyResettableClass,
                "num_samples": 4,
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        }, reuse_actors=False)
        self.assertEqual([t.last_result["num_resets"] for t in trials], [0, 0, 0, 0])

    def testTrialReuseEnabled(self):
        trials = run_experiments({
            "foo": {
                "run": MyResettableClass,
                "num_samples": 4,
                "config": {
                    "script_min_iter_time_s": 0,
                },
            }
        }, reuse_actors=True)
        self.assertEqual([t.last_result["num_resets"] for t in trials], [0, 1, 2, 3])


if __name__ == "__main__":
    unittest.main(verbosity=2)
