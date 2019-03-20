from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.tune import Trainable, run_experiments
from ray.tune.error import TuneError
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.PAUSE


def create_resettable_class():
    class MyResettableClass(Trainable):
        def _setup(self, config):
            self.config = config
            self.num_resets = 0
            self.iter = 0

        def _train(self):
            self.iter += 1
            return {"num_resets": self.num_resets, "done": self.iter > 1}

        def _save(self, chkpt_dir):
            return {"iter": self.iter}

        def _restore(self, item):
            self.iter = item["iter"]

        def reset_config(self, new_config):
            if "fake_reset_not_supported" in self.config:
                return False
            self.num_resets += 1
            return True

    return MyResettableClass


class ActorReuseTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0)

    def tearDown(self):
        ray.shutdown()

    def testTrialReuseDisabled(self):
        trials = run_experiments(
            {
                "foo": {
                    "run": create_resettable_class(),
                    "num_samples": 4,
                    "config": {},
                }
            },
            reuse_actors=False,
            scheduler=FrequentPausesScheduler())
        self.assertEqual([t.last_result["num_resets"] for t in trials],
                         [0, 0, 0, 0])

    def testTrialReuseEnabled(self):
        trials = run_experiments(
            {
                "foo": {
                    "run": create_resettable_class(),
                    "num_samples": 4,
                    "config": {},
                }
            },
            reuse_actors=True,
            scheduler=FrequentPausesScheduler())
        self.assertEqual([t.last_result["num_resets"] for t in trials],
                         [1, 2, 3, 4])

    def testTrialReuseEnabledError(self):
        def run():
            run_experiments(
                {
                    "foo": {
                        "run": create_resettable_class(),
                        "max_failures": 1,
                        "num_samples": 4,
                        "config": {
                            "fake_reset_not_supported": True
                        },
                    }
                },
                reuse_actors=True,
                scheduler=FrequentPausesScheduler())

        self.assertRaises(TuneError, lambda: run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
