import os
import pickle
import unittest
import sys
from collections import defaultdict

import ray
from ray import tune, logger
from ray.tune import Trainable, run_experiments, register_trainable
from ray.tune.error import TuneError
from ray.tune.function_runner import wrap_function
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.PAUSE


def create_resettable_class():
    class MyResettableClass(Trainable):
        def setup(self, config):
            self.config = config
            self.num_resets = 0
            self.iter = 0
            self.msg = config.get("message", "No message")

        def step(self):
            self.iter += 1

            print("PRINT_STDOUT: {}".format(self.msg))
            print("PRINT_STDERR: {}".format(self.msg), file=sys.stderr)
            logger.info("LOG_STDERR: {}".format(self.msg))

            return {
                "id": self.config["id"],
                "num_resets": self.num_resets,
                "done": self.iter > 1,
                "iter": self.iter
            }

        def save_checkpoint(self, chkpt_dir):
            return {"iter": self.iter}

        def load_checkpoint(self, item):
            self.iter = item["iter"]

        def reset_config(self, new_config):
            if "fake_reset_not_supported" in self.config:
                return False
            self.num_resets += 1
            self.msg = new_config.get("message", "No message")
            return True

    return MyResettableClass


def create_resettable_function(num_resets: defaultdict):
    def trainable(config, checkpoint_dir=None):
        if checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "chkpt"), "rb") as fp:
                step = pickle.load(fp)
        else:
            step = 0

        while step < 2:
            step += 1
            with tune.checkpoint_dir(step) as checkpoint_dir:
                with open(os.path.join(checkpoint_dir, "chkpt"), "wb") as fp:
                    pickle.dump(step, fp)
            tune.report(**{
                "done": step >= 2,
                "iter": step,
                "id": config["id"]
            })

    trainable = wrap_function(trainable)

    class ResetCountTrainable(trainable):
        def reset_config(self, new_config):
            num_resets[self.trial_id] += 1
            return super().reset_config(new_config)

    return ResetCountTrainable


class ActorReuseTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1, num_gpus=0)
        os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"

    def tearDown(self):
        ray.shutdown()

    def _run_trials_with_frequent_pauses(self, trainable, reuse=False):
        trials = run_experiments(
            {
                "foo": {
                    "run": trainable,
                    "num_samples": 1,
                    "config": {
                        "id": tune.grid_search([0, 1, 2, 3])
                    },
                }
            },
            reuse_actors=reuse,
            scheduler=FrequentPausesScheduler(),
            verbose=0)
        return trials

    def testTrialReuseDisabled(self):
        trials = self._run_trials_with_frequent_pauses(
            create_resettable_class(), reuse=False)
        self.assertEqual([t.last_result["id"] for t in trials], [0, 1, 2, 3])
        self.assertEqual([t.last_result["iter"] for t in trials], [2, 2, 2, 2])
        self.assertEqual([t.last_result["num_resets"] for t in trials],
                         [0, 0, 0, 0])

    def testTrialReuseDisabledFunction(self):
        num_resets = defaultdict(lambda: 0)
        trials = self._run_trials_with_frequent_pauses(
            create_resettable_function(num_resets), reuse=False)
        self.assertEqual([t.last_result["id"] for t in trials], [0, 1, 2, 3])
        self.assertEqual([t.last_result["iter"] for t in trials], [2, 2, 2, 2])
        self.assertEqual([num_resets[t.trial_id] for t in trials],
                         [0, 0, 0, 0])

    def testTrialReuseEnabled(self):
        trials = self._run_trials_with_frequent_pauses(
            create_resettable_class(), reuse=True)
        self.assertEqual([t.last_result["id"] for t in trials], [0, 1, 2, 3])
        self.assertEqual([t.last_result["iter"] for t in trials], [2, 2, 2, 2])
        self.assertEqual([t.last_result["num_resets"] for t in trials],
                         [1, 2, 3, 4])

    def testTrialReuseEnabledFunction(self):
        num_resets = defaultdict(lambda: 0)
        trials = self._run_trials_with_frequent_pauses(
            create_resettable_function(num_resets), reuse=True)
        self.assertEqual([t.last_result["id"] for t in trials], [0, 1, 2, 3])
        self.assertEqual([t.last_result["iter"] for t in trials], [2, 2, 2, 2])
        self.assertEqual([num_resets[t.trial_id] for t in trials],
                         [0, 0, 0, 0])

    def testReuseEnabledError(self):
        def run():
            run_experiments(
                {
                    "foo": {
                        "run": create_resettable_class(),
                        "max_failures": 1,
                        "num_samples": 1,
                        "config": {
                            "id": tune.grid_search([0, 1, 2, 3]),
                            "fake_reset_not_supported": True
                        },
                    }
                },
                reuse_actors=True,
                scheduler=FrequentPausesScheduler())

        self.assertRaises(TuneError, lambda: run())

    def testTrialReuseLogToFile(self):
        register_trainable("foo2", create_resettable_class())

        # Log to default files
        [trial1, trial2] = tune.run(
            "foo2",
            config={
                "message": tune.grid_search(["First", "Second"]),
                "id": -1
            },
            log_to_file=True,
            scheduler=FrequentPausesScheduler(),
            reuse_actors=True).trials

        # Check trial 1
        self.assertEqual(trial1.last_result["num_resets"], 1)
        self.assertTrue(os.path.exists(os.path.join(trial1.logdir, "stdout")))
        self.assertTrue(os.path.exists(os.path.join(trial1.logdir, "stderr")))
        with open(os.path.join(trial1.logdir, "stdout"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDOUT: First", content)
            self.assertNotIn("PRINT_STDOUT: Second", content)
        with open(os.path.join(trial1.logdir, "stderr"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDERR: First", content)
            self.assertIn("LOG_STDERR: First", content)
            self.assertNotIn("PRINT_STDERR: Second", content)
            self.assertNotIn("LOG_STDERR: Second", content)

        # Check trial 2
        self.assertEqual(trial2.last_result["num_resets"], 2)
        self.assertTrue(os.path.exists(os.path.join(trial2.logdir, "stdout")))
        self.assertTrue(os.path.exists(os.path.join(trial2.logdir, "stderr")))
        with open(os.path.join(trial2.logdir, "stdout"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDOUT: Second", content)
            self.assertNotIn("PRINT_STDOUT: First", content)
        with open(os.path.join(trial2.logdir, "stderr"), "rt") as fp:
            content = fp.read()
            self.assertIn("PRINT_STDERR: Second", content)
            self.assertIn("LOG_STDERR: Second", content)
            self.assertNotIn("PRINT_STDERR: First", content)
            self.assertNotIn("LOG_STDERR: First", content)


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
