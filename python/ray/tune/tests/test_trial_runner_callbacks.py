import os
import shutil
import sys
import tempfile
import time
import unittest
from typing import Dict

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune.checkpoint_manager import Checkpoint
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import TRAINING_ITERATION

from ray.tune.trial import Trial
from ray.tune.trial_runner import Callback, TrialRunner


class TestCallback(Callback):
    def __init__(self):
        self.state = {}

    def on_step_begin(self, trial_runner: "TrialRunner"):
        self.state["step_begin"] = trial_runner.__getstate__()

    def on_step_end(self, trial_runner: "TrialRunner"):
        self.state["step_end"] = trial_runner.__getstate__()

    def on_trial_start(self, trial_runner: "TrialRunner", trial: Trial):
        self.state["trial_start"] = (trial_runner.__getstate__(),
                                     trial.__getstate__())

    def on_trial_restore(self, trial_runner: "TrialRunner", trial: Trial):
        self.state["trial_restore"] = (trial_runner.__getstate__(),
                                       trial.__getstate__())

    def on_trial_save(self, trial_runner: "TrialRunner", trial: Trial):
        self.state["trial_save"] = (trial_runner.__getstate__(),
                                    trial.__getstate__())

    def on_trial_result(self, trial_runner: "TrialRunner", trial: Trial,
                        result: Dict):
        self.state["trial_result"] = (trial_runner.__getstate__(),
                                      trial.__getstate__(), result)

    def on_trial_complete(self, trial_runner: "TrialRunner", trial: Trial):
        self.state["trial_complete"] = (trial_runner.__getstate__(),
                                        trial.__getstate__())

    def on_trial_fail(self, trial_runner: "TrialRunner", trial: Trial):
        self.state["trial_fail"] = (trial_runner.__getstate__(),
                                    trial.__getstate__())


class _MockTrialExecutor(RayTrialExecutor):
    def __init__(self):
        super().__init__()
        self.results = {}
        self.next_trial = None
        self.failed_trial = None

    def fetch_result(self, trial):
        return self.results.get(trial, {})

    def get_next_available_trial(self):
        return self.next_trial or super().get_next_available_trial()

    def get_next_failed_trial(self):
        return self.failed_trial or super().get_next_failed_trial()


class TrialRunnerCallbacks(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.callback = TestCallback()
        self.executor = _MockTrialExecutor()
        self.trial_runner = TrialRunner(
            trial_executor=self.executor, callbacks=[self.callback])

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            del os.environ["CUDA_VISIBLE_DEVICES"]
        shutil.rmtree(self.tmpdir)

    def testCallbackSteps(self):
        trials = [
            Trial("__fake", trial_id="one"),
            Trial("__fake", trial_id="two")
        ]
        for t in trials:
            self.trial_runner.add_trial(t)

        self.trial_runner.step()

        # Trial 1 has been started
        self.assertEqual(self.callback.state["trial_start"][0]["_iteration"],
                         0)
        self.assertEqual(self.callback.state["trial_start"][1]["trial_id"],
                         "one")

        # All these events haven't happened, yet
        self.assertTrue(
            all(k not in self.callback.state for k in [
                "trial_restore", "trial_save", "trial_result",
                "trial_complete", "trial_fail"
            ]))

        self.trial_runner.step()

        # Iteration not increased yet
        self.assertEqual(self.callback.state["step_begin"]["_iteration"], 1)

        # Iteration increased
        self.assertEqual(self.callback.state["step_end"]["_iteration"], 2)

        # Second trial has been just started
        self.assertEqual(self.callback.state["trial_start"][0]["_iteration"],
                         1)
        self.assertEqual(self.callback.state["trial_start"][1]["trial_id"],
                         "two")

        cp = Checkpoint(Checkpoint.PERSISTENT, "__checkpoint",
                        {TRAINING_ITERATION: 0})

        # Let the first trial save a checkpoint
        trials[0].saving_to = cp
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_save"][0]["_iteration"], 2)
        self.assertEqual(self.callback.state["trial_save"][1]["trial_id"],
                         "one")

        # Let the second trial send a result
        trials[1].last_result = {
            TRAINING_ITERATION: 1,
            "metric": 800,
            "done": False
        }
        self.executor.results[trials[1]] = trials[1].last_result
        self.executor.next_trial = trials[1]
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_result"][0]["_iteration"],
                         3)
        self.assertEqual(self.callback.state["trial_result"][1]["trial_id"],
                         "two")
        self.assertEqual(self.callback.state["trial_result"][2]["metric"], 800)

        # Let the second trial restore from a checkpoint
        trials[1].restoring_from = cp
        self.executor.results[trials[1]] = trials[1].last_result
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_restore"][0]["_iteration"],
                         4)
        self.assertEqual(self.callback.state["trial_restore"][1]["trial_id"],
                         "two")

        # Let the second trial finish
        trials[1].restoring_from = None
        self.executor.results[trials[1]] = {
            TRAINING_ITERATION: 2,
            "metric": 900,
            "done": True
        }
        self.trial_runner.step()
        self.assertEqual(
            self.callback.state["trial_complete"][0]["_iteration"], 5)
        self.assertEqual(self.callback.state["trial_complete"][1]["trial_id"],
                         "two")

        # Let the first trial error
        self.executor.failed_trial = trials[0]
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_fail"][0]["_iteration"], 6)
        self.assertEqual(self.callback.state["trial_fail"][1]["trial_id"],
                         "one")

    def testCallbacksEndToEnd(self):
        def train(config):
            if config["do"] == "save":
                with tune.checkpoint_dir(0):
                    pass
                tune.report(metric=1)
            elif config["do"] == "fail":
                raise RuntimeError("I am failing on purpose.")
            elif config["do"] == "delay":
                time.sleep(2)
                tune.report(metric=20)

        config = {"do": tune.grid_search(["save", "fail", "delay"])}

        tune.run(
            train,
            config=config,
            raise_on_failed_trial=False,
            callbacks=[self.callback])

        self.assertEqual(self.callback.state["trial_fail"][1]["config"]["do"],
                         "fail")
        self.assertEqual(self.callback.state["trial_save"][1]["config"]["do"],
                         "save")
        self.assertEqual(
            self.callback.state["trial_result"][1]["config"]["do"], "delay")
        self.assertEqual(
            self.callback.state["trial_complete"][1]["config"]["do"], "delay")


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
