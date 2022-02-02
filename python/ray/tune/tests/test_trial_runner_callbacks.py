import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from collections import OrderedDict

import ray
from ray import tune
from ray.exceptions import RayActorError
from ray.rllib import _register_all
from ray.tune.checkpoint_manager import Checkpoint
from ray.tune.logger import DEFAULT_LOGGERS, LoggerCallback, LegacyLoggerCallback
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.result import TRAINING_ITERATION
from ray.tune.syncer import SyncConfig, SyncerCallback

from ray.tune.callback import warnings
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune import Callback
from ray.tune.utils.callback import create_default_callbacks
from ray.tune.experiment import Experiment


class TestCallback(Callback):
    def __init__(self):
        self.state = OrderedDict()

    def setup(self, **info):
        self.state["setup"] = info

    def on_step_begin(self, **info):
        self.state["step_begin"] = info

    def on_step_end(self, **info):
        self.state["step_end"] = info

    def on_trial_start(self, **info):
        self.state["trial_start"] = info

    def on_trial_restore(self, **info):
        self.state["trial_restore"] = info

    def on_trial_save(self, **info):
        self.state["trial_save"] = info

    def on_trial_result(self, **info):
        self.state["trial_result"] = info
        result = info["result"]
        trial = info["trial"]
        assert result.get(TRAINING_ITERATION, None) != trial.last_result.get(
            TRAINING_ITERATION, None
        )

    def on_trial_complete(self, **info):
        self.state["trial_complete"] = info

    def on_trial_error(self, **info):
        self.state["trial_fail"] = info

    def on_experiment_end(self, **info):
        self.state["experiment_end"] = info


class _MockTrialExecutor(RayTrialExecutor):
    def __init__(self):
        super().__init__()
        self.next_trial = None
        self.results = {}
        self.should_fail_in_fetch_result = False

    def fetch_result(self, trial):
        if self.should_fail_in_fetch_result:
            raise RayActorError(
                "The actor died unexpectedly before finishing this task."
            )
        else:
            return [self.results.get(trial, {})]

    def get_next_available_trial(self, timeout=None):
        return self.next_trial or super().get_next_available_trial()


class TrialRunnerCallbacks(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "1"

        ray.init()
        self.tmpdir = tempfile.mkdtemp()
        self.callback = TestCallback()
        self.executor = _MockTrialExecutor()
        self.trial_runner = TrialRunner(
            trial_executor=self.executor, callbacks=[self.callback]
        )
        # experiment would never be None normally, but it's fine for testing
        self.trial_runner.setup_experiments(experiments=[None], total_num_samples=1)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects
        if "CUDA_VISIBLE_DEVICES" in os.environ:
            del os.environ["CUDA_VISIBLE_DEVICES"]
        shutil.rmtree(self.tmpdir)

    def testCallbackSteps(self):
        trials = [Trial("__fake", trial_id="one"), Trial("__fake", trial_id="two")]
        for t in trials:
            self.trial_runner.add_trial(t)

        self.executor.next_trial = trials[0]
        self.trial_runner.step()

        # Trial 1 has been started
        self.assertEqual(self.callback.state["trial_start"]["iteration"], 0)
        self.assertEqual(self.callback.state["trial_start"]["trial"].trial_id, "one")

        # All these events haven't happened, yet
        self.assertTrue(
            all(
                k not in self.callback.state
                for k in [
                    "trial_restore",
                    "trial_save",
                    "trial_result",
                    "trial_complete",
                    "trial_fail",
                    "experiment_end",
                ]
            )
        )

        self.executor.next_trial = trials[1]
        self.trial_runner.step()

        # Iteration not increased yet
        self.assertEqual(self.callback.state["step_begin"]["iteration"], 1)

        # Iteration increased
        self.assertEqual(self.callback.state["step_end"]["iteration"], 2)

        # Second trial has been just started
        self.assertEqual(self.callback.state["trial_start"]["iteration"], 1)
        self.assertEqual(self.callback.state["trial_start"]["trial"].trial_id, "two")

        cp = Checkpoint(Checkpoint.PERSISTENT, "__checkpoint", {TRAINING_ITERATION: 0})

        # Let the first trial save a checkpoint
        self.executor.next_trial = trials[0]
        trials[0].saving_to = cp
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_save"]["iteration"], 2)
        self.assertEqual(self.callback.state["trial_save"]["trial"].trial_id, "one")

        # Let the second trial send a result
        result = {TRAINING_ITERATION: 1, "metric": 800, "done": False}
        self.executor.results[trials[1]] = result
        self.executor.next_trial = trials[1]
        self.assertTrue(not trials[1].has_reported_at_least_once)
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_result"]["iteration"], 3)
        self.assertEqual(self.callback.state["trial_result"]["trial"].trial_id, "two")
        self.assertEqual(self.callback.state["trial_result"]["result"]["metric"], 800)
        self.assertEqual(trials[1].last_result["metric"], 800)

        # Let the second trial restore from a checkpoint
        trials[1].restoring_from = cp
        self.executor.results[trials[1]] = trials[1].last_result
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_restore"]["iteration"], 4)
        self.assertEqual(self.callback.state["trial_restore"]["trial"].trial_id, "two")

        # Let the second trial finish
        trials[1].restoring_from = None
        self.executor.results[trials[1]] = {
            TRAINING_ITERATION: 2,
            "metric": 900,
            "done": True,
        }
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_complete"]["iteration"], 5)
        self.assertEqual(self.callback.state["trial_complete"]["trial"].trial_id, "two")

        # Let the first trial error
        self.executor.next_trial = trials[0]
        self.executor.should_fail_in_fetch_result = True
        self.trial_runner.step()
        self.assertEqual(self.callback.state["trial_fail"]["iteration"], 6)
        self.assertEqual(self.callback.state["trial_fail"]["trial"].trial_id, "one")

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
            train, config=config, raise_on_failed_trial=False, callbacks=[self.callback]
        )

        self.assertIn("setup", self.callback.state)
        self.assertTrue(self.callback.state["setup"] is not None)
        keys = Experiment.PUBLIC_KEYS.copy()
        keys.add("total_num_samples")
        for key in keys:
            self.assertIn(key, self.callback.state["setup"])
        # check if it was added first
        self.assertTrue(list(self.callback.state)[0] == "setup")
        self.assertEqual(
            self.callback.state["trial_fail"]["trial"].config["do"], "fail"
        )
        self.assertEqual(
            self.callback.state["trial_save"]["trial"].config["do"], "save"
        )
        self.assertEqual(
            self.callback.state["trial_result"]["trial"].config["do"], "delay"
        )
        self.assertEqual(
            self.callback.state["trial_complete"]["trial"].config["do"], "delay"
        )
        self.assertIn("experiment_end", self.callback.state)
        # check if it was added last
        self.assertTrue(list(self.callback.state)[-1] == "experiment_end")

    def testCallbackReordering(self):
        """SyncerCallback should come after LoggerCallback callbacks"""

        def get_positions(callbacks):
            first_logger_pos = None
            last_logger_pos = None
            syncer_pos = None
            for i, callback in enumerate(callbacks):
                if isinstance(callback, LoggerCallback):
                    if first_logger_pos is None:
                        first_logger_pos = i
                    last_logger_pos = i
                elif isinstance(callback, SyncerCallback):
                    syncer_pos = i
            return first_logger_pos, last_logger_pos, syncer_pos

        # Auto creation of loggers, no callbacks, no syncer
        callbacks = create_default_callbacks(None, SyncConfig(), None)
        first_logger_pos, last_logger_pos, syncer_pos = get_positions(callbacks)
        self.assertLess(last_logger_pos, syncer_pos)

        # Auto creation of loggers with callbacks
        callbacks = create_default_callbacks([Callback()], SyncConfig(), None)
        first_logger_pos, last_logger_pos, syncer_pos = get_positions(callbacks)
        self.assertLess(last_logger_pos, syncer_pos)

        # Auto creation of loggers with existing logger (but no CSV/JSON)
        callbacks = create_default_callbacks([LoggerCallback()], SyncConfig(), None)
        first_logger_pos, last_logger_pos, syncer_pos = get_positions(callbacks)
        self.assertLess(last_logger_pos, syncer_pos)

        # This should throw an error as the syncer comes before the logger
        with self.assertRaises(ValueError):
            callbacks = create_default_callbacks(
                [SyncerCallback(None), LoggerCallback()], SyncConfig(), None
            )

        # This should be reordered but preserve the regular callback order
        [mc1, mc2, mc3] = [Callback(), Callback(), Callback()]
        # Has to be legacy logger to avoid logger callback creation
        lc = LegacyLoggerCallback(logger_classes=DEFAULT_LOGGERS)
        callbacks = create_default_callbacks([mc1, mc2, lc, mc3], SyncConfig(), None)
        first_logger_pos, last_logger_pos, syncer_pos = get_positions(callbacks)
        self.assertLess(last_logger_pos, syncer_pos)
        self.assertLess(callbacks.index(mc1), callbacks.index(mc2))
        self.assertLess(callbacks.index(mc2), callbacks.index(mc3))
        self.assertLess(callbacks.index(lc), callbacks.index(mc3))
        # Syncer callback is appended
        self.assertLess(callbacks.index(mc3), syncer_pos)

    @patch.object(warnings, "warn")
    def testCallbackSetupBackwardsCompatible(self, mocked_warning_method):
        class NoExperimentInSetupCallback(Callback):
            # Old method definition didn't take in **experiment.public_spec
            def setup(self):
                return

        callback = NoExperimentInSetupCallback()
        trial_runner = TrialRunner(callbacks=[callback])
        trial_runner.setup_experiments(
            experiments=[Experiment("", lambda x: x)], total_num_samples=1
        )
        mocked_warning_method.assert_called_once()
        self.assertIn("Please update", mocked_warning_method.call_args_list[0][0][0])


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
