# coding: utf-8

import os
import pytest
import time
import unittest

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import Trainable
from ray.tune.callback import Callback
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.result import PID, TRAINING_ITERATION, TRIAL_ID
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, Checkpoint
from ray.tune.resources import Resources
from ray.cluster_utils import Cluster
from ray.tune.utils.placement_groups import PlacementGroupFactory
from unittest.mock import patch


class TrialExecutorInsufficientResourcesTest(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S"] = "0"
        self.cluster = Cluster(
            initialize_head=True,
            connect=True,
            head_node_args={
                "num_cpus": 4,
                "num_gpus": 2,
            },
        )

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()

    # no autoscaler case, resource is not sufficient. Log warning for now.
    @patch.object(ray.tune.insufficient_resources_manager.logger, "warning")
    def testRaiseErrorNoAutoscaler(self, mocked_warn):
        class FailureInjectorCallback(Callback):
            """Adds random failure injection to the TrialExecutor."""

            def __init__(self, steps=4):
                self._step = 0
                self.steps = steps

            def on_step_begin(self, iteration, trials, **info):
                self._step += 1
                if self._step >= self.steps:
                    raise RuntimeError

        def train(config):
            pass

        with self.assertRaises(RuntimeError):
            tune.run(
                train,
                callbacks=[
                    # Make sure that the test is not stuck forever,
                    # as what would happen for the users now, unfortunately.
                    FailureInjectorCallback(),
                ],
                resources_per_trial={
                    "cpu": 5,  # more than what the cluster can offer.
                    "gpu": 3,
                },
            )
        msg = (
            "Ignore this message if the cluster is autoscaling. "
            "You asked for 5.0 cpu and 3.0 gpu per trial, "
            "but the cluster only has 4.0 cpu and 2.0 gpu. "
            "Stop the tuning job and "
            "adjust the resources requested per trial "
            "(possibly via `resources_per_trial` "
            "or via `num_workers` for rllib) "
            "and/or add more resources to your Ray runtime."
        )
        mocked_warn.assert_called_once_with(msg)


class RayTrialExecutorTest(unittest.TestCase):
    def setUp(self):
        # Wait up to five seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"
        os.environ["TUNE_TRIAL_RESULT_WAIT_TIME_S"] = "99999"

        self.trial_executor = RayTrialExecutor()
        ray.init(num_cpus=2, ignore_reinit_error=True)
        _register_all()  # Needed for flaky tests

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testStartStop(self):
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        running = self.trial_executor.get_running_trials()
        self.assertEqual(1, len(running))
        self.trial_executor.stop_trial(trial)

    def testAsyncSave(self):
        """Tests that saved checkpoint value not immediately set."""
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        checkpoint = self.trial_executor.save(trial, Checkpoint.PERSISTENT)
        self.assertEqual(checkpoint, trial.saving_to)
        self.assertEqual(trial.checkpoint.value, None)
        self.process_trial_save(trial)
        self.assertEqual(checkpoint, trial.checkpoint)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def testSaveRestore(self):
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        self.trial_executor.save(trial, Checkpoint.PERSISTENT)
        self.process_trial_save(trial)
        self.trial_executor.restore(trial)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def testPauseResume(self):
        """Tests that pausing works for trials in flight."""
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.pause_trial(trial)
        self.assertEqual(Trial.PAUSED, trial.status)
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def testSavePauseResumeErrorRestore(self):
        """Tests that pause checkpoint does not replace restore checkpoint."""
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        # Save
        checkpoint = self.trial_executor.save(trial, Checkpoint.PERSISTENT)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.assertEqual(checkpoint.storage, Checkpoint.PERSISTENT)
        # Process save result (simulates trial runner)
        self.process_trial_save(trial)
        # Train
        self.trial_executor.continue_training(trial)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        # Pause
        self.trial_executor.pause_trial(trial)
        self.assertEqual(Trial.PAUSED, trial.status)
        self.assertEqual(trial.checkpoint.storage, Checkpoint.MEMORY)
        # Resume
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        # Error
        trial.set_status(Trial.ERROR)
        # Restore
        self.trial_executor.restore(trial)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def testStartFailure(self):
        _global_registry.register(TRAINABLE_CLASS, "asdf", None)
        trial = Trial("asdf", resources=Resources(1, 0))
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.ERROR, trial.status)

    def testPauseResume2(self):
        """Tests that pausing works for trials being processed."""
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.fetch_result(trial)
        self.trial_executor.pause_trial(trial)
        self.assertEqual(Trial.PAUSED, trial.status)
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def _testPauseAndStart(self, result_buffer_length):
        """Tests that unpausing works for trials being processed."""
        os.environ["TUNE_RESULT_BUFFER_LENGTH"] = f"{result_buffer_length}"
        os.environ["TUNE_RESULT_BUFFER_MIN_TIME_S"] = "1"

        # Need a new trial executor so the ENV vars are parsed again
        self.trial_executor = RayTrialExecutor()

        base = max(result_buffer_length, 1)

        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        self.assertEqual(trial.last_result.get(TRAINING_ITERATION), base)
        self.trial_executor.pause_trial(trial)
        self.assertEqual(Trial.PAUSED, trial.status)
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        trial.last_result = self.trial_executor.fetch_result(trial)[-1]
        self.assertEqual(trial.last_result.get(TRAINING_ITERATION), base * 2)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def testPauseAndStartNoBuffer(self):
        self._testPauseAndStart(0)

    def testPauseAndStartTrivialBuffer(self):
        self._testPauseAndStart(1)

    def testPauseAndStartActualBuffer(self):
        self._testPauseAndStart(8)

    def testNoResetTrial(self):
        """Tests that reset handles NotImplemented properly."""
        trial = Trial("__fake")
        self.trial_executor.start_trial(trial)
        exists = self.trial_executor.reset_trial(trial, {}, "modified_mock")
        self.assertEqual(exists, False)
        self.assertEqual(Trial.RUNNING, trial.status)

    def testResetTrial(self):
        """Tests that reset works as expected."""

        class B(Trainable):
            def step(self):
                return dict(timesteps_this_iter=1, done=True)

            def reset_config(self, config):
                self.config = config
                return True

        trials = self.generate_trials(
            {
                "run": B,
                "config": {"foo": 0},
            },
            "grid_search",
        )
        trial = trials[0]
        self.trial_executor.start_trial(trial)
        exists = self.trial_executor.reset_trial(trial, {"hi": 1}, "modified_mock")
        self.assertEqual(exists, True)
        self.assertEqual(trial.config.get("hi"), 1)
        self.assertEqual(trial.experiment_tag, "modified_mock")
        self.assertEqual(Trial.RUNNING, trial.status)

    def testForceTrialCleanup(self):
        class B(Trainable):
            def step(self):
                print("Step start")
                time.sleep(10)
                print("Step done")
                return dict(my_metric=1, timesteps_this_iter=1, done=True)

            def reset_config(self, config):
                self.config = config
                return True

            def cleanup(self):
                print("Cleanup start")
                time.sleep(10)
                print("Cleanup done")

        # First check if the trials terminate gracefully by default
        trials = self.generate_trials(
            {
                "run": B,
                "config": {"foo": 0},
            },
            "grid_search",
        )
        trial = trials[0]
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        time.sleep(5)
        print("Stop trial")
        self.trial_executor.stop_trial(trial)
        print("Start trial cleanup")
        start = time.time()
        self.trial_executor.cleanup([trial])
        self.assertGreaterEqual(time.time() - start, 12.0)

        # Check forceful termination. It should run for much less than the
        # sleep periods in the Trainable
        trials = self.generate_trials(
            {
                "run": B,
                "config": {"foo": 0},
            },
            "grid_search",
        )
        trial = trials[0]
        os.environ["TUNE_FORCE_TRIAL_CLEANUP_S"] = "1"
        self.trial_executor = RayTrialExecutor()
        os.environ["TUNE_FORCE_TRIAL_CLEANUP_S"] = "0"
        self.trial_executor.start_trial(trial)
        self.assertEqual(Trial.RUNNING, trial.status)
        time.sleep(5)
        print("Stop trial")
        self.trial_executor.stop_trial(trial)
        print("Start trial cleanup")
        start = time.time()
        self.trial_executor.cleanup([trial])
        self.assertLess(time.time() - start, 5.0)

        # also check if auto-filled metrics were returned
        self.assertIn(PID, trial.last_result)
        self.assertIn(TRIAL_ID, trial.last_result)
        self.assertNotIn("my_metric", trial.last_result)

    @staticmethod
    def generate_trials(spec, name):
        suggester = BasicVariantGenerator()
        suggester.add_configurations({name: spec})
        trials = []
        while not suggester.is_finished():
            trial = suggester.next_trial()
            if trial:
                trials.append(trial)
            else:
                break
        return trials

    def process_trial_save(self, trial):
        """Simulates trial runner save."""
        checkpoint = trial.saving_to
        checkpoint_value = self.trial_executor.fetch_result(trial)[-1]
        checkpoint.value = checkpoint_value
        trial.on_checkpoint(checkpoint)


class RayExecutorPlacementGroupTest(unittest.TestCase):
    def setUp(self):
        self.head_cpus = 8
        self.head_gpus = 4
        self.head_custom = 16

        self.cluster = Cluster(
            initialize_head=True,
            connect=True,
            head_node_args={
                "num_cpus": self.head_cpus,
                "num_gpus": self.head_gpus,
                "resources": {"custom": self.head_custom},
                "_system_config": {"num_heartbeats_timeout": 10},
            },
        )
        # Pytest doesn't play nicely with imports
        _register_all()

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()
        _register_all()  # re-register the evicted objects

    def testResourcesAvailableWithPlacementGroup(self):
        def train(config):
            tune.report(metric=0, resources=ray.available_resources())

        head_bundle = {"CPU": 1, "GPU": 0, "custom": 4}
        child_bundle = {"CPU": 2, "GPU": 1, "custom": 3}

        placement_group_factory = PlacementGroupFactory(
            [head_bundle, child_bundle, child_bundle]
        )

        out = tune.run(train, resources_per_trial=placement_group_factory)

        available = {
            key: val
            for key, val in out.trials[0].last_result["resources"].items()
            if key in ["CPU", "GPU", "custom"]
        }

        if not available:
            self.skipTest(
                "Warning: Ray reported no available resources, "
                "but this is an error on the Ray core side. "
                "Skipping this test for now."
            )

        self.assertDictEqual(
            available,
            {
                "CPU": self.head_cpus - 5.0,
                "GPU": self.head_gpus - 2.0,
                "custom": self.head_custom - 10.0,
            },
        )

    def testPlacementGroupFactoryEquality(self):
        """
        Test that two different placement group factory objects are considered
        equal and evaluate to the same hash.
        """
        from collections import Counter

        pgf_1 = PlacementGroupFactory(
            [{"CPU": 2, "GPU": 4, "custom": 7}, {"GPU": 2, "custom": 1, "CPU": 3}],
            "PACK",
            "no_name",
            None,
        )

        pgf_2 = PlacementGroupFactory(
            [
                {
                    "custom": 7,
                    "GPU": 4,
                    "CPU": 2,
                },
                {"custom": 1, "GPU": 2, "CPU": 3},
            ],
            strategy="PACK",
            name="no_name",
            lifetime=None,
        )

        pgf_3 = PlacementGroupFactory(
            [
                {"custom": 7, "GPU": 4, "CPU": 2.0, "custom2": 0},
                {"custom": 1.0, "GPU": 2, "CPU": 3, "custom2": 0},
            ],
            strategy="PACK",
            name="no_name",
            lifetime=None,
        )

        self.assertEqual(pgf_1, pgf_2)
        self.assertEqual(pgf_2, pgf_3)

        # Hash testing
        counter = Counter()
        counter[pgf_1] += 1
        counter[pgf_2] += 1
        counter[pgf_3] += 1

        self.assertEqual(counter[pgf_1], 3)
        self.assertEqual(counter[pgf_2], 3)
        self.assertEqual(counter[pgf_3], 3)


class LocalModeExecutorTest(RayTrialExecutorTest):
    def setUp(self):
        ray.init(local_mode=True)
        self.trial_executor = RayTrialExecutor()

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testForceTrialCleanup(self):
        self.skipTest(
            "Skipping as force trial cleanup is not applicable" " for local mode."
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
