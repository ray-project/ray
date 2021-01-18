# coding: utf-8
import unittest
from unittest.mock import patch

import ray
from ray import tune
from ray.rllib import _register_all
from ray.tune import Trainable
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.result import TRAINING_ITERATION
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.trial import Trial, Checkpoint
from ray.tune.resources import Resources
from ray.cluster_utils import Cluster
from ray.util import placement_group


class RayTrialExecutorTest(unittest.TestCase):
    def setUp(self):
        self.trial_executor = RayTrialExecutor(queue_trials=False)
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
        checkpoint = self.trial_executor.pause_trial(trial)
        self.assertEqual(Trial.PAUSED, trial.status)
        self.trial_executor.start_trial(trial, checkpoint)
        self.assertEqual(Trial.RUNNING, trial.status)
        self.trial_executor.stop_trial(trial)
        self.assertEqual(Trial.TERMINATED, trial.status)

    def _testPauseUnpause(self, result_buffer_length):
        """Tests that unpausing works for trials being processed."""
        with patch(
                "ray.tune.ray_trial_executor.TUNE_RESULT_BUFFER_LENGTH",
                result_buffer_length
        ), patch("ray.tune.ray_trial_executor.TUNE_RESULT_BUFFER_MIN_TIME_S",
                 1):
            base = max(result_buffer_length, 1)

            trial = Trial("__fake")
            self.trial_executor.start_trial(trial)
            self.assertEqual(Trial.RUNNING, trial.status)
            trial.last_result = self.trial_executor.fetch_result(trial)[-1]
            self.assertEqual(trial.last_result.get(TRAINING_ITERATION), base)
            self.trial_executor.pause_trial(trial)
            self.assertEqual(Trial.PAUSED, trial.status)
            self.trial_executor.unpause_trial(trial)
            self.assertEqual(Trial.PENDING, trial.status)
            self.trial_executor.start_trial(trial)
            self.assertEqual(Trial.RUNNING, trial.status)
            trial.last_result = self.trial_executor.fetch_result(trial)[-1]
            self.assertEqual(
                trial.last_result.get(TRAINING_ITERATION), base * 2)
            self.trial_executor.stop_trial(trial)
            self.assertEqual(Trial.TERMINATED, trial.status)

    def testPauseUnpauseNoBuffer(self):
        self._testPauseUnpause(0)

    def testPauseUnpauseTrivialBuffer(self):
        self._testPauseUnpause(1)

    def testPauseUnpauseActualBuffer(self):
        self._testPauseUnpause(8)

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

        trials = self.generate_trials({
            "run": B,
            "config": {
                "foo": 0
            },
        }, "grid_search")
        trial = trials[0]
        self.trial_executor.start_trial(trial)
        exists = self.trial_executor.reset_trial(trial, {"hi": 1},
                                                 "modified_mock")
        self.assertEqual(exists, True)
        self.assertEqual(trial.config.get("hi"), 1)
        self.assertEqual(trial.experiment_tag, "modified_mock")
        self.assertEqual(Trial.RUNNING, trial.status)

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


class RayExecutorQueueTest(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster(
            initialize_head=True,
            connect=True,
            head_node_args={
                "num_cpus": 1,
                "_system_config": {
                    "num_heartbeats_timeout": 10
                }
            })
        self.trial_executor = RayTrialExecutor(
            queue_trials=True, refresh_period=0)
        # Pytest doesn't play nicely with imports
        _register_all()

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()
        _register_all()  # re-register the evicted objects

    def testQueueTrial(self):
        """Tests that reset handles NotImplemented properly."""

        def create_trial(cpu, gpu=0):
            return Trial("__fake", resources=Resources(cpu=cpu, gpu=gpu))

        cpu_only = create_trial(1, 0)
        self.assertTrue(self.trial_executor.has_resources(cpu_only.resources))
        self.trial_executor.start_trial(cpu_only)

        gpu_only = create_trial(0, 1)
        self.assertTrue(self.trial_executor.has_resources(gpu_only.resources))

    def testHeadBlocking(self):
        def create_trial(cpu, gpu=0):
            return Trial("__fake", resources=Resources(cpu=cpu, gpu=gpu))

        gpu_trial = create_trial(1, 1)
        self.assertTrue(self.trial_executor.has_resources(gpu_trial.resources))
        self.trial_executor.start_trial(gpu_trial)

        # TODO(rliaw): This behavior is probably undesirable, but right now
        # trials with different resource requirements is not often used.
        cpu_only_trial = create_trial(1, 0)
        self.assertFalse(
            self.trial_executor.has_resources(cpu_only_trial.resources))

        self.cluster.add_node(num_cpus=1, num_gpus=1)
        self.cluster.wait_for_nodes()

        self.assertTrue(
            self.trial_executor.has_resources(cpu_only_trial.resources))
        self.trial_executor.start_trial(cpu_only_trial)

        cpu_only_trial2 = create_trial(1, 0)
        self.assertTrue(
            self.trial_executor.has_resources(cpu_only_trial2.resources))
        self.trial_executor.start_trial(cpu_only_trial2)

        cpu_only_trial3 = create_trial(1, 0)
        self.assertFalse(
            self.trial_executor.has_resources(cpu_only_trial3.resources))


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
                "resources": {
                    "custom": self.head_custom
                },
                "_system_config": {
                    "num_heartbeats_timeout": 10
                }
            })
        # Pytest doesn't play nicely with imports
        _register_all()

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()
        _register_all()  # re-register the evicted objects

    def testResourcesAvailableNoPlacementGroup(self):
        def train(config):
            tune.report(metric=0, resources=ray.available_resources())

        out = tune.run(
            train,
            resources_per_trial={
                "cpu": 1,
                "gpu": 1,
                "custom_resources": {
                    "custom": 3
                },
                "extra_cpu": 3,
                "extra_gpu": 1,
                "extra_custom_resources": {
                    "custom": 4
                },
            })

        # Only `cpu`, `gpu`, and `custom_resources` will be "really" reserved,
        # the extra_* will just be internally reserved by Tune.
        self.assertDictEqual({
            key: val
            for key, val in out.trials[0].last_result["resources"].items()
            if key in ["CPU", "GPU", "custom"]
        }, {
            "CPU": self.head_cpus - 1.0,
            "GPU": self.head_gpus - 1.0,
            "custom": self.head_custom - 3.0
        })

    def testResourcesAvailableWithPlacementGroup(self):
        def train(config):
            tune.report(metric=0, resources=ray.available_resources())

        def placement_group_factory():
            head_bundle = {"CPU": 1, "GPU": 0, "custom": 4}
            child_bundle = {"CPU": 2, "GPU": 1, "custom": 3}

            return placement_group([head_bundle, child_bundle, child_bundle])

        out = tune.run(train, resources_per_trial=placement_group_factory)

        self.assertDictEqual({
            key: val
            for key, val in out.trials[0].last_result["resources"].items()
            if key in ["CPU", "GPU", "custom"]
        }, {
            "CPU": self.head_cpus - 5.0,
            "GPU": self.head_gpus - 2.0,
            "custom": self.head_custom - 10.0
        })


class LocalModeExecutorTest(RayTrialExecutorTest):
    def setUp(self):
        ray.init(local_mode=True)
        self.trial_executor = RayTrialExecutor(queue_trials=False)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
