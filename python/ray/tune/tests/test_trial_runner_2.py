from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import unittest

import ray
from ray.rllib import _register_all

from ray.tune import TuneError
from ray.tune.schedulers import FIFOScheduler
from ray.tune.result import DONE
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.resources import Resources
from ray.tune.suggest import BasicVariantGenerator

if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


def create_mock_components():
    class _MockScheduler(FIFOScheduler):
        errored_trials = []

        def on_trial_error(self, trial_runner, trial):
            self.errored_trials += [trial]

    class _MockSearchAlg(BasicVariantGenerator):
        errored_trials = []

        def on_trial_complete(self, trial_id, error=False, **kwargs):
            if error:
                self.errored_trials += [trial_id]

    searchalg = _MockSearchAlg()
    scheduler = _MockScheduler()
    return searchalg, scheduler


class TrialRunnerTest2(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testErrorHandling(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        _global_registry.register(TRAINABLE_CLASS, "asdf", None)
        trials = [Trial("asdf", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testThrowOnOverstep(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        runner.step()
        self.assertRaises(TuneError, runner.step)

    def testFailureRecoveryDisabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 0,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 1)
        self.assertEqual(len(searchalg.errored_trials), 1)
        self.assertEqual(len(scheduler.errored_trials), 1)

    def testFailureRecoveryEnabled(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)

        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(len(searchalg.errored_trials), 0)
        self.assertEqual(len(scheduler.errored_trials), 0)

    def testFailureRecoveryNodeRemoval(self):
        ray.init(num_cpus=1, num_gpus=1)
        searchalg, scheduler = create_mock_components()

        runner = TrialRunner(searchalg, scheduler=scheduler)

        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        with patch("ray.cluster_resources") as resource_mock:
            resource_mock.return_value = {"CPU": 1, "GPU": 1}
            runner.step()
            self.assertEqual(trials[0].status, Trial.RUNNING)

            runner.step()
            self.assertEqual(trials[0].status, Trial.RUNNING)

            # Mimic a node failure
            resource_mock.return_value = {"CPU": 0, "GPU": 0}
            runner.step()
            self.assertEqual(trials[0].status, Trial.PENDING)
            self.assertEqual(trials[0].num_failures, 1)
            self.assertEqual(len(searchalg.errored_trials), 0)
            self.assertEqual(len(scheduler.errored_trials), 1)

    def testFailureRecoveryMaxFailures(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_freq": 1,
            "max_failures": 2,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 1)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[0].num_failures, 2)
        runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 3)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        path = runner.trial_executor.save(trials[0])
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].runner.get_info.remote()), 1)
        self.addCleanup(os.remove, path)

    def testRestoreMetricsAfterCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        path = runner.trial_executor.save(trials[0])
        runner.trial_executor.stop_trial(trials[0])
        kwargs["restore_path"] = path

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        runner.step()
        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 10)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 1)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)
        runner.step()
        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 20)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 2)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)
        self.addCleanup(os.remove, path)

    def testCheckpointingAtEnd(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "checkpoint_at_end": True,
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)
        self.assertEqual(trials[0].has_checkpoint(), True)

    def testResultDone(self):
        """Tests that last_result is marked `done` after trial is complete."""
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        runner.step()
        self.assertNotEqual(trials[0].last_result[DONE], True)
        runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        runner.trial_executor.pause_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.PAUSED)

        runner.trial_executor.resume_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), 1)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
