from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import sys
import tempfile
import unittest

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import TuneError, register_trainable
from ray.tune.ray_trial_executor import RayTrialExecutor
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.result import DONE
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.experiment import Experiment
from ray.tune.trial import Trial
from ray.tune.trial_runner import TrialRunner
from ray.tune.resources import Resources, json_to_resources, resources_to_json
from ray.tune.suggest import BasicVariantGenerator
from ray.tune.suggest.suggestion import (_MockSuggestionAlgorithm,
                                         SuggestionAlgorithm)

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


class TrialRunnerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testTrialStatus(self):
        ray.init()
        trial = Trial("__fake")
        trial_executor = RayTrialExecutor()
        self.assertEqual(trial.status, Trial.PENDING)
        trial_executor.start_trial(trial)
        self.assertEqual(trial.status, Trial.RUNNING)
        trial_executor.stop_trial(trial)
        self.assertEqual(trial.status, Trial.TERMINATED)
        trial_executor.stop_trial(trial, error=True)
        self.assertEqual(trial.status, Trial.ERROR)

    def testExperimentTagTruncation(self):
        ray.init()

        def train(config, reporter):
            reporter(timesteps_total=1)

        trial_executor = RayTrialExecutor()
        register_trainable("f1", train)

        experiments = {
            "foo": {
                "run": "f1",
                "config": {
                    "a" * 50: tune.sample_from(lambda spec: 5.0 / 7),
                    "b" * 50: tune.sample_from(lambda spec: "long" * 40)
                },
            }
        }

        for name, spec in experiments.items():
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            for trial in trial_generator.next_trials():
                trial_executor.start_trial(trial)
                self.assertLessEqual(len(trial.logdir), 200)
                trial_executor.stop_trial(trial)

    def testExtraResources(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=0, extra_cpu=3, extra_gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=0, custom_resources={"a": 2}),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testExtraCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(
                cpu=1, gpu=0, extra_custom_resources={"a": 2}),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertTrue(sum(t.status == Trial.RUNNING for t in trials) < 2)
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testCustomResources2(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        runner = TrialRunner()
        resource1 = Resources(cpu=1, gpu=0, extra_custom_resources={"a": 2})
        self.assertTrue(runner.has_resources(resource1))
        resource2 = Resources(cpu=1, gpu=0, custom_resources={"a": 2})
        self.assertTrue(runner.has_resources(resource2))
        resource3 = Resources(cpu=1, gpu=0, custom_resources={"a": 3})
        self.assertFalse(runner.has_resources(resource3))
        resource4 = Resources(cpu=1, gpu=0, extra_custom_resources={"a": 3})
        self.assertFalse(runner.has_resources(resource4))

    def testFractionalGpus(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "resources": Resources(cpu=1, gpu=0.5),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)
        ]
        for t in trials:
            runner.add_trial(t)

        for _ in range(10):
            runner.step()

        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.PENDING)
        self.assertEqual(trials[3].status, Trial.PENDING)

    def testResourceNumericalError(self):
        resource = Resources(cpu=0.99, gpu=0.99, custom_resources={"a": 0.99})
        small_resource = Resources(
            cpu=0.33, gpu=0.33, custom_resources={"a": 0.33})
        for i in range(3):
            resource = Resources.subtract(resource, small_resource)
        self.assertTrue(resource.is_nonnegative())

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 1
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.TERMINATED)

    def testMultiStepRun(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.RUNNING)

    def testMultiStepRun2(self):
        """Checks that runner.step throws when overstepping."""
        ray.init(num_cpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertRaises(TuneError, runner.step)

    def testChangeResources(self):
        """Checks that resource requirements can be changed on fly."""
        ray.init(num_cpus=2)

        class ChangingScheduler(FIFOScheduler):
            def on_trial_result(self, trial_runner, trial, result):
                if result["training_iteration"] == 1:
                    executor = trial_runner.trial_executor
                    executor.stop_trial(trial, stop_logger=False)
                    trial.update_resources(2, 0)
                    executor.start_trial(trial)
                return TrialScheduler.CONTINUE

        runner = TrialRunner(scheduler=ChangingScheduler())
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 2
            },
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(runner.trial_executor._committed_resources.cpu, 1)
        self.assertRaises(ValueError, lambda: trials[0].update_resources(2, 0))

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(runner.trial_executor._committed_resources.cpu, 2)

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

    def testStepHook(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()

        def on_step_begin(self, trialrunner):
            self._update_avail_resources()
            cnt = self.pre_step if hasattr(self, "pre_step") else 0
            setattr(self, "pre_step", cnt + 1)

        def on_step_end(self, trialrunner):
            cnt = self.pre_step if hasattr(self, "post_step") else 0
            setattr(self, "post_step", 1 + cnt)

        import types
        runner.trial_executor.on_step_begin = types.MethodType(
            on_step_begin, runner.trial_executor)
        runner.trial_executor.on_step_end = types.MethodType(
            on_step_end, runner.trial_executor)

        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        runner.step()
        self.assertEqual(runner.trial_executor.pre_step, 1)
        self.assertEqual(runner.trial_executor.post_step, 1)

    def testStopTrial(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {
                "training_iteration": 5
            },
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs),
            Trial("__fake", **kwargs)
        ]
        for t in trials:
            runner.add_trial(t)
        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

        # Stop trial while running
        runner.stop_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.PENDING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.PENDING)

        # Stop trial while pending
        runner.stop_trial(trials[-1])
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(trials[-1].status, Trial.TERMINATED)

    def testSearchAlgNotification(self):
        """Checks notification of trial to the Search Algorithm."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        self.assertEqual(searcher.counter["result"], 1)
        self.assertEqual(searcher.counter["complete"], 1)

    def testSearchAlgFinished(self):
        """Checks that SearchAlg is Finished before all trials are done."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 1}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgSchedulerInteraction(self):
        """Checks that TrialScheduler killing trial will notify SearchAlg."""

        class _MockScheduler(FIFOScheduler):
            def on_trial_result(self, *args, **kwargs):
                return TrialScheduler.STOP

        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(max_concurrent=10)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher, scheduler=_MockScheduler())
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertTrue(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgSchedulerEarlyStop(self):
        """Early termination notif to Searcher can be turned off."""

        class _MockScheduler(FIFOScheduler):
            def on_trial_result(self, *args, **kwargs):
                return TrialScheduler.STOP

        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {"run": "__fake", "stop": {"training_iteration": 2}}
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(use_early_stopped_trials=True)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher, scheduler=_MockScheduler())
        runner.step()
        runner.step()
        self.assertEqual(len(searcher.final_results), 1)

        searcher = _MockSuggestionAlgorithm(use_early_stopped_trials=False)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher, scheduler=_MockScheduler())
        runner.step()
        runner.step()
        self.assertEqual(len(searcher.final_results), 0)

    def testSearchAlgStalled(self):
        """Checks that runner and searcher state is maintained when stalled."""
        ray.init(num_cpus=4, num_gpus=2)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 3,
            "stop": {
                "training_iteration": 1
            }
        }
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher = _MockSuggestionAlgorithm(max_concurrent=1)
        searcher.add_configurations(experiments)
        runner = TrialRunner(search_alg=searcher)
        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[0].status, Trial.RUNNING)

        runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        trials = runner.get_trials()
        runner.step()
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        searcher.stall = True

        runner.step()
        self.assertEqual(trials[1].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)

        self.assertTrue(all(trial.is_finished() for trial in trials))
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())

        searcher.stall = False

        runner.step()
        trials = runner.get_trials()
        self.assertEqual(trials[2].status, Trial.RUNNING)
        self.assertEqual(len(searcher.live_trials), 1)

        runner.step()
        self.assertEqual(trials[2].status, Trial.TERMINATED)
        self.assertEqual(len(searcher.live_trials), 0)
        self.assertTrue(searcher.is_finished())
        self.assertTrue(runner.is_finished())

    def testSearchAlgFinishes(self):
        """Empty SearchAlg changing state in `next_trials` does not crash."""

        class FinishFastAlg(SuggestionAlgorithm):
            _index = 0

            def next_trials(self):
                trials = []
                self._index += 1

                for trial in self._trial_generator:
                    trials += [trial]
                    break

                if self._index > 4:
                    self._finished = True
                return trials

            def _suggest(self, trial_id):
                return {}

        ray.init(num_cpus=2)
        experiment_spec = {
            "run": "__fake",
            "num_samples": 2,
            "stop": {
                "training_iteration": 1
            }
        }
        searcher = FinishFastAlg()
        experiments = [Experiment.from_json("test", experiment_spec)]
        searcher.add_configurations(experiments)

        runner = TrialRunner(search_alg=searcher)
        self.assertFalse(runner.is_finished())
        runner.step()  # This launches a new run
        runner.step()  # This launches a 2nd run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # This kills the first run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # This kills the 2nd run
        self.assertFalse(searcher.is_finished())
        self.assertFalse(runner.is_finished())
        runner.step()  # this converts self._finished to True
        self.assertTrue(searcher.is_finished())
        self.assertRaises(TuneError, runner.step)

    def testTrialSaveRestore(self):
        """Creates different trials to test runner.checkpoint/restore."""
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()

        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        trials = [
            Trial(
                "__fake",
                trial_id="trial_terminate",
                stopping_criterion={"training_iteration": 1},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[0])
        runner.step()  # start
        runner.step()
        self.assertEquals(trials[0].status, Trial.TERMINATED)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_fail",
                stopping_criterion={"training_iteration": 3},
                checkpoint_freq=1,
                config={"mock_error": True})
        ]
        runner.add_trial(trials[1])
        runner.step()
        runner.step()
        runner.step()
        self.assertEquals(trials[1].status, Trial.ERROR)

        trials += [
            Trial(
                "__fake",
                trial_id="trial_succ",
                stopping_criterion={"training_iteration": 2},
                checkpoint_freq=1)
        ]
        runner.add_trial(trials[2])
        runner.step()
        self.assertEquals(len(runner.trial_executor.get_checkpoints()), 3)
        self.assertEquals(trials[2].status, Trial.RUNNING)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        for tid in ["trial_terminate", "trial_fail"]:
            original_trial = runner.get_trial(tid)
            restored_trial = runner2.get_trial(tid)
            self.assertEqual(original_trial.status, restored_trial.status)

        restored_trial = runner2.get_trial("trial_succ")
        self.assertEqual(Trial.PENDING, restored_trial.status)

        runner2.step()
        runner2.step()
        runner2.step()
        self.assertRaises(TuneError, runner2.step)
        shutil.rmtree(tmpdir)

    def testTrialNoSave(self):
        """Check that non-checkpointing trials are not saved."""
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()

        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(
            Trial(
                "__fake",
                trial_id="non_checkpoint",
                stopping_criterion={"training_iteration": 2}))

        while not all(t.status == Trial.TERMINATED
                      for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="checkpoint",
                checkpoint_at_end=True,
                stopping_criterion={"training_iteration": 2}))

        while not all(t.status == Trial.TERMINATED
                      for t in runner.get_trials()):
            runner.step()

        runner.add_trial(
            Trial(
                "__fake",
                trial_id="pending",
                stopping_criterion={"training_iteration": 2}))

        runner.step()
        runner.step()

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        new_trials = runner2.get_trials()
        self.assertEquals(len(new_trials), 3)
        self.assertTrue(
            runner2.get_trial("non_checkpoint").status == Trial.TERMINATED)
        self.assertTrue(
            runner2.get_trial("checkpoint").status == Trial.TERMINATED)
        self.assertTrue(runner2.get_trial("pending").status == Trial.PENDING)
        self.assertTrue(not runner2.get_trial("pending").last_result)
        runner2.step()
        shutil.rmtree(tmpdir)

    def testCheckpointWithFunction(self):
        ray.init()
        trial = Trial(
            "__fake",
            config={"callbacks": {
                "on_episode_start": lambda i: i,
            }},
            checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for i in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        new_trial = runner2.get_trials()[0]
        self.assertTrue("callbacks" in new_trial.config)
        self.assertTrue("on_episode_start" in new_trial.config["callbacks"])
        shutil.rmtree(tmpdir)

    def testCheckpointOverwrite(self):
        def count_checkpoints(cdir):
            return sum((fname.startswith("experiment_state")
                        and fname.endswith(".json"))
                       for fname in os.listdir(cdir))

        ray.init()
        trial = Trial("__fake", checkpoint_freq=1)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(trial)
        for i in range(5):
            runner.step()
        # force checkpoint
        runner.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 1)

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        for i in range(5):
            runner2.step()
        self.assertEquals(count_checkpoints(tmpdir), 2)

        runner2.checkpoint()
        self.assertEquals(count_checkpoints(tmpdir), 2)
        shutil.rmtree(tmpdir)

    def testUserCheckpoint(self):
        ray.init(num_cpus=3)
        tmpdir = tempfile.mkdtemp()
        runner = TrialRunner(local_checkpoint_dir=tmpdir, checkpoint_period=0)
        runner.add_trial(Trial("__fake", config={"user_checkpoint_freq": 2}))
        trials = runner.get_trials()

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        runner.step()  # 0
        self.assertFalse(trials[0].has_checkpoint())
        runner.step()  # 1
        self.assertFalse(trials[0].has_checkpoint())
        runner.step()  # 2
        self.assertTrue(trials[0].has_checkpoint())

        runner2 = TrialRunner(resume="LOCAL", local_checkpoint_dir=tmpdir)
        runner2.step()
        trials2 = runner2.get_trials()
        self.assertEqual(ray.get(trials2[0].runner.get_info.remote()), 1)
        shutil.rmtree(tmpdir)


class SearchAlgorithmTest(unittest.TestCase):
    def testNestedSuggestion(self):
        class TestSuggestion(SuggestionAlgorithm):
            def _suggest(self, trial_id):
                return {"a": {"b": {"c": {"d": 4, "e": 5}}}}

        alg = TestSuggestion()
        alg.add_configurations({"test": {"run": "__fake"}})
        trial = alg.next_trials()[0]
        self.assertTrue("e=5" in trial.experiment_tag)
        self.assertTrue("d=4" in trial.experiment_tag)


class ResourcesTest(unittest.TestCase):
    def testSubtraction(self):
        resource_1 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={
                "a": 1,
                "b": 2
            },
            extra_custom_resources={
                "a": 1,
                "b": 1
            })
        resource_2 = Resources(
            1,
            0,
            0,
            1,
            custom_resources={
                "a": 1,
                "b": 2
            },
            extra_custom_resources={
                "a": 1,
                "b": 1
            })
        new_res = Resources.subtract(resource_1, resource_2)
        self.assertTrue(new_res.cpu == 0)
        self.assertTrue(new_res.gpu == 0)
        self.assertTrue(new_res.extra_cpu == 0)
        self.assertTrue(new_res.extra_gpu == 0)
        self.assertTrue(all(k == 0 for k in new_res.custom_resources.values()))
        self.assertTrue(
            all(k == 0 for k in new_res.extra_custom_resources.values()))

    def testDifferentResources(self):
        resource_1 = Resources(1, 0, 0, 1, custom_resources={"a": 1, "b": 2})
        resource_2 = Resources(1, 0, 0, 1, custom_resources={"a": 1, "c": 2})
        new_res = Resources.subtract(resource_1, resource_2)
        assert "c" in new_res.custom_resources
        assert "b" in new_res.custom_resources
        self.assertTrue(new_res.cpu == 0)
        self.assertTrue(new_res.gpu == 0)
        self.assertTrue(new_res.extra_cpu == 0)
        self.assertTrue(new_res.extra_gpu == 0)
        self.assertTrue(new_res.get("a") == 0)

    def testSerialization(self):
        original = Resources(1, 0, 0, 1, custom_resources={"a": 1, "b": 2})
        jsoned = resources_to_json(original)
        new_resource = json_to_resources(jsoned)
        self.assertEquals(original, new_resource)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
