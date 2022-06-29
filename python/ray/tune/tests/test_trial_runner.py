import os
import sys
import unittest

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import TuneError, register_trainable
from ray.tune.execution.ray_trial_executor import RayTrialExecutor
from ray.tune.resources import Resources
from ray.tune.schedulers import TrialScheduler, FIFOScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.experiment import Trial
from ray.tune.execution.trial_runner import TrialRunner
from ray.tune.utils.mock import TrialStatusSnapshotTaker, TrialStatusSnapshot
from ray.tune.execution.placement_groups import PlacementGroupFactory


class TrialRunnerTest(unittest.TestCase):
    def setUp(self):
        _register_all()  # re-register the evicted objects

    def tearDown(self):
        ray.shutdown()

    def testExperimentTagTruncation(self):
        ray.init(num_cpus=2)

        def train(config, reporter):
            reporter(timesteps_total=1)

        trial_executor = RayTrialExecutor()
        register_trainable("f1", train)

        experiments = {
            "foo": {
                "run": "f1",
                "config": {
                    "a" * 50: tune.sample_from(lambda spec: 5.0 / 7),
                    "b" * 50: tune.sample_from(lambda spec: "long" * 40),
                },
            }
        }

        for name, spec in experiments.items():
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            while not trial_generator.is_finished():
                trial = trial_generator.next_trial()
                if not trial:
                    break
                trial_executor.start_trial(trial)
                self.assertLessEqual(len(os.path.basename(trial.logdir)), 200)
                trial_executor.stop_trial(trial)

    def testExtraResources(self):
        ray.init(num_cpus=4, num_gpus=2)
        snapshot = TrialStatusSnapshot()
        runner = TrialRunner(callbacks=[TrialStatusSnapshotTaker(snapshot)])
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "placement_group_factory": PlacementGroupFactory(
                [{"CPU": 1}, {"CPU": 3, "GPU": 1}]
            ),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()

        self.assertLess(snapshot.max_running_trials(), 2)
        self.assertTrue(snapshot.all_trials_are_terminated())

    def testCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        # Since each trial will occupy the full custom resources,
        # there are at most 1 trial running at any given moment.
        snapshot = TrialStatusSnapshot()
        runner = TrialRunner(callbacks=[TrialStatusSnapshotTaker(snapshot)])
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "a": 2}]),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()

        self.assertLess(snapshot.max_running_trials(), 2)
        self.assertTrue(snapshot.all_trials_are_terminated())

    def testExtraCustomResources(self):
        ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
        # Since each trial will occupy the full custom resources,
        # there are at most 1 trial running at any given moment.
        snapshot = TrialStatusSnapshot()
        runner = TrialRunner(callbacks=[TrialStatusSnapshotTaker(snapshot)])
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "placement_group_factory": PlacementGroupFactory([{"CPU": 1}, {"a": 2}]),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()

        self.assertLess(snapshot.max_running_trials(), 2)
        self.assertTrue(snapshot.all_trials_are_terminated())

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
            Trial("__fake", **kwargs),
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
        small_resource = Resources(cpu=0.33, gpu=0.33, custom_resources={"a": 0.33})
        for i in range(3):
            resource = Resources.subtract(resource, small_resource)
        self.assertTrue(resource.is_nonnegative())

    def testResourceScheduler(self):
        ray.init(num_cpus=4, num_gpus=1)
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]

        snapshot = TrialStatusSnapshot()
        runner = TrialRunner(callbacks=[TrialStatusSnapshotTaker(snapshot)])
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()

        self.assertLess(snapshot.max_running_trials(), 2)
        self.assertTrue(snapshot.all_trials_are_terminated())

    def testMultiStepRun(self):
        ray.init(num_cpus=4, num_gpus=2)
        kwargs = {
            "stopping_criterion": {"training_iteration": 5},
            "resources": Resources(cpu=1, gpu=1),
        }
        trials = [Trial("__fake", **kwargs), Trial("__fake", **kwargs)]
        snapshot = TrialStatusSnapshot()
        runner = TrialRunner(callbacks=[TrialStatusSnapshotTaker(snapshot)])
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()

        self.assertTrue(snapshot.all_trials_are_terminated())

    def testMultiStepRun2(self):
        """Checks that runner.step throws when overstepping."""
        ray.init(num_cpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertRaises(TuneError, runner.step)

    def testChangeResources(self):
        """Checks that resource requirements can be changed on fly."""
        ray.init(num_cpus=2)

        class ChangingScheduler(FIFOScheduler):
            def __init__(self):
                self._has_received_one_trial_result = False

            # For figuring out how many runner.step there are.
            def has_received_one_trial_result(self):
                return self._has_received_one_trial_result

            def on_trial_result(self, trial_runner, trial, result):
                if result["training_iteration"] == 1:
                    self._has_received_one_trial_result = True
                    executor = trial_runner.trial_executor
                    executor.pause_trial(trial)
                    trial.update_resources(dict(cpu=2, gpu=0))
                return TrialScheduler.NOOP

        scheduler = ChangingScheduler()
        runner = TrialRunner(scheduler=scheduler)
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=0),
        }
        trials = [Trial("__fake", **kwargs)]
        for t in trials:
            runner.add_trial(t)

        runner.step()
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(
            runner.trial_executor._pg_manager.occupied_resources().get("CPU"), 1
        )
        self.assertRaises(
            ValueError, lambda: trials[0].update_resources(dict(cpu=2, gpu=0))
        )

        while not scheduler.has_received_one_trial_result():
            runner.step()
        self.assertEqual(trials[0].status, Trial.PAUSED)
        # extra step for tune loop to stage the resource requests.
        runner.step()
        self.assertEqual(
            runner.trial_executor._pg_manager.occupied_resources().get("CPU"), 2
        )

    def testQueueFilling(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"

        ray.init(num_cpus=4)

        def f1(config):
            for i in range(10):
                yield i

        tune.register_trainable("f1", f1)

        search_alg = BasicVariantGenerator()
        search_alg.add_configurations(
            {
                "foo": {
                    "run": "f1",
                    "num_samples": 100,
                    "config": {
                        "a": tune.sample_from(lambda spec: 5.0 / 7),
                        "b": tune.sample_from(lambda spec: "long" * 40),
                    },
                    "resources_per_trial": {"cpu": 2},
                }
            }
        )

        runner = TrialRunner(search_alg=search_alg)

        runner.step()
        runner.step()
        runner.step()
        self.assertEqual(len(runner._trials), 3)

        runner.step()
        self.assertEqual(len(runner._trials), 3)

        self.assertEqual(runner._trials[0].status, Trial.RUNNING)
        self.assertEqual(runner._trials[1].status, Trial.RUNNING)
        self.assertEqual(runner._trials[2].status, Trial.PENDING)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
