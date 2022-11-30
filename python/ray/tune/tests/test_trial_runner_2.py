import json
import os
import shutil
import sys
import tempfile
import unittest

import ray
from ray.air import CheckpointConfig
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.rllib import _register_all

from ray.tune import TuneError
from ray.tune.schedulers import FIFOScheduler
from ray.tune.result import DONE
from ray.tune.registry import _global_registry, TRAINABLE_CLASS
from ray.tune.experiment import Trial
from ray.tune.execution.trial_runner import TrialRunner
from ray.tune.resources import Resources
from ray.tune.search import BasicVariantGenerator
from ray.tune.tests.tune_test_util import TrialResultObserver
from ray.tune.trainable.util import TrainableUtil


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
    def setUp(self):
        os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testErrorHandling(self):
        ray.init(num_cpus=4, num_gpus=2)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
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
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            "max_failures": 0,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
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
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            "max_failures": 1,
            "config": {
                "mock_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[0].num_failures, 1)
        self.assertEqual(len(searchalg.errored_trials), 0)
        # Notice this is 1 since during recovery, the previously errored trial
        # is "requeued". This will call scheduler.on_trial_error.
        # Searcher.on_trial_error is, however, not called in this process.
        self.assertEqual(len(scheduler.errored_trials), 1)

    def testFailureRecoveryMaxFailures(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            "max_failures": 2,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        self.assertEqual(trials[0].num_failures, 3)

    def testFailFast(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(fail_fast=True)
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            "max_failures": 0,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].status, Trial.ERROR)
        # Somehow with `fail_fast=True`, if one errors out, the others are
        # then stopped with `TERMINATED` status.
        self.assertEqual(trials[1].status, Trial.TERMINATED)
        self.assertRaises(TuneError, lambda: runner.step())

    def testFailFastRaise(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner(fail_fast=TrialRunner.RAISE)
        kwargs = {
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
            "max_failures": 0,
            "config": {
                "mock_error": True,
                "persistent_error": True,
            },
        }
        runner.add_trial(Trial("__fake", **kwargs))
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        with self.assertRaises(Exception):
            while not runner.is_finished():
                runner.step()

        # Not critical checks. Only to showcase the difference
        # with none raise type FailFast.
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(trials[1].status, Trial.PENDING)

    def testCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 1},
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()  # Start trial
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)
        runner.step()  # Process result, dispatch save
        runner.step()  # Process save, stop trial
        kwargs["restore_path"] = trials[0].checkpoint.dir_or_data
        self.assertEqual(trials[0].status, Trial.TERMINATED)

        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()
        self.assertEqual(trials[1].status, Trial.PENDING)
        runner.step()  # Start trial, dispatch restore
        self.assertEqual(trials[1].status, Trial.RUNNING)

        runner.step()  # Process restore
        self.assertEqual(trials[0].status, Trial.TERMINATED)
        self.assertEqual(trials[1].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[1].runner.get_info.remote()), 1)
        self.addCleanup(shutil.rmtree, trials[0].checkpoint.dir_or_data)

    def testRestoreMetricsAfterCheckpointing(self):
        ray.init(num_cpus=1, num_gpus=1)

        observer = TrialResultObserver()
        runner = TrialRunner(callbacks=[observer])
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
            "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()

        self.assertEqual(trials[0].status, Trial.TERMINATED)

        kwargs["restore_path"] = trials[0].checkpoint.dir_or_data
        kwargs.pop("stopping_criterion")
        kwargs.pop("checkpoint_config")  # No checkpointing for next trial
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        observer.reset()
        while not observer.just_received_a_result():
            runner.step()
        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 10)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 1)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)

        while not observer.just_received_a_result():
            runner.step()

        self.assertEqual(trials[1].last_result["timesteps_since_restore"], 20)
        self.assertEqual(trials[1].last_result["iterations_since_restore"], 2)
        self.assertGreater(trials[1].last_result["time_since_restore"], 0)
        self.addCleanup(shutil.rmtree, trials[0].checkpoint.dir_or_data)

    def testCheckpointingAtEnd(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "checkpoint_config": CheckpointConfig(checkpoint_at_end=True),
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)
        self.assertEqual(trials[0].has_checkpoint(), True)

    def testResultDone(self):
        """Tests that last_result is marked `done` after trial is complete."""
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        while not runner.is_finished():
            runner.step()
        self.assertEqual(trials[0].last_result[DONE], True)

    def testPauseThenResume(self):
        ray.init(num_cpus=1, num_gpus=1)
        runner = TrialRunner()
        kwargs = {
            "stopping_criterion": {"training_iteration": 2},
            "resources": Resources(cpu=1, gpu=1),
        }
        runner.add_trial(Trial("__fake", **kwargs))
        trials = runner.get_trials()

        runner.step()  # Start trial
        runner.step()  # Process result
        self.assertEqual(trials[0].status, Trial.RUNNING)
        self.assertEqual(ray.get(trials[0].runner.get_info.remote()), None)

        self.assertEqual(ray.get(trials[0].runner.set_info.remote(1)), 1)

        runner.trial_executor.pause_trial(trials[0])
        self.assertEqual(trials[0].status, Trial.PAUSED)

    def testPauseResumeCheckpointCount(self):
        ray.init(num_cpus=2)
        tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tempdir)

        # The Trial `local_dir` must match up with the `local_checkpoint_dir`
        # passed into the TrialRunner. This creates an experiment directory structure
        # that matches a real Tune run, where the trial logdir is created as a
        # subdirectory of the experiment directory. This is the directory
        # structure assumed by `TrialRunner.resume`.
        # Example:
        # local_checkpoint_dir/
        #     experiment_state.json
        #     trial.logdir/
        #         checkpoint_00000/
        trial = Trial(
            "__fake",
            local_dir=tempdir,
            checkpoint_config=CheckpointConfig(num_to_keep=2),
        )
        trial.init_logdir()
        trial.checkpoint_manager.set_delete_fn(lambda cp: shutil.rmtree(cp.dir_or_data))

        def write_checkpoint(trial: Trial, index: int):
            checkpoint_dir = TrainableUtil.make_checkpoint_dir(
                trial.logdir, index=index
            )
            result = {"training_iteration": index}
            with open(os.path.join(checkpoint_dir, "cp.json"), "w") as f:
                json.dump(result, f)

            tune_cp = _TrackedCheckpoint(
                dir_or_data=checkpoint_dir,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=result,
            )
            trial.saving_to = tune_cp

            return checkpoint_dir

        def get_checkpoint_dirs(trial: Trial):
            return [d for d in os.listdir(trial.logdir) if d.startswith("checkpoint_")]

        runner = TrialRunner(local_checkpoint_dir=tempdir)
        runner.add_trial(trial)

        # Write 1 checkpoint
        result = write_checkpoint(trial, 1)
        runner._on_saving_result(trial, result)

        # Expect 1 checkpoint
        cp_dirs = get_checkpoint_dirs(trial)
        self.assertEqual(len(cp_dirs), 1, msg=f"Checkpoint dirs: {cp_dirs}")

        # Write second checkpoint
        result = write_checkpoint(trial, 2)
        runner._on_saving_result(trial, result)

        # Expect 2 checkpoints
        cp_dirs = get_checkpoint_dirs(trial)
        self.assertEqual(len(cp_dirs), 2, msg=f"Checkpoint dirs: {cp_dirs}")

        # Write third checkpoint
        result = write_checkpoint(trial, 3)
        runner._on_saving_result(trial, result)

        # Expect 2 checkpoints because num_to_keep = 2
        cp_dirs = get_checkpoint_dirs(trial)
        self.assertEqual(len(cp_dirs), 2, msg=f"Checkpoint dirs: {cp_dirs}")

        # Re-instantiate trial runner and resume
        runner.checkpoint(force=True)
        runner = TrialRunner(local_checkpoint_dir=tempdir)
        runner.resume()

        trial = runner.get_trials()[0]
        trial.checkpoint_manager.set_delete_fn(lambda cp: shutil.rmtree(cp.dir_or_data))

        # Write fourth checkpoint
        result = write_checkpoint(trial, 4)
        runner._on_saving_result(trial, result)

        # Expect 2 checkpoints because num_to_keep = 2
        cp_dirs = get_checkpoint_dirs(trial)
        self.assertEqual(len(cp_dirs), 2, msg=f"Checkpoint dirs: {cp_dirs}")

        # Write fifth checkpoint
        result = write_checkpoint(trial, 5)
        runner._on_saving_result(trial, result)

        # Expect 2 checkpoints because num_to_keep = 2
        cp_dirs = get_checkpoint_dirs(trial)
        self.assertEqual(len(cp_dirs), 2, msg=f"Checkpoint dirs: {cp_dirs}")

        # Checkpoints before restore should be deleted
        self.assertIn("checkpoint_000004", cp_dirs)
        self.assertIn("checkpoint_000005", cp_dirs)

        self.assertNotIn("checkpoint_000002", cp_dirs)
        self.assertNotIn("checkpoint_000003", cp_dirs)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
