import json
import os
import shutil

import pytest
import sys

import ray
from ray.train import CheckpointConfig
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.constants import TRAINING_ITERATION
from ray.tune import PlacementGroupFactory
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.result import DONE
from ray.tune.schedulers import FIFOScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.trainable import TrainableUtil


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


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


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_save_restore(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Test that a checkpoint is saved and can be used to restore a trainable.

    The trainable saves a checkpoint and terminates. We then start another trial
    that should restore from the saved checkpoint and assert that it picks up
    the state and continues to run to termination.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testCheckpointing
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testRestoreMetricsAfterCheckpointing  # noqa
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        experiment_path=str(tmpdir),
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 1},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    runner.step()  # Start trial

    while trials[0].status != Trial.RUNNING:
        runner.step()

    # Set some state that will be saved in the checkpoint
    assert ray.get(trials[0].runner.set_info.remote(1)) == 1

    while trials[0].status != Trial.TERMINATED:
        runner.step()

    assert trials[0].checkpoint.metrics[TRAINING_ITERATION] == 1
    assert trials[0].last_result[TRAINING_ITERATION] == 1
    assert trials[0].last_result["iterations_since_restore"] == 1

    # Prepare new trial
    kwargs["restore_path"] = trials[0].checkpoint.dir_or_data
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    assert trials[1].status == Trial.PENDING

    # Start trial, restore, run to termination
    while trials[1].status != Trial.RUNNING:
        runner.step()

    # Restore
    runner.step()

    assert ray.get(trials[1].runner.get_info.remote()) == 1

    # Run to termination
    while trials[1].status != Trial.TERMINATED:
        runner.step()

    assert trials[1].checkpoint.metrics[TRAINING_ITERATION] == 2
    assert trials[1].last_result[TRAINING_ITERATION] == 2
    assert trials[1].last_result["iterations_since_restore"] == 1
    assert trials[1].last_result["time_since_restore"] > 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_at_end(ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir):
    """Test that a checkpoint is saved at end for class trainables with that config.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testCheckpointingAtEnd
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testResultDone
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        experiment_path=str(tmpdir),
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "checkpoint_config": CheckpointConfig(checkpoint_at_end=True),
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    while not runner.is_finished():
        runner.step()

    assert trials[0].has_checkpoint()
    assert trials[0].last_result[DONE]


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_pause_resume_trial(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Test that trial that is paused and resumed picks up its last checkpoint.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testPauseThenResume
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        experiment_path=str(tmpdir),
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    while trials[0].status != Trial.RUNNING:
        runner.step()

    assert ray.get(trials[0].runner.get_info.remote()) is None
    assert ray.get(trials[0].runner.set_info.remote(1)) == 1

    runner._schedule_trial_pause(trials[0], should_checkpoint=True)

    while trials[0].status != Trial.PAUSED:
        runner.step()

    assert trials[0].has_checkpoint()
    assert DONE not in trials[0].last_result

    # Start again
    runner._set_trial_status(trials[0], Trial.PENDING)

    while trials[0].status != Trial.RUNNING:
        runner.step()

    assert ray.get(trials[0].runner.get_info.remote()) == 1

    while trials[0].status != Trial.TERMINATED:
        runner.step()

    assert trials[0].checkpoint.metrics[TRAINING_ITERATION] == 2
    assert trials[0].last_result[TRAINING_ITERATION] == 2
    assert trials[0].last_result["iterations_since_restore"] == 1
    assert trials[0].last_result["time_since_restore"] > 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_num_to_keep(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Test that only num_to_keep checkpoints are kept.

    This should also hold true when the experiment is resumed.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testPauseResumeCheckpointCount
    """
    trial = Trial(
        "__fake",
        experiment_path=str(tmpdir),
        checkpoint_config=CheckpointConfig(num_to_keep=2),
    )
    trial.init_local_path()
    trial.checkpoint_manager.set_delete_fn(lambda cp: shutil.rmtree(cp.dir_or_data))

    def write_checkpoint(trial: Trial, index: int):
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            trial.local_path, index=index
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
        return [d for d in os.listdir(trial.local_path) if d.startswith("checkpoint_")]

    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        experiment_path=str(tmpdir),
    )

    runner.add_trial(trial)

    # Write 1 checkpoint
    result = write_checkpoint(trial, 1)
    runner._on_saving_result(trial, result)

    # Expect 1 checkpoint
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 1, f"Checkpoint dirs: {cp_dirs}"

    # Write second checkpoint
    result = write_checkpoint(trial, 2)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Write third checkpoint
    result = write_checkpoint(trial, 3)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Re-instantiate trial runner and resume
    runner.checkpoint(force=True)
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        experiment_path=str(tmpdir),
    )
    runner.resume()

    trial = runner.get_trials()[0]
    trial.checkpoint_manager.set_delete_fn(lambda cp: shutil.rmtree(cp.dir_or_data))

    # Write fourth checkpoint
    result = write_checkpoint(trial, 4)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Write fifth checkpoint
    result = write_checkpoint(trial, 5)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Checkpoints before restore should be deleted
    assert "checkpoint_000004" in cp_dirs
    assert "checkpoint_000005" in cp_dirs

    assert "checkpoint_000002" not in cp_dirs
    assert "checkpoint_000003" not in cp_dirs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
