import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Type

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.tune import SyncConfig
from ray.tune.callback import CallbackList
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.utils.callback import _create_default_callbacks


class _ExperimentCheckpointCreator:
    def __init__(
        self,
        runner_cls: Type[TuneController],
        experiment_path: str,
        experiment_name: str = "test_experiment",
    ):
        outer = self

        class _TestRunnerCls(runner_cls):
            def _get_trial_checkpoints(self):
                return outer.get_trial_checkpoints()

        self.experiment_path = experiment_path
        self.experiment_name = experiment_name
        self.runner = _TestRunnerCls(
            experiment_path=experiment_path, experiment_dir_name=experiment_name
        )

        # Also, create any default logger callback artifacts.
        self.callbacks = CallbackList(
            _create_default_callbacks([], sync_config=SyncConfig(syncer=None))
        )

    def save_checkpoint(self):
        self.runner.save_to_dir()

    def get_trials(self):
        return self.runner.get_trials()

    def get_trial_checkpoints(self):
        return {
            trial.trial_id: trial.get_json_state() for trial in self.runner.get_trials()
        }

    def trial_result(self, trial: Trial, result: Dict):
        trial.update_last_result(result)
        trial.invalidate_json_state()
        self.callbacks.on_trial_result(
            iteration=-1,  # Dummy value
            trials=self.get_trials(),
            trial=trial,
            result=result,
        )

    def trial_checkpoint(
        self,
        trial: Trial,
        checkpoint_data: Any,
        checkpoint_storage: str = CheckpointStorage.PERSISTENT,
    ):
        checkpoint_storage = checkpoint_storage or CheckpointStorage.PERSISTENT
        checkpoint = _TrackedCheckpoint(
            dir_or_data=checkpoint_data,
            storage_mode=checkpoint_storage,
            metrics=trial.last_result,
        )
        trial.on_checkpoint(checkpoint)
        trial.invalidate_json_state()

    def create_trial(
        self,
        trial_id: str,
        config: Dict,
        trainable_name: Optional[str] = "trainable",
        **kwargs,
    ) -> Trial:
        trial = Trial(
            trial_id=trial_id,
            config=config,
            experiment_path=self.experiment_path,
            experiment_dir_name=self.experiment_name,
            trainable_name=trainable_name,
            stub=True,
            **kwargs,
        )
        trial.init_local_path()
        self.runner.add_trial(trial)
        self.callbacks.on_trial_start(
            iteration=-1,  # Dummy value
            trials=self.get_trials(),
            trial=trial,
        )

        return trial


@contextmanager
def create_test_experiment_checkpoint(
    local_experiment_path: Optional[str] = None,
    tune_runner_cls: Type[TuneController] = TuneController,
):
    local_experiment_path = local_experiment_path or tempfile.mkdtemp()

    Path(local_experiment_path).mkdir(exist_ok=True)

    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"
    creator = _ExperimentCheckpointCreator(
        runner_cls=tune_runner_cls, experiment_path=local_experiment_path
    )

    yield creator

    creator.save_checkpoint()
