import os
import tempfile
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any, Dict, Optional, Type

from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.tune.execution.trial_runner import TrialRunner, _TuneControllerBase
from ray.tune.experiment import Trial
from ray.tune.trainable import TrainableUtil


class _ExperimentCheckpointCreator:
    def __init__(
        self,
        runner_cls: Type[_TuneControllerBase],
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
            local_to_remote_path_fn=partial(
                TrainableUtil.get_remote_storage_path,
                logdir=trial.local_path,
                remote_checkpoint_dir=trial.remote_path,
            ),
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

        return trial


@contextmanager
def create_test_experiment_checkpoint(
    local_experiment_path: Optional[str] = None,
    tune_runner_cls: Type[_TuneControllerBase] = TrialRunner,
):
    local_experiment_path = local_experiment_path or tempfile.mkdtemp()

    Path(local_experiment_path).mkdir(exist_ok=True)

    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "1"
    creator = _ExperimentCheckpointCreator(
        runner_cls=tune_runner_cls, experiment_path=local_experiment_path
    )

    yield creator

    creator.save_checkpoint()
