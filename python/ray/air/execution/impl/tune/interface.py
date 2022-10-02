from typing import Optional, Dict, TYPE_CHECKING

from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.tune import PlacementGroupFactory
from ray.tune.experiment import Trial


if TYPE_CHECKING:
    from ray.air.execution.impl.tune.tune_controller import TuneController


class TuneControllerInterface:
    def __init__(self, tune_controller: "TuneController"):
        self._tune_controller = tune_controller


class LegacyTrialRunner(TuneControllerInterface):
    def __init__(self, tune_controller: "TuneController"):
        super(LegacyTrialRunner, self).__init__(tune_controller=tune_controller)
        self._legacy_executor = LegacyRayTrialExecutor(tune_controller)

    @property
    def trial_executor(self):
        return self._legacy_executor

    def update_trial_resources(self, trial: Trial, resources: PlacementGroupFactory):
        self._tune_controller.update_trial_resources(trial=trial, resources=resources)


class LegacyRayTrialExecutor:
    def __init__(self, tune_controller: "TuneController"):
        self._tune_controller = tune_controller

    def pause_trial(self, trial: Trial):
        pass

    def force_reconcilation_on_next_step_end(self) -> None:
        pass

    def has_resources_for_trial(self, trial: Trial) -> bool:
        return True

    def save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TrackedCheckpoint:
        trial.last_result = result
        return self._tune_controller._schedule_save(
            trial.runner, storage=storage, _metrics=result
        )
