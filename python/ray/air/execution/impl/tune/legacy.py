from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.tune.experiment import Trial


class LegacyRayTrialExecutor:
    def __init__(self, tune_controller: TuneController):
        self._tune_controller = tune_controller

    def pause_trial(self, trial: Trial):
        pass

    def force_reconcilation_on_next_step_end(self):
        pass


class LegacyTrialRunner:
    def __init__(self, tune_controller: TuneController):
        self._tune_controller = tune_controller
        self._legacy_executor = LegacyRayTrialExecutor(tune_controller)

    @property
    def trial_executor(self):
        return self._legacy_executor
