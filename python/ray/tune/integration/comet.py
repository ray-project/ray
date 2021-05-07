from typing import Dict, List

from ray.tune.logger import LoggerCallback
from ray.tune.trial import Trial

def _import_comet():
    try:
        import comet_ml
    except ImportError:
        raise RuntimeError("pip install 'comet-ml' to use "
                     "CometLoggerCallback")


class CometLoggerCallback(LoggerCallback):
    def __init__(self, online: bool = True, tags: List[str] = None, **kwargs):
        _import_comet()
        self.online = online
        self.tags = tags
        self.experiment_kwargs = kwargs

        # Mapping from trial to experiment object.
        self._trial_experiments = {}

    def log_trial_start(self, trial: "Trial"):
        from comet_ml import Experiment, OfflineExperiment

        if trial not in self._trial_experiments:
            experiment_cls = Experiment if self.online else OfflineExperiment
            experiment = experiment_cls(**self.experiment_kwargs)
            self._trial_experiments[trial] = experiment
        else:
            experiment = self._trial_experiments[trial]

        experiment.set_name(str(trial))
        experiment.add_tags(self.tags)
        config = trial.config.copy()
        experiment.log_parameters(config)

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        experiment = self._trial_experiments[trial]
        experiment.log_metrics(result)
