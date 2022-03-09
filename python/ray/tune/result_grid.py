from ray.ml.result import Result
from ray.tune import ExperimentAnalysis
from ray.tune.trial import Trial
from ray.util import PublicAPI


# TODO(xwjiang): Change to alpha
@PublicAPI(stability="beta")
class ResultGrid:
    """A summary returned by ``tuner.fit()`` call.

    You can use it to inspect the trials run as well as obtaining the best result.
    """

    def __init__(self, experiment_analysis: ExperimentAnalysis):
        self._experiment_analysis = experiment_analysis

    def _trial_to_result(self, trial: Trial) -> Result:
        result = Result(checkpoint=trial.checkpoint, metrics=trial.last_result)
        return result

    def get_best_result(self) -> Result:
        """Get the best result from all the trials run.

        Note, "best" here is determined by "metric" and "mode" specified in your Tuner's
        TuneConfig.

        Trials are compared using their "last" results. In a similar notion, the last
        checkpoint of the best trial is returned as part of the result."""
        return self._trial_to_result(self._experiment_analysis.best_trial)

    def __len__(self) -> int:
        return len(self._experiment_analysis.trials)

    def __getitem__(self, i) -> Result:
        """The "last" result and "last" checkpoint of the trial is returned."""
        return self._trial_to_result(self._experiment_analysis.trials[i])
