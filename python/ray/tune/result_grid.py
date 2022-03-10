from ray.ml.result import Result
from ray.tune import ExperimentAnalysis
from ray.tune.trial import Trial
from ray.util import PublicAPI


# TODO(xwjiang): Change to alpha
@PublicAPI(stability="beta")
class ResultGrid:
    """A set of ``Result`` objects returned from a call to ``tuner.fit()``.

    You can use it to inspect the trials run as well as obtaining the best result.

    Usage pattern:
    .. code-block:: python
        result_grid = tuner.fit()
        for i in range(len(result_grid)):
            print(result_grid[i])
        best_result = result_grid.get_best_result()
        best_checkpoint = best_result.checkpoint
        best_metric = best_result.metric

    Note only terminated trials are included in the final result grid.
    If one wants to inspect errored trials, one may look at console output,
    where there is a highlight of errored trials, together with path to error file.

    """

    def __init__(self, experiment_analysis: ExperimentAnalysis):
        self._experiment_analysis = experiment_analysis
        self._terminated_trials = [
            t for t in experiment_analysis.trials if t.status == Trial.TERMINATED
        ]

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
        return len(self._terminated_trials)

    def __getitem__(self, i) -> Result:
        """Returns the i'th result in the grid."""
        return self._trial_to_result(self._terminated_trials[i])
