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
            result = result_grid[i]
            if result.status == Trial.TERMINATED:
                print(f"Trial finishes successfully with metric {result.metric}.")
            elif result.error:
                print(f"Trial errors out with {result.error}.")
        best_result = result_grid.get_best_result()
        best_checkpoint = best_result.checkpoint
        best_metric = best_result.metric

    Note trials of all status are included in the final result grid.
    In case if a trial is not terminated, its most latest result and checkpoint as
    perceived by Tune driver component will be presented.
    """

    def __init__(self, experiment_analysis: ExperimentAnalysis):
        self._experiment_analysis = experiment_analysis

    @staticmethod
    def _trial_to_result(trial: Trial) -> Result:
        if trial.error_file:
            with open(trial.error_file, "r") as f:
                error_msg = f.read()
        result = Result(
            checkpoint=trial.checkpoint,
            metrics=trial.last_result,
            status=Trial.status,
            error=error_msg,
        )
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
        """Returns the i'th result in the grid."""
        return self._trial_to_result(self._experiment_analysis.trials[i])
