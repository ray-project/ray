from typing import Optional

from ray.ml.checkpoint import Checkpoint
from ray.ml.result import Result
from ray.tune import ExperimentAnalysis
from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
class ResultGrid:
    """A set of ``Result`` objects returned from a call to ``tuner.fit()``.

    You can use it to inspect the trials run as well as obtaining the best result.

    The constructor is a private API.

    Usage pattern:

    .. code-block:: python

        result_grid = tuner.fit()
        for i in range(len(result_grid)):
            result = result_grid[i]
            if not result.error:
                print(f"Trial finishes successfully with metric {result.metric}.")
            else:
                print(f"Trial errors out with {result.error}.")
        best_result = result_grid.get_best_result()
        best_checkpoint = best_result.checkpoint
        best_metric = best_result.metric

    Note that trials of all statuses are included in the final result grid.
    If a trial is not in terminated state, its latest result and checkpoint as
    seen by Tune will be provided.
    """

    def __init__(self, experiment_analysis: ExperimentAnalysis):
        self._experiment_analysis = experiment_analysis

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

    @staticmethod
    def _populate_exception(trial: Trial) -> Optional[TuneError]:
        if trial.error_file:
            with open(trial.error_file, "r") as f:
                return TuneError(f.read())
        return None

    def _trial_to_result(self, trial: Trial) -> Result:
        result = Result(
            checkpoint=Checkpoint.from_directory(trial.checkpoint.value)
            if trial.checkpoint.value
            else None,
            metrics=trial.last_result,
            error=self._populate_exception(trial),
        )
        return result
