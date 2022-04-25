import os
from typing import Optional, Union

from ray.cloudpickle import cloudpickle
from ray.exceptions import RayTaskError
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

    def get_best_result(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        scope: str = "last",
        filter_nan_and_inf: bool = True,
    ) -> Result:
        """Get the best result from all the trials run.

        Args:
            metric: Key for trial info to order on. Defaults to
                the metric specified in your Tuner's ``TuneConfig``.
            mode: One of [min, max]. Defaults to the mode specified
                in your Tuner's ``TuneConfig``.
            scope: One of [all, last, avg, last-5-avg, last-10-avg].
                If `scope=last`, only look at each trial's final step for
                `metric`, and compare across trials based on `mode=[min,max]`.
                If `scope=avg`, consider the simple average over all steps
                for `metric` and compare across trials based on
                `mode=[min,max]`. If `scope=last-5-avg` or `scope=last-10-avg`,
                consider the simple average over the last 5 or 10 steps for
                `metric` and compare across trials based on `mode=[min,max]`.
                If `scope=all`, find each trial's min/max score for `metric`
                based on `mode`, and compare trials based on `mode=[min,max]`.
            filter_nan_and_inf: If True (default), NaN or infinite
                values are disregarded and these trials are never selected as
                the best trial.
        """
        return self._trial_to_result(
            self._experiment_analysis.get_best_trial(
                metric=metric,
                mode=mode,
                scope=scope,
                filter_nan_and_inf=filter_nan_and_inf,
            )
        )

    def __len__(self) -> int:
        return len(self._experiment_analysis.trials)

    def __getitem__(self, i) -> Result:
        """Returns the i'th result in the grid."""
        return self._trial_to_result(self._experiment_analysis.trials[i])

    @staticmethod
    def _populate_exception(trial: Trial) -> Optional[Union[TuneError, RayTaskError]]:
        if trial.pickled_error_file and os.path.exists(trial.pickled_error_file):
            with open(trial.pickled_error_file, "rb") as f:
                e = cloudpickle.load(f)
                return e
        elif trial.error_file and os.path.exists(trial.error_file):
            with open(trial.error_file, "r") as f:
                return TuneError(f.read())
        return None

    def _trial_to_result(self, trial: Trial) -> Result:
        result = Result(
            checkpoint=Checkpoint.from_directory(trial.checkpoint.value)
            if trial.checkpoint.value
            else None,
            metrics=trial.last_result.copy(),
            error=self._populate_exception(trial),
        )
        return result
