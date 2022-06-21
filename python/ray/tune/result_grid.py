import os
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd

from ray.air.result import Result
from ray.cloudpickle import cloudpickle
from ray.exceptions import RayTaskError
from ray.tune import ExperimentAnalysis
from ray.tune.error import TuneError
from ray.tune.trial import Trial
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.air.config import CheckpointingConfig


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

    def __init__(
        self,
        experiment_analysis: ExperimentAnalysis,
        checkpointing_config: Optional["CheckpointingConfig"] = None,
    ):
        self._experiment_analysis = experiment_analysis
        # Used to determine best checkpoint
        self._checkpointing_config = checkpointing_config

    def _resolve_checkpointing_config(
        self,
        checkpointing_config: "CheckpointingConfig",
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> "CheckpointingConfig":
        # Lazy import to avoid circular dependency
        from ray.air.config import CheckpointingConfig

        metric = metric or self._experiment_analysis.default_metric
        mode = mode or self._experiment_analysis.default_mode

        if not isinstance(checkpointing_config, CheckpointingConfig):
            if checkpointing_config and self._checkpointing_config:
                checkpointing_config = self._checkpointing_config
            else:
                checkpointing_config = CheckpointingConfig(
                    checkpoint_score_metric=metric, checkpoint_score_mode=mode
                )
        return checkpointing_config

    def get_best_result(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        scope: str = "last",
        filter_nan_and_inf: bool = True,
        checkpointing_config: Union[bool, "CheckpointingConfig"] = True,
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
            checkpointing_config: If True (default), will use the
                ``CheckpointingConfig`` object set in Trainer's ``RunConfig``
                to determine the best checkpoint of the trial.
                If False, or if the ``CheckpointingConfig`` object was not set, will use
                ``metric`` and ``mode`` as set here.
                Can also be a ``CheckpointingConfig`` object, in which case it will
                be used directly.
        """
        if not metric and not self._experiment_analysis.default_metric:
            raise ValueError(
                "No metric is provided. Either pass in a `metric` arg to "
                "`get_best_result` or specify a metric in the "
                "`TuneConfig` of your `Tuner`."
            )
        if not mode and not self._experiment_analysis.default_mode:
            raise ValueError(
                "No mode is provided. Either pass in a `mode` arg to "
                "`get_best_result` or specify a mode in the "
                "`TuneConfig` of your `Tuner`."
            )

        best_trial = self._experiment_analysis.get_best_trial(
            metric=metric,
            mode=mode,
            scope=scope,
            filter_nan_and_inf=filter_nan_and_inf,
        )
        if not best_trial:
            error_msg = (
                "No best trial found for the given metric: "
                f"{metric or self._experiment_analysis.default_metric}. "
                "This means that no trial has reported this metric"
            )
            error_msg += (
                ", or all values reported for this metric are NaN. To not ignore NaN "
                "values, you can set the `filter_nan_and_inf` arg to False."
                if filter_nan_and_inf
                else "."
            )
            raise RuntimeError(error_msg)

        checkpointing_config = self._resolve_checkpointing_config(
            checkpointing_config, metric=metric, mode=mode
        )

        return self._trial_to_result(
            best_trial, checkpointing_config=checkpointing_config
        )

    def get_dataframe(
        self,
        filter_metric: Optional[str] = None,
        filter_mode: Optional[str] = None,
    ) -> pd.DataFrame:
        """Return dataframe of all trials with their configs and reported results.

        Per default, this returns the last reported results for each trial.

        If ``filter_metric`` and ``filter_mode`` are set, the results from each
        trial are filtered for this metric and mode. For example, if
        ``filter_metric="some_metric"`` and ``filter_mode="max"``, for each trial,
        every received result is checked, and the one where ``some_metric`` is
        maximal is returned.


        Example:

            .. code-block:: python

                result_grid = Tuner.fit(...)

                # Get last reported results per trial
                df = result_grid.get_dataframe()

                # Get best ever reported accuracy per trial
                df = result_grid.get_dataframe(metric="accuracy", mode="max")

        Args:
            filter_metric: Metric to filter best result for.
            filter_mode: If ``filter_metric`` is given, one of ``["min", "max"]``
                to specify if we should find the minimum or maximum result.

        Returns:
            Pandas DataFrame with each trial as a row and their results as columns.
        """
        return self._experiment_analysis.dataframe(
            metric=filter_metric, mode=filter_mode
        )

    def __len__(self) -> int:
        return len(self._experiment_analysis.trials)

    def __getitem__(self, i: int) -> Result:
        """Returns the i'th result in the grid."""
        return self.get(i)

    def get(
        self, i: int, *, checkpointing_config: Union[bool, "CheckpointingConfig"] = True
    ):
        """Returns the i'th result in the grid.

        Args:
            i: index to return.
            checkpointing_config: If True (default), will use the
                ``CheckpointingConfig`` object set in Trainer's ``RunConfig``
                to determine the best checkpoint of the trial.
                If False, or if the ``CheckpointingConfig`` object was not set, will use
                ``metric`` and ``mode`` as set here.
                Can also be a ``CheckpointingConfig`` object, in which case it will
                 be used directly.
        """

        checkpointing_config = self._resolve_checkpointing_config(checkpointing_config)

        return self._trial_to_result(
            self._experiment_analysis.trials[i],
            checkpointing_config=checkpointing_config,
        )

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

    def _trial_to_result(
        self, trial: Trial, checkpointing_config: Optional["CheckpointingConfig"]
    ) -> Result:
        checkpoint = trial.checkpoint.to_air_checkpoint()

        checkpoint_metric = (
            checkpointing_config.checkpoint_score_metric
            if checkpointing_config
            else None
        )
        checkpoint_mode = (
            checkpointing_config.checkpoint_score_mode_not_none
            if checkpointing_config and checkpoint_metric
            else None
        )
        try:
            best_checkpoint = self._experiment_analysis.get_best_checkpoint(
                trial, metric=checkpoint_metric, mode=checkpoint_mode
            )
        except ValueError:
            best_checkpoint = None

        result = Result(
            checkpoint=checkpoint,
            best_checkpoint=best_checkpoint,
            metrics=trial.last_result.copy(),
            error=self._populate_exception(trial),
            dataframe=self._experiment_analysis.trial_dataframes.get(trial.logdir),
        )
        return result
