import copy
import io
import json
import logging
import os
from numbers import Number
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow.fs

from ray.air.constants import EXPR_PROGRESS_FILE, EXPR_RESULT_FILE, TRAINING_ITERATION
from ray.tune import Checkpoint
from ray.train._internal.storage import _exists_at_fs_path, get_fs_and_path
from ray.tune.execution.experiment_state import _find_newest_experiment_checkpoint
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.result import CONFIG_PREFIX, DEFAULT_METRIC
from ray.tune.utils import flatten_dict
from ray.tune.utils.serialization import TuneFunctionDecoder
from ray.tune.utils.util import is_nan, is_nan_or_inf, unflattened_lookup
from ray.util.annotations import PublicAPI

try:
    import pandas as pd
    from pandas import DataFrame
except ImportError:
    pd = None
    DataFrame = None


logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class ExperimentAnalysis:
    """Analyze results from a Ray Train/Tune experiment.

    To use this class, the run must store the history of reported metrics
    in log files (e.g., `result.json` and `progress.csv`).
    This is the default behavior, unless default loggers are explicitly excluded
    with the `TUNE_DISABLE_AUTO_CALLBACK_LOGGERS=1` environment variable.

    Parameters:
        experiment_checkpoint_path: Path to an `experiment_state.json` file,
            or a directory that contains an `experiment_state.json` file.
        default_metric: Default metric for comparing results. Can be
            overwritten with the ``metric`` parameter in the respective
            functions.
        default_mode: Default mode for comparing results. Has to be one
            of [min, max]. Can be overwritten with the ``mode`` parameter
            in the respective functions.
        trials: List of trials that can be accessed via `analysis.trials`.
    """

    def __init__(
        self,
        experiment_checkpoint_path: Union[str, os.PathLike],
        *,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        trials: Optional[List[Trial]] = None,
        default_metric: Optional[str] = None,
        default_mode: Optional[str] = None,
    ):
        self.default_metric = default_metric
        if default_mode and default_mode not in ["min", "max"]:
            raise ValueError("`default_mode` has to be None or one of [min, max]")
        self.default_mode = default_mode
        if self.default_metric is None and self.default_mode is not None:
            # If only a mode was passed, use anonymous metric
            self.default_metric = DEFAULT_METRIC

        # Resolve the filesystem if not specified.
        if storage_filesystem:
            self._fs = storage_filesystem
        else:
            self._fs, experiment_checkpoint_path = get_fs_and_path(
                experiment_checkpoint_path
            )

        # Find the json state file.
        experiment_checkpoint_path = str(experiment_checkpoint_path)
        if experiment_checkpoint_path.endswith(".json"):
            self._experiment_fs_path = os.path.dirname(experiment_checkpoint_path)
            self._experiment_json_fs_path = experiment_checkpoint_path
        else:
            self._experiment_fs_path = experiment_checkpoint_path

            experiment_json_fs_path = _find_newest_experiment_checkpoint(
                experiment_path=self._experiment_fs_path, fs=self._fs
            )
            if experiment_json_fs_path is None:
                pattern = TuneController.CKPT_FILE_TMPL.format("*")
                raise ValueError(
                    f"No experiment snapshot file of form '{pattern}' was found at: "
                    f"({self._fs.type_name}, {self._experiment_fs_path})\n"
                    "Please check if you specified the correct experiment path, "
                    "which should be a combination of the `storage_path` and `name` "
                    "specified in your run."
                )

            self._experiment_json_fs_path = experiment_json_fs_path

        self.trials = trials or self._load_trials()
        self._trial_dataframes = self._fetch_trial_dataframes()
        self._configs = self.get_all_configs()

    def _load_trials(self) -> List[Trial]:
        with self._fs.open_input_stream(self._experiment_json_fs_path) as f:
            experiment_state = json.loads(f.readall(), cls=TuneFunctionDecoder)

        experiment_fs_path = Path(self._experiment_fs_path)

        trials = []
        trial_states = experiment_state["trial_data"]
        for trial_json_state, trial_runtime_metadata in trial_states:
            trial = Trial.from_json_state(trial_json_state, stub=True)
            trial.restore_run_metadata(trial_runtime_metadata)

            new_storage = copy.copy(trial.storage)
            new_storage.storage_fs_path = experiment_fs_path.parent.as_posix()
            new_storage.storage_filesystem = self._fs
            new_storage.experiment_dir_name = experiment_fs_path.name
            trial.set_storage(new_storage)

            trials.append(trial)
        return trials

    def _fetch_trial_dataframe(self, trial: Trial) -> DataFrame:
        force_dtype = {"trial_id": str}  # Never convert trial_id to float.

        # If there were no reported results, there will be no files into a DataFrame
        if trial.last_result is None:
            return DataFrame()

        json_fs_path = Path(trial.storage.trial_fs_path, EXPR_RESULT_FILE).as_posix()
        csv_fs_path = Path(trial.storage.trial_fs_path, EXPR_PROGRESS_FILE).as_posix()
        # Prefer reading the JSON if it exists.
        if _exists_at_fs_path(trial.storage.storage_filesystem, json_fs_path):
            with trial.storage.storage_filesystem.open_input_stream(json_fs_path) as f:
                content = f.readall().decode("utf-8").rstrip("\n")
                if not content:
                    return DataFrame()
                json_list = [json.loads(row) for row in content.split("\n")]
            df = pd.json_normalize(json_list, sep="/")
        # Fallback to reading the CSV.
        elif _exists_at_fs_path(trial.storage.storage_filesystem, csv_fs_path):
            with trial.storage.storage_filesystem.open_input_stream(csv_fs_path) as f:
                csv_str = f.readall().decode("utf-8")
            df = pd.read_csv(io.StringIO(csv_str), dtype=force_dtype)
        else:
            raise FileNotFoundError(
                f"Could not fetch metrics for {trial}: both {EXPR_RESULT_FILE} and "
                f"{EXPR_PROGRESS_FILE} were not found at {trial.storage.trial_fs_path}"
            )

        return df

    def _fetch_trial_dataframes(self) -> Dict[str, DataFrame]:
        """Fetches trial dataframes from files.

        Returns:
            A dictionary mapping trial_id -> pd.DataFrame
        """
        failures = []

        trial_dfs = {}
        for trial in self.trials:
            try:
                trial_dfs[trial.trial_id] = self._fetch_trial_dataframe(trial)
            except Exception as e:
                failures.append((trial, e))
                trial_dfs[trial.trial_id] = DataFrame()
                continue

        if failures:
            fail_str = "\n".join(
                [f"- {trial}: {repr(error)}" for trial, error in failures]
            )
            logger.warning(
                f"Failed to fetch metrics for {len(failures)} trial(s):\n{fail_str}"
            )
        return trial_dfs

    def get_all_configs(self, prefix: bool = False) -> Dict[str, Dict]:
        """Returns all trial hyperparameter configurations.

        Args:
            prefix: If True, flattens the config dict
                and prepends `config/`.

        Returns:
            Dict[str, Dict]: Mapping trial_id -> config dict
        """
        return {
            trial.trial_id: (
                flatten_dict({CONFIG_PREFIX: trial.config}) if prefix else trial.config
            )
            for trial in self.trials
        }

    @property
    def experiment_path(self) -> str:
        """Path pointing to the experiment directory on persistent storage.

        This can point to a remote storage location (e.g. S3) or to a local
        location (path on the head node)."""
        return self._experiment_fs_path

    @property
    def best_trial(self) -> Trial:
        """Get the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_trial`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_trial(metric, mode)` method to set the metric "
                "and mode explicitly."
            )
        return self.get_best_trial(self.default_metric, self.default_mode)

    @property
    def best_config(self) -> Dict:
        """Get the config of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_config(metric, mode, scope)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_config`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_config(metric, mode)` method to set the metric "
                "and mode explicitly."
            )
        return self.get_best_config(self.default_metric, self.default_mode)

    @property
    def best_checkpoint(self) -> Checkpoint:
        """Get the checkpoint path of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_checkpoint(trial, metric, mode)` instead.

        Returns:
            :class:`Checkpoint <ray.tune.Checkpoint>` object.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_checkpoint`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_checkpoint(trial, metric, mode)` method to set the "
                "metric and mode explicitly."
            )
        best_trial = self.best_trial
        if not best_trial:
            raise ValueError(
                f"No best trial found. Please check if you specified the "
                f"correct default metric ({self.default_metric}) and mode "
                f"({self.default_mode})."
            )
        return self.get_best_checkpoint(
            best_trial, self.default_metric, self.default_mode
        )

    @property
    def best_dataframe(self) -> DataFrame:
        """Get the full result dataframe of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode)` and use it to look for the dataframe
        in the `self.trial_dataframes` dict.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_result`, pass a `metric` and `mode` "
                "parameter to `tune.run()`."
            )
        return self.trial_dataframes[self.best_trial.trial_id]

    @property
    def best_result(self) -> Dict:
        """Get the last result of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope).last_result` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_result`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use "
                "`get_best_trial(metric, mode).last_result` to set "
                "the metric and mode explicitly and fetch the last result."
            )
        return self.best_trial.last_result

    def _delimiter(self):
        return os.environ.get("TUNE_RESULT_DELIM", "/")

    @property
    def best_result_df(self) -> DataFrame:
        """Get the best result of the experiment as a pandas dataframe.

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_trial(metric, mode, scope).last_result` instead.
        """
        if not pd:
            raise ValueError(
                "`best_result_df` requires pandas. Install with "
                "`pip install pandas`."
            )

        best_result = flatten_dict(self.best_result, delimiter=self._delimiter())
        return pd.DataFrame.from_records([best_result], index="trial_id")

    @property
    def results(self) -> Dict[str, Dict]:
        """Get the last result of the all trials of the experiment"""
        return {trial.trial_id: trial.last_result for trial in self.trials}

    @property
    def results_df(self) -> DataFrame:
        """Get all the last results as a pandas dataframe."""
        if not pd:
            raise ValueError(
                "`results_df` requires pandas. Install with `pip install pandas`."
            )
        return pd.DataFrame.from_records(
            [
                flatten_dict(trial.last_result, delimiter=self._delimiter())
                for trial in self.trials
            ],
            index="trial_id",
        )

    @property
    def trial_dataframes(self) -> Dict[str, DataFrame]:
        """List of all dataframes of the trials.

        Each dataframe is indexed by iterations and contains reported
        metrics.
        """
        return self._trial_dataframes

    def dataframe(
        self, metric: Optional[str] = None, mode: Optional[str] = None
    ) -> DataFrame:
        """Returns a pandas.DataFrame object constructed from the trials.

        This function will look through all observed results of each trial
        and return the one corresponding to the passed ``metric`` and
        ``mode``: If ``mode=min``, it returns the result with the lowest
        *ever* observed ``metric`` for this trial (this is not necessarily
        the last)! For ``mode=max``, it's the highest, respectively. If
        ``metric=None`` or ``mode=None``, the last result will be returned.

        Args:
            metric: Key for trial info to order on. If None, uses last result.
            mode: One of [None, "min", "max"].

        Returns:
            pd.DataFrame: Constructed from a result dict of each trial.
        """
        # Do not validate metric/mode here or set from default metric/mode!
        # Otherwise we will get confusing results as the lowest ever observed
        # result may not be the last result.
        if mode and mode not in ["min", "max"]:
            raise ValueError("If set, `mode` has to be one of [min, max]")

        if mode and not metric:
            raise ValueError(
                "If a `mode` is passed to `ExperimentAnalysis.dataframe(),"
                " you'll also have to pass a `metric`!"
            )

        rows = self._retrieve_rows(metric=metric, mode=mode)
        all_configs = self.get_all_configs(prefix=True)
        for path, config in all_configs.items():
            if path in rows:
                rows[path].update(config)
                rows[path].update(logdir=path)
        return pd.DataFrame(list(rows.values()))

    def _get_trial_checkpoints_with_metric(
        self, trial: Trial, metric: Optional[str] = None
    ) -> List[Tuple[Checkpoint, Number]]:
        """Get all checkpoints and a specified metric of a trial.

        Args:
            trial: The log directory of a trial, or a trial instance.
            metric: key for trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.

        Returns:
            List of [Checkpoint, metric] for all checkpoints of the trial.
        """
        metric = metric or self.default_metric or TRAINING_ITERATION

        best_checkpoint_results = (
            trial.run_metadata.checkpoint_manager.best_checkpoint_results
        )
        best_checkpoints = [
            (checkpoint_result.checkpoint, checkpoint_result.metrics)
            for checkpoint_result in best_checkpoint_results
        ]
        # Support nested metrics given as flattened strings, e.g.
        # "info/learner/default_policy/policy_loss".
        return [
            (checkpoint, unflattened_lookup(metric, metrics))
            for checkpoint, metrics in best_checkpoints
        ]

    def get_best_checkpoint(
        self,
        trial: Trial,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """Gets best persistent checkpoint path of provided trial.

        Any checkpoints with an associated metric value of ``nan`` will be filtered out.

        Args:
            trial: The log directory of a trial, or a trial instance.
            metric: key of trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.
            mode: One of [min, max]. Defaults to ``self.default_mode``.

        Returns:
            A :class:`Checkpoint <ray.tune.Checkpoint>` object
        """
        metric = metric or self.default_metric or TRAINING_ITERATION
        mode = self._validate_mode(mode)

        checkpoints_and_metrics = self._get_trial_checkpoints_with_metric(trial, metric)

        # Filter out nan. Sorting nan values leads to undefined behavior.
        checkpoints_and_metrics = list(
            filter(lambda x: not is_nan(x[1]), checkpoints_and_metrics)
        )

        if not checkpoints_and_metrics:
            logger.error(f"No checkpoints have been found for trial {trial}.")
            return None

        score_order_factor = -1 if mode == "min" else 1
        best_checkpoint, _ = max(
            checkpoints_and_metrics, key=lambda x: score_order_factor * x[1]
        )
        return best_checkpoint

    def get_best_trial(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        scope: str = "last",
        filter_nan_and_inf: bool = True,
    ) -> Optional[Trial]:
        """Retrieve the best trial object.

        Compares all trials' scores on ``metric``.
        If ``metric`` is not specified, ``self.default_metric`` will be used.
        If `mode` is not specified, ``self.default_mode`` will be used.
        These values are usually initialized by passing the ``metric`` and
        ``mode`` parameters to ``tune.run()``.

        Args:
            metric: Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode: One of [min, max]. Defaults to ``self.default_mode``.
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

        Returns:
            The best trial for the provided metric. If no trials contain the provided
                metric, or if the value for the metric is NaN for all trials,
                then returns None.
        """
        if len(self.trials) == 1:
            return self.trials[0]

        metric = self._validate_metric(metric)
        mode = self._validate_mode(mode)

        if scope not in ["all", "last", "avg", "last-5-avg", "last-10-avg"]:
            raise ValueError(
                "ExperimentAnalysis: attempting to get best trial for "
                'metric {} for scope {} not in ["all", "last", "avg", '
                '"last-5-avg", "last-10-avg"]. '
                "If you didn't pass a `metric` parameter to `tune.run()`, "
                "you have to pass one when fetching the best trial.".format(
                    metric, scope
                )
            )
        best_trial = None
        best_metric_score = None

        for trial in self.trials:
            if metric not in trial.metric_analysis:
                continue

            if scope in ["last", "avg", "last-5-avg", "last-10-avg"]:
                metric_score = trial.metric_analysis[metric][scope]
            else:
                metric_score = trial.metric_analysis[metric][mode]

            if filter_nan_and_inf and is_nan_or_inf(metric_score):
                continue

            if best_metric_score is None:
                best_metric_score = metric_score
                best_trial = trial
                continue

            if (mode == "max") and (best_metric_score < metric_score):
                best_metric_score = metric_score
                best_trial = trial
            elif (mode == "min") and (best_metric_score > metric_score):
                best_metric_score = metric_score
                best_trial = trial

        if not best_trial:
            logger.warning(
                "Could not find best trial. Did you pass the correct `metric` "
                "parameter?"
            )
        return best_trial

    def get_best_config(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        scope: str = "last",
    ) -> Optional[Dict]:
        """Retrieve the best config corresponding to the trial.

        Compares all trials' scores on `metric`.
        If ``metric`` is not specified, ``self.default_metric`` will be used.
        If `mode` is not specified, ``self.default_mode`` will be used.
        These values are usually initialized by passing the ``metric`` and
        ``mode`` parameters to ``tune.run()``.

        Args:
            metric: Key for trial info to order on. Defaults to
                ``self.default_metric``.
            mode: One of [min, max]. Defaults to ``self.default_mode``.
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
        """
        best_trial = self.get_best_trial(metric, mode, scope)
        return best_trial.config if best_trial else None

    def get_last_checkpoint(
        self, trial=None, metric="training_iteration", mode="max"
    ) -> Optional[Checkpoint]:
        """Gets the last checkpoint of the provided trial,
        i.e., with the highest "training_iteration".

        If no trial is specified, it loads the best trial according to the
        provided metric and mode (defaults to max. training iteration).

        Args:
            trial: If None, load the best trial automatically.
            metric: If no trial is specified, use this metric to identify
                the best trial and load the last checkpoint from this trial.
            mode: If no trial is specified, use the metric and this mode
                to identify the best trial and load the last checkpoint from it.

        Returns:
            Path for last checkpoint of trial
        """
        trial = trial or self.get_best_trial(metric, mode)
        return self.get_best_checkpoint(trial, TRAINING_ITERATION, "max")

    def _validate_metric(self, metric: str) -> str:
        if not metric and not self.default_metric:
            raise ValueError(
                "No `metric` has been passed and  `default_metric` has "
                "not been set. Please specify the `metric` parameter."
            )
        return metric or self.default_metric

    def _validate_mode(self, mode: str) -> str:
        if not mode and not self.default_mode:
            raise ValueError(
                "No `mode` has been passed and  `default_mode` has "
                "not been set. Please specify the `mode` parameter."
            )
        if mode and mode not in ["min", "max"]:
            raise ValueError("If set, `mode` has to be one of [min, max]")
        return mode or self.default_mode

    def _retrieve_rows(
        self, metric: Optional[str] = None, mode: Optional[str] = None
    ) -> Dict[str, Any]:
        assert mode is None or mode in ["max", "min"]
        assert not mode or metric
        rows = {}
        for path, df in self.trial_dataframes.items():
            if df.empty:
                continue
            if metric not in df:
                idx = -1
            elif mode == "max":
                idx = df[metric].idxmax()
            elif mode == "min":
                idx = df[metric].idxmin()
            else:
                idx = -1
            try:
                rows[path] = df.iloc[idx].to_dict()
            except TypeError:
                # idx is nan
                logger.warning(
                    "Warning: Non-numerical value(s) encountered for {}".format(path)
                )

        return rows

    def __getstate__(self) -> Dict[str, Any]:
        """Ensure that trials are marked as stubs when pickling,
        so that they can be loaded later without the trainable
        being registered.
        """
        state = self.__dict__.copy()

        def make_stub_if_needed(trial: Trial) -> Trial:
            if trial.stub:
                return trial
            trial_copy = Trial(trial.trainable_name, stub=True)
            trial_copy.__setstate__(trial.__getstate__())
            return trial_copy

        state["trials"] = [make_stub_if_needed(t) for t in state["trials"]]
        return state
