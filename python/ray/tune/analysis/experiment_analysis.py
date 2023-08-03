import json
import logging
import os
import tempfile
import traceback
from typing import Any, Dict, List, Optional, Tuple, Union
from numbers import Number
from pathlib import Path

from ray.air._internal.remote_storage import (
    download_from_uri,
    is_directory,
    is_local_path,
    list_at_uri,
)
from ray.air._internal.uri_utils import _join_path_or_uri, URI
from ray.air.checkpoint import Checkpoint
from ray.air.constants import (
    EXPR_PROGRESS_FILE,
    EXPR_RESULT_FILE,
    EXPR_PARAM_FILE,
    TRAINING_ITERATION,
)
from ray.tune.syncer import SyncConfig
from ray.tune.utils import flatten_dict
from ray.tune.utils.serialization import TuneFunctionDecoder
from ray.tune.utils.util import is_nan_or_inf, is_nan
from ray.util import log_once

try:
    import pandas as pd
    from pandas import DataFrame
except ImportError:
    pd = None
    DataFrame = None

from ray.tune.error import TuneError
from ray.tune.result import (
    DEFAULT_METRIC,
    CONFIG_PREFIX,
)
from ray.tune.experiment import Trial
from ray.tune.execution.experiment_state import _find_newest_experiment_checkpoint
from ray.tune.trainable.util import TrainableUtil
from ray.tune.utils.util import unflattened_lookup

from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

DEFAULT_FILE_TYPE = "csv"


@PublicAPI(stability="beta")
class ExperimentAnalysis:
    """Analyze results from a Tune experiment.

    To use this class, the experiment must be executed with the JsonLogger.

    Parameters:
        experiment_checkpoint_path: Path to a json file or directory
            representing an experiment state, or a directory containing
            multiple experiment states (a run's ``local_dir``).
            Corresponds to Experiment.local_dir/Experiment.name/
            experiment_state.json
        trials: List of trials that can be accessed via
            `analysis.trials`.
        default_metric: Default metric for comparing results. Can be
            overwritten with the ``metric`` parameter in the respective
            functions.
        default_mode: Default mode for comparing results. Has to be one
            of [min, max]. Can be overwritten with the ``mode`` parameter
            in the respective functions.

    Example:
        >>> from ray import tune
        >>> tune.run( # doctest: +SKIP
        ...     my_trainable, name="my_exp", local_dir="~/tune_results")
        >>> analysis = ExperimentAnalysis( # doctest: +SKIP
        ...     experiment_checkpoint_path="~/tune_results/my_exp/state.json")
    """

    def __init__(
        self,
        experiment_checkpoint_path: str,
        trials: Optional[List[Trial]] = None,
        default_metric: Optional[str] = None,
        default_mode: Optional[str] = None,
        remote_storage_path: Optional[str] = None,
        # Deprecate: Remove in 2.7
        sync_config: Optional[SyncConfig] = None,
    ):
        self._local_experiment_path: str = None
        self._remote_experiment_path: Optional[str] = None

        # If the user passes in a remote checkpoint path,
        # Set the remote experiment path to this path, and set
        # the local experiment path to a temp directory.
        if not is_local_path(experiment_checkpoint_path):
            self._remote_experiment_path = experiment_checkpoint_path

            # Create a temp directory to store downloaded checkpoint files if
            # they are pulled from a remote `experiment_checkpoint_path`.
            self._local_experiment_path = tempfile.TemporaryDirectory(
                prefix="experiment_analysis_"
            ).name
            os.makedirs(self._local_experiment_path, exist_ok=True)

        # Load the experiment checkpoints and their parent paths.
        # This is important for when experiment folders have been
        # relocated (e.g. from a ray cluster to local disk or GCS/S3)-
        self._experiment_states = []
        self._checkpoints_and_paths: List[Tuple[dict, os.PathLike]] = []
        self._load_checkpoints(experiment_checkpoint_path)
        assert self._checkpoints_and_paths

        self.trials = trials

        self._configs = {}
        self._trial_dataframes = {}

        self.default_metric = default_metric
        if default_mode and default_mode not in ["min", "max"]:
            raise ValueError("`default_mode` has to be None or one of [min, max]")
        self.default_mode = default_mode
        self._file_type = self._validate_filetype(None)

        if self.default_metric is None and self.default_mode:
            # If only a mode was passed, use anonymous metric
            self.default_metric = DEFAULT_METRIC

        # TODO(ml-team): Remove in 2.7 along with sync_config parameter
        if sync_config:
            raise DeprecationWarning(
                "Using `sync_config` to specify an `upload_dir` for initializing an "
                "`ExperimentAnalysis` is deprecated. Remove this parameter and specify "
                "`remote_storage_path` instead."
            )

        if not self._local_experiment_path:
            self._local_experiment_path = str(self._checkpoints_and_paths[0][1])

        if not self._remote_experiment_path and remote_storage_path:
            self._remote_experiment_path = str(
                URI(remote_storage_path) / Path(self._local_experiment_path).name
            )

        if not pd:
            logger.warning(
                "pandas not installed. Run `pip install pandas` for "
                "ExperimentAnalysis utilities."
            )
        else:
            self.fetch_trial_dataframes()

    @property
    def _local_path(self) -> str:
        return self._local_experiment_path

    @property
    def _remote_path(self) -> str:
        return self._remote_experiment_path

    @property
    def experiment_path(self) -> str:
        """Path pointing to the experiment directory on persistent storage.

        This can point to a remote storage location (e.g. S3) or to a local
        location (path on the head node).

        For instance, if your remote storage path is ``s3://bucket/location``,
        this will point to ``s3://bucket/location/experiment_name``.
        """
        return self._remote_path or self._local_path

    def _convert_local_to_cloud_path(self, local_path: str):
        """Convert local path into cloud storage path.

        Example:
        local_path = "/a/b/c.json"
        self._remote_experiment_path = "s3://bucket?param=abcd"
        self._local_experiment_path = "/a/b"

        -> "s3://bucket/c?param=abcd"
        """
        if not self._remote_experiment_path:
            return None

        rel_path = str(Path(local_path).relative_to(self._local_experiment_path))
        return str(URI(self._remote_experiment_path) / rel_path)

    def _load_checkpoints(self, experiment_checkpoint_path: str) -> List[str]:
        # Get the latest checkpoints from the checkpoint_path.
        latest_checkpoints = self._get_latest_checkpoint(experiment_checkpoint_path)
        if not latest_checkpoints:
            raise ValueError(
                f"`{experiment_checkpoint_path}` must either be a path to an "
                "experiment checkpoint file, or a directory containing an experiment "
                "checkpoint file."
            )
        # Collect all checkpoints and their directory paths.
        # These are used to infer the `local_dir` from the checkpoints
        # in case the experiment folder had been moved from its original
        # location (e.g. from a ray cluster to a GCS/S3 bucket or to local disk).
        self._load_checkpoints_from_latest(latest_checkpoints)

    def _load_checkpoints_from_latest(self, latest_checkpoint: List[str]) -> None:
        # Collect all checkpoints and their directory paths.
        for path in latest_checkpoint:
            with open(path) as f:
                experiment_state = json.load(f, cls=TuneFunctionDecoder)
                self._experiment_states.append(experiment_state)

            if "checkpoints" not in experiment_state:
                raise TuneError("Experiment state invalid; no checkpoints found.")

            self._checkpoints_and_paths += [
                (cp, Path(path).parent) for cp in experiment_state["checkpoints"]
            ]

    def _maybe_download_experiment_checkpoint(
        self, experiment_checkpoint_path: str
    ) -> Optional[str]:
        """Downloads the experiment checkpoint from a remote path if needed.

        Args:
            experiment_checkpoint_path: The local or remote path to the experiment
                checkpoint file.

        Returns:
            str: The local copy of the experiment checkpoint.
                If a local path is passed in, this method will return that immediately.
                If a remote path is passed in, this will try to download that file.
                Will return None if the download failed.
        """
        if is_local_path(experiment_checkpoint_path):
            return os.path.expanduser(experiment_checkpoint_path)

        assert self._local_path and self._remote_path

        experiment_path = Path(URI(self._remote_path).path)
        # s3://bucket/exp_dir/nested/experiment_state.json
        #   -> bucket/exp_dir/nested/experiment_state.json
        checkpoint_path = Path(URI(experiment_checkpoint_path).path)

        assert experiment_path in checkpoint_path.parents
        #   -> nested/experiment_state.json
        relative_path = checkpoint_path.relative_to(experiment_path)

        # Download to:
        #   -> {self._local_path}/nested/experiment_state.json
        local_path = os.path.join(self._local_path, relative_path)
        try:
            download_from_uri(experiment_checkpoint_path, local_path)
        except FileNotFoundError:
            return None

        return local_path

    def _get_latest_checkpoint_from_dir(
        self, experiment_checkpoint_path: str, top_level: bool = True
    ) -> List[str]:
        """Gets the latest experiment checkpoints from a given directory.

        Args:
            experiment_checkpoint_path: A local or remote path to a directory
                containing at least one experiment checkpoint file.
            top_level: True if this is the first directory level. False if
                we are searching in a subdirectory. (Max recursion depth of 1.)

        Returns:
            list: A list of local paths pointing to the latest experiment checkpoint
            file for each experiment found within the given directory.
        """
        latest_checkpoint = _find_newest_experiment_checkpoint(
            experiment_checkpoint_path
        )

        latest_checkpoints = []
        if latest_checkpoint:
            assert not is_directory(
                latest_checkpoint
            ), "This should point to an actual experiment checkpoint file."
            latest_checkpoints.extend(self._get_latest_checkpoint(latest_checkpoint))

        if not latest_checkpoint and top_level:
            # If no checkpoint in this folder the sub-directory is searched.
            # In this case also multiple experiment folders could exist in
            # the same root. In this case the length of `latest_checkpoint`
            # will be greater than 1.
            for subdir in list_at_uri(experiment_checkpoint_path):
                full_path = _join_path_or_uri(experiment_checkpoint_path, subdir)
                if is_directory(full_path):
                    latest_checkpoints.extend(
                        self._get_latest_checkpoint_from_dir(full_path, top_level=False)
                    )

        return latest_checkpoints

    def _get_latest_checkpoint(self, experiment_checkpoint_path: str) -> List[str]:
        """Gets the latest experiment checkpoints corresponding to a given path.

        Acceptable path inputs (either local or remote):
        - A path to an experiment checkpoint file.
        - A path to an experiment directory, which contains an experiment checkpoint
          file at the directory's top-level.
        - A path to a directory that contains multiple experiment directories,
          where each subdirectory contains an experiment checkpoint file.

        Returns:
            list: A list of local paths pointing to the latest experiment checkpoint
            file for each experiment corresponding to the given path.
        """
        if is_directory(experiment_checkpoint_path):
            return self._get_latest_checkpoint_from_dir(experiment_checkpoint_path)

        local_experiment_checkpoint_path = self._maybe_download_experiment_checkpoint(
            experiment_checkpoint_path
        )

        if (
            not local_experiment_checkpoint_path
            or not Path(local_experiment_checkpoint_path).exists()
        ):
            raise ValueError(
                f"The file `{experiment_checkpoint_path}` does not "
                f"exist and cannot be loaded for experiment analysis."
            )

        assert Path(local_experiment_checkpoint_path).is_file()

        return [local_experiment_checkpoint_path]

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
            :class:`Checkpoint <ray.train.Checkpoint>` object.
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
    def best_logdir(self) -> str:
        """Get the logdir of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_logdir(metric, mode)` instead.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_logdir`, pass a `metric` and `mode` "
                "parameter to `tune.run()`. Alternatively, use the "
                "`get_best_logdir(metric, mode, scope)` method to set the "
                "metric and mode explicitly."
            )
        return self.get_best_logdir(self.default_metric, self.default_mode)

    @property
    def best_dataframe(self) -> DataFrame:
        """Get the full result dataframe of the best trial of the experiment

        The best trial is determined by comparing the last trial results
        using the `metric` and `mode` parameters passed to `tune.run()`.

        If you didn't pass these parameters, use
        `get_best_logdir(metric, mode)` and use it to look for the dataframe
        in the `self.trial_dataframes` dict.
        """
        if not self.default_metric or not self.default_mode:
            raise ValueError(
                "To fetch the `best_result`, pass a `metric` and `mode` "
                "parameter to `tune.run()`."
            )
        best_logdir = self.best_logdir
        return self.trial_dataframes[best_logdir]

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

    def get_trial_checkpoints_paths(
        self, trial: Trial, metric: Optional[str] = None
    ) -> List[Tuple[str, Number]]:
        """Gets paths and metrics of all persistent checkpoints of a trial.

        Args:
            trial: The log directory of a trial, or a trial instance.
            metric: key for trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.

        Returns:
            List of [path, metric] for all persistent checkpoints of the trial.
        """
        metric = metric or self.default_metric or TRAINING_ITERATION

        if isinstance(trial, str):
            trial_dir = os.path.expanduser(trial)
            # Get checkpoints from logdir.
            chkpt_df = TrainableUtil.get_checkpoints_paths(trial_dir)

            # Join with trial dataframe to get metrics.
            trial_df = self.trial_dataframes[trial_dir]
            path_metric_df = chkpt_df.merge(
                trial_df, on="training_iteration", how="inner"
            )
            return path_metric_df[["chkpt_path", metric]].values.tolist()
        elif isinstance(trial, Trial):
            checkpoints = trial.get_trial_checkpoints()
            # Support metrics given as paths, e.g.
            # "info/learner/default_policy/policy_loss".
            return [
                (c.dir_or_data, unflattened_lookup(metric, c.metrics))
                for c in checkpoints
            ]
        else:
            raise ValueError("trial should be a string or a Trial instance.")

    def get_best_checkpoint(
        self,
        trial: Trial,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        return_path: bool = False,
    ) -> Optional[Union[Checkpoint, str]]:
        """Gets best persistent checkpoint path of provided trial.

        Any checkpoints with an associated metric value of ``nan`` will be filtered out.

        Args:
            trial: The log directory of a trial, or a trial instance.
            metric: key of trial info to return, e.g. "mean_accuracy".
                "training_iteration" is used by default if no value was
                passed to ``self.default_metric``.
            mode: One of [min, max]. Defaults to ``self.default_mode``.
            return_path: If True, only returns the path (and not the
                ``Checkpoint`` object). If using Ray client, it is not
                guaranteed that this path is available on the local
                (client) node. Can also contain a cloud URI.

        Returns:
            :class:`Checkpoint <ray.train.Checkpoint>` object or string
            if ``return_path=True``.
        """
        metric = metric or self.default_metric or TRAINING_ITERATION
        mode = self._validate_mode(mode)

        checkpoint_paths = self.get_trial_checkpoints_paths(trial, metric)

        # Filter out nan. Sorting nan values leads to undefined behavior.
        checkpoint_paths = [
            (path, metric) for path, metric in checkpoint_paths if not is_nan(metric)
        ]

        if not checkpoint_paths:
            logger.error(f"No checkpoints have been found for trial {trial}.")
            return None

        a = -1 if mode == "max" else 1
        best_path_metrics = sorted(checkpoint_paths, key=lambda x: a * x[1])

        best_path, best_metric = best_path_metrics[0]
        cloud_path = self._convert_local_to_cloud_path(best_path)

        if cloud_path:
            # Prefer cloud path over local path for downsteam processing
            if return_path:
                return cloud_path
            return Checkpoint.from_uri(cloud_path)
        elif os.path.exists(best_path):
            if return_path:
                return best_path
            return Checkpoint.from_directory(best_path)
        else:
            if log_once("checkpoint_not_available"):
                logger.error(
                    f"The requested checkpoint for trial {trial} is not available on "
                    f"this node, most likely because you are using Ray client or "
                    f"disabled checkpoint synchronization. To avoid this, enable "
                    f"checkpoint synchronization to cloud storage by specifying a "
                    f"`SyncConfig`. The checkpoint may be available on a different "
                    f"node - please check this location on worker nodes: {best_path}"
                )
            if return_path:
                return best_path
            return None

    def get_all_configs(self, prefix: bool = False) -> Dict[str, Dict]:
        """Returns a list of all configurations.

        Args:
            prefix: If True, flattens the config dict
                and prepends `config/`.

        Returns:
            Dict[str, Dict]: Dict of all configurations of trials, indexed by
                their trial dir.
        """
        fail_count = 0
        failed_paths = []
        for path in self._get_trial_paths():
            try:
                param_file = os.path.join(path, EXPR_PARAM_FILE)
                if not os.path.exists(param_file) and self._remote_path:
                    download_from_uri(
                        self._convert_local_to_cloud_path(param_file), param_file
                    )

                with open(os.path.join(path, EXPR_PARAM_FILE)) as f:
                    config = json.load(f)
                if prefix:
                    self._configs[path] = flatten_dict({CONFIG_PREFIX: config})
                else:
                    self._configs[path] = config
            except Exception:
                logger.debug(
                    f"Exception occurred when loading trial configs. "
                    f"See traceback:\n{traceback.format_exc()}"
                )
                fail_count += 1
                failed_paths.append(path)

        if fail_count:
            failed_paths_str = "\n".join([f"- {path}" for path in failed_paths])
            logger.warning(
                f"Failed to read the config for {fail_count} trials:\n"
                f"{failed_paths_str}"
            )

        return self._configs

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

    def get_best_logdir(
        self,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        scope: str = "last",
    ) -> Optional[str]:
        """Retrieve the logdir corresponding to the best trial.

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
        return best_trial.local_path if best_trial else None

    def get_last_checkpoint(self, trial=None, metric="training_iteration", mode="max"):
        """Gets the last persistent checkpoint path of the provided trial,
        i.e., with the highest "training_iteration".

        If no trial is specified, it loads the best trial according to the
        provided metric and mode (defaults to max. training iteration).

        Args:
            trial: The log directory or an instance of a trial.
                If None, load the latest trial automatically.
            metric: If no trial is specified, use this metric to identify
                the best trial and load the last checkpoint from this trial.
            mode: If no trial is specified, use the metric and this mode
                to identify the best trial and load the last checkpoint from it.

        Returns:
            Path for last checkpoint of trial
        """
        if trial is None:
            trial = self.get_best_logdir(metric, mode)

        return self.get_best_checkpoint(trial, "training_iteration", "max")

    def fetch_trial_dataframes(self) -> Dict[str, DataFrame]:
        """Fetches trial dataframes from files.

        Returns:
            A dictionary containing "trial dir" to Dataframe.
        """
        fail_count = 0
        failed_paths = []
        force_dtype = {"trial_id": str}  # Never convert trial_id to float.
        for path in self._get_trial_paths():
            try:
                if self._file_type == "json":
                    json_file = os.path.join(path, EXPR_RESULT_FILE)
                    if not os.path.exists(json_file) and self._remote_path:
                        download_from_uri(
                            self._convert_local_to_cloud_path(json_file), json_file
                        )

                    with open(json_file, "r") as f:
                        json_list = [json.loads(line) for line in f if line]
                    df = pd.json_normalize(json_list, sep="/")
                elif self._file_type == "csv":
                    csv_file = os.path.join(path, EXPR_PROGRESS_FILE)
                    if not os.path.exists(csv_file) and self._remote_path:
                        download_from_uri(
                            self._convert_local_to_cloud_path(csv_file), csv_file
                        )

                    df = pd.read_csv(csv_file, dtype=force_dtype)
                self.trial_dataframes[path] = df
            except Exception:
                logger.debug(
                    f"Exception occurred when loading trial results. See traceback:\n"
                    f"{traceback.format_exc()}"
                )
                fail_count += 1
                failed_paths.append(path)

        if fail_count:
            failed_paths_str = "\n".join([f"- {path}" for path in failed_paths])
            logger.warning(
                f"Failed to read the results for {fail_count} trials:\n"
                f"{failed_paths_str}"
            )

        return self.trial_dataframes

    def stats(self) -> Dict:
        """Returns a dictionary of the statistics of the experiment.

        If ``experiment_checkpoint_path`` pointed to a directory of
        experiments, the dict will be in the format of
        ``{experiment_session_id: stats}``."""
        if len(self._experiment_states) == 1:
            return self._experiment_states[0]["stats"]
        else:
            return {
                experiment_state["runner_data"]["_session_str"]: experiment_state[
                    "stats"
                ]
                for experiment_state in self._experiment_states
            }

    def set_filetype(self, file_type: Optional[str] = None):
        """Overrides the existing file type.

        Args:
            file_type: Read results from json or csv files. Has to be one
                of [None, json, csv]. Defaults to csv.
        """
        self._file_type = self._validate_filetype(file_type)
        self.fetch_trial_dataframes()
        return True

    def runner_data(self) -> Dict:
        """Returns a dictionary of the TuneController data.

        If ``experiment_checkpoint_path`` pointed to a directory of
        experiments, the dict will be in the format of
        ``{experiment_session_id: TuneController_data}``."""
        if len(self._experiment_states) == 1:
            return self._experiment_states[0]["runner_data"]
        else:
            return {
                experiment_state["runner_data"]["_session_str"]: experiment_state[
                    "runner_data"
                ]
                for experiment_state in self._experiment_states
            }

    def _get_trial_paths(self) -> List[str]:
        if self.trials:
            # We do not need to set the relative path here
            # Maybe assert that t.local_path is in local_base_path?
            _trial_paths = [str(t.local_path) for t in self.trials]
        else:
            logger.info(
                "No trial data passed in during `ExperimentAnalysis` initialization -- "
                "you are most likely loading the experiment after it has completed.\n"
                "Loading trial data from the experiment checkpoint file. "
                "This may result in loading some stale information, "
                "since checkpointing is periodic."
            )
            self.trials = []
            for trial_json_state, path in self._checkpoints_and_paths:
                try:
                    trial = Trial.from_json_state(trial_json_state, stub=True)
                    trial.local_experiment_path = str(path)
                except Exception:
                    logger.warning(
                        f"Could not load trials from experiment checkpoint. "
                        f"This means your experiment checkpoint is likely "
                        f"faulty or incomplete, and you won't have access "
                        f"to all analysis methods. "
                        f"Observed error:\n{traceback.format_exc()}"
                    )
                    continue
                self.trials.append(trial)

            self.trials.sort(key=lambda trial: trial.trial_id)
            _trial_paths = [str(trial.local_path) for trial in self.trials]

        if not _trial_paths:
            raise TuneError("No trials found.")
        return _trial_paths

    def _validate_filetype(self, file_type: Optional[str] = None):
        if file_type not in {None, "json", "csv"}:
            raise ValueError("`file_type` has to be None or one of [json, csv].")
        return file_type or DEFAULT_FILE_TYPE

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
            if mode == "max":
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
