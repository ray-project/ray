from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import json
import logging
import os
import pandas as pd

from ray.tune.error import TuneError
from ray.tune.result import EXPR_PROGRESS_FILE, EXPR_PARAM_FILE
from ray.tune.util import flatten_dict

logger = logging.getLogger(__name__)

UNNEST_KEYS = ("config", "last_result")


def unnest_checkpoints(checkpoints):
    checkpoint_dicts = []
    for g in checkpoints:
        checkpoint = copy.deepcopy(g)
        for key in UNNEST_KEYS:
            if key not in checkpoint:
                continue
            try:
                unnest_dict = flatten_dict(checkpoint.pop(key))
                checkpoint.update(unnest_dict)
            except Exception:
                logger.debug("Failed to flatten dict.")
        checkpoint = flatten_dict(checkpoint)
        checkpoint_dicts.append(checkpoint)
    return checkpoint_dicts


class Analysis(object):
    """Analyze all results from a directory of experiments."""

    def __init__(self, experiment_dir):
        experiment_dir = os.path.expanduser(experiment_dir)
        if not os.path.isdir(experiment_dir):
            raise ValueError(
                "{} is not a valid directory.".format(experiment_dir))
        self._experiment_dir = experiment_dir
        self._configs = {}
        self._trial_dataframes = {}
        self.fetch_trial_dataframes()

    def fetch_trial_dataframes(self):
        fail_count = 0
        for path in self._get_trial_paths():
            try:
                self.trial_dataframes[path] = pd.read_csv(
                    os.path.join(path, EXPR_PROGRESS_FILE))
            except Exception:
                fail_count += 1

        if fail_count:
            logger.debug(
                "Couldn't read results from {} paths".format(fail_count))
        return self.trial_dataframes

    def get_all_configs(self, prefix=False):
        fail_count = 0
        for path in self._get_trial_paths():
            try:
                with open(os.path.join(path, EXPR_PARAM_FILE)) as f:
                    config = json.load(f)
                    if prefix:
                        for k in list(config):
                            config["config:" + k] = config.pop(k)
                    self._configs[path] = config
            except Exception:
                fail_count += 1

        if fail_count:
            logger.warning(
                "Couldn't read config from {} paths".format(fail_count))
        return self._configs

    def dataframe(self, metric=None, mode=None):
        """Returns a pandas.DataFrame object constructed from the trials.

        Args:
            metric (str): Key for trial info to order on.
                If None, uses last result.
            mode (str): One of [min, max].

        """
        rows = self._retrieve_rows(metric=metric, mode=mode)
        all_configs = self.get_all_configs(prefix=True)
        for path, config in all_configs.items():
            if path in rows:
                rows[path].update(config)
                rows[path].update(logdir=path)
        return pd.DataFrame(list(rows.values()))

    def get_best_config(self, metric, mode="max"):
        """Retrieve the best config corresponding to the trial.

        Args:
            metric (str): Key for trial info to order on.
            mode (str): One of [min, max].

        """
        rows = self._retrieve_rows(metric=metric, mode=mode)
        all_configs = self.get_all_configs()
        compare_op = max if mode == "max" else min
        best_path = compare_op(rows, key=lambda k: rows[k][metric])
        return all_configs[best_path]

    def _retrieve_rows(self, metric=None, mode=None):
        assert mode is None or mode in ["max", "min"]
        rows = {}
        for path, df in self.trial_dataframes.items():
            if mode == "max":
                idx = df[metric].idxmax()
            elif mode == "min":
                idx = df[metric].idxmin()
            else:
                idx = -1
            rows[path] = df.iloc[idx].to_dict()

        return rows

    def _get_trial_paths(self):
        _trial_paths = []
        for trial_path, _, files in os.walk(self._experiment_dir):
            if EXPR_PROGRESS_FILE in files:
                _trial_paths += [trial_path]

        if not _trial_paths:
            raise TuneError("No trials found in {}.".format(
                self._experiment_dir))
        return _trial_paths

    def get_best_logdir(self, metric, mode="max"):
        df = self.dataframe()
        if mode == "max":
            return df.iloc[df[metric].idxmax()].logdir
        elif mode == "min":
            return df.iloc[df[metric].idxmin()].logdir

    @property
    def trial_dataframes(self):
        return self._trial_dataframes


class ExperimentAnalysis(Analysis):
    """Analyze results from a Tune experiment.

    Parameters:
        experiment_checkpoint_path (str): Path to a json file
            representing an experiment state. Corresponds to
            Experiment.local_dir/Experiment.name/experiment_state.json

    Example:
        >>> tune.run(my_trainable, name="my_exp", local_dir="~/tune_results")
        >>> analysis = ExperimentAnalysis(
        >>>     experiment_checkpoint_path="~/tune_results/my_exp/state.json")
    """

    def __init__(self, experiment_checkpoint_path, trials=None):
        """Initializer.

        Args:
            experiment_path (str): Path to where experiment is located.
            trials (list|None): List of trials that can be accessed via
                `analysis.trials`.
        """
        with open(experiment_checkpoint_path) as f:
            _experiment_state = json.load(f)

        if "checkpoints" not in _experiment_state:
            raise TuneError("Experiment state invalid; no checkpoints found.")
        self._checkpoints = _experiment_state["checkpoints"]
        self.trials = trials
        super(ExperimentAnalysis, self).__init__(
            os.path.dirname(experiment_checkpoint_path))

    def stats(self):
        """Returns a dictionary of the statistics of the experiment."""
        return self._experiment_state.get("stats")

    def runner_data(self):
        """Returns a dictionary of the TrialRunner data."""
        return self._experiment_state.get("runner_data")

    def _get_trial_paths(self):
        """Overwrites Analysis to only have trials of one experiment."""
        _trial_paths = [
            checkpoint["logdir"] for checkpoint in self._checkpoints
        ]
        if not _trial_paths:
            raise TuneError("No trials found.")
        return _trial_paths
