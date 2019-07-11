from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import glob
import json
import logging
import os
import pandas as pd

from ray.tune.error import TuneError
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


class ExperimentAnalysis(object):
    """Analyze results from a Tune experiment.

    Parameters:
        experiment_path (str): Path to where experiment is located.
            Corresponds to Experiment.local_dir/Experiment.name

    Example:
        >>> tune.run(my_trainable, name="my_exp", local_dir="~/tune_results")
        >>> analysis = ExperimentAnalysis(
        >>>     experiment_path="~/tune_results/my_exp")
    """

    def __init__(self, experiment_path, trials=None):
        """Initializer.

        Args:
            experiment_path (str): Path to where experiment is located.
            trials (list|None): List of trials that can be accessed via
                `analysis.trials`.
        """
        experiment_path = os.path.expanduser(experiment_path)
        if not os.path.isdir(experiment_path):
            raise TuneError(
                "{} is not a valid directory.".format(experiment_path))
        experiment_state_paths = glob.glob(
            os.path.join(experiment_path, "experiment_state*.json"))
        if not experiment_state_paths:
            raise TuneError(
                "No experiment state found in {}!".format(experiment_path))
        experiment_filename = max(
            list(experiment_state_paths))  # if more than one, pick latest
        with open(experiment_filename) as f:
            self._experiment_state = json.load(f)

        if "checkpoints" not in self._experiment_state:
            raise TuneError("Experiment state invalid; no checkpoints found.")
        self._checkpoints = self._experiment_state["checkpoints"]
        self._scrubbed_checkpoints = unnest_checkpoints(self._checkpoints)
        self.trials = trials
        self._dataframe = None

    def get_all_trial_dataframes(self):
        trial_dfs = {}
        for checkpoint in self._checkpoints:
            logdir = checkpoint["logdir"]
            progress = max(glob.glob(os.path.join(logdir, "progress.csv")))
            trial_dfs[checkpoint["trial_id"]] = pd.read_csv(progress)
        return trial_dfs

    def dataframe(self, refresh=False):
        """Returns a pandas.DataFrame object constructed from the trials.

        Args:
            refresh (bool): Clears the cache which may have an existing copy.

        """
        if self._dataframe is None or refresh:
            self._dataframe = pd.DataFrame(self._scrubbed_checkpoints)
        return self._dataframe

    def stats(self):
        """Returns a dictionary of the statistics of the experiment."""
        return self._experiment_state.get("stats")

    def runner_data(self):
        """Returns a dictionary of the TrialRunner data."""
        return self._experiment_state.get("runner_data")

    def trial_dataframe(self, trial_id):
        """Returns a pandas.DataFrame constructed from one trial."""
        for checkpoint in self._checkpoints:
            if checkpoint["trial_id"] == trial_id:
                logdir = checkpoint["logdir"]
                progress = max(glob.glob(os.path.join(logdir, "progress.csv")))
                return pd.read_csv(progress)
        raise ValueError("Trial id {} not found".format(trial_id))

    def get_best_trainable(self, metric, trainable_cls, mode="max"):
        """Returns the best Trainable based on the experiment metric.

        Args:
            metric (str): Key for trial info to order on.
            mode (str): One of [min, max].

        """
        return trainable_cls(config=self.get_best_config(metric, mode=mode))

    def get_best_config(self, metric, mode="max"):
        """Retrieve the best config from the best trial.

        Args:
            metric (str): Key for trial info to order on.
            mode (str): One of [min, max].

        """
        return self.get_best_info(metric, flatten=False, mode=mode)["config"]

    def get_best_logdir(self, metric, mode="max"):
        df = self.dataframe()
        if mode == "max":
            return df.iloc[df[metric].idxmax()].logdir
        elif mode == "min":
            return df.iloc[df[metric].idxmin()].logdir

    def get_best_info(self, metric, mode="max", flatten=True):
        """Retrieve the best trial based on the experiment metric.

        Args:
            metric (str): Key for trial info to order on.
            mode (str): One of [min, max].
            flatten (bool): Assumes trial info is flattened, where
                nested entries are concatenated like `info:metric`.
        """
        optimize_op = max if mode == "max" else min
        if flatten:
            return optimize_op(
                self._scrubbed_checkpoints, key=lambda d: d.get(metric, 0))
        return optimize_op(
            self._checkpoints, key=lambda d: d["last_result"].get(metric, 0))
