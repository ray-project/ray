from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import json
import logging
import os
import pandas as pd

from ray.tune.error import TuneError
from ray.tune.util import flatten_dict
from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)

UNNEST_KEYS = ("config", "last_result")


def unnest_checkpoints(checkpoints):
    checkpoint_dicts = []
    for g in checkpoints:
        for key in UNNEST_KEYS:
            if key not in g:
                continue
            unnest_dict = flatten_dict(g.pop(key))
            g.update(unnest_dict)
        g = flatten_dict(g)
        checkpoint_dicts.append(g)
    return checkpoint_dicts


class ExperimentAnalysis():
    """Analyze results from a Tune experiment.

    Parameters:
        experiment_path (str): Path to where experiment is located.
            Corresponds to Experiment.local_dir/Experiment.name

    Example:
        >>> tune.run(my_trainable, name="my_exp", local_dir="~/tune_results")
        >>> analysis = ExperimentAnalysis(
        >>>     experiment_path="~/tune_results/my_exp")
    """

    def __init__(self, experiment_path):
        experiment_path = os.path.expanduser(experiment_path)
        if not os.path.isdir(experiment_path):
            raise TuneError(
                "{} is not a valid directory.".format(experiment_path))
        experiment_state_paths = glob.glob(
            os.path.join(experiment_path, "experiment_state*.json"))
        if not experiment_state_paths:
            raise TuneError("No experiment state found!")
        experiment_filename = max(
            list(experiment_state_paths))  # if more than one, pick latest
        with open(os.path.join(experiment_path, experiment_filename)) as f:
            self._experiment_state = json.load(f)

        if "checkpoints" not in self._experiment_state:
            raise TuneError("Experiment state invalid; no checkpoints found.")
        self._checkpoints = unnest_checkpoints(
            self._experiment_state["checkpoints"])

    def dataframe(self):
        """Returns a pandas.DataFrame object constructed from the trials."""
        return pd.DataFrame(self._checkpoints)

    def stats(self):
        """Returns a dictionary of the statistics of the experiment."""
        return self._experiment_state.get("stats")

    def runner_data(self):
        """Returns a dictionary of the TrialRunner data."""
        return self._experiment_state.get("runner_data")

    def trial_dataframe(self, trial_id):
        """Returns a pandas.DataFrame constructed from one trial.

        TODO:
            This should read the entire progress.csv of the trial.
        """
        df = self.dataframe()
        return df.loc[df["trial_id"] == trial_id]

    def get_best_trainable(self, metric):
        """Returns the best Trainable based on the experiment metric."""
        return Trainable(config=self.get_best_config(metric))

    def get_best_config(self, metric):
        """Retrieve the best config from the best trial."""
        return self._get_best_trial(metric)["config"]

    def _get_best_trial(self, metric):
        """Retrieve the best trial based on the experiment metric."""
        return max(
            self._checkpoints, key=lambda d: d["last_result"].get(metric, 0))

    def _get_sorted_trials(self, metric):
        """Retrive trials in sorted order based on the experiment metric."""
        return sorted(
            self._checkpoints,
            key=lambda d: d["last_result"].get(metric, 0),
            reverse=True)
