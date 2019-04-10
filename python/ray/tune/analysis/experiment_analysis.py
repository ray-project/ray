from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import pandas as pd

from ray.tune.util import flatten_dict
from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)


class ExperimentAnalysis():
    def __init__(self, experiment_path):
        experiment_path = os.path.expanduser(experiment_path)
        if not os.path.isdir(experiment_path):
            raise ValueError(
                "{} is not a valid directory.".format(experiment_path))
        (self._experiment_dir, self._experiment_trials,
         experiment_state_paths) = next(os.walk(experiment_path))
        if len(self._experiment_trials) == 0:
            raise ValueError("This is not a valid directory"
                             "because it has zero trial directories.")
        if len(experiment_state_paths) == 0:
            raise ValueError("This is not a valid directory"
                             "because it has no experiment state JSON file.")
        experiment_filename = max(
            list(experiment_state_paths))  # if more than one, pick latest
        with open(os.path.join(experiment_path, experiment_filename)) as f:
            self._experiment_state = json.load(f)

        if "checkpoints" not in self._experiment_state:
            raise ValueError("The experiment state JSON file is not valid"
                             "because it must have checkpoints.")
        self._checkpoints = self._experiment_state["checkpoints"]

    def dataframe(self):
        flattened_checkpoints = [flatten_dict(c) for c in self._checkpoints]
        return pd.DataFrame(flattened_checkpoints)

    def checkpoints(
            self):  # this is the data that tune.run returns (as list of Trial)
        return self._checkpoints

    def stats(self):
        return self._experiment_state.get("stats")

    def runner_data(self):
        return self._experiment_state.get("runner_data")

    def trial_dataframe(self, trial_id):
        """Returns a dataframe for one trial."""
        df = self.dataframe()
        return df.loc[df['trial_id'] == trial_id]

    def get_best_trainable(self, metric):
        return Trainable(config=self.get_best_config(metric))

    def get_best_config(self, metric):
        return self.get_best_trial(metric)["config"]

    def get_best_trial(self, metric):
        """Retrieve the best trial."""
        return max(
            self._checkpoints, key=lambda d: d['last_result'].get(metric, 0))
