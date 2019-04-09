from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import glob
import pandas as pd

from ray.tune.util import flatten_dict
from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)

def parse_exp(experiment_path):
    experiment_path = os.path.expanduser(experiment_path)
    experiment_dir, experiment_trials, experiment_state_paths = next(os.walk(experiment_path))
    experiment_filename = max(list(experiment_state_paths)) # if more than one, pick latest
    with open(os.path.join(experiment_path, experiment_filename)) as f:
        experiment_state = json.load(f)
    return experiment_dir, experiment_trials, experiment_state 


class ExperimentAnalysis():
    def __init__(self, experiment_path):
        # TODO(Adi): Raise ValueError if experiment_path is not "ray_results"
        self._experiment_dir, self._experiment_trials, self._experiment_state = parse_exp(experiment_path)
        self._checkpoints = self._experiment_state["checkpoints"]

    def dataframe(self):
        flattened_checkpoints = [flatten_dict(c) for c in self._checkpoints] 
        return pd.DataFrame(flattened_checkpoints)

    def checkpoints(self): # this is the data that tune.run returns (as list of Trial)
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
        return max(self._checkpoints, key=lambda d: d['last_result'].get(metric, 0))
