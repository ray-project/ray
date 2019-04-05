from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import glob
import pandas as pd

from ray.tune.util import flatten_dict

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
        self._experiment_dir, self._experiment_trials, self._experiment_state = parse_exp(experiment_path)
        self._checkpoints = self._experiment_state["checkpoints"]

    def dataframe(self):
        checkpoint_dicts = self._checkpoints
        checkpoint_dicts = [flatten_dict(g) for g in checkpoint_dicts]
        return pd.DataFrame(checkpoint_dicts)

    def checkpoints(self): # this is the data that tune.run returns (as list of Trial)
        return self._checkpoints

    def stats(self):
        return self._experiment_state.get("stats")
    
    def runner_data(self):
        return self._experiment_state.get("runner_data")

    def trial_dataframe(self, trial_id):
        """Returns a dataframe for one trial."""
        checkpoint_dicts = self._experiment_state["checkpoints"]
        trial_dict = filter(lambda d: d["trial_id"] == trial_id, checkpoint_dicts)
        return pd.DataFrame(trial_dict)

    def get_best_trainable(self, metric):
        # get best trainable if not a function
        # return Trainable(restore=trial.checkpoint)
        return Trainable(config=get_best_config(metric))

    def get_best_config(self, metric):
        return self.get_best_trial()["config"]
    
    def get_best_trial(trial_list, metric):
        """Retrieve the best trial."""
        return max(self._checkpoints, key=lambda trial: trial.last_result.get(metric, 0))
