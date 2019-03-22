from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os

from ray.tune.util import flatten_dict

logger = logging.getLogger(__name__)


def get_experiment_state(experiment_path):
    experiment_path = os.path.expanduser(experiment_path)
    experiment_state_paths = glob.glob(
        os.path.join(experiment_path, "experiment_state*.json"))
    if not experiment_state_paths:
        return
    experiment_filename = max(list(experiment_state_paths))

    with open(experiment_filename) as f:
        experiment_state = json.load(f)
    return experiment_state


class ExperimentAnalysis():
    def __init__(self, experiment_directory):
        self._dir = experiment_directory
        # TODO: If experiment_state doesn't exist, should still parse
        # internal folders
        self._experiment_state = get_experiment_state(experiment_path)
        # Have some internal notion of trial

    def dataframe(self):
        checkpoint_dicts = self._experiment_state["checkpoints"]
        checkpoint_dicts = [flatten_dict(g) for g in checkpoint_dicts]
        return pd.DataFrame(checkpoint_dicts)

    def stats(self):
        return self._experiment_state.get("stats")

    def trial_dataframe(self, trial_id):
        """Returns a dataframe for one trial."""
        pass

    def get_best_trainable(self, metric):
        # get best trainable if not a function
        # return Trainable(restore=trial.checkpoint)
        pass

    def get_best_config(self, metric):
        return

    def get_best_result(self, metric):
        """Retrieve the last result from the best trial."""
        return {metric: get_best_trial(trial_list, metric).last_result[metric]}
