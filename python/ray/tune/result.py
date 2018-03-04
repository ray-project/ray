from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import os


"""
When using ray.tune with custom training scripts, you must periodically report
training status back to Ray by calling reporter(result).

Most of the fields are optional, the only required one is timesteps_total.

In RLlib, the supplied algorithms fill in TrainingResult for you.
"""

# Where ray.tune writes result files by default
DEFAULT_RESULTS_DIR = os.path.expanduser("~/ray_results")


TrainingResult = namedtuple("TrainingResult", [
    # (Required) Accumulated timesteps for this entire experiment.
    "timesteps_total",

    # (Optional) If training is terminated.
    "done",

    # (Optional) Custom metadata to report for this iteration.
    "info",

    # (Optional) The mean episode reward if applicable.
    "episode_reward_mean",

    # (Optional) The mean episode length if applicable.
    "episode_len_mean",

    # (Optional) The number of episodes total.
    "episodes_total",

    # (Optional) The current training accuracy if applicable.
    "mean_accuracy",

    # (Optional) The current validation accuracy if applicable.
    "mean_validation_accuracy",

    # (Optional) The current training loss if applicable.
    "mean_loss",

    # (Auto-filled) The negated current training loss.
    "neg_mean_loss",

    # (Auto-filled) Unique string identifier for this experiment. This id is
    # preserved across checkpoint / restore calls.
    "experiment_id",

    # (Auto-filled) The index of this training iteration, e.g. call to train().
    "training_iteration",

    # (Auto-filled) Number of timesteps in the simulator in this iteration.
    "timesteps_this_iter",

    # (Auto-filled) Time in seconds this iteration took to run. This may be
    # overriden in order to override the system-computed time difference.
    "time_this_iter_s",

    # (Auto-filled) Accumulated time in seconds for this entire experiment.
    "time_total_s",

    # (Auto-filled) The pid of the training process.
    "pid",

    # (Auto-filled) A formatted date of when the result was processed.
    "date",

    # (Auto-filled) A UNIX timestamp of when the result was processed.
    "timestamp",

    # (Auto-filled) The hostname of the machine hosting the training process.
    "hostname",

    # (Auto-filled) The node ip of the machine hosting the training process.
    "node_ip",

    # (Auto=filled) The current hyperparameter configuration.
    "config",
])


TrainingResult.__new__.__defaults__ = (None,) * len(TrainingResult._fields)
