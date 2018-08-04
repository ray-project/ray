from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple

_RLTrainingResult = namedtuple("RLTrainingResult", [
    "timesteps_total",
    "done",
    "info",
    "episode_reward_mean",
    "episode_reward_min",
    "episode_reward_max",
    "episode_len_mean",
    "episodes_total",
    "policy_reward_mean",
    "mean_accuracy",
    "mean_validation_accuracy",
    "mean_loss",
    "neg_mean_loss",
    "experiment_id",
    "training_iteration",
    "timesteps_this_iter",
    "time_this_iter_s",
    "time_total_s",
    "pid",
    "date",
    "timestamp",
    "hostname",
    "node_ip",
    "config",
])

_RLTrainingResult.__new__.__defaults__ = (None, ) * len(
    _RLTrainingResult._fields)


# In Python2, this is the only way to document this namedtuple class.
class RLTrainingResult(_RLTrainingResult):
    """TrainingResult for RLlib.

    Most of the fields are optional, the only required one is timesteps_total.
    In RLlib, the supplied algorithms fill in TrainingResult for you.

    Attributes:
        timesteps_total (int): (Required) Accumulated timesteps for this
            entire experiment.
        done (bool): (Optional) If training is terminated.
        info (dict): (Optional) Custom metadata to report for this iteration.
        episode_reward_mean (float): (Optional) The mean episode reward
            if applicable.
        episode_reward_min (float): (Optional) The min episode reward
            if applicable.
        episode_reward_max (float): (Optional) The max episode reward
            if applicable.
        episode_len_mean (float): (Optional) The mean episode length
            if applicable.
        episodes_total (int): (Optional) The number of episodes total.
        policy_reward_mean (float): (Optional) Per-policy reward information
            in multi-agent RL.
        mean_accuracy (float): (Optional) The current training accuracy
            if applicable.
        mean_validation_accuracy (float): (Optional) The current
            validation accuracy if applicable.
        mean_loss (float): (Optional) The current training loss if applicable.
        neg_mean_loss (float): (Auto-filled) The negated current training loss.
        experiment_id (str): (Auto-filled) Unique string identifier
            for this experiment. This id is preserved
            across checkpoint / restore calls.
        training_iteration (int): (Auto-filled) The index of this
            training iteration, e.g. call to train().
        timesteps_this_iter (int): (Auto-filled) Number of timesteps
            in the simulator in this iteration.
        time_this_iter_s (float): (Auto-filled) Time in seconds
            this iteration took to run. This may be overriden in order to
            override the system-computed time difference.
        time_total_s (float): (Auto-filled) Accumulated time in seconds
            for this entire experiment.
        pid (str): (Auto-filled) The pid of the training process.
        date (str): (Auto-filled) A formatted date of
            when the result was processed.
        timestamp (str): (Auto-filled) A UNIX timestamp of
            when the result was processed.
        hostname (str): (Auto-filled) The hostname of the machine
            hosting the training process.
        node_ip (str): (Auto-filled) The node ip of the machine
            hosting the training process.
    """

    # stackoverflow.com/questions/1606436/adding-docstrings-to-namedtuples
    __slots__ = ()
