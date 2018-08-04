from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import os


_RLTrainingResult = namedtuple(
    "RLTrainingResult",
    [
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


_RLTrainingResult.__new__.__defaults__ = (None,) * len(_RLTrainingResult._fields)


class RLTrainingResult(_RLTrainingResult):
    """TrainingResult for RLlib.

    Wraps a namedtuple because in Python2, this is the only way to
    document this namedtuple class.

    Most of the fields are optional, the only required one is timesteps_total.

    In RLlib, the supplied algorithms fill in TrainingResult for you.

    Attributes:
        timesteps_total: (Required) Accumulated timesteps for this entire experiment.
        done: (Optional) If training is terminated.
        info: (Optional) Custom metadata to report for this iteration.
        episode_reward_mean: (Optional) The mean episode reward if applicable.
        episode_reward_min: (Optional) The min episode reward if applicable.
        episode_reward_max: (Optional) The max episode reward if applicable.
        episode_len_mean: (Optional) The mean episode length if applicable.
        episodes_total: (Optional) The number of episodes total.
        policy_reward_mean: (Optional) Per-policy reward information in multi-agent RL.
        mean_accuracy: (Optional) The current training accuracy if applicable.
        mean_validation_accuracy: (Optional) The current validation accuracy if applicable.
        mean_loss: (Optional) The current training loss if applicable.
        neg_mean_loss: (Auto-filled) The negated current training loss.
        experiment_id: (Auto-filled) Unique string identifier for this experiment. This id is preserved across checkpoint / restore calls.
        training_iteration: (Auto-filled) The index of this training iteration, e.g. call to train().
        timesteps_this_iter: (Auto-filled) Number of timesteps in the simulator in this iteration.
        time_this_iter_s: (Auto-filled) Time in seconds this iteration took to run. This may be overriden in order to override the system-computed time difference.
        time_total_s: (Auto-filled) Accumulated time in seconds for this entire experiment.
        pid: (Auto-filled) The pid of the training process.
        date: (Auto-filled) A formatted date of when the result was processed.
        timestamp: (Auto-filled) A UNIX timestamp of when the result was processed.
        hostname: (Auto-filled) The hostname of the machine hosting the training process.
        node_ip: (Auto-filled) The node ip of the machine hosting the training process.
    """


    # stackoverflow.com/questions/1606436/adding-docstrings-to-namedtuples
    __slots__ = ()


