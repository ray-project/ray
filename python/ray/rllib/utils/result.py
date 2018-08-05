from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune.result import TrainingResult


class RLTrainingResult(TrainingResult):
    """TrainingResult for RLlib.

    In RLlib, the supplied algorithms fill in TrainingResult for you.

    Attributes:
        timesteps_total (int): (Required) Accumulated timesteps for this
            entire experiment.
        timesteps_this_iter (int): (Auto-filled) Number of timesteps
            in the simulator in this iteration.
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
    """

    @property
    def timesteps_total(self):
        return self.get("timesteps_total")

    @property
    def timesteps_this_iter(self):
        return self.get("timesteps_this_iter")

    @property
    def info(self):
        return self.get("info")

    @property
    def episode_reward_mean(self):
        return self.get("episode_reward_mean")

    @property
    def episode_reward_min(self):
        return self.get("episode_reward_min")

    @property
    def episode_reward_max(self):
        return self.get("episode_reward_max")

    @property
    def episode_len_mean(self):
        return self.get("episode_len_mean")

    @property
    def episodes_total(self):
        return self.get("episodes_total")

    @property
    def policy_reward_mean(self):
        return self.get("policy_reward_mean")

    @property
    def mean_accuracy(self):
        return self.get("mean_accuracy")

    @property
    def mean_validation_accuracy(self):
        return self.get("mean_validation_accuracy")

    @property
    def mean_loss(self):
        return self.get("mean_loss")

    @property
    def neg_mean_loss(self):
        return self.get("neg_mean_loss")
