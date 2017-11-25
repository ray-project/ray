from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal
from collections import namedtuple
from ray.rllib.ppo.filter import MeanStdFilter, NoFilter


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def process_rollout(rollout, reward_filter, gamma, lambda_=1.0):
    """Given a rollout, compute its returns and the advantage.

    TODO(rliaw): generalize this"""
    batch_si = np.asarray(rollout.data["state"])
    batch_a = np.asarray(rollout.data["action"])
    rewards = np.asarray(rollout.data["reward"])
    vpred_t = np.asarray(rollout.data["value"] + [rollout.last_r])

    rewards_plus_v = np.asarray(rollout.data["reward"] + [rollout.last_r])
    batch_r = discount(rewards_plus_v, gamma)[:-1]
    delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
    # This formula for the advantage comes "Generalized Advantage Estimation":
    # https://arxiv.org/abs/1506.02438
    batch_adv = discount(delta_t, gamma * lambda_)
    for i in range(batch_adv.shape[0]):
        batch_adv[i] = reward_filter(batch_adv[i])

    features = rollout.data["features"][0]
    return Batch(batch_si, batch_a, batch_adv, batch_r, rollout.is_terminal(),
                 features)


def get_filter(filter_config, shape):
    if filter_config == "MeanStdFilter":
        return MeanStdFilter(shape, clip=None)
    elif filter_config == "NoFilter":
        return NoFilter()
    else:
        raise Exception("Unknown observation_filter: " +
                        str(filter_config))


def get_policy_cls(config):
    if config["use_lstm"]:
        from ray.rllib.a3c.shared_model_lstm import SharedModelLSTM
        policy_cls = SharedModelLSTM
    elif config["use_pytorch"]:
        from ray.rllib.a3c.shared_torch_policy import SharedTorchPolicy
        policy_cls = SharedTorchPolicy
    else:
        from ray.rllib.a3c.shared_model import SharedModel
        policy_cls = SharedModel
    return policy_cls


Batch = namedtuple(
    "Batch", ["si", "a", "adv", "r", "terminal", "features"])

CompletedRollout = namedtuple(
    "CompletedRollout", ["episode_length", "episode_reward"])
