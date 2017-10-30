from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal
from collections import namedtuple


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def process_rollout(rollout, gamma, lambda_=1.0):
    """Given a rollout, compute its returns and the advantage."""
    batch_si = np.asarray(rollout.states)
    batch_a = np.asarray(rollout.actions)
    rewards = np.asarray(rollout.rewards)
    vpred_t = np.asarray(rollout.values + [rollout.r])

    rewards_plus_v = np.asarray(rollout.rewards + [rollout.r])
    batch_r = discount(rewards_plus_v, gamma)[:-1]
    delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
    # This formula for the advantage comes "Generalized Advantage Estimation":
    # https://arxiv.org/abs/1506.02438
    batch_adv = discount(delta_t, gamma * lambda_)

    features = rollout.features[0]
    return Batch(batch_si, batch_a, batch_adv, batch_r, rollout.terminal,
                 features)


Batch = namedtuple(
    "Batch", ["si", "a", "adv", "r", "terminal", "features"])

CompletedRollout = namedtuple(
    "CompletedRollout", ["episode_length", "episode_reward"])
