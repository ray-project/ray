from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def process_rollout(rollout, reward_filter, gamma, lambda_=1.0, gae=True):
    """Given a rollout, compute its returns and the advantage."""

    traj = {}
    for key in rollout.data:
        traj[key] = np.asarray(rollout.data[key])

    if gae:
        assert "value" in rollout.data, "Values not found!"
        vpred_t = np.asarray(rollout.data["value"] + [rollout.last_r])
        delta_t = traj["rewards"] + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes
        # "Generalized Advantage Estimation":
        # https://arxiv.org/abs/1506.02438
        traj["advantages"] = discount(delta_t, gamma * lambda_)
        traj["value_targets"] = traj["advantages"] + traj["vf_preds"]
    else:
        # TODO(rliaw): make sure this is right
        rewards_plus_v = np.asarray(rollout.data["reward"] + [rollout.last_r])
        traj["advantages"] = discount(rewards_plus_v, gamma)[:-1]

    for i in range(traj["advantages"].shape[0]):
        traj["advantages"][i] = reward_filter(traj["advantages"][i])
    return traj
