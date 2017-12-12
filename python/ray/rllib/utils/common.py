from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def process_rollout(rollout, reward_filter, gamma, lambda_=1.0, use_gae=True):
    """Given a rollout, compute its returns and the advantage."""

    # TODO(rliaw): need to figure out what's the "right way" to

    traj = {}
    trajsize = len(rollout.data["actions"])
    for key in rollout.data:
        if key not in ["terminal", "features"]:
            if rollou
            traj[key] = np.concatenate(rollout.data[key])
        else:
            traj[key] = np.asarray(rollout.data[key])

    if use_gae:
        assert "vf_preds" in rollout.data, "Values not found!"
        vpred_t = np.stack(rollout.data["vf_preds"] + [np.array([rollout.last_r])]).squeeze()
        delta_t = traj["rewards"] + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes
        # "Generalized Advantage Estimation":
        # https://arxiv.org/abs/1506.02438
        traj["advantages"] = discount(delta_t, gamma * lambda_)
        traj["value_targets"] = traj["advantages"] + traj["vf_preds"]
    else:
        # TODO(rliaw): make sure this is right
        rewards_plus_v = np.stack(rollout.data["vf_preds"] + [np.array([rollout.last_r])]).squeeze()
        traj["advantages"] = discount(rewards_plus_v, gamma)[:-1]

    for i in range(traj["advantages"].shape[0]):
        traj["advantages"][i] = reward_filter(traj["advantages"][i])

    for val in traj.values():
        assert val.shape[0] == trajsize, "Rollout stacked incorrectly!"
    return traj
