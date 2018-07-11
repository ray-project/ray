from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal
from ray.rllib.evaluation.sample_batch import SampleBatch


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def compute_advantages(rollout, last_r, gamma, lambda_=1.0, use_gae=True):
    """Given a rollout, compute its value targets and the advantage.

    Args:
        rollout (PartialRollout): Partial Rollout Object
        last_r (float): Value estimation for last observation
        gamma (float): Parameter for GAE
        lambda_ (float): Parameter for GAE
        use_gae (bool): Using Generalized Advantage Estamation

    Returns:
        SampleBatch (SampleBatch): Object with experience from rollout and
            processed rewards.
    """

    traj = {}
    trajsize = len(rollout["actions"])
    for key in rollout:
        traj[key] = np.stack(rollout[key])

    if use_gae:
        assert "vf_preds" in rollout, "Values not found!"
        vpred_t = np.concatenate([rollout["vf_preds"], np.array([last_r])])
        delta_t = traj["rewards"] + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes
        # "Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        traj["advantages"] = discount(delta_t, gamma * lambda_)
        traj["value_targets"] = (
            traj["advantages"] + traj["vf_preds"]).copy().astype(np.float32)
    else:
        rewards_plus_v = np.concatenate(
            [rollout["rewards"], np.array([last_r])])
        traj["advantages"] = discount(rewards_plus_v, gamma)[:-1]

    traj["advantages"] = traj["advantages"].copy().astype(np.float32)

    assert all(val.shape[0] == trajsize for val in traj.values()), \
        "Rollout stacked incorrectly!"
    return SampleBatch(traj)
