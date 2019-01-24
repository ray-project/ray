from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import scipy.signal
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


@DeveloperAPI
def compute_advantages(rollout, last_r, gamma=0.9, lambda_=1.0, use_gae=True):
    """Given a rollout, compute its value targets and the advantage.

    Args:
        rollout (SampleBatch): SampleBatch of a single trajectory
        last_r (float): Value estimation for last observation
        gamma (float): Discount factor.
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
        # TODO(ekl): support using a critic without GAE
        traj["value_targets"] = np.zeros_like(traj["advantages"])

    traj["advantages"] = traj["advantages"].copy().astype(np.float32)

    assert all(val.shape[0] == trajsize for val in traj.values()), \
        "Rollout stacked incorrectly!"
    return SampleBatch(traj)


@DeveloperAPI
def compute_targets(rollout, action_space, last_r=0.0, gamma=0.9, lambda_=1.0):
    """Given a rollout, compute targets.

    Used for categorical crossentropy loss on the policy. Also assumes
    there is a value function. Uses GAE to calculate advantages.

    Args:
        rollout (SampleBatch): SampleBatch of a single trajectory
        action_space (gym.Space): Dimensions of the advantage targets.
        last_r (float): Value estimation for last observation
        gamma (float): Discount factor.
        lambda_ (float): Parameter for GAE
    """

    rollout = compute_advantages(rollout, last_r, gamma=gamma, lambda_=lambda_)
    rollout["adv_targets"] = np.zeros((rollout.count, action_space.n))
    rollout["adv_targets"][np.arange(rollout.count), rollout["actions"]] = \
        rollout["advantages"]
    rollout["value_targets"] = rollout["rewards"].copy()
    rollout["value_targets"][:-1] += gamma * rollout["vf_preds"][1:]
    return rollout
