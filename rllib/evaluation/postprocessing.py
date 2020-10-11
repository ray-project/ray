import numpy as np
import scipy.signal
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI


def discount_cumsum(x: np.ndarray, gamma: float) -> float:
    """Calculates the discounted cumulative sum over a reward sequence `x`.

    y[t] - discount*y[t+1] = x[t]
    reversed(y)[t] - discount*reversed(y)[t-1] = reversed(x)[t]

    Args:
        gamma (float): The discount factor gamma.

    Returns:
        float: The discounted cumulative sum over the reward sequence `x`.
    """
    return scipy.signal.lfilter([1], [1, float(-gamma)], x[::-1], axis=0)[::-1]


class Postprocessing:
    """Constant definitions for postprocessing."""

    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"


@DeveloperAPI
def compute_advantages(rollout: SampleBatch,
                       last_r: float,
                       gamma: float = 0.9,
                       lambda_: float = 1.0,
                       use_gae: bool = True,
                       use_critic: bool = True):
    """
    Given a rollout, compute its value targets and the advantages.

    Args:
        rollout (SampleBatch): SampleBatch of a single trajectory.
        last_r (float): Value estimation for last observation.
        gamma (float): Discount factor.
        lambda_ (float): Parameter for GAE.
        use_gae (bool): Using Generalized Advantage Estimation.
        use_critic (bool): Whether to use critic (value estimates). Setting
            this to False will use 0 as baseline.

    Returns:
        SampleBatch (SampleBatch): Object with experience from rollout and
            processed rewards.
    """

    rollout_size = len(rollout[SampleBatch.ACTIONS])

    assert SampleBatch.VF_PREDS in rollout or not use_critic, \
        "use_critic=True but values not found"
    assert use_critic or not use_gae, \
        "Can't use gae without using a value function"

    if use_gae:
        vpred_t = np.concatenate(
            [rollout[SampleBatch.VF_PREDS],
             np.array([last_r])])
        delta_t = (
            rollout[SampleBatch.REWARDS] + gamma * vpred_t[1:] - vpred_t[:-1])
        # This formula for the advantage comes from:
        # "Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        rollout[Postprocessing.ADVANTAGES] = discount_cumsum(
            delta_t, gamma * lambda_)
        rollout[Postprocessing.VALUE_TARGETS] = (
            rollout[Postprocessing.ADVANTAGES] +
            rollout[SampleBatch.VF_PREDS]).astype(np.float32)
    else:
        rewards_plus_v = np.concatenate(
            [rollout[SampleBatch.REWARDS],
             np.array([last_r])])
        discounted_returns = discount_cumsum(rewards_plus_v,
                                             gamma)[:-1].astype(np.float32)

        if use_critic:
            rollout[Postprocessing.
                    ADVANTAGES] = discounted_returns - rollout[SampleBatch.
                                                               VF_PREDS]
            rollout[Postprocessing.VALUE_TARGETS] = discounted_returns
        else:
            rollout[Postprocessing.ADVANTAGES] = discounted_returns
            rollout[Postprocessing.VALUE_TARGETS] = np.zeros_like(
                rollout[Postprocessing.ADVANTAGES])

    rollout[Postprocessing.ADVANTAGES] = rollout[
        Postprocessing.ADVANTAGES].astype(np.float32)

    assert all(val.shape[0] == rollout_size for key, val in rollout.items()), \
        "Rollout stacked incorrectly!"
    return rollout
