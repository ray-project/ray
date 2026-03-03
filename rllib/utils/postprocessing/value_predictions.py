import numpy as np

from ray._common.deprecation import Deprecated
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@Deprecated(
    error=True,
    old="ray.rllib.utils.postprocessing.value_predictions.compute_value_targets",
    new="ray.rllib.utils.postprocessing.value_predictions.compute_value_targets_with_bootstrap",
)
def compute_value_targets(
    values,
    rewards,
    terminateds,
    truncateds,
    gamma: float,
    lambda_: float,
):
    ...


@DeveloperAPI
def compute_value_targets_with_bootstrap(
    values,
    rewards,
    terminateds,
    bootstrap_value: float,
    gamma: float,
    lambda_: float,
):
    """Computes GAE value targets for a single episode with an explicit bootstrap value.

    Unlike `compute_value_targets`, this function operates on a **single episode** and
    takes an explicit `bootstrap_value` scalar (V(s_L)) instead of inferring it from
    an artificially appended extra timestep.

    For terminated episodes pass `bootstrap_value=0.0` and `terminateds[-1]=True`.
    For truncated / in-progress episodes pass the value-function prediction at the
    final observation (`V(s_L)`) and `terminateds[-1]=False`.

    Args:
        values: VF predictions for this episode, shape (T,).
        rewards: Rewards for this episode, shape (T,).
        terminateds: Per-timestep termination flags, shape (T,).
            Only `terminateds[-1]` is expected to be True (for terminated episodes).
        bootstrap_value: V(s_L) — the value at the observation *after* the last
            action. 0.0 for terminated episodes.
        gamma: Discount factor.
        lambda_: GAE lambda parameter.

    Returns:
        Value targets, shape (T,), same dtype as `values`.
    """
    T = len(values)
    advantages = np.zeros_like(values)
    last_gae = 0.0
    next_val = bootstrap_value
    for t in reversed(range(T)):
        nonterminal = 1.0 - terminateds[t]
        delta = rewards[t] + gamma * next_val * nonterminal - values[t]
        last_gae = delta + gamma * lambda_ * nonterminal * last_gae
        advantages[t] = last_gae
        next_val = values[t]
    return advantages + values
