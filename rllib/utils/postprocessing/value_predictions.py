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


@DeveloperAPI
def compute_value_targets_batched(
    values,
    rewards,
    terminateds,
    episode_lens,
    bootstrap_values,
    gamma: float,
    lambda_: float,
):
    """Vectorized GAE computation across multiple episodes.

    Pads all episodes to the same length, then runs the reverse GAE scan using
    numpy operations that process all episodes simultaneously per timestep.

    Args:
        values: Concatenated VF predictions, shape (sum(episode_lens),).
        rewards: Concatenated rewards, shape (sum(episode_lens),).
        terminateds: Concatenated termination flags, shape (sum(episode_lens),).
        episode_lens: List of per-episode lengths.
        bootstrap_values: Bootstrap values, one per episode (list or array).
        gamma: Discount factor.
        lambda_: GAE lambda parameter.

    Returns:
        Concatenated value targets, shape (sum(episode_lens),).
    """
    num_eps = len(episode_lens)
    if num_eps == 0:
        return np.empty(0, dtype=values.dtype)

    lens = np.asarray(episode_lens)
    max_len = int(lens.max())
    dtype = values.dtype

    # Build scatter/gather indices: concat <-> (num_eps, max_len).
    row_idx = np.repeat(np.arange(num_eps), lens)
    offsets = np.empty(num_eps + 1, dtype=np.int64)
    offsets[0] = 0
    np.cumsum(lens, out=offsets[1:])
    col_idx = np.arange(values.shape[0]) - np.repeat(offsets[:-1], lens)

    # Scatter into padded 2D arrays.
    # Padding: values=0, rewards=0, terminateds=1 (terminated).
    vals_2d = np.zeros((num_eps, max_len), dtype=dtype)
    rews_2d = np.zeros((num_eps, max_len), dtype=dtype)
    term_2d = np.ones((num_eps, max_len), dtype=dtype)

    vals_2d[row_idx, col_idx] = values
    rews_2d[row_idx, col_idx] = rewards
    term_2d[row_idx, col_idx] = terminateds

    nonterminal = 1.0 - term_2d
    bs_vals = np.asarray(bootstrap_values, dtype=dtype)

    # next_values: value at t+1 within each episode, bootstrap at episode end.
    next_vals = np.zeros((num_eps, max_len), dtype=dtype)
    if max_len > 1:
        next_vals[:, :-1] = vals_2d[:, 1:]
    next_vals[np.arange(num_eps), lens - 1] = bs_vals

    # Vectorized delta and coefficient computation.
    deltas = rews_2d + gamma * next_vals * nonterminal - vals_2d
    coeff = gamma * lambda_ * nonterminal

    # Reverse scan, vectorized across all episodes per timestep.
    advantages = np.empty_like(deltas)
    advantages[:, max_len - 1] = deltas[:, max_len - 1]
    for t in range(max_len - 2, -1, -1):
        advantages[:, t] = deltas[:, t] + coeff[:, t] * advantages[:, t + 1]

    # Value targets = GAE advantages + baseline values.
    value_targets_2d = advantages + vals_2d

    # Gather valid positions back into concatenated form.
    return value_targets_2d[row_idx, col_idx]
