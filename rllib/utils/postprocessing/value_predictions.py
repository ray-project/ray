import numpy as np

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def compute_value_targets(
    values,
    rewards,
    terminateds,
    truncateds,
    gamma: float,
    lambda_: float,
):
    """Computes GAE value targets given vf predictions and rewards.

    Convention (Gymnasium-aligned, matches ``AddOneTsToEpisodesAndTruncate``):
      ``terminateds[t] = True``  =>  no s_{t+1}; gate t -> t+1 bootstrap.
      ``truncateds[t]  = True``  =>  step t ends an episode chunk; V(s_{t+1})
                                     remains a valid bootstrap, but GAE must
                                     not propagate across the boundary.

    Advantages = targets - vf_predictions.

    See https://pseudo-rnd-thoughts.github.io/blog/visualising-gae/ for visualisation.
    """
    # 1 if the transition t -> t+1 exists (not a terminal at t), else 0.
    non_terminal = 1.0 - terminateds
    # 1 if GAE may propagate from t+1 back into t, else 0. Both terminal and
    # chunk-boundary steps stop the recursion.
    propagate = non_terminal * (1.0 - truncateds)

    # V(s_{t+1}) per timestep. The trailing 0.0 is a dummy: the corresponding
    # td_residual is masked out downstream by `loss_mask`, and the recursion
    # carrying it is gated by `propagate`.
    next_state_values = np.append(values[1:], 0.0)

    # TD residual: delta_t = r_t + gamma * (1 - terminated_t) * V(s_{t+1}) - V(s_t)
    # Truncation does NOT zero the bootstrap -- V(s_{t+1}) is a valid
    # prediction at a truncation boundary.
    td_residuals = rewards + gamma * non_terminal * next_state_values - values

    # GAE backward recursion. `running_advantage` carries advantage[t+1] into
    # iteration t and is killed at terminal / truncation boundaries by
    # `propagate`.
    advantages = np.zeros_like(rewards, dtype=np.float32)
    running_advantage = 0.0
    for t in reversed(range(td_residuals.shape[0])):
        running_advantage = (
            td_residuals[t] + gamma * lambda_ * propagate[t] * running_advantage
        )
        advantages[t] = running_advantage

    # target_t = advantage_t + V(s_t).
    return (advantages + values).astype(np.float32)


def extract_bootstrapped_values(vf_preds, episode_lengths, T):
    """Returns a bootstrapped value batch given value predictions.

    Note that the incoming value predictions must have happened over (artificially)
    elongated episodes (by 1 timestep at the end). This way, we can either extract the
    `vf_preds` at these extra timesteps (as "bootstrap values") or skip over them
    entirely if they lie in the middle of the T-slices.

    For example, given an episodes structure like this:
    01234a 0123456b 01c 012- 0123e 012-
    where each episode is separated by a space and goes from 0 to n and ends in an
    artificially elongated timestep (denoted by 'a', 'b', 'c', '-', or 'e'), where '-'
    means that the episode was terminated and the bootstrap value at the end should be
    zero and 'a', 'b', 'c', etc.. represent truncated episode ends with computed vf
    estimates.
    The output for the above sequence (and T=4) should then be:
    4 3 b 2 3 -

    Args:
        vf_preds: The computed value function predictions over the artificially
            elongated episodes (by one timestep at the end).
        episode_lengths: The original (correct) episode lengths, NOT counting the
            artificially added timestep at the end.
        T: The size of the time dimension by which to slice the data. Note that the
            sum of all episode lengths (`sum(episode_lengths)`) must be dividable by T.

    Returns:
        The batch of bootstrapped values.
    """
    bootstrapped_values = []
    if sum(episode_lengths) % T != 0:
        raise ValueError(
            "Can only extract bootstrapped values if the sum of episode lengths "
            f"({sum(episode_lengths)}) is dividable by the given T ({T})!"
        )

    # Loop over all episode lengths and collect bootstrap values.
    # Do not alter incoming `episode_lengths` list.
    episode_lengths = episode_lengths[:]
    i = -1
    while i < len(episode_lengths) - 1:
        i += 1
        eps_len = episode_lengths[i]
        # We can make another T-stride inside this episode ->
        # - Use a vf prediction within the episode as bootstrapped value.
        # - "Fix" the episode_lengths array and continue within the same episode.
        if T < eps_len:
            bootstrapped_values.append(vf_preds[T])
            vf_preds = vf_preds[T:]
            episode_lengths[i] -= T
            i -= 1
        # We can make another T-stride inside this episode, but will then be at the end
        # of it ->
        # - Use the value function prediction at the artificially added timestep
        #   as bootstrapped value.
        # - Skip the additional timestep at the end and ,ove on with next episode.
        elif T == eps_len:
            bootstrapped_values.append(vf_preds[T])
            vf_preds = vf_preds[T + 1 :]
        # The episode fits entirely into the T-stride ->
        # - Move on to next episode ("fix" its length by make it seemingly longer).
        else:
            # Skip bootstrap value of current episode (not needed).
            vf_preds = vf_preds[1:]
            # Make next episode seem longer.
            episode_lengths[i + 1] += eps_len

    return np.array(bootstrapped_values)
