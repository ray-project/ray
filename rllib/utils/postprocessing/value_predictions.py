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
    """Computes value function (vf) targets given vf predictions and rewards.

    Note that advantages can then easily be computed via the formula:
    advantages = targets - vf_predictions
    """
    # Force-set all values at terminals (not at truncations!) to 0.0.
    orig_values = flat_values = values * (1.0 - terminateds)

    flat_values = np.append(flat_values, 0.0)
    intermediates = rewards + gamma * (1 - lambda_) * flat_values[1:]
    continues = 1.0 - terminateds

    Rs = []
    last = flat_values[-1]
    for t in reversed(range(intermediates.shape[0])):
        last = intermediates[t] + continues[t] * gamma * lambda_ * last
        Rs.append(last)
        if truncateds[t]:
            last = orig_values[t]

    # Reverse back to correct (time) direction.
    value_targets = np.stack(list(reversed(Rs)), axis=0)

    return value_targets.astype(np.float32)


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
