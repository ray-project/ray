import numpy as np

from ray.rllib.utils.annotations import OldAPIStack


@OldAPIStack
def compute_value_targets(
    values,
    rewards,
    terminateds,
    truncateds,
    gamma: float,
    lambda_: float,
):
    """Computes value function (vf) targets given vf predictions and rewards.

    Note that advantages can then easily be computeed via the formula:
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
