import numpy as np


def compute_advantages_and_value_targets(
    values,
    rewards,
    terminateds,
    truncateds,
    gamma: float,
    lambda_: float,
):

    orig_shape = values.shape
    # Force-set all values at terminals (not at truncations!) to 0.0.
    orig_values = flat_values = values * (1.0 - terminateds)

    # Data has an extra time rank -> reshape everything into single sequence.
    #time_rank = False
    #if len(orig_shape) == 2:
    #    time_rank = True
    #    flat_values = flat_values.reshape((-1,))
    #    rewards = rewards.reshape((-1,))

    flat_values = np.append(flat_values, 0.0)
    intermediates = (rewards + gamma * (1 - lambda_) * flat_values[1:])
    continues = 1.0 - terminateds

    #if time_rank:
    #    Rs = []
    #    intermediates = intermediates.reshape(orig_shape)
    #    for row in reversed(range(orig_shape[0])):
    #        last = orig_values[row + 1][0] if row != orig_shape[0] - 1 else 0.0
    #        for t in reversed(range(intermediates.shape[1])):
    #            last = intermediates[row][t] + continues[row][t] * gamma * lambda_ * last
    #            Rs.append(last)
    #            if truncateds[row][t]:
    #                last = orig_values[row][t]
    #else:
    Rs = []
    last = flat_values[-1]
    for t in reversed(range(intermediates.shape[0])):
        last = intermediates[t] + continues[t] * gamma * lambda_ * last
        Rs.append(last)
        if truncateds[t]:
            last = orig_values[t]

    # Reverse back to correct (time) direction.
    targets = np.stack(list(reversed(Rs)), axis=0)

    ## Reshape `targets` back to shape=(B, T) if necessary.
    #if time_rank:
    #    targets = targets.reshape(orig_shape)

    # Targets = Advantages + Value predictions
    advantages = targets - orig_values

    return advantages, targets
