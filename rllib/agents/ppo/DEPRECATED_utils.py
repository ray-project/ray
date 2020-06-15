import numpy as np


def flatten(weights, start=0, stop=2):
    """This methods reshapes all values in a dictionary.

    The indices from start to stop will be flattened into a single index.

    Args:
        weights: A dictionary mapping keys to numpy arrays.
        start: The starting index.
        stop: The ending index.
    """
    for key, val in weights.items():
        new_shape = val.shape[0:start] + (-1, ) + val.shape[stop:]
        weights[key] = val.reshape(new_shape)
    return weights


def concatenate(weights_list):
    keys = weights_list[0].keys()
    result = {}
    for key in keys:
        result[key] = np.concatenate([l[key] for l in weights_list])
    return result


def shuffle(trajectory):
    permutation = np.random.permutation(trajectory["actions"].shape[0])
    for key, val in trajectory.items():
        trajectory[key] = val[permutation]
    return trajectory
