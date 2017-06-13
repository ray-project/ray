from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf


def flatten(weights, start=0, stop=2):
  """This methods reshapes all values in a dictionary.

  The indices from start to stop will be flattened into a single index.

  Args:
    weights: A dictionary mapping keys to numpy arrays.
    start: The starting index.
    stop: The ending index.
  """
  for key, val in weights.items():
    new_shape = val.shape[0:start] + (-1,) + val.shape[stop:]
    weights[key] = val.reshape(new_shape)
  return weights


def concatenate(weights_list):
  keys = weights_list[0].keys()
  result = {}
  for key in keys:
    result[key] = np.concatenate([l[key] for l in weights_list])
  return result


def shuffle(trajectory):
  permutation = np.random.permutation(trajectory["dones"].shape[0])
  for key, val in trajectory.items():
    trajectory[key] = val[permutation]
  return trajectory


def make_divisible_by(array, n):
  return array[0:array.shape[0] - array.shape[0] % n]


def average_gradients(tower_grads):
  """
  Average gradients across towers.

  Calculate the average gradient for each shared variable across all towers.
  Note that this function provides a synchronization point across all towers.

  Args:
    tower_grads: List of lists of (gradient, variable) tuples. The outer list
      is over individual gradients. The inner list is over the gradient
      calculation for each tower.

  Returns:
     List of pairs of (gradient, variable) where the gradient has been averaged
     across all towers.

  TODO(ekl): We could use NCCL if this becomes a bottleneck.
  """

  average_grads = []
  for grad_and_vars in zip(*tower_grads):

    # Note that each grad_and_vars looks like the following:
    #   ((grad0_gpu0, var0_gpu0), ... , (grad0_gpuN, var0_gpuN))
    grads = []
    for g, _ in grad_and_vars:
      if g is not None:
        # Add 0 dimension to the gradients to represent the tower.
        expanded_g = tf.expand_dims(g, 0)

        # Append on a 'tower' dimension which we will average over below.
        grads.append(expanded_g)

    # Average over the 'tower' dimension.
    grad = tf.concat(axis=0, values=grads)
    grad = tf.reduce_mean(grad, 0)

    # Keep in mind that the Variables are redundant because they are shared
    # across towers. So .. we will just return the first tower's pointer to
    # the Variable.
    v = grad_and_vars[0][1]
    grad_and_var = (grad, v)
    average_grads.append(grad_and_var)

  return average_grads
