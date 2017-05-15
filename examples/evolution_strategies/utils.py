# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf


def compute_ranks(x):
  """Returns ranks in [0, len(x))

  Note: This is different from scipy.stats.rankdata, which returns ranks in
  [1, len(x)].
  """
  assert x.ndim == 1
  ranks = np.empty(len(x), dtype=int)
  ranks[x.argsort()] = np.arange(len(x))
  return ranks


def compute_centered_ranks(x):
  y = compute_ranks(x.ravel()).reshape(x.shape).astype(np.float32)
  y /= (x.size - 1)
  y -= 0.5
  return y


def make_session(single_threaded):
  if not single_threaded:
    return tf.InteractiveSession()
  return tf.InteractiveSession(
      config=tf.ConfigProto(inter_op_parallelism_threads=1,
                            intra_op_parallelism_threads=1))


def itergroups(items, group_size):
  assert group_size >= 1
  group = []
  for x in items:
    group.append(x)
    if len(group) == group_size:
      yield tuple(group)
      del group[:]
  if group:
    yield tuple(group)


def batched_weighted_sum(weights, vecs, batch_size):
  total = 0
  num_items_summed = 0
  for batch_weights, batch_vecs in zip(itergroups(weights, batch_size),
                                       itergroups(vecs, batch_size)):
    assert len(batch_weights) == len(batch_vecs) <= batch_size
    total += np.dot(np.asarray(batch_weights, dtype=np.float32),
                    np.asarray(batch_vecs, dtype=np.float32))
    num_items_summed += len(batch_weights)
  return total, num_items_summed


class RunningStat(object):
  def __init__(self, shape, eps):
    self.sum = np.zeros(shape, dtype=np.float32)
    self.sumsq = np.full(shape, eps, dtype=np.float32)
    self.count = eps

  def increment(self, s, ssq, c):
    self.sum += s
    self.sumsq += ssq
    self.count += c

  @property
  def mean(self):
    return self.sum / self.count

  @property
  def std(self):
    return np.sqrt(np.maximum(self.sumsq / self.count - np.square(self.mean),
                              1e-2))

  def set_from_init(self, init_mean, init_std, init_count):
    self.sum[:] = init_mean * init_count
    self.sumsq[:] = (np.square(init_mean) + np.square(init_std)) * init_count
    self.count = init_count
