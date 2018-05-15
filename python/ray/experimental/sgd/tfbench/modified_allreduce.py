# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Utilities for allreduce."""

from __future__ import print_function

import collections as pycoll
import re
import numpy as np

from six.moves import xrange  # pylint: disable=redefined-builtin
import tensorflow as tf

from tensorflow.contrib import nccl
from tensorflow.contrib.all_reduce.python import all_reduce
from allreduce import *


def sum_gradients_all_reduce(dev_prefixes,
                             tower_grads,
                             num_workers,
                             alg,
                             num_shards,
                             gpu_indices,
                             agg_small_grads_max_bytes=0):
  """Apply all-reduce algorithm over specified gradient tensors.

  Args:
    dev_prefixes: list of prefix strings to use to generate PS device names.
    tower_grads: the gradients to reduce.
    num_workers: number of worker processes across entire job.
    alg: the all-reduce algorithm to apply.
    num_shards: alg-specific sharding factor.
    gpu_indices: indices of local GPUs in order usable for ring-reduce.
    agg_small_grads_max_bytes: largest tensor eligible for aggregation,
      in number of bytes.

  Returns:
    list of reduced tensors, packing values
  """
  alg_contains_shuffle = contains_any(alg, ['pscpu', 'psgpu'])
  is_hierarchical = '/' in alg
  if 'pscpu' in alg:
    aux_devices = [prefix + '/cpu:0' for prefix in dev_prefixes]
  elif 'psgpu' in alg:
    aux_devices = [
        prefix + '/gpu:%d' % i
        for i in range(len(gpu_indices))
        for prefix in dev_prefixes
    ]
  else:
    aux_devices = ['/job:localhost/cpu:0']
  aux_device_groups = group_device_names(aux_devices, num_shards
                                         if alg_contains_shuffle else 1)
  group_index = 0
  if agg_small_grads_max_bytes > 0:
    tower_grads, packing = pack_small_tensors(
        tower_grads,
        max_bytes=agg_small_grads_max_bytes)

  else:
    packing = None
  new_tower_grads = []
  if alg == 'better':
    raw_devices = ['/gpu:%i' % (i) for i in gpu_indices]
    agg_grads = aggregate_gradients_using_copy_with_device_selection(
      tower_grads, raw_devices)
    for arr in tower_grads:
      new_tower_grads.append(
          [(g, v) for (_, v), (g, _) in zip(arr, agg_grads)])
  else:
    reduced_gv_list = []
    for grad_and_vars in zip(*tower_grads):
      reduced_gv_list.append(
          sum_grad_and_var_all_reduce(
              grad_and_vars, num_workers, alg, gpu_indices, aux_devices
              if is_hierarchical else aux_device_groups[group_index], num_shards))
      group_index = (group_index + 1) % len(aux_device_groups)
    new_tower_grads = [list(x) for x in zip(*reduced_gv_list)]
  return new_tower_grads, packing

def print_stats(sizes):
  def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
  stats = {
    "avg": np.mean(sizes),
    "median": np.median(sizes),
    "total size": np.sum(sizes)
  }
  print("Stats " + ", ".join(
    ["%s: %s" % (k, sizeof_fmt(v)) for k, v in stats.items()]))
  other_stats = {
    "len": len(sizes)
  }
  print(", ".join(["%s: %f" % (k, v) for k, v in other_stats.items()]))


def pack_small_tensors(tower_grads, max_bytes=0):
  """Concatenate gradients together more intelligently.

  Does binpacking
  Args:
    tower_grads: List of lists of (gradient, variable) tuples.
    max_bytes: Int giving max number of bytes in a tensor that
      may be considered small.
  """
  assert max_bytes >= 0
  orig_grads = [g for g, _ in tower_grads[0]]
  # Check to make sure sizes are accurate; not entirely important
  assert all(g.dtype == tf.float32 for g in orig_grads)
  sizes = [4 * g.shape.num_elements() for g in orig_grads]
  print("Before packing")
  print_stats(sizes)
  small_ranges = []
  large_indices = []
  new_sizes = []

  def end_interval(indices, small_ranges, large_indices):
    if len(indices) > 1:
      small_ranges.insert(0, [indices[0], indices[-1]])
    else:
      large_indices.insert(0, indices[0])

  cur_range = []
  cur_size = 0
  for i, s in reversed(list(enumerate(sizes))):
    if cur_size > max_bytes:
      end_interval(cur_range, small_ranges, large_indices)
      new_sizes.insert(0, cur_size)
      cur_range = []
      cur_size = 0
    cur_range.insert(0, i)
    cur_size += s
  end_interval(cur_range, small_ranges, large_indices)
  new_sizes.insert(0, cur_size)

  print("After packing")
  print_stats(new_sizes)
  num_gv = len(orig_grads)
  packing = {}
  if len(small_ranges):
    new_tower_grads = []
    for dev_idx, gv_list in enumerate(tower_grads):
      assert len(gv_list) == num_gv
      new_gv_list = []
      for r in small_ranges:
        key = '%d:%d' % (dev_idx, len(new_gv_list))
        new_gv_list.append((pack_range(key, packing, gv_list, r),
                            'packing_var_placeholder'))
      for i in large_indices:
        new_gv_list.append(gv_list[i])
      new_tower_grads.append(new_gv_list)
    return new_tower_grads, packing
  else:
    return tower_grads, None
