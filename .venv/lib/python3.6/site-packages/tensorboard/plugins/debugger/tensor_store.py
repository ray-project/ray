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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import tensorflow as tf
from tensorboard.plugins.debugger import tensor_helper
from tensorboard.util import tb_logging
from tensorflow.python.debug.lib import debug_data

logger = tb_logging.get_logger()

# TODO(cais): Defer some tensor values to TensorBase, within a bytes limit,
#   before discarding them.


class _TensorValueDiscarded(object):

  def __init__(self, watch_key, time_index):
    self._watch_key = watch_key
    self._time_index = time_index

  @property
  def watch_key(self):
    return self._watch_key

  @property
  def time_index(self):
    return self._time_index

  @property
  def nbytes(self):
    return 0


class _WatchStore(object):
  """The store for a single debug tensor watch.

  Discards data according to pre-set byte limit.
  """

  def __init__(self,
               watch_key,
               mem_bytes_limit=10e6):
    """Constructor of _WatchStore.

    The overflowing works as follows:
    The most recent tensor values are stored in memory, up to `mem_bytes_limit`
    bytes. But at least one (the most recent) value is always stored in memory.
    For older tensors exceeding that limit, they are discarded.

    Args:
      watch_key: A string representing the debugger tensor watch, with th
        format:
          <NODE_NAME>:<OUTPUT_SLOT>:<DEBUG_OP_NAME>
        e.g.,
          'Dense_1/BiasAdd:0:DebugIdentity'.
      mem_bytes_limit: Limit on number of bytes to store in memory.
    """

    self._watch_key = watch_key
    self._mem_bytes_limit = mem_bytes_limit
    self._in_mem_bytes = 0
    self._disposed = False
    self._data = []  # A map from index to tensor value.

  def add(self, value):
    """Add a tensor the watch store."""
    if self._disposed:
      raise ValueError(
          'Cannot add value: this _WatchStore instance is already disposed')
    self._data.append(value)
    if hasattr(value, 'nbytes'):
      self._in_mem_bytes += value.nbytes
      self._ensure_bytes_limits()

  def _ensure_bytes_limits(self):
    # TODO(cais): Thread safety?
    if self._in_mem_bytes <= self._mem_bytes_limit:
      return

    i = len(self._data) - 1
    cum_mem_size = 0
    while i >= 0:
      if hasattr(self._data[i], 'nbytes'):
        cum_mem_size += self._data[i].nbytes
      if i < len(self._data) - 1 and cum_mem_size > self._mem_bytes_limit:
        # Always keep at least one time index in the memory.
        break
      i -= 1
    # i is now the last time index to discard.

    # Mark remaining ones as discarded.
    while i >= 0:
      if not isinstance(self._data[i], _TensorValueDiscarded):
        self._data[i] = _TensorValueDiscarded(self._watch_key, i)
      i -= 1

  def num_total(self):
    """Get the total number of values."""
    return len(self._data)

  def num_in_memory(self):
    """Get number of values in memory."""
    n = len(self._data) - 1
    while n >= 0:
      if isinstance(self._data[n], _TensorValueDiscarded):
        break
      n -= 1
    return len(self._data) - 1 - n

  def num_discarded(self):
    """Get the number of values discarded due to exceeding both limits."""
    if not self._data:
      return 0
    n = 0
    while n < len(self._data):
      if not isinstance(self._data[n], _TensorValueDiscarded):
        break
      n += 1
    return n

  def query(self, time_indices):
    """Query the values at given time indices.

    Args:
      time_indices: 0-based time indices to query, as a `list` of `int`.

    Returns:
      Values as a list of `numpy.ndarray` (for time indices in memory) or
      `None` (for time indices discarded).
    """
    if self._disposed:
      raise ValueError(
          'Cannot query: this _WatchStore instance is already disposed')
    if not isinstance(time_indices, (tuple, list)):
      time_indices = [time_indices]
    output = []
    for time_index in time_indices:
      if isinstance(self._data[time_index], _TensorValueDiscarded):
        output.append(None)
      else:
        data_item = self._data[time_index]
        if (hasattr(data_item, 'dtype') and
            tensor_helper.translate_dtype(data_item.dtype) == 'string'):
          _, _, data_item = tensor_helper.array_view(data_item)
          data_item = np.array(
              tensor_helper.process_buffers_for_display(data_item),
              dtype=np.object)
        output.append(data_item)

    return output

  def dispose(self):
    self._disposed = True


class TensorStore(object):

  def __init__(self, watch_mem_bytes_limit=10e6):
    """Constructor of TensorStore.

    Args:
      watch_mem_bytes_limit: Limit on number of bytes to store in memory for
        each watch key.
    """
    self._watch_mem_bytes_limit = watch_mem_bytes_limit
    self._tensor_data = dict()  # A map from watch key to _WatchStore instances.

  def add(self, watch_key, tensor_value):
    """Add a tensor value.

    Args:
      watch_key: A string representing the debugger tensor watch, e.g.,
        'Dense_1/BiasAdd:0:DebugIdentity'.
      tensor_value: The value of the tensor as a numpy.ndarray.
    """
    if watch_key not in self._tensor_data:
      self._tensor_data[watch_key] = _WatchStore(
          watch_key,
          mem_bytes_limit=self._watch_mem_bytes_limit)
    self._tensor_data[watch_key].add(tensor_value)

  def query(self,
            watch_key,
            time_indices=None,
            slicing=None,
            mapping=None):
    """Query tensor store for a given watch_key.

    Args:
      watch_key: The watch key to query.
      time_indices: A numpy-style slicing string for time indices. E.g.,
        `-1`, `:-2`, `[::2]`. If not provided (`None`), will use -1.
      slicing: A numpy-style slicing string for individual time steps.
      mapping: An mapping string or a list of them. Supported mappings:
        `{None, 'image/png', 'health-pill'}`.

    Returns:
      The potentially sliced values as a nested list of values or its mapped
        format. A `list` of nested `list` of values.

    Raises:
      ValueError: If the shape of the sliced array is incompatible with mapping
        mode. Or if the mapping type is invalid.
    """
    if watch_key not in self._tensor_data:
      raise KeyError("watch_key not found: %s" % watch_key)

    if time_indices is None:
      time_indices = '-1'
    time_slicing = tensor_helper.parse_time_indices(time_indices)
    all_time_indices = list(range(self._tensor_data[watch_key].num_total()))
    sliced_time_indices = all_time_indices[time_slicing]
    if not isinstance(sliced_time_indices, list):
      sliced_time_indices = [sliced_time_indices]

    recombine_and_map = False
    step_mapping = mapping
    if len(sliced_time_indices) > 1 and mapping not in (None, ):
      recombine_and_map = True
      step_mapping = None

    output = []
    for index in sliced_time_indices:
      value = self._tensor_data[watch_key].query(index)[0]
      if (value is not None and
          not isinstance(value, debug_data.InconvertibleTensorProto)):
        output.append(tensor_helper.array_view(
            value, slicing=slicing, mapping=step_mapping)[2])
      else:
        output.append(None)

    if recombine_and_map:
      if mapping == 'image/png':
        output = tensor_helper.array_to_base64_png(output)
      elif mapping and mapping != 'none':
        logger.warn(
            'Unsupported mapping mode after recomining time steps: %s',
            mapping)
    return output

  def dispose(self):
    for watch_key in self._tensor_data:
      self._tensor_data[watch_key].dispose()
