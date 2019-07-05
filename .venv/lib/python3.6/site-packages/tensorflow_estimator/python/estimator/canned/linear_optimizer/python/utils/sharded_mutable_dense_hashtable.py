# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Sharded mutable dense hash table."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools

from six.moves import range

from tensorflow.python.eager import context
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import control_flow_ops
from tensorflow.python.ops import data_flow_ops
from tensorflow.python.ops import gen_lookup_ops
from tensorflow.python.ops import lookup_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.training.saver import BaseSaverBuilder


class _MutableDenseHashTable(lookup_ops.LookupInterface):
  """Copy of tf.contrib.lookup.MutableDenseHashTable."""

  # TODO(b/118148303): Swap this with the core version
  def __init__(self,
               key_dtype,
               value_dtype,
               default_value,
               empty_key,
               deleted_key,
               initial_num_buckets=None,
               shared_name=None,
               name="MutableDenseHashTable",
               checkpoint=True):
    """Creates an empty `_MutableDenseHashTable` object.

    Creates a table, the type of its keys and values are specified by key_dtype
    and value_dtype, respectively.

    Args:
      key_dtype: the type of the key tensors.
      value_dtype: the type of the value tensors.
      default_value: The value to use if a key is missing in the table.
      empty_key: the key to use to represent empty buckets internally. Must not
        be used in insert, remove or lookup operations.
      deleted_key: the key to use to represent deleted buckets internally. Must
        not be used in insert, remove or lookup operations and be different from
        the empty_key.
      initial_num_buckets: the initial number of buckets.
      shared_name: If non-empty, this table will be shared under
        the given name across multiple sessions.
      name: A name for the operation (optional).
      checkpoint: if True, the contents of the table are saved to and restored
        from checkpoints. If `shared_name` is empty for a checkpointed table, it
        is shared using the table node name.

    Returns:
      A `_MutableDenseHashTable` object.

    Raises:
      ValueError: If checkpoint is True and no name was specified.
    """
    self._default_value = ops.convert_to_tensor(
        default_value, dtype=value_dtype, name="default_value")
    self._key_dtype = key_dtype
    self._value_dtype = value_dtype
    self._initial_num_buckets = initial_num_buckets
    self._value_shape = self._default_value.get_shape()
    self._checkpoint = checkpoint
    self._name = name

    self._empty_key = ops.convert_to_tensor(
        empty_key, dtype=key_dtype, name="empty_key")
    self._deleted_key = ops.convert_to_tensor(
        deleted_key, dtype=key_dtype, name="deleted_key")
    if context.executing_eagerly() and shared_name is None:
      # TODO(allenl): This will leak memory due to kernel caching by the
      # shared_name attribute value (but is better than the alternative of
      # sharing everything by default when executing eagerly; hopefully creating
      # tables in a loop is uncommon).
      shared_name = "table_%d" % (ops.uid(),)
    self._shared_name = shared_name
    super(_MutableDenseHashTable, self).__init__(key_dtype, value_dtype)

    self._resource_handle = self.create_resource()
    if checkpoint:
      saveable = _MutableDenseHashTable._Saveable(self, name)
      if not context.executing_eagerly():
        ops.add_to_collection(ops.GraphKeys.SAVEABLE_OBJECTS, saveable)

  def create_resource(self):
    # The table must be shared if checkpointing is requested for multi-worker
    # training to work correctly. Use the node name if no shared_name has been
    # explicitly specified.
    use_node_name_sharing = self._checkpoint and self._shared_name is None
    table_ref = gen_lookup_ops.mutable_dense_hash_table_v2(
        empty_key=self._empty_key,
        deleted_key=self._deleted_key,
        shared_name=self._shared_name,
        use_node_name_sharing=use_node_name_sharing,
        value_dtype=self._value_dtype,
        value_shape=self._value_shape,
        initial_num_buckets=self._initial_num_buckets,
        name=self._name)
    if context.executing_eagerly():
      self._table_name = None
    else:
      self._table_name = table_ref.op.name.split("/")[-1]
    return table_ref

  @property
  def name(self):
    return self._table_name

  def size(self, name=None):
    """Compute the number of elements in this table.

    Args:
      name: A name for the operation (optional).

    Returns:
      A scalar tensor containing the number of elements in this table.
    """
    with ops.name_scope(name, "%s_Size" % self.name,
                        [self.resource_handle]) as name:
      with ops.colocate_with(self.resource_handle):
        return gen_lookup_ops.lookup_table_size_v2(
            self.resource_handle, name=name)

  def lookup(self, keys, name=None):
    """Looks up `keys` in a table, outputs the corresponding values.

    The `default_value` is used for keys not present in the table.

    Args:
      keys: Keys to look up. Can be a tensor of any shape. Must match the
        table's key_dtype.
      name: A name for the operation (optional).

    Returns:
      A tensor containing the values in the same shape as `keys` using the
        table's value type.

    Raises:
      TypeError: when `keys` do not match the table data types.
    """
    with ops.name_scope(name, "%s_lookup_table_find" % self.name,
                        [self.resource_handle, keys]) as name:
      keys = ops.convert_to_tensor(keys, dtype=self._key_dtype, name="keys")
      with ops.colocate_with(self.resource_handle):
        values = gen_lookup_ops.lookup_table_find_v2(
            self.resource_handle, keys, self._default_value, name=name)

    return values

  def insert(self, keys, values, name=None):
    """Associates `keys` with `values`.

    Args:
      keys: Keys to insert. Can be a tensor of any shape. Must match the
        table's key type.
      values: Values to be associated with keys. Must be a tensor of the same
        shape as `keys` and match the table's value type.
      name: A name for the operation (optional).

    Returns:
      The created Operation.

    Raises:
      TypeError: when `keys` or `values` doesn't match the table data
        types.
    """
    with ops.name_scope(name, "%s_lookup_table_insert" % self.name,
                        [self.resource_handle, keys, values]) as name:
      keys = ops.convert_to_tensor(keys, dtype=self._key_dtype, name="keys")
      values = ops.convert_to_tensor(
          values, dtype=self._value_dtype, name="values")
      with ops.colocate_with(self.resource_handle):
        op = gen_lookup_ops.lookup_table_insert_v2(
            self.resource_handle, keys, values, name=name)
      return op

  def export(self, name=None):
    """Returns tensors of all keys and values in the table.

    Args:
      name: A name for the operation (optional).

    Returns:
      A pair of tensors with the first tensor containing all keys and the
        second tensors containing all values in the table.
    """
    with ops.name_scope(name, "%s_lookup_table_export_values" % self.name,
                        [self.resource_handle]) as name:
      with ops.colocate_with(self.resource_handle):
        exported_keys, exported_values = gen_lookup_ops.lookup_table_export_v2(
            self.resource_handle, self._key_dtype, self._value_dtype, name=name)

    return exported_keys, exported_values

  def _gather_saveables_for_checkpoint(self):
    """For object-based checkpointing."""
    return {"table": functools.partial(
        _MutableDenseHashTable._Saveable, table=self)}

  class _Saveable(BaseSaverBuilder.SaveableObject):
    """SaveableObject implementation for _MutableDenseHashTable."""

    def __init__(self, table, name):
      tensors = table.export()
      specs = [
          BaseSaverBuilder.SaveSpec(tensors[0], "", name + "-keys"),
          BaseSaverBuilder.SaveSpec(tensors[1], "", name + "-values")
      ]
      # pylint: disable=protected-access
      super(_MutableDenseHashTable._Saveable, self).__init__(table, specs, name)

    def restore(self, restored_tensors, restored_shapes):
      del restored_shapes  # unused
      # pylint: disable=protected-access
      with ops.colocate_with(self.op.resource_handle):
        return gen_lookup_ops.lookup_table_import_v2(
            self.op.resource_handle, restored_tensors[0], restored_tensors[1])


# TODO(rohanj): This should subclass Checkpointable and implement
# _gather_saveables_for_checkpoint.
class _ShardedMutableDenseHashTable(object):
  """A sharded version of _MutableDenseHashTable.

  It is designed to be interface compatible with LookupInterface and
  MutableDenseHashTable, with the exception of the export method, which is
  replaced by an export_sharded method.

  The _ShardedMutableDenseHashTable keeps `num_shards` _MutableDenseHashTable
  internally. The shard is computed via the modulo operation on the key.
  """

  def __init__(self,
               key_dtype,
               value_dtype,
               default_value,
               empty_key,
               deleted_key,
               num_shards=1,
               checkpoint=True,
               name="ShardedMutableHashTable"):
    self._key_dtype = key_dtype
    self._value_dtype = value_dtype
    with ops.name_scope(name, "sharded_mutable_hash_table") as scope:
      table_shards = []
      for i in range(num_shards):
        self._table_name = scope
        table_shards.append(
            _MutableDenseHashTable(
                key_dtype=key_dtype,
                value_dtype=value_dtype,
                default_value=default_value,
                empty_key=empty_key,
                deleted_key=deleted_key,
                checkpoint=checkpoint,
                name="%s-%d-of-%d" % (name, i + 1, num_shards)))
      self._table_shards = table_shards
      # TODO(andreasst): add a value_shape() method to LookupInterface
      # pylint: disable=protected-access
      self._value_shape = self._table_shards[0]._value_shape
      # pylint: enable=protected-access

  @property
  def name(self):
    return self._table_name

  @property
  def _num_shards(self):
    return len(self._table_shards)

  @property
  def table_shards(self):
    return self._table_shards

  def size(self, name=None):
    with ops.name_scope(name, "sharded_mutable_hash_table_size"):
      sizes = [
          self._table_shards[i].size() for i in range(self._num_shards)
      ]
      return math_ops.add_n(sizes)

  def _shard_indices(self, keys):
    key_shape = keys.get_shape()
    if key_shape.ndims > 1:
      # If keys are a matrix (i.e. a single key is a vector), we use the first
      # element of each key vector to determine the shard.
      keys = array_ops.slice(keys, [0, 0], [key_shape.dims[0].value, 1])
      keys = array_ops.reshape(keys, [-1])
    indices = math_ops.mod(math_ops.abs(keys), self._num_shards)
    return math_ops.cast(indices, dtypes.int32)

  def _check_keys(self, keys):
    if keys.get_shape().ndims != 1 and keys.get_shape().ndims != 2:
      raise ValueError("Expected a vector or matrix for keys, got %s." %
                       keys.get_shape())

  def lookup(self, keys, name=None):
    """Looks up `keys` in a table, outputs the corresponding values."""
    if keys.dtype.base_dtype != self._key_dtype:
      raise TypeError("Signature mismatch. Keys must be dtype %s, got %s." %
                      (self._key_dtype, keys.dtype))
    self._check_keys(keys)
    num_shards = self._num_shards
    if num_shards == 1:
      return self._table_shards[0].lookup(keys, name=name)

    shard_indices = self._shard_indices(keys)
    key_shards = data_flow_ops.dynamic_partition(keys, shard_indices,
                                                 num_shards)
    value_shards = [
        self._table_shards[i].lookup(key_shards[i], name=name)
        for i in range(num_shards)
    ]

    num_keys = array_ops.shape(keys)[0]
    original_indices = math_ops.range(num_keys)
    partitioned_indices = data_flow_ops.dynamic_partition(original_indices,
                                                          shard_indices,
                                                          num_shards)
    return data_flow_ops.dynamic_stitch(partitioned_indices, value_shards)

  def insert(self, keys, values, name=None):
    """Inserts `keys` in a table."""
    self._check_keys(keys)
    num_shards = self._num_shards
    if num_shards == 1:
      return self._table_shards[0].insert(keys, values, name=name)

    shard_indices = self._shard_indices(keys)
    key_shards = data_flow_ops.dynamic_partition(keys, shard_indices,
                                                 num_shards)
    value_shards = data_flow_ops.dynamic_partition(values, shard_indices,
                                                   num_shards)
    return_values = [
        self._table_shards[i].insert(key_shards[i], value_shards[i], name=name)
        for i in range(num_shards)
    ]

    return control_flow_ops.group(*return_values)

  def export_sharded(self, name=None):
    """Returns lists of the keys and values tensors in the sharded table.

    Args:
      name: name of the table.

    Returns:
      A pair of lists with the first list containing the key tensors and the
        second list containing the value tensors from each shard.
    """
    keys_list = []
    values_list = []
    for table_shard in self._table_shards:
      exported_keys, exported_values = table_shard.export(name=name)
      keys_list.append(exported_keys)
      values_list.append(exported_values)
    return keys_list, values_list
