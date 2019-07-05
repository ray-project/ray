"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: bigtable_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.deprecation import deprecated_endpoints
from tensorflow.python.util import dispatch as _dispatch
from tensorflow.python.util.tf_export import tf_export


@_dispatch.add_dispatch_list
@tf_export('bigtable_client')
def bigtable_client(project_id, instance_id, connection_pool_size, max_receive_message_size=-1, container="", shared_name="", name=None):
  r"""TODO: add doc.

  Args:
    project_id: A `string`.
    instance_id: A `string`.
    connection_pool_size: An `int`.
    max_receive_message_size: An optional `int`. Defaults to `-1`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableClient", name, _ctx._post_execution_callbacks, "project_id",
        project_id, "instance_id", instance_id, "connection_pool_size",
        connection_pool_size, "max_receive_message_size",
        max_receive_message_size, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_client_eager_fallback(
            project_id=project_id, instance_id=instance_id,
            connection_pool_size=connection_pool_size,
            max_receive_message_size=max_receive_message_size,
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_client, project_id=project_id, instance_id=instance_id,
                               connection_pool_size=connection_pool_size,
                               max_receive_message_size=max_receive_message_size,
                               container=container, shared_name=shared_name,
                               name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  project_id = _execute.make_str(project_id, "project_id")
  instance_id = _execute.make_str(instance_id, "instance_id")
  connection_pool_size = _execute.make_int(connection_pool_size, "connection_pool_size")
  if max_receive_message_size is None:
    max_receive_message_size = -1
  max_receive_message_size = _execute.make_int(max_receive_message_size, "max_receive_message_size")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableClient", project_id=project_id, instance_id=instance_id,
                          connection_pool_size=connection_pool_size,
                          max_receive_message_size=max_receive_message_size,
                          container=container, shared_name=shared_name,
                          name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_client, project_id=project_id, instance_id=instance_id,
                           connection_pool_size=connection_pool_size,
                           max_receive_message_size=max_receive_message_size,
                           container=container, shared_name=shared_name,
                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("project_id", _op.get_attr("project_id"), "instance_id",
            _op.get_attr("instance_id"), "connection_pool_size",
            _op.get_attr("connection_pool_size"), "max_receive_message_size",
            _op.get_attr("max_receive_message_size"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "BigtableClient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_client_eager_fallback(project_id, instance_id, connection_pool_size, max_receive_message_size=-1, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_client
  """
  _ctx = ctx if ctx else _context.context()
  project_id = _execute.make_str(project_id, "project_id")
  instance_id = _execute.make_str(instance_id, "instance_id")
  connection_pool_size = _execute.make_int(connection_pool_size, "connection_pool_size")
  if max_receive_message_size is None:
    max_receive_message_size = -1
  max_receive_message_size = _execute.make_int(max_receive_message_size, "max_receive_message_size")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("project_id", project_id, "instance_id", instance_id,
  "connection_pool_size", connection_pool_size, "max_receive_message_size",
  max_receive_message_size, "container", container, "shared_name",
  shared_name)
  _result = _execute.execute(b"BigtableClient", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BigtableClient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableClient")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_lookup_dataset')
def bigtable_lookup_dataset(keys_dataset, table, column_families, columns, name=None):
  r"""TODO: add doc.

  Args:
    keys_dataset: A `Tensor` of type `variant`.
    table: A `Tensor` of type `resource`.
    column_families: A `Tensor` of type `string`.
    columns: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableLookupDataset", name, _ctx._post_execution_callbacks,
        keys_dataset, table, column_families, columns)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_lookup_dataset_eager_fallback(
            keys_dataset, table, column_families, columns, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_lookup_dataset, keys_dataset=keys_dataset, table=table,
                                       column_families=column_families,
                                       columns=columns, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableLookupDataset", keys_dataset=keys_dataset, table=table,
                                 column_families=column_families,
                                 columns=columns, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_lookup_dataset, keys_dataset=keys_dataset, table=table,
                                   column_families=column_families,
                                   columns=columns, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtableLookupDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_lookup_dataset_eager_fallback(keys_dataset, table, column_families, columns, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_lookup_dataset
  """
  _ctx = ctx if ctx else _context.context()
  keys_dataset = _ops.convert_to_tensor(keys_dataset, _dtypes.variant)
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  column_families = _ops.convert_to_tensor(column_families, _dtypes.string)
  columns = _ops.convert_to_tensor(columns, _dtypes.string)
  _inputs_flat = [keys_dataset, table, column_families, columns]
  _attrs = None
  _result = _execute.execute(b"BigtableLookupDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BigtableLookupDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableLookupDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_prefix_key_dataset')
def bigtable_prefix_key_dataset(table, prefix, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    prefix: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtablePrefixKeyDataset", name, _ctx._post_execution_callbacks,
        table, prefix)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_prefix_key_dataset_eager_fallback(
            table, prefix, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_prefix_key_dataset, table=table, prefix=prefix,
                                           name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtablePrefixKeyDataset", table=table, prefix=prefix, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_prefix_key_dataset, table=table, prefix=prefix, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtablePrefixKeyDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_prefix_key_dataset_eager_fallback(table, prefix, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_prefix_key_dataset
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  prefix = _ops.convert_to_tensor(prefix, _dtypes.string)
  _inputs_flat = [table, prefix]
  _attrs = None
  _result = _execute.execute(b"BigtablePrefixKeyDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BigtablePrefixKeyDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtablePrefixKeyDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_range_key_dataset')
def bigtable_range_key_dataset(table, start_key, end_key, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    start_key: A `Tensor` of type `string`.
    end_key: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableRangeKeyDataset", name, _ctx._post_execution_callbacks,
        table, start_key, end_key)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_range_key_dataset_eager_fallback(
            table, start_key, end_key, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_range_key_dataset, table=table, start_key=start_key,
                                          end_key=end_key, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableRangeKeyDataset", table=table, start_key=start_key,
                                   end_key=end_key, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_range_key_dataset, table=table, start_key=start_key,
                                      end_key=end_key, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtableRangeKeyDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_range_key_dataset_eager_fallback(table, start_key, end_key, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_range_key_dataset
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  start_key = _ops.convert_to_tensor(start_key, _dtypes.string)
  end_key = _ops.convert_to_tensor(end_key, _dtypes.string)
  _inputs_flat = [table, start_key, end_key]
  _attrs = None
  _result = _execute.execute(b"BigtableRangeKeyDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BigtableRangeKeyDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableRangeKeyDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_sample_key_pairs_dataset')
def bigtable_sample_key_pairs_dataset(table, prefix, start_key, end_key, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    prefix: A `Tensor` of type `string`.
    start_key: A `Tensor` of type `string`.
    end_key: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableSampleKeyPairsDataset", name, _ctx._post_execution_callbacks,
        table, prefix, start_key, end_key)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_sample_key_pairs_dataset_eager_fallback(
            table, prefix, start_key, end_key, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_sample_key_pairs_dataset, table=table, prefix=prefix,
                                                 start_key=start_key,
                                                 end_key=end_key, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableSampleKeyPairsDataset", table=table, prefix=prefix,
                                         start_key=start_key, end_key=end_key,
                                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_sample_key_pairs_dataset, table=table, prefix=prefix,
                                             start_key=start_key,
                                             end_key=end_key, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtableSampleKeyPairsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_sample_key_pairs_dataset_eager_fallback(table, prefix, start_key, end_key, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_sample_key_pairs_dataset
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  prefix = _ops.convert_to_tensor(prefix, _dtypes.string)
  start_key = _ops.convert_to_tensor(start_key, _dtypes.string)
  end_key = _ops.convert_to_tensor(end_key, _dtypes.string)
  _inputs_flat = [table, prefix, start_key, end_key]
  _attrs = None
  _result = _execute.execute(b"BigtableSampleKeyPairsDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BigtableSampleKeyPairsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableSampleKeyPairsDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_sample_keys_dataset')
def bigtable_sample_keys_dataset(table, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableSampleKeysDataset", name, _ctx._post_execution_callbacks,
        table)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_sample_keys_dataset_eager_fallback(
            table, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_sample_keys_dataset, table=table, name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableSampleKeysDataset", table=table, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_sample_keys_dataset, table=table, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtableSampleKeysDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_sample_keys_dataset_eager_fallback(table, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_sample_keys_dataset
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  _inputs_flat = [table]
  _attrs = None
  _result = _execute.execute(b"BigtableSampleKeysDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BigtableSampleKeysDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableSampleKeysDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_scan_dataset')
def bigtable_scan_dataset(table, prefix, start_key, end_key, column_families, columns, probability, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    prefix: A `Tensor` of type `string`.
    start_key: A `Tensor` of type `string`.
    end_key: A `Tensor` of type `string`.
    column_families: A `Tensor` of type `string`.
    columns: A `Tensor` of type `string`.
    probability: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableScanDataset", name, _ctx._post_execution_callbacks, table,
        prefix, start_key, end_key, column_families, columns, probability)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_scan_dataset_eager_fallback(
            table, prefix, start_key, end_key, column_families, columns,
            probability, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_scan_dataset, table=table, prefix=prefix,
                                     start_key=start_key, end_key=end_key,
                                     column_families=column_families,
                                     columns=columns, probability=probability,
                                     name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableScanDataset", table=table, prefix=prefix,
                               start_key=start_key, end_key=end_key,
                               column_families=column_families,
                               columns=columns, probability=probability,
                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_scan_dataset, table=table, prefix=prefix,
                                 start_key=start_key, end_key=end_key,
                                 column_families=column_families,
                                 columns=columns, probability=probability,
                                 name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BigtableScanDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_scan_dataset_eager_fallback(table, prefix, start_key, end_key, column_families, columns, probability, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_scan_dataset
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  prefix = _ops.convert_to_tensor(prefix, _dtypes.string)
  start_key = _ops.convert_to_tensor(start_key, _dtypes.string)
  end_key = _ops.convert_to_tensor(end_key, _dtypes.string)
  column_families = _ops.convert_to_tensor(column_families, _dtypes.string)
  columns = _ops.convert_to_tensor(columns, _dtypes.string)
  probability = _ops.convert_to_tensor(probability, _dtypes.float32)
  _inputs_flat = [table, prefix, start_key, end_key, column_families, columns, probability]
  _attrs = None
  _result = _execute.execute(b"BigtableScanDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BigtableScanDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableScanDataset")(None)


@_dispatch.add_dispatch_list
@tf_export('bigtable_table')
def bigtable_table(client, table_name, container="", shared_name="", name=None):
  r"""TODO: add doc.

  Args:
    client: A `Tensor` of type `resource`.
    table_name: A `string`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BigtableTable", name, _ctx._post_execution_callbacks, client,
        "table_name", table_name, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return bigtable_table_eager_fallback(
            client, table_name=table_name, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bigtable_table, client=client, table_name=table_name,
                              container=container, shared_name=shared_name,
                              name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  table_name = _execute.make_str(table_name, "table_name")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigtableTable", client=client, table_name=table_name,
                         container=container, shared_name=shared_name,
                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bigtable_table, client=client, table_name=table_name,
                          container=container, shared_name=shared_name,
                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_name", _op.get_attr("table_name"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "BigtableTable", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bigtable_table_eager_fallback(client, table_name, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bigtable_table
  """
  _ctx = ctx if ctx else _context.context()
  table_name = _execute.make_str(table_name, "table_name")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  client = _ops.convert_to_tensor(client, _dtypes.resource)
  _inputs_flat = [client]
  _attrs = ("table_name", table_name, "container", container, "shared_name",
  shared_name)
  _result = _execute.execute(b"BigtableTable", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BigtableTable", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BigtableTable")(None)


@_dispatch.add_dispatch_list
@tf_export('dataset_to_bigtable')
def dataset_to_bigtable(table, input_dataset, column_families, columns, timestamp, name=None):
  r"""TODO: add doc.

  Args:
    table: A `Tensor` of type `resource`.
    input_dataset: A `Tensor` of type `variant`.
    column_families: A `Tensor` of type `string`.
    columns: A `Tensor` of type `string`.
    timestamp: A `Tensor` of type `int64`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DatasetToBigtable", name, _ctx._post_execution_callbacks, table,
        input_dataset, column_families, columns, timestamp)
      return _result
    except _core._FallbackException:
      try:
        return dataset_to_bigtable_eager_fallback(
            table, input_dataset, column_families, columns, timestamp,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              dataset_to_bigtable, table=table, input_dataset=input_dataset,
                                   column_families=column_families,
                                   columns=columns, timestamp=timestamp,
                                   name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DatasetToBigtable", table=table, input_dataset=input_dataset,
                             column_families=column_families, columns=columns,
                             timestamp=timestamp, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          dataset_to_bigtable, table=table, input_dataset=input_dataset,
                               column_families=column_families,
                               columns=columns, timestamp=timestamp,
                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def dataset_to_bigtable_eager_fallback(table, input_dataset, column_families, columns, timestamp, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dataset_to_bigtable
  """
  _ctx = ctx if ctx else _context.context()
  table = _ops.convert_to_tensor(table, _dtypes.resource)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  column_families = _ops.convert_to_tensor(column_families, _dtypes.string)
  columns = _ops.convert_to_tensor(columns, _dtypes.string)
  timestamp = _ops.convert_to_tensor(timestamp, _dtypes.int64)
  _inputs_flat = [table, input_dataset, column_families, columns, timestamp]
  _attrs = None
  _result = _execute.execute(b"DatasetToBigtable", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("DatasetToBigtable")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BigtableClient"
#   output_arg {
#     name: "client"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "project_id"
#     type: "string"
#   }
#   attr {
#     name: "instance_id"
#     type: "string"
#   }
#   attr {
#     name: "connection_pool_size"
#     type: "int"
#   }
#   attr {
#     name: "max_receive_message_size"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableLookupDataset"
#   input_arg {
#     name: "keys_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "column_families"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "columns"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtablePrefixKeyDataset"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "prefix"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableRangeKeyDataset"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "start_key"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "end_key"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableSampleKeyPairsDataset"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "prefix"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "start_key"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "end_key"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableSampleKeysDataset"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableScanDataset"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "prefix"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "start_key"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "end_key"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "column_families"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "columns"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "probability"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "BigtableTable"
#   input_arg {
#     name: "client"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "DatasetToBigtable"
#   input_arg {
#     name: "table"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "column_families"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "columns"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "timestamp"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\313\001\n\016BigtableClient\032\n\n\006client\030\024\"\024\n\nproject_id\022\006string\"\025\n\013instance_id\022\006string\"\033\n\024connection_pool_size\022\003int\",\n\030max_receive_message_size\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\ne\n\025BigtableLookupDataset\022\020\n\014keys_dataset\030\025\022\t\n\005table\030\024\022\023\n\017column_families\030\007\022\013\n\007columns\030\007\032\n\n\006handle\030\025\210\001\001\n@\n\030BigtablePrefixKeyDataset\022\t\n\005table\030\024\022\n\n\006prefix\030\007\032\n\n\006handle\030\025\210\001\001\nO\n\027BigtableRangeKeyDataset\022\t\n\005table\030\024\022\r\n\tstart_key\030\007\022\013\n\007end_key\030\007\032\n\n\006handle\030\025\210\001\001\na\n\035BigtableSampleKeyPairsDataset\022\t\n\005table\030\024\022\n\n\006prefix\030\007\022\r\n\tstart_key\030\007\022\013\n\007end_key\030\007\032\n\n\006handle\030\025\210\001\001\n5\n\031BigtableSampleKeysDataset\022\t\n\005table\030\024\032\n\n\006handle\030\025\210\001\001\n\212\001\n\023BigtableScanDataset\022\t\n\005table\030\024\022\n\n\006prefix\030\007\022\r\n\tstart_key\030\007\022\013\n\007end_key\030\007\022\023\n\017column_families\030\007\022\013\n\007columns\030\007\022\017\n\013probability\030\001\032\n\n\006handle\030\025\210\001\001\ns\n\rBigtableTable\022\n\n\006client\030\024\032\t\n\005table\030\024\"\024\n\ntable_name\022\006string\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\ne\n\021DatasetToBigtable\022\t\n\005table\030\024\022\021\n\rinput_dataset\030\025\022\023\n\017column_families\030\007\022\013\n\007columns\030\007\022\r\n\ttimestamp\030\t\210\001\001")
