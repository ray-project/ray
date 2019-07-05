"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_stats_accumulator_ops_py_wrap.cc
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
@tf_export('create_stats_accumulator_scalar')
def create_stats_accumulator_scalar(stats_accumulator_handle, stamp_token, name=None):
  r"""Creates a scalar stats accumulator.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the stats accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CreateStatsAccumulatorScalar", name, _ctx._post_execution_callbacks,
        stats_accumulator_handle, stamp_token)
      return _result
    except _core._FallbackException:
      try:
        return create_stats_accumulator_scalar_eager_fallback(
            stats_accumulator_handle, stamp_token, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_stats_accumulator_scalar, stats_accumulator_handle=stats_accumulator_handle,
                                               stamp_token=stamp_token,
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
        "CreateStatsAccumulatorScalar", stats_accumulator_handle=stats_accumulator_handle,
                                        stamp_token=stamp_token, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_stats_accumulator_scalar, stats_accumulator_handle=stats_accumulator_handle,
                                           stamp_token=stamp_token, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_stats_accumulator_scalar_eager_fallback(stats_accumulator_handle, stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_stats_accumulator_scalar
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  _inputs_flat = [stats_accumulator_handle, stamp_token]
  _attrs = None
  _result = _execute.execute(b"CreateStatsAccumulatorScalar", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateStatsAccumulatorScalar")(None)


@_dispatch.add_dispatch_list
@tf_export('create_stats_accumulator_tensor')
def create_stats_accumulator_tensor(stats_accumulator_handle, stamp_token, per_slot_gradient_shape, per_slot_hessian_shape, name=None):
  r"""Creates a tensor stats accumulator.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    per_slot_gradient_shape: A `Tensor` of type `int64`.
      a vector that defines the shape of gradients.
    per_slot_hessian_shape: A `Tensor` of type `int64`.
      a vector that defines the shape of hessians.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CreateStatsAccumulatorTensor", name, _ctx._post_execution_callbacks,
        stats_accumulator_handle, stamp_token, per_slot_gradient_shape,
        per_slot_hessian_shape)
      return _result
    except _core._FallbackException:
      try:
        return create_stats_accumulator_tensor_eager_fallback(
            stats_accumulator_handle, stamp_token, per_slot_gradient_shape,
            per_slot_hessian_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_stats_accumulator_tensor, stats_accumulator_handle=stats_accumulator_handle,
                                               stamp_token=stamp_token,
                                               per_slot_gradient_shape=per_slot_gradient_shape,
                                               per_slot_hessian_shape=per_slot_hessian_shape,
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
        "CreateStatsAccumulatorTensor", stats_accumulator_handle=stats_accumulator_handle,
                                        stamp_token=stamp_token,
                                        per_slot_gradient_shape=per_slot_gradient_shape,
                                        per_slot_hessian_shape=per_slot_hessian_shape,
                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_stats_accumulator_tensor, stats_accumulator_handle=stats_accumulator_handle,
                                           stamp_token=stamp_token,
                                           per_slot_gradient_shape=per_slot_gradient_shape,
                                           per_slot_hessian_shape=per_slot_hessian_shape,
                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_stats_accumulator_tensor_eager_fallback(stats_accumulator_handle, stamp_token, per_slot_gradient_shape, per_slot_hessian_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_stats_accumulator_tensor
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  per_slot_gradient_shape = _ops.convert_to_tensor(per_slot_gradient_shape, _dtypes.int64)
  per_slot_hessian_shape = _ops.convert_to_tensor(per_slot_hessian_shape, _dtypes.int64)
  _inputs_flat = [stats_accumulator_handle, stamp_token, per_slot_gradient_shape, per_slot_hessian_shape]
  _attrs = None
  _result = _execute.execute(b"CreateStatsAccumulatorTensor", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateStatsAccumulatorTensor")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_add')
def stats_accumulator_scalar_add(stats_accumulator_handles, stamp_token, partition_ids, feature_ids, gradients, hessians, name=None):
  r"""Updates the scalar stats accumulator.

  Args:
    stats_accumulator_handles: A list of at least 1 `Tensor` objects with type `resource`.
      A list of handles to the stats accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    partition_ids: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `int32`.
      A list of vectors of partition_ids.
    feature_ids: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `int64`.
      Rank 2 tensor of feature id and feature dimension ids.
    gradients: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `float32`.
      A list of vectors of gradients for each slot in
      <partition_id, feature_id, feature_dimension_id>.
    hessians: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `float32`.
      A list of vectors of hessians for each slot in
      <partition_id, feature_id, feature_dimension_id>.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarAdd", name, _ctx._post_execution_callbacks,
        stats_accumulator_handles, stamp_token, partition_ids, feature_ids,
        gradients, hessians)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_add_eager_fallback(
            stats_accumulator_handles, stamp_token, partition_ids,
            feature_ids, gradients, hessians, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_add, stats_accumulator_handles=stats_accumulator_handles,
                                            stamp_token=stamp_token,
                                            partition_ids=partition_ids,
                                            feature_ids=feature_ids,
                                            gradients=gradients,
                                            hessians=hessians, name=name)
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
  if not isinstance(stats_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_accumulator_handles' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % stats_accumulator_handles)
  _attr_num_resource_handles = len(stats_accumulator_handles)
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % partition_ids)
  if len(partition_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'partition_ids' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(partition_ids), _attr_num_resource_handles))
  if not isinstance(feature_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'feature_ids' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % feature_ids)
  if len(feature_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'feature_ids' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(feature_ids), _attr_num_resource_handles))
  if not isinstance(gradients, (list, tuple)):
    raise TypeError(
        "Expected list for 'gradients' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % gradients)
  if len(gradients) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'gradients' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(gradients), _attr_num_resource_handles))
  if not isinstance(hessians, (list, tuple)):
    raise TypeError(
        "Expected list for 'hessians' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % hessians)
  if len(hessians) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'hessians' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(hessians), _attr_num_resource_handles))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StatsAccumulatorScalarAdd", stats_accumulator_handles=stats_accumulator_handles,
                                     stamp_token=stamp_token,
                                     partition_ids=partition_ids,
                                     feature_ids=feature_ids,
                                     gradients=gradients, hessians=hessians,
                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_add, stats_accumulator_handles=stats_accumulator_handles,
                                        stamp_token=stamp_token,
                                        partition_ids=partition_ids,
                                        feature_ids=feature_ids,
                                        gradients=gradients,
                                        hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def stats_accumulator_scalar_add_eager_fallback(stats_accumulator_handles, stamp_token, partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_add
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(stats_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_accumulator_handles' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % stats_accumulator_handles)
  _attr_num_resource_handles = len(stats_accumulator_handles)
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % partition_ids)
  if len(partition_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'partition_ids' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(partition_ids), _attr_num_resource_handles))
  if not isinstance(feature_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'feature_ids' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % feature_ids)
  if len(feature_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'feature_ids' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(feature_ids), _attr_num_resource_handles))
  if not isinstance(gradients, (list, tuple)):
    raise TypeError(
        "Expected list for 'gradients' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % gradients)
  if len(gradients) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'gradients' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(gradients), _attr_num_resource_handles))
  if not isinstance(hessians, (list, tuple)):
    raise TypeError(
        "Expected list for 'hessians' argument to "
        "'stats_accumulator_scalar_add' Op, not %r." % hessians)
  if len(hessians) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'hessians' to 'stats_accumulator_scalar_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(hessians), _attr_num_resource_handles))
  stats_accumulator_handles = _ops.convert_n_to_tensor(stats_accumulator_handles, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  partition_ids = _ops.convert_n_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_n_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_n_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_n_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = list(stats_accumulator_handles) + [stamp_token] + list(partition_ids) + list(feature_ids) + list(gradients) + list(hessians)
  _attrs = ("num_resource_handles", _attr_num_resource_handles)
  _result = _execute.execute(b"StatsAccumulatorScalarAdd", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("StatsAccumulatorScalarAdd")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_deserialize')
def stats_accumulator_scalar_deserialize(stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians, name=None):
  r"""Resets the scalar stats accumulator with the serialized state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the stats accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    num_updates: A `Tensor` of type `int64`.
      Number of times stats were added to this accumulator since last
      flush.
    partition_ids: A `Tensor` of type `int32`. A vector of partition_ids.
    feature_ids: A `Tensor` of type `int64`.
      Rank 2 tensor of feature id and feature dimension ids.
    gradients: A `Tensor` of type `float32`.
      A vector of gradients for each slot in <partition_id, feature_id,
      feature_dimension_id>.
    hessians: A `Tensor` of type `float32`.
      A vector of hessians for each slot in <partition_id, feature_id,
      feature_dimension_id>
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarDeserialize", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle, stamp_token,
        num_updates, partition_ids, feature_ids, gradients, hessians)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_deserialize_eager_fallback(
            stats_accumulator_handle, stamp_token, num_updates, partition_ids,
            feature_ids, gradients, hessians, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_deserialize, stats_accumulator_handle=stats_accumulator_handle,
                                                    stamp_token=stamp_token,
                                                    num_updates=num_updates,
                                                    partition_ids=partition_ids,
                                                    feature_ids=feature_ids,
                                                    gradients=gradients,
                                                    hessians=hessians,
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
        "StatsAccumulatorScalarDeserialize", stats_accumulator_handle=stats_accumulator_handle,
                                             stamp_token=stamp_token,
                                             num_updates=num_updates,
                                             partition_ids=partition_ids,
                                             feature_ids=feature_ids,
                                             gradients=gradients,
                                             hessians=hessians, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_deserialize, stats_accumulator_handle=stats_accumulator_handle,
                                                stamp_token=stamp_token,
                                                num_updates=num_updates,
                                                partition_ids=partition_ids,
                                                feature_ids=feature_ids,
                                                gradients=gradients,
                                                hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def stats_accumulator_scalar_deserialize_eager_fallback(stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  num_updates = _ops.convert_to_tensor(num_updates, _dtypes.int64)
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = [stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorScalarDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("StatsAccumulatorScalarDeserialize")(None)


_stats_accumulator_scalar_flush_outputs = ["num_updates",
                                          "output_partition_ids",
                                          "output_feature_ids",
                                          "output_gradients",
                                          "output_hessians"]
_StatsAccumulatorScalarFlushOutput = _collections.namedtuple(
    "StatsAccumulatorScalarFlush", _stats_accumulator_scalar_flush_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_flush')
def stats_accumulator_scalar_flush(stats_accumulator_handle, stamp_token, next_stamp_token, name=None):
  r"""Flushes the scalar stats accumulator to output and resets the internal state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the stats accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token for the next iteration.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (num_updates, output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    num_updates: A `Tensor` of type `int64`. Number of times stats were added to this accumulator since last
          flush.
      output_partition_ids A vector of partition_ids for the slots.
    output_partition_ids: A `Tensor` of type `int32`.
    output_feature_ids: A `Tensor` of type `int64`. Rank 2 tensor of feature id and feature dimension ids.
    output_gradients: A `Tensor` of type `float32`. A vector of gradients, with a value for each slot
      in <output_partition_id, output_feature_id>.
    output_hessians: A `Tensor` of type `float32`. A vector of hessians, with a value for each slot
      in <output_partition_id, output_feature_id>.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarFlush", name, _ctx._post_execution_callbacks,
        stats_accumulator_handle, stamp_token, next_stamp_token)
      _result = _StatsAccumulatorScalarFlushOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_flush_eager_fallback(
            stats_accumulator_handle, stamp_token, next_stamp_token,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_flush, stats_accumulator_handle=stats_accumulator_handle,
                                              stamp_token=stamp_token,
                                              next_stamp_token=next_stamp_token,
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
        "StatsAccumulatorScalarFlush", stats_accumulator_handle=stats_accumulator_handle,
                                       stamp_token=stamp_token,
                                       next_stamp_token=next_stamp_token,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_flush, stats_accumulator_handle=stats_accumulator_handle,
                                          stamp_token=stamp_token,
                                          next_stamp_token=next_stamp_token,
                                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorScalarFlush", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarFlushOutput._make(_result)
  return _result



def stats_accumulator_scalar_flush_eager_fallback(stats_accumulator_handle, stamp_token, next_stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_flush
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  _inputs_flat = [stats_accumulator_handle, stamp_token, next_stamp_token]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorScalarFlush", 5,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorScalarFlush", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarFlushOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorScalarFlush")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_is_initialized')
def stats_accumulator_scalar_is_initialized(stats_accumulator_handle, name=None):
  r"""Checks whether a stats accumulator has been initialized.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarIsInitialized", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_is_initialized_eager_fallback(
            stats_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_is_initialized, stats_accumulator_handle=stats_accumulator_handle,
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
        "StatsAccumulatorScalarIsInitialized", stats_accumulator_handle=stats_accumulator_handle,
                                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_is_initialized, stats_accumulator_handle=stats_accumulator_handle,
                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorScalarIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stats_accumulator_scalar_is_initialized_eager_fallback(stats_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_is_initialized
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  _inputs_flat = [stats_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorScalarIsInitialized", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorScalarIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("StatsAccumulatorScalarIsInitialized")(None)


_stats_accumulator_scalar_make_summary_outputs = ["output_partition_ids",
                                                 "output_feature_ids",
                                                 "output_gradients",
                                                 "output_hessians"]
_StatsAccumulatorScalarMakeSummaryOutput = _collections.namedtuple(
    "StatsAccumulatorScalarMakeSummary",
    _stats_accumulator_scalar_make_summary_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_make_summary')
def stats_accumulator_scalar_make_summary(partition_ids, feature_ids, gradients, hessians, name=None):
  r"""TODO: add doc.

  Args:
    partition_ids: A `Tensor` of type `int32`.
    feature_ids: A `Tensor` of type `int64`.
    gradients: A `Tensor` of type `float32`.
    hessians: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    output_partition_ids: A `Tensor` of type `int32`.
    output_feature_ids: A `Tensor` of type `int64`.
    output_gradients: A `Tensor` of type `float32`.
    output_hessians: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarMakeSummary", name,
        _ctx._post_execution_callbacks, partition_ids, feature_ids, gradients,
        hessians)
      _result = _StatsAccumulatorScalarMakeSummaryOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_make_summary_eager_fallback(
            partition_ids, feature_ids, gradients, hessians, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_make_summary, partition_ids=partition_ids,
                                                     feature_ids=feature_ids,
                                                     gradients=gradients,
                                                     hessians=hessians,
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
        "StatsAccumulatorScalarMakeSummary", partition_ids=partition_ids,
                                             feature_ids=feature_ids,
                                             gradients=gradients,
                                             hessians=hessians, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_make_summary, partition_ids=partition_ids,
                                                 feature_ids=feature_ids,
                                                 gradients=gradients,
                                                 hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorScalarMakeSummary", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarMakeSummaryOutput._make(_result)
  return _result



def stats_accumulator_scalar_make_summary_eager_fallback(partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_make_summary
  """
  _ctx = ctx if ctx else _context.context()
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = [partition_ids, feature_ids, gradients, hessians]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorScalarMakeSummary", 4,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorScalarMakeSummary", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarMakeSummaryOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorScalarMakeSummary")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_resource_handle_op')
def stats_accumulator_scalar_resource_handle_op(container="", shared_name="", name=None):
  r"""TODO: add doc.

  Args:
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
        "StatsAccumulatorScalarResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_resource_handle_op, container=container,
                                                           shared_name=shared_name,
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
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StatsAccumulatorScalarResourceHandleOp", container=container,
                                                  shared_name=shared_name,
                                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_resource_handle_op, container=container,
                                                       shared_name=shared_name,
                                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "StatsAccumulatorScalarResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stats_accumulator_scalar_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_resource_handle_op
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name)
  _result = _execute.execute(b"StatsAccumulatorScalarResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorScalarResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("StatsAccumulatorScalarResourceHandleOp")(None)


_stats_accumulator_scalar_serialize_outputs = ["stamp_token", "num_updates",
                                              "output_partition_ids",
                                              "output_feature_ids",
                                              "output_gradients",
                                              "output_hessians"]
_StatsAccumulatorScalarSerializeOutput = _collections.namedtuple(
    "StatsAccumulatorScalarSerialize",
    _stats_accumulator_scalar_serialize_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_scalar_serialize')
def stats_accumulator_scalar_serialize(stats_accumulator_handle, name=None):
  r"""Serializes the scalar stats accumulator state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the stats accumulator.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, num_updates, output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    stamp_token: A `Tensor` of type `int64`. The current stamp token for the resource.
    num_updates: A `Tensor` of type `int64`. Number of times stats were added to this accumulator since last
          flush.
      output_partition_ids A vector of partition_ids for the slots.
    output_partition_ids: A `Tensor` of type `int32`.
    output_feature_ids: A `Tensor` of type `int64`. Rank 2 tensor of feature id and feature dimension ids.
    output_gradients: A `Tensor` of type `float32`. A vector of gradients, with a value for each slot
      in <output_partition_id, output_feature_id>.
    output_hessians: A `Tensor` of type `float32`. A vector of hessians, with a value for each slot
      in <output_partition_id, output_feature_id>.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorScalarSerialize", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle)
      _result = _StatsAccumulatorScalarSerializeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_scalar_serialize_eager_fallback(
            stats_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_scalar_serialize, stats_accumulator_handle=stats_accumulator_handle,
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
        "StatsAccumulatorScalarSerialize", stats_accumulator_handle=stats_accumulator_handle,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_scalar_serialize, stats_accumulator_handle=stats_accumulator_handle,
                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorScalarSerialize", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarSerializeOutput._make(_result)
  return _result



def stats_accumulator_scalar_serialize_eager_fallback(stats_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_scalar_serialize
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  _inputs_flat = [stats_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorScalarSerialize", 6,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorScalarSerialize", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorScalarSerializeOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorScalarSerialize")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_add')
def stats_accumulator_tensor_add(stats_accumulator_handles, stamp_token, partition_ids, feature_ids, gradients, hessians, name=None):
  r"""Updates the tensor stats accumulator.

  Args:
    stats_accumulator_handles: A list of at least 1 `Tensor` objects with type `resource`.
      A list of handles to the stats accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    partition_ids: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `int32`.
      A list of vectors of partition_ids.
    feature_ids: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `int64`.
      Rank 2 tensor of feature id and feature dimension ids.
    gradients: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `float32`.
      A list of vectors of gradients for each slot in
      <partition_id, feature_id, feature_dimension_id>.
    hessians: A list with the same length as `stats_accumulator_handles` of `Tensor` objects with type `float32`.
      A list of vectors of hessians for each slot in
      <partition_id, feature_id, feature_dimension_id>.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorAdd", name, _ctx._post_execution_callbacks,
        stats_accumulator_handles, stamp_token, partition_ids, feature_ids,
        gradients, hessians)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_add_eager_fallback(
            stats_accumulator_handles, stamp_token, partition_ids,
            feature_ids, gradients, hessians, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_add, stats_accumulator_handles=stats_accumulator_handles,
                                            stamp_token=stamp_token,
                                            partition_ids=partition_ids,
                                            feature_ids=feature_ids,
                                            gradients=gradients,
                                            hessians=hessians, name=name)
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
  if not isinstance(stats_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_accumulator_handles' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % stats_accumulator_handles)
  _attr_num_resource_handles = len(stats_accumulator_handles)
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % partition_ids)
  if len(partition_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'partition_ids' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(partition_ids), _attr_num_resource_handles))
  if not isinstance(feature_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'feature_ids' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % feature_ids)
  if len(feature_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'feature_ids' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(feature_ids), _attr_num_resource_handles))
  if not isinstance(gradients, (list, tuple)):
    raise TypeError(
        "Expected list for 'gradients' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % gradients)
  if len(gradients) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'gradients' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(gradients), _attr_num_resource_handles))
  if not isinstance(hessians, (list, tuple)):
    raise TypeError(
        "Expected list for 'hessians' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % hessians)
  if len(hessians) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'hessians' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(hessians), _attr_num_resource_handles))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StatsAccumulatorTensorAdd", stats_accumulator_handles=stats_accumulator_handles,
                                     stamp_token=stamp_token,
                                     partition_ids=partition_ids,
                                     feature_ids=feature_ids,
                                     gradients=gradients, hessians=hessians,
                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_add, stats_accumulator_handles=stats_accumulator_handles,
                                        stamp_token=stamp_token,
                                        partition_ids=partition_ids,
                                        feature_ids=feature_ids,
                                        gradients=gradients,
                                        hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def stats_accumulator_tensor_add_eager_fallback(stats_accumulator_handles, stamp_token, partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_add
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(stats_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_accumulator_handles' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % stats_accumulator_handles)
  _attr_num_resource_handles = len(stats_accumulator_handles)
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % partition_ids)
  if len(partition_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'partition_ids' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(partition_ids), _attr_num_resource_handles))
  if not isinstance(feature_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'feature_ids' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % feature_ids)
  if len(feature_ids) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'feature_ids' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(feature_ids), _attr_num_resource_handles))
  if not isinstance(gradients, (list, tuple)):
    raise TypeError(
        "Expected list for 'gradients' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % gradients)
  if len(gradients) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'gradients' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(gradients), _attr_num_resource_handles))
  if not isinstance(hessians, (list, tuple)):
    raise TypeError(
        "Expected list for 'hessians' argument to "
        "'stats_accumulator_tensor_add' Op, not %r." % hessians)
  if len(hessians) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'hessians' to 'stats_accumulator_tensor_add' Op with length %d "
        "must match length %d of argument 'stats_accumulator_handles'." %
        (len(hessians), _attr_num_resource_handles))
  stats_accumulator_handles = _ops.convert_n_to_tensor(stats_accumulator_handles, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  partition_ids = _ops.convert_n_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_n_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_n_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_n_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = list(stats_accumulator_handles) + [stamp_token] + list(partition_ids) + list(feature_ids) + list(gradients) + list(hessians)
  _attrs = ("num_resource_handles", _attr_num_resource_handles)
  _result = _execute.execute(b"StatsAccumulatorTensorAdd", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("StatsAccumulatorTensorAdd")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_deserialize')
def stats_accumulator_tensor_deserialize(stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians, name=None):
  r"""Resets the tensor stats accumulator with the serialized state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    num_updates: A `Tensor` of type `int64`.
      Number of times stats were added to this accumulator since last
      flush.
    partition_ids: A `Tensor` of type `int32`. A vector of partition_ids.
    feature_ids: A `Tensor` of type `int64`.
      Rank 2 tensor of feature id and feature dimension ids.
    gradients: A `Tensor` of type `float32`.
      A vector of gradients for each slot in <partition_id, feature_id,
      feature_dimension_id>
    hessians: A `Tensor` of type `float32`.
      A vector of hessians for each slot in <partition_id, feature_id,
      feature_dimension_id>.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorDeserialize", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle, stamp_token,
        num_updates, partition_ids, feature_ids, gradients, hessians)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_deserialize_eager_fallback(
            stats_accumulator_handle, stamp_token, num_updates, partition_ids,
            feature_ids, gradients, hessians, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_deserialize, stats_accumulator_handle=stats_accumulator_handle,
                                                    stamp_token=stamp_token,
                                                    num_updates=num_updates,
                                                    partition_ids=partition_ids,
                                                    feature_ids=feature_ids,
                                                    gradients=gradients,
                                                    hessians=hessians,
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
        "StatsAccumulatorTensorDeserialize", stats_accumulator_handle=stats_accumulator_handle,
                                             stamp_token=stamp_token,
                                             num_updates=num_updates,
                                             partition_ids=partition_ids,
                                             feature_ids=feature_ids,
                                             gradients=gradients,
                                             hessians=hessians, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_deserialize, stats_accumulator_handle=stats_accumulator_handle,
                                                stamp_token=stamp_token,
                                                num_updates=num_updates,
                                                partition_ids=partition_ids,
                                                feature_ids=feature_ids,
                                                gradients=gradients,
                                                hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def stats_accumulator_tensor_deserialize_eager_fallback(stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  num_updates = _ops.convert_to_tensor(num_updates, _dtypes.int64)
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = [stats_accumulator_handle, stamp_token, num_updates, partition_ids, feature_ids, gradients, hessians]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorTensorDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("StatsAccumulatorTensorDeserialize")(None)


_stats_accumulator_tensor_flush_outputs = ["num_updates",
                                          "output_partition_ids",
                                          "output_feature_ids",
                                          "output_gradients",
                                          "output_hessians"]
_StatsAccumulatorTensorFlushOutput = _collections.namedtuple(
    "StatsAccumulatorTensorFlush", _stats_accumulator_tensor_flush_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_flush')
def stats_accumulator_tensor_flush(stats_accumulator_handle, stamp_token, next_stamp_token, name=None):
  r"""Flushes the stats accumulator to output and resets the internal state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token to be used for the next iteration.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (num_updates, output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    num_updates: A `Tensor` of type `int64`. Number of times stats were added to this accumulator since last
      flush.
    output_partition_ids: A `Tensor` of type `int32`. A vector of partition_ids for the slots.
    output_feature_ids: A `Tensor` of type `int64`. Rank 2 tensor of feature id and feature dimension ids.
    output_gradients: A `Tensor` of type `float32`. A tensor of gradients, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>.
    output_hessians: A `Tensor` of type `float32`. A tensor of hessians, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>>.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorFlush", name, _ctx._post_execution_callbacks,
        stats_accumulator_handle, stamp_token, next_stamp_token)
      _result = _StatsAccumulatorTensorFlushOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_flush_eager_fallback(
            stats_accumulator_handle, stamp_token, next_stamp_token,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_flush, stats_accumulator_handle=stats_accumulator_handle,
                                              stamp_token=stamp_token,
                                              next_stamp_token=next_stamp_token,
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
        "StatsAccumulatorTensorFlush", stats_accumulator_handle=stats_accumulator_handle,
                                       stamp_token=stamp_token,
                                       next_stamp_token=next_stamp_token,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_flush, stats_accumulator_handle=stats_accumulator_handle,
                                          stamp_token=stamp_token,
                                          next_stamp_token=next_stamp_token,
                                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorTensorFlush", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorFlushOutput._make(_result)
  return _result



def stats_accumulator_tensor_flush_eager_fallback(stats_accumulator_handle, stamp_token, next_stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_flush
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  _inputs_flat = [stats_accumulator_handle, stamp_token, next_stamp_token]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorTensorFlush", 5,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorTensorFlush", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorFlushOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorTensorFlush")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_is_initialized')
def stats_accumulator_tensor_is_initialized(stats_accumulator_handle, name=None):
  r"""Checks whether a tensor stats accumulator has been initialized.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorIsInitialized", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_is_initialized_eager_fallback(
            stats_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_is_initialized, stats_accumulator_handle=stats_accumulator_handle,
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
        "StatsAccumulatorTensorIsInitialized", stats_accumulator_handle=stats_accumulator_handle,
                                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_is_initialized, stats_accumulator_handle=stats_accumulator_handle,
                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorTensorIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stats_accumulator_tensor_is_initialized_eager_fallback(stats_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_is_initialized
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  _inputs_flat = [stats_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorTensorIsInitialized", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorTensorIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("StatsAccumulatorTensorIsInitialized")(None)


_stats_accumulator_tensor_make_summary_outputs = ["output_partition_ids",
                                                 "output_feature_ids",
                                                 "output_gradients",
                                                 "output_hessians"]
_StatsAccumulatorTensorMakeSummaryOutput = _collections.namedtuple(
    "StatsAccumulatorTensorMakeSummary",
    _stats_accumulator_tensor_make_summary_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_make_summary')
def stats_accumulator_tensor_make_summary(partition_ids, feature_ids, gradients, hessians, name=None):
  r"""Summarizes the stats by summing the <gradients, hessians> that are for the same

  <partition_id, feature_id, feature_dimension_id>.

  Args:
    partition_ids: A `Tensor` of type `int32`. A vector of partition_ids.
    feature_ids: A `Tensor` of type `int64`.
      Rank 2 tensor of feature id and feature dimension ids.
    gradients: A `Tensor` of type `float32`.
      A vector of gradients for each slot in <partition_id, feature_id,
      feature_dimension_id>.
    hessians: A `Tensor` of type `float32`.
      A vector of hessians for each slot in <partition_id, feature_id,
      feature_dimension_id>.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    output_partition_ids: A `Tensor` of type `int32`. A vector of partition_ids for the slots.
    output_feature_ids: A `Tensor` of type `int64`. A rank2 tensor of feature_ids and dimensions for the slots.
    output_gradients: A `Tensor` of type `float32`. A tensor of gradients, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>.
    output_hessians: A `Tensor` of type `float32`. A tensor of hessians, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorMakeSummary", name,
        _ctx._post_execution_callbacks, partition_ids, feature_ids, gradients,
        hessians)
      _result = _StatsAccumulatorTensorMakeSummaryOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_make_summary_eager_fallback(
            partition_ids, feature_ids, gradients, hessians, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_make_summary, partition_ids=partition_ids,
                                                     feature_ids=feature_ids,
                                                     gradients=gradients,
                                                     hessians=hessians,
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
        "StatsAccumulatorTensorMakeSummary", partition_ids=partition_ids,
                                             feature_ids=feature_ids,
                                             gradients=gradients,
                                             hessians=hessians, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_make_summary, partition_ids=partition_ids,
                                                 feature_ids=feature_ids,
                                                 gradients=gradients,
                                                 hessians=hessians, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorTensorMakeSummary", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorMakeSummaryOutput._make(_result)
  return _result



def stats_accumulator_tensor_make_summary_eager_fallback(partition_ids, feature_ids, gradients, hessians, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_make_summary
  """
  _ctx = ctx if ctx else _context.context()
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  _inputs_flat = [partition_ids, feature_ids, gradients, hessians]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorTensorMakeSummary", 4,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorTensorMakeSummary", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorMakeSummaryOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorTensorMakeSummary")(None)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_resource_handle_op')
def stats_accumulator_tensor_resource_handle_op(container="", shared_name="", name=None):
  r"""TODO: add doc.

  Args:
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
        "StatsAccumulatorTensorResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_resource_handle_op, container=container,
                                                           shared_name=shared_name,
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
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StatsAccumulatorTensorResourceHandleOp", container=container,
                                                  shared_name=shared_name,
                                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_resource_handle_op, container=container,
                                                       shared_name=shared_name,
                                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "StatsAccumulatorTensorResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stats_accumulator_tensor_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_resource_handle_op
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name)
  _result = _execute.execute(b"StatsAccumulatorTensorResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorTensorResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("StatsAccumulatorTensorResourceHandleOp")(None)


_stats_accumulator_tensor_serialize_outputs = ["stamp_token", "num_updates",
                                              "output_partition_ids",
                                              "output_feature_ids",
                                              "output_gradients",
                                              "output_hessians"]
_StatsAccumulatorTensorSerializeOutput = _collections.namedtuple(
    "StatsAccumulatorTensorSerialize",
    _stats_accumulator_tensor_serialize_outputs)


@_dispatch.add_dispatch_list
@tf_export('stats_accumulator_tensor_serialize')
def stats_accumulator_tensor_serialize(stats_accumulator_handle, name=None):
  r"""Serializes the scalar stats accumulator state.

  Args:
    stats_accumulator_handle: A `Tensor` of type `resource`.
      handle to the tree ensemble resource to be created.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, num_updates, output_partition_ids, output_feature_ids, output_gradients, output_hessians).

    stamp_token: A `Tensor` of type `int64`. Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    num_updates: A `Tensor` of type `int64`. Number of times stats were added to this accumulator since last
      flush.
    output_partition_ids: A `Tensor` of type `int32`. A vector of partition_ids for the slots.
    output_feature_ids: A `Tensor` of type `int64`. Rank 2 tensor of feature id and feature dimension ids.
    output_gradients: A `Tensor` of type `float32`. A tensor of gradients, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>.
    output_hessians: A `Tensor` of type `float32`. A tensor of hessians, first dimension matches slots
      in <partition_id, feature_id, feature_dimension_id>.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatsAccumulatorTensorSerialize", name,
        _ctx._post_execution_callbacks, stats_accumulator_handle)
      _result = _StatsAccumulatorTensorSerializeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return stats_accumulator_tensor_serialize_eager_fallback(
            stats_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stats_accumulator_tensor_serialize, stats_accumulator_handle=stats_accumulator_handle,
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
        "StatsAccumulatorTensorSerialize", stats_accumulator_handle=stats_accumulator_handle,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stats_accumulator_tensor_serialize, stats_accumulator_handle=stats_accumulator_handle,
                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StatsAccumulatorTensorSerialize", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorSerializeOutput._make(_result)
  return _result



def stats_accumulator_tensor_serialize_eager_fallback(stats_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stats_accumulator_tensor_serialize
  """
  _ctx = ctx if ctx else _context.context()
  stats_accumulator_handle = _ops.convert_to_tensor(stats_accumulator_handle, _dtypes.resource)
  _inputs_flat = [stats_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"StatsAccumulatorTensorSerialize", 6,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatsAccumulatorTensorSerialize", _inputs_flat, _attrs, _result, name)
  _result = _StatsAccumulatorTensorSerializeOutput._make(_result)
  return _result

_ops.RegisterShape("StatsAccumulatorTensorSerialize")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "CreateStatsAccumulatorScalar"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "CreateStatsAccumulatorTensor"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "per_slot_gradient_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "per_slot_hessian_shape"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorScalarAdd"
#   input_arg {
#     name: "stats_accumulator_handles"
#     type: DT_RESOURCE
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#     number_attr: "num_resource_handles"
#   }
#   attr {
#     name: "num_resource_handles"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorScalarDeserialize"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorScalarFlush"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "next_stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorScalarIsInitialized"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorScalarMakeSummary"
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "StatsAccumulatorScalarResourceHandleOp"
#   output_arg {
#     name: "resource"
#     type: DT_RESOURCE
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
#   name: "StatsAccumulatorScalarSerialize"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorTensorAdd"
#   input_arg {
#     name: "stats_accumulator_handles"
#     type: DT_RESOURCE
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#     number_attr: "num_resource_handles"
#   }
#   attr {
#     name: "num_resource_handles"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorTensorDeserialize"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorTensorFlush"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "next_stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorTensorIsInitialized"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "StatsAccumulatorTensorMakeSummary"
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "StatsAccumulatorTensorResourceHandleOp"
#   output_arg {
#     name: "resource"
#     type: DT_RESOURCE
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
#   name: "StatsAccumulatorTensorSerialize"
#   input_arg {
#     name: "stats_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_updates"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_feature_ids"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_gradients"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_hessians"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nP\n\034CreateStatsAccumulatorScalar\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\210\001\001\n\211\001\n\034CreateStatsAccumulatorTensor\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\033\n\027per_slot_gradient_shape\030\t\022\032\n\026per_slot_hessian_shape\030\t\210\001\001\n\236\002\n\031StatsAccumulatorScalarAdd\0223\n\031stats_accumulator_handles\030\024*\024num_resource_handles\022\017\n\013stamp_token\030\t\022\'\n\rpartition_ids\030\003*\024num_resource_handles\022%\n\013feature_ids\030\t*\024num_resource_handles\022#\n\tgradients\030\001*\024num_resource_handles\022\"\n\010hessians\030\001*\024num_resource_handles\"\037\n\024num_resource_handles\022\003int(\0010\001\210\001\001\n\247\001\n!StatsAccumulatorScalarDeserialize\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\017\n\013num_updates\030\t\022\021\n\rpartition_ids\030\003\022\017\n\013feature_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\210\001\001\n\323\001\n\033StatsAccumulatorScalarFlush\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\032\017\n\013num_updates\030\t\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\210\001\001\nZ\n#StatsAccumulatorScalarIsInitialized\022\034\n\030stats_accumulator_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\n\301\001\n!StatsAccumulatorScalarMakeSummary\022\021\n\rpartition_ids\030\003\022\017\n\013feature_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\nm\n&StatsAccumulatorScalarResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\301\001\n\037StatsAccumulatorScalarSerialize\022\034\n\030stats_accumulator_handle\030\024\032\017\n\013stamp_token\030\t\032\017\n\013num_updates\030\t\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\210\001\001\n\236\002\n\031StatsAccumulatorTensorAdd\0223\n\031stats_accumulator_handles\030\024*\024num_resource_handles\022\017\n\013stamp_token\030\t\022\'\n\rpartition_ids\030\003*\024num_resource_handles\022%\n\013feature_ids\030\t*\024num_resource_handles\022#\n\tgradients\030\001*\024num_resource_handles\022\"\n\010hessians\030\001*\024num_resource_handles\"\037\n\024num_resource_handles\022\003int(\0010\001\210\001\001\n\247\001\n!StatsAccumulatorTensorDeserialize\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\017\n\013num_updates\030\t\022\021\n\rpartition_ids\030\003\022\017\n\013feature_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\210\001\001\n\323\001\n\033StatsAccumulatorTensorFlush\022\034\n\030stats_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\032\017\n\013num_updates\030\t\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\210\001\001\nZ\n#StatsAccumulatorTensorIsInitialized\022\034\n\030stats_accumulator_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\n\301\001\n!StatsAccumulatorTensorMakeSummary\022\021\n\rpartition_ids\030\003\022\017\n\013feature_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\nm\n&StatsAccumulatorTensorResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\301\001\n\037StatsAccumulatorTensorSerialize\022\034\n\030stats_accumulator_handle\030\024\032\017\n\013stamp_token\030\t\032\017\n\013num_updates\030\t\032\030\n\024output_partition_ids\030\003\032\026\n\022output_feature_ids\030\t\032\024\n\020output_gradients\030\001\032\023\n\017output_hessians\030\001\210\001\001")
