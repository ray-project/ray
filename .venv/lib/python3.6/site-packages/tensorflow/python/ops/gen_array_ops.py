"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: array_ops.cc
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


def batch_matrix_band_part(input, num_lower, num_upper, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`.
    num_lower: A `Tensor` of type `int64`.
    num_upper: A `Tensor` of type `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixBandPart", name, _ctx._post_execution_callbacks, input,
        num_lower, num_upper)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_band_part_eager_fallback(
            input, num_lower, num_upper, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixBandPart", input=input, num_lower=num_lower,
                               num_upper=num_upper, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixBandPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_band_part_eager_fallback(input, num_lower, num_upper, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_band_part
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  num_lower = _ops.convert_to_tensor(num_lower, _dtypes.int64)
  num_upper = _ops.convert_to_tensor(num_upper, _dtypes.int64)
  _inputs_flat = [input, num_lower, num_upper]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchMatrixBandPart", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixBandPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_diag(diagonal, name=None):
  r"""TODO: add doc.

  Args:
    diagonal: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `diagonal`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixDiag", name, _ctx._post_execution_callbacks, diagonal)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_diag_eager_fallback(
            diagonal, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixDiag", diagonal=diagonal, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_diag_eager_fallback(diagonal, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_diag
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (diagonal,) = _execute.args_to_matching_eager([diagonal], _ctx)
  _inputs_flat = [diagonal]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchMatrixDiag", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_diag_part(input, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixDiagPart", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_diag_part_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixDiagPart", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixDiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_diag_part_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_diag_part
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchMatrixDiagPart", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixDiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_matrix_set_diag(input, diagonal, name=None):
  r"""TODO: add doc.

  Args:
    input: A `Tensor`.
    diagonal: A `Tensor`. Must have the same type as `input`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchMatrixSetDiag", name, _ctx._post_execution_callbacks, input,
        diagonal)
      return _result
    except _core._FallbackException:
      try:
        return batch_matrix_set_diag_eager_fallback(
            input, diagonal, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchMatrixSetDiag", input=input, diagonal=diagonal, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BatchMatrixSetDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_matrix_set_diag_eager_fallback(input, diagonal, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_matrix_set_diag
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, diagonal], _ctx)
  (input, diagonal) = _inputs_T
  _inputs_flat = [input, diagonal]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BatchMatrixSetDiag", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchMatrixSetDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_to_space(input, crops, block_size, name=None):
  r"""BatchToSpace for 4-D tensors of type T.

  This is a legacy version of the more general BatchToSpaceND.

  Rearranges (permutes) data from batch into blocks of spatial data, followed by
  cropping. This is the reverse transformation of SpaceToBatch. More specifically,
  this op outputs a copy of the input tensor where values from the `batch`
  dimension are moved in spatial blocks to the `height` and `width` dimensions,
  followed by cropping along the `height` and `width` dimensions.

  Args:
    input: A `Tensor`. 4-D tensor with shape
      `[batch*block_size*block_size, height_pad/block_size, width_pad/block_size,
        depth]`. Note that the batch size of the input tensor must be divisible by
      `block_size * block_size`.
    crops: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D tensor of non-negative integers with shape `[2, 2]`. It specifies
      how many elements to crop from the intermediate result across the spatial
      dimensions as follows:

          crops = [[crop_top, crop_bottom], [crop_left, crop_right]]
    block_size: An `int` that is `>= 2`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchToSpace",
        name, _ctx._post_execution_callbacks, input, crops, "block_size",
        block_size)
      return _result
    except _core._FallbackException:
      try:
        return batch_to_space_eager_fallback(
            input, crops, block_size=block_size, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  block_size = _execute.make_int(block_size, "block_size")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchToSpace", input=input, crops=crops, block_size=block_size,
                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "block_size", _op.get_attr("block_size"),
            "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "BatchToSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_to_space_eager_fallback(input, crops, block_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_to_space
  """
  _ctx = ctx if ctx else _context.context()
  block_size = _execute.make_int(block_size, "block_size")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (crops,) = _execute.args_to_matching_eager([crops], _ctx, _dtypes.int32)
  _inputs_flat = [input, crops]
  _attrs = ("T", _attr_T, "block_size", block_size, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"BatchToSpace", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchToSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export(v1=['batch_to_space_nd', 'manip.batch_to_space_nd'])
@deprecated_endpoints('batch_to_space_nd', 'manip.batch_to_space_nd')
def batch_to_space_nd(input, block_shape, crops, name=None):
  r"""BatchToSpace for N-D tensors of type T.

  This operation reshapes the "batch" dimension 0 into `M + 1` dimensions of shape
  `block_shape + [batch]`, interleaves these blocks back into the grid defined by
  the spatial dimensions `[1, ..., M]`, to obtain a result with the same rank as
  the input.  The spatial dimensions of this intermediate result are then
  optionally cropped according to `crops` to produce the output.  This is the
  reverse of SpaceToBatch.  See below for a precise description.

  Args:
    input: A `Tensor`.
      N-D with shape `input_shape = [batch] + spatial_shape + remaining_shape`,
      where spatial_shape has M dimensions.
    block_shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D with shape `[M]`, all values must be >= 1.
    crops: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D with shape `[M, 2]`, all values must be >= 0.
        `crops[i] = [crop_start, crop_end]` specifies the amount to crop from input
        dimension `i + 1`, which corresponds to spatial dimension `i`.  It is
        required that
        `crop_start[i] + crop_end[i] <= block_shape[i] * input_shape[i + 1]`.

      This operation is equivalent to the following steps:

      1. Reshape `input` to `reshaped` of shape:
           [block_shape[0], ..., block_shape[M-1],
            batch / prod(block_shape),
            input_shape[1], ..., input_shape[N-1]]

      2. Permute dimensions of `reshaped` to produce `permuted` of shape
           [batch / prod(block_shape),

            input_shape[1], block_shape[0],
            ...,
            input_shape[M], block_shape[M-1],

            input_shape[M+1], ..., input_shape[N-1]]

      3. Reshape `permuted` to produce `reshaped_permuted` of shape
           [batch / prod(block_shape),

            input_shape[1] * block_shape[0],
            ...,
            input_shape[M] * block_shape[M-1],

            input_shape[M+1],
            ...,
            input_shape[N-1]]

      4. Crop the start and end of dimensions `[1, ..., M]` of
         `reshaped_permuted` according to `crops` to produce the output of shape:
           [batch / prod(block_shape),

            input_shape[1] * block_shape[0] - crops[0,0] - crops[0,1],
            ...,
            input_shape[M] * block_shape[M-1] - crops[M-1,0] - crops[M-1,1],

            input_shape[M+1], ..., input_shape[N-1]]

      Some examples:

      (1) For the following input of shape `[4, 1, 1, 1]`, `block_shape = [2, 2]`, and
          `crops = [[0, 0], [0, 0]]`:

      ```
      [[[[1]]], [[[2]]], [[[3]]], [[[4]]]]
      ```

      The output tensor has shape `[1, 2, 2, 1]` and value:

      ```
      x = [[[[1], [2]], [[3], [4]]]]
      ```

      (2) For the following input of shape `[4, 1, 1, 3]`, `block_shape = [2, 2]`, and
          `crops = [[0, 0], [0, 0]]`:

      ```
      [[[1, 2, 3]], [[4, 5, 6]], [[7, 8, 9]], [[10, 11, 12]]]
      ```

      The output tensor has shape `[1, 2, 2, 3]` and value:

      ```
      x = [[[[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]]]]
      ```

      (3) For the following input of shape `[4, 2, 2, 1]`, `block_shape = [2, 2]`, and
          `crops = [[0, 0], [0, 0]]`:

      ```
      x = [[[[1], [3]], [[9], [11]]],
           [[[2], [4]], [[10], [12]]],
           [[[5], [7]], [[13], [15]]],
           [[[6], [8]], [[14], [16]]]]
      ```

      The output tensor has shape `[1, 4, 4, 1]` and value:

      ```
      x = [[[1],   [2],  [3],  [4]],
           [[5],   [6],  [7],  [8]],
           [[9],  [10], [11],  [12]],
           [[13], [14], [15],  [16]]]
      ```

      (4) For the following input of shape `[8, 1, 3, 1]`, `block_shape = [2, 2]`, and
          `crops = [[0, 0], [2, 0]]`:

      ```
      x = [[[[0], [1], [3]]], [[[0], [9], [11]]],
           [[[0], [2], [4]]], [[[0], [10], [12]]],
           [[[0], [5], [7]]], [[[0], [13], [15]]],
           [[[0], [6], [8]]], [[[0], [14], [16]]]]
      ```

      The output tensor has shape `[2, 2, 4, 1]` and value:

      ```
      x = [[[[1],   [2],  [3],  [4]],
            [[5],   [6],  [7],  [8]]],
           [[[9],  [10], [11],  [12]],
            [[13], [14], [15],  [16]]]]
      ```
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BatchToSpaceND", name, _ctx._post_execution_callbacks, input,
        block_shape, crops)
      return _result
    except _core._FallbackException:
      try:
        return batch_to_space_nd_eager_fallback(
            input, block_shape, crops, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              batch_to_space_nd, input=input, block_shape=block_shape,
                                 crops=crops, name=name)
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
        "BatchToSpaceND", input=input, block_shape=block_shape, crops=crops,
                          name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          batch_to_space_nd, input=input, block_shape=block_shape,
                             crops=crops, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tblock_shape",
            _op.get_attr("Tblock_shape"), "Tcrops", _op.get_attr("Tcrops"))
  _execute.record_gradient(
      "BatchToSpaceND", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_to_space_nd_eager_fallback(input, block_shape, crops, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_to_space_nd
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tblock_shape, (block_shape,) = _execute.args_to_matching_eager([block_shape], _ctx, _dtypes.int32)
  _attr_Tcrops, (crops,) = _execute.args_to_matching_eager([crops], _ctx, _dtypes.int32)
  _inputs_flat = [input, block_shape, crops]
  _attrs = ("T", _attr_T, "Tblock_shape", _attr_Tblock_shape, "Tcrops",
  _attr_Tcrops)
  _result = _execute.execute(b"BatchToSpaceND", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchToSpaceND", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('bitcast')
def bitcast(input, type, name=None):
  r"""Bitcasts a tensor from one type to another without copying data.

  Given a tensor `input`, this operation returns a tensor that has the same buffer
  data as `input` with datatype `type`.

  If the input datatype `T` is larger than the output datatype `type` then the
  shape changes from [...] to [..., sizeof(`T`)/sizeof(`type`)].

  If `T` is smaller than `type`, the operator requires that the rightmost
  dimension be equal to sizeof(`type`)/sizeof(`T`). The shape then goes from
  [..., sizeof(`type`)/sizeof(`T`)] to [...].

  *NOTE*: Bitcast is implemented as a low-level cast, so machines with different
  endian orderings will give different results.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int64`, `int32`, `uint8`, `uint16`, `uint32`, `uint64`, `int8`, `int16`, `complex64`, `complex128`, `qint8`, `quint8`, `qint16`, `quint16`, `qint32`.
    type: A `tf.DType` from: `tf.bfloat16, tf.half, tf.float32, tf.float64, tf.int64, tf.int32, tf.uint8, tf.uint16, tf.uint32, tf.uint64, tf.int8, tf.int16, tf.complex64, tf.complex128, tf.qint8, tf.quint8, tf.qint16, tf.quint16, tf.qint32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Bitcast",
        name, _ctx._post_execution_callbacks, input, "type", type)
      return _result
    except _core._FallbackException:
      try:
        return bitcast_eager_fallback(
            input, type=type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bitcast, input=input, type=type, name=name)
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
  type = _execute.make_type(type, "type")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Bitcast", input=input, type=type, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bitcast, input=input, type=type, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "type", _op.get_attr("type"))
  _execute.record_gradient(
      "Bitcast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bitcast_eager_fallback(input, type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bitcast
  """
  _ctx = ctx if ctx else _context.context()
  type = _execute.make_type(type, "type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "type", type)
  _result = _execute.execute(b"Bitcast", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Bitcast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def broadcast_args(s0, s1, name=None):
  r"""Return the shape of s0 op s1 with broadcast.

  Given `s0` and `s1`, tensors that represent shapes, compute `r0`, the
  broadcasted shape. `s0`, `s1` and `r0` are all integer vectors.

  Args:
    s0: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    s1: A `Tensor`. Must have the same type as `s0`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `s0`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BroadcastArgs", name, _ctx._post_execution_callbacks, s0, s1)
      return _result
    except _core._FallbackException:
      try:
        return broadcast_args_eager_fallback(
            s0, s1, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BroadcastArgs", s0=s0, s1=s1, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BroadcastArgs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def broadcast_args_eager_fallback(s0, s1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function broadcast_args
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([s0, s1], _ctx, _dtypes.int32)
  (s0, s1) = _inputs_T
  _inputs_flat = [s0, s1]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BroadcastArgs", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BroadcastArgs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_broadcast_gradient_args_outputs = ["r0", "r1"]
_BroadcastGradientArgsOutput = _collections.namedtuple(
    "BroadcastGradientArgs", _broadcast_gradient_args_outputs)


def broadcast_gradient_args(s0, s1, name=None):
  r"""Return the reduction indices for computing gradients of s0 op s1 with broadcast.

  This is typically used by gradient computations for a broadcasting operation.

  Args:
    s0: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    s1: A `Tensor`. Must have the same type as `s0`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (r0, r1).

    r0: A `Tensor`. Has the same type as `s0`.
    r1: A `Tensor`. Has the same type as `s0`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BroadcastGradientArgs", name, _ctx._post_execution_callbacks, s0, s1)
      _result = _BroadcastGradientArgsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return broadcast_gradient_args_eager_fallback(
            s0, s1, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BroadcastGradientArgs", s0=s0, s1=s1, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BroadcastGradientArgs", _inputs_flat, _attrs, _result, name)
  _result = _BroadcastGradientArgsOutput._make(_result)
  return _result



def broadcast_gradient_args_eager_fallback(s0, s1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function broadcast_gradient_args
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([s0, s1], _ctx, _dtypes.int32)
  (s0, s1) = _inputs_T
  _inputs_flat = [s0, s1]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BroadcastGradientArgs", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BroadcastGradientArgs", _inputs_flat, _attrs, _result, name)
  _result = _BroadcastGradientArgsOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('broadcast_to')
def broadcast_to(input, shape, name=None):
  r"""Broadcast an array for a compatible shape.

  Broadcasting is the process of making arrays to have compatible shapes
  for arithmetic operations. Two shapes are compatible if for each
  dimension pair they are either equal or one of them is one. When trying
  to broadcast a Tensor to a shape, it starts with the trailing dimensions,
  and works its way forward.

  For example,
  ```
  >>> x = tf.constant([1, 2, 3])
  >>> y = tf.broadcast_to(x, [3, 3])
  >>> sess.run(y)
  array([[1, 2, 3],
         [1, 2, 3],
         [1, 2, 3]], dtype=int32)
  ```
  In the above example, the input Tensor with the shape of `[1, 3]`
  is broadcasted to output Tensor with shape of `[3, 3]`.

  Args:
    input: A `Tensor`. A Tensor to broadcast.
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      An 1-D `int` Tensor. The shape of the desired output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BroadcastTo",
        name, _ctx._post_execution_callbacks, input, shape)
      return _result
    except _core._FallbackException:
      try:
        return broadcast_to_eager_fallback(
            input, shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              broadcast_to, input=input, shape=shape, name=name)
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
        "BroadcastTo", input=input, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          broadcast_to, input=input, shape=shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "BroadcastTo", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def broadcast_to_eager_fallback(input, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function broadcast_to
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tidx, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  _inputs_flat = [input, shape]
  _attrs = ("T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"BroadcastTo", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BroadcastTo", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('debugging.check_numerics', v1=['debugging.check_numerics', 'check_numerics'])
@deprecated_endpoints('check_numerics')
def check_numerics(tensor, message, name=None):
  r"""Checks a tensor for NaN and Inf values.

  When run, reports an `InvalidArgument` error if `tensor` has any values
  that are not a number (NaN) or infinity (Inf). Otherwise, passes `tensor` as-is.

  Args:
    tensor: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    message: A `string`. Prefix of the error message.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CheckNumerics", name, _ctx._post_execution_callbacks, tensor,
        "message", message)
      return _result
    except _core._FallbackException:
      try:
        return check_numerics_eager_fallback(
            tensor, message=message, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              check_numerics, tensor=tensor, message=message, name=name)
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
  message = _execute.make_str(message, "message")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "CheckNumerics", tensor=tensor, message=message, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          check_numerics, tensor=tensor, message=message, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "message", _op.get_attr("message"))
  _execute.record_gradient(
      "CheckNumerics", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def check_numerics_eager_fallback(tensor, message, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function check_numerics
  """
  _ctx = ctx if ctx else _context.context()
  message = _execute.make_str(message, "message")
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _inputs_flat = [tensor]
  _attrs = ("T", _attr_T, "message", message)
  _result = _execute.execute(b"CheckNumerics", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CheckNumerics", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def concat(concat_dim, values, name=None):
  r"""Concatenates tensors along one dimension.

  Args:
    concat_dim: A `Tensor` of type `int32`.
      0-D.  The dimension along which to concatenate.  Must be in the
      range [0, rank(values)).
    values: A list of at least 2 `Tensor` objects with the same type.
      The `N` Tensors to concatenate. Their ranks and types must match,
      and their sizes must match in all dimensions except `concat_dim`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Concat", name,
        _ctx._post_execution_callbacks, concat_dim, values)
      return _result
    except _core._FallbackException:
      try:
        return concat_eager_fallback(
            concat_dim, values, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'concat' Op, not %r." % values)
  _attr_N = len(values)
  _, _, _op = _op_def_lib._apply_op_helper(
        "Concat", concat_dim=concat_dim, values=values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "Concat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def concat_eager_fallback(concat_dim, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function concat
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'concat' Op, not %r." % values)
  _attr_N = len(values)
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  concat_dim = _ops.convert_to_tensor(concat_dim, _dtypes.int32)
  _inputs_flat = [concat_dim] + list(values)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"Concat", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Concat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def concat_offset(concat_dim, shape, name=None):
  r"""Computes offsets of concat inputs within its output.

  For example:

  ```
  # 'x' is [2, 2, 7]
  # 'y' is [2, 3, 7]
  # 'z' is [2, 5, 7]
  concat_offset(2, [x, y, z]) => [0, 0, 0], [0, 2, 0], [0, 5, 0]
  ```

  This is typically used by gradient computations for a concat operation.

  Args:
    concat_dim: A `Tensor` of type `int32`.
      The dimension along which to concatenate.
    shape: A list of at least 2 `Tensor` objects with type `int32`.
      The `N` int32 vectors representing shape of tensors being concatenated.
    name: A name for the operation (optional).

  Returns:
    A list with the same length as `shape` of `Tensor` objects with type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ConcatOffset",
        name, _ctx._post_execution_callbacks, concat_dim, shape)
      return _result
    except _core._FallbackException:
      try:
        return concat_offset_eager_fallback(
            concat_dim, shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'shape' argument to "
        "'concat_offset' Op, not %r." % shape)
  _attr_N = len(shape)
  _, _, _op = _op_def_lib._apply_op_helper(
        "ConcatOffset", concat_dim=concat_dim, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"))
  _execute.record_gradient(
      "ConcatOffset", _inputs_flat, _attrs, _result, name)
  return _result



def concat_offset_eager_fallback(concat_dim, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function concat_offset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'shape' argument to "
        "'concat_offset' Op, not %r." % shape)
  _attr_N = len(shape)
  concat_dim = _ops.convert_to_tensor(concat_dim, _dtypes.int32)
  shape = _ops.convert_n_to_tensor(shape, _dtypes.int32)
  _inputs_flat = [concat_dim] + list(shape)
  _attrs = ("N", _attr_N)
  _result = _execute.execute(b"ConcatOffset", _attr_N, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ConcatOffset", _inputs_flat, _attrs, _result, name)
  return _result


def concat_v2(values, axis, name=None):
  r"""Concatenates tensors along one dimension.

  Args:
    values: A list of at least 2 `Tensor` objects with the same type.
      List of `N` Tensors to concatenate. Their ranks and types must match,
      and their sizes must match in all dimensions except `concat_dim`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D.  The dimension along which to concatenate.  Must be in the
      range [-rank(values), rank(values)).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ConcatV2",
        name, _ctx._post_execution_callbacks, values, axis)
      return _result
    except _core._FallbackException:
      try:
        return concat_v2_eager_fallback(
            values, axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'concat_v2' Op, not %r." % values)
  _attr_N = len(values)
  _, _, _op = _op_def_lib._apply_op_helper(
        "ConcatV2", values=values, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"), "Tidx",
            _op.get_attr("Tidx"))
  _execute.record_gradient(
      "ConcatV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def concat_v2_eager_fallback(values, axis, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function concat_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'concat_v2' Op, not %r." % values)
  _attr_N = len(values)
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = list(values) + [axis]
  _attrs = ("N", _attr_N, "T", _attr_T, "Tidx", _attr_Tidx)
  _result = _execute.execute(b"ConcatV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ConcatV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def conjugate_transpose(x, perm, name=None):
  r"""Shuffle dimensions of x according to a permutation and conjugate the result.

  The output `y` has the same rank as `x`. The shapes of `x` and `y` satisfy:
    `y.shape[i] == x.shape[perm[i]] for i in [0, 1, ..., rank(x) - 1]`
    `y[i,j,k,...,s,t,u] == conj(x[perm[i], perm[j], perm[k],...,perm[s], perm[t], perm[u]])`

  Args:
    x: A `Tensor`.
    perm: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ConjugateTranspose", name, _ctx._post_execution_callbacks, x, perm)
      return _result
    except _core._FallbackException:
      try:
        return conjugate_transpose_eager_fallback(
            x, perm, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ConjugateTranspose", x=x, perm=perm, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tperm", _op.get_attr("Tperm"))
  _execute.record_gradient(
      "ConjugateTranspose", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conjugate_transpose_eager_fallback(x, perm, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function conjugate_transpose
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Tperm, (perm,) = _execute.args_to_matching_eager([perm], _ctx, _dtypes.int32)
  _inputs_flat = [x, perm]
  _attrs = ("T", _attr_T, "Tperm", _attr_Tperm)
  _result = _execute.execute(b"ConjugateTranspose", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ConjugateTranspose", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def const(value, dtype, name=None):
  r"""Returns a constant tensor.

  Args:
    value: A `tf.TensorProto`. Attr `value` is the tensor to return.
    dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Const", name,
        _ctx._post_execution_callbacks, "value", value, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return const_eager_fallback(
            value=value, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  value = _execute.make_tensor(value, "value")
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Const", value=value, dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("value", _op.get_attr("value"), "dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "Const", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def const_eager_fallback(value, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function const
  """
  _ctx = ctx if ctx else _context.context()
  value = _execute.make_tensor(value, "value")
  dtype = _execute.make_type(dtype, "dtype")
  _inputs_flat = []
  _attrs = ("value", value, "dtype", dtype)
  _result = _execute.execute(b"Const", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Const", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def debug_gradient_identity(input, name=None):
  r"""Identity op for gradient debugging.

  This op is hidden from public in Python. It is used by TensorFlow Debugger to
  register gradient tensors for gradient debugging.
  This op operates on non-reference-type tensors.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DebugGradientIdentity", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return debug_gradient_identity_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "DebugGradientIdentity", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DebugGradientIdentity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def debug_gradient_identity_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function debug_gradient_identity
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"DebugGradientIdentity", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DebugGradientIdentity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def debug_gradient_ref_identity(input, name=None):
  r"""Identity op for gradient debugging.

  This op is hidden from public in Python. It is used by TensorFlow Debugger to
  register gradient tensors for gradient debugging.
  This op operates on reference-type tensors.

  Args:
    input: A mutable `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("debug_gradient_ref_identity op does not support eager execution. Arg 'output' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "DebugGradientRefIdentity", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DebugGradientRefIdentity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def debug_gradient_ref_identity_eager_fallback(input, name=None, ctx=None):
  raise RuntimeError("debug_gradient_ref_identity op does not support eager execution. Arg 'output' is a ref.")

def deep_copy(x, name=None):
  r"""Makes a copy of `x`.

  Args:
    x: A `Tensor`. The source tensor of type `T`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DeepCopy",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return deep_copy_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "DeepCopy", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DeepCopy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def deep_copy_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function deep_copy
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"DeepCopy", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DeepCopy", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def depth_to_space(input, block_size, data_format="NHWC", name=None):
  r"""DepthToSpace for tensors of type T.

  Rearranges data from depth into blocks of spatial data.
  This is the reverse transformation of SpaceToDepth. More specifically,
  this op outputs a copy of the input tensor where values from the `depth`
  dimension are moved in spatial blocks to the `height` and `width` dimensions.
  The attr `block_size` indicates the input block size and how the data is moved.

    * Chunks of data of size `block_size * block_size` from depth are rearranged
      into non-overlapping blocks of size `block_size x block_size`
    * The width the output tensor is `input_depth * block_size`, whereas the
      height is `input_height * block_size`.
    * The Y, X coordinates within each block of the output image are determined
      by the high order component of the input channel index.
    * The depth of the input tensor must be divisible by
      `block_size * block_size`.

  The `data_format` attr specifies the layout of the input and output tensors
  with the following options:
    "NHWC": `[ batch, height, width, channels ]`
    "NCHW": `[ batch, channels, height, width ]`
    "NCHW_VECT_C":
        `qint8 [ batch, channels / 4, height, width, 4 ]`

  It is useful to consider the operation as transforming a 6-D Tensor.
  e.g. for data_format = NHWC,
       Each element in the input tensor can be specified via 6 coordinates,
       ordered by decreasing memory layout significance as:
       n,iY,iX,bY,bX,oC  (where n=batch index, iX, iY means X or Y coordinates
                          within the input image, bX, bY means coordinates
                          within the output block, oC means output channels).
       The output would be the input transposed to the following layout:
       n,iY,bY,iX,bX,oC

  This operation is useful for resizing the activations between convolutions
  (but keeping all data), e.g. instead of pooling. It is also useful for training
  purely convolutional models.

  For example, given an input of shape `[1, 1, 1, 4]`, data_format = "NHWC" and
  block_size = 2:

  ```
  x = [[[[1, 2, 3, 4]]]]

  ```

  This operation will output a tensor of shape `[1, 2, 2, 1]`:

  ```
     [[[[1], [2]],
       [[3], [4]]]]
  ```

  Here, the input has a batch of 1 and each batch element has shape `[1, 1, 4]`,
  the corresponding output will have 2x2 elements and will have a depth of
  1 channel (1 = `4 / (block_size * block_size)`).
  The output element shape is `[2, 2, 1]`.

  For an input tensor with larger depth, here of shape `[1, 1, 1, 12]`, e.g.

  ```
  x = [[[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]]]
  ```

  This operation, for block size of 2, will return the following tensor of shape
  `[1, 2, 2, 3]`

  ```
     [[[[1, 2, 3], [4, 5, 6]],
       [[7, 8, 9], [10, 11, 12]]]]

  ```

  Similarly, for the following input of shape `[1 2 2 4]`, and a block size of 2:

  ```
  x =  [[[[1, 2, 3, 4],
         [5, 6, 7, 8]],
        [[9, 10, 11, 12],
         [13, 14, 15, 16]]]]
  ```

  the operator will return the following tensor of shape `[1 4 4 1]`:

  ```
  x = [[[ [1],   [2],  [5],  [6]],
        [ [3],   [4],  [7],  [8]],
        [ [9],  [10], [13],  [14]],
        [ [11], [12], [15],  [16]]]]

  ```

  Args:
    input: A `Tensor`.
    block_size: An `int` that is `>= 2`.
      The size of the spatial block, same as in Space2Depth.
    data_format: An optional `string` from: `"NHWC", "NCHW", "NCHW_VECT_C"`. Defaults to `"NHWC"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DepthToSpace",
        name, _ctx._post_execution_callbacks, input, "block_size", block_size,
        "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return depth_to_space_eager_fallback(
            input, block_size=block_size, data_format=data_format, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  block_size = _execute.make_int(block_size, "block_size")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DepthToSpace", input=input, block_size=block_size,
                        data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "block_size", _op.get_attr("block_size"),
            "data_format", _op.get_attr("data_format"))
  _execute.record_gradient(
      "DepthToSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def depth_to_space_eager_fallback(input, block_size, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function depth_to_space
  """
  _ctx = ctx if ctx else _context.context()
  block_size = _execute.make_int(block_size, "block_size")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "block_size", block_size, "data_format",
  data_format)
  _result = _execute.execute(b"DepthToSpace", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DepthToSpace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.dequantize', v1=['quantization.dequantize', 'dequantize'])
@deprecated_endpoints('dequantize')
def dequantize(input, min_range, max_range, mode="MIN_COMBINED", name=None):
  r"""Dequantize the 'input' tensor into a float Tensor.

  [min_range, max_range] are scalar floats that specify the range for
  the 'input' data. The 'mode' attribute controls exactly which calculations are
  used to convert the float values to their quantized equivalents.

  In 'MIN_COMBINED' mode, each value of the tensor will undergo the following:

  ```
  if T == qint8: in[i] += (range(T) + 1)/ 2.0
  out[i] = min_range + (in[i]* (max_range - min_range) / range(T))
  ```
  here `range(T) = numeric_limits<T>::max() - numeric_limits<T>::min()`

  *MIN_COMBINED Mode Example*

  If the input comes from a QuantizedRelu6, the output type is
  quint8 (range of 0-255) but the possible range of QuantizedRelu6 is
  0-6.  The min_range and max_range values are therefore 0.0 and 6.0.
  Dequantize on quint8 will take each value, cast to float, and multiply
  by 6 / 255.
  Note that if quantizedtype is qint8, the operation will additionally add
  each value by 128 prior to casting.

  If the mode is 'MIN_FIRST', then this approach is used:

  ```c++
  num_discrete_values = 1 << (# of bits in T)
  range_adjust = num_discrete_values / (num_discrete_values - 1)
  range = (range_max - range_min) * range_adjust
  range_scale = range / num_discrete_values
  const double offset_input = static_cast<double>(input) - lowest_quantized;
  result = range_min + ((input - numeric_limits<T>::min()) * range_scale)
  ```

  *SCALED mode Example*

  `SCALED` mode matches the quantization approach used in
  `QuantizeAndDequantize{V2|V3}`.

  If the mode is `SCALED`, we do not use the full range of the output type,
  choosing to elide the lowest possible value for symmetry (e.g., output range is
  -127 to 127, not -128 to 127 for signed 8 bit quantization), so that 0.0 maps to
  0.

  We first find the range of values in our tensor. The
  range we use is always centered on 0, so we find m such that
  ```c++
    m = max(abs(input_min), abs(input_max))
  ```

  Our input tensor range is then `[-m, m]`.

  Next, we choose our fixed-point quantization buckets, `[min_fixed, max_fixed]`.
  If T is signed, this is
  ```
    num_bits = sizeof(T) * 8
    [min_fixed, max_fixed] =
        [-(1 << (num_bits - 1) - 1), (1 << (num_bits - 1)) - 1]
  ```

  Otherwise, if T is unsigned, the fixed-point range is
  ```
    [min_fixed, max_fixed] = [0, (1 << num_bits) - 1]
  ```

  From this we compute our scaling factor, s:
  ```c++
    s = (2 * m) / (max_fixed - min_fixed)
  ```

  Now we can dequantize the elements of our tensor:
  ```c++
  result = input * s
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
    min_range: A `Tensor` of type `float32`.
      The minimum scalar value possibly produced for the input.
    max_range: A `Tensor` of type `float32`.
      The maximum scalar value possibly produced for the input.
    mode: An optional `string` from: `"MIN_COMBINED", "MIN_FIRST", "SCALED"`. Defaults to `"MIN_COMBINED"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Dequantize",
        name, _ctx._post_execution_callbacks, input, min_range, max_range,
        "mode", mode)
      return _result
    except _core._FallbackException:
      try:
        return dequantize_eager_fallback(
            input, min_range, max_range, mode=mode, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              dequantize, input=input, min_range=min_range,
                          max_range=max_range, mode=mode, name=name)
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
  if mode is None:
    mode = "MIN_COMBINED"
  mode = _execute.make_str(mode, "mode")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Dequantize", input=input, min_range=min_range, max_range=max_range,
                      mode=mode, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          dequantize, input=input, min_range=min_range, max_range=max_range,
                      mode=mode, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "mode", _op.get_attr("mode"))
  _execute.record_gradient(
      "Dequantize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dequantize_eager_fallback(input, min_range, max_range, mode="MIN_COMBINED", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dequantize
  """
  _ctx = ctx if ctx else _context.context()
  if mode is None:
    mode = "MIN_COMBINED"
  mode = _execute.make_str(mode, "mode")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  min_range = _ops.convert_to_tensor(min_range, _dtypes.float32)
  max_range = _ops.convert_to_tensor(max_range, _dtypes.float32)
  _inputs_flat = [input, min_range, max_range]
  _attrs = ("T", _attr_T, "mode", mode)
  _result = _execute.execute(b"Dequantize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Dequantize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.tensor_diag', v1=['linalg.tensor_diag', 'diag'])
@deprecated_endpoints('diag')
def diag(diagonal, name=None):
  r"""Returns a diagonal tensor with a given diagonal values.

  Given a `diagonal`, this operation returns a tensor with the `diagonal` and
  everything else padded with zeros. The diagonal is computed as follows:

  Assume `diagonal` has dimensions [D1,..., Dk], then the output is a tensor of
  rank 2k with dimensions [D1,..., Dk, D1,..., Dk] where:

  `output[i1,..., ik, i1,..., ik] = diagonal[i1, ..., ik]` and 0 everywhere else.

  For example:

  ```
  # 'diagonal' is [1, 2, 3, 4]
  tf.diag(diagonal) ==> [[1, 0, 0, 0]
                         [0, 2, 0, 0]
                         [0, 0, 3, 0]
                         [0, 0, 0, 4]]
  ```

  Args:
    diagonal: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
      Rank k tensor where k is at most 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `diagonal`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Diag", name,
        _ctx._post_execution_callbacks, diagonal)
      return _result
    except _core._FallbackException:
      try:
        return diag_eager_fallback(
            diagonal, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              diag, diagonal=diagonal, name=name)
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
        "Diag", diagonal=diagonal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          diag, diagonal=diagonal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Diag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def diag_eager_fallback(diagonal, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function diag
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (diagonal,) = _execute.args_to_matching_eager([diagonal], _ctx)
  _inputs_flat = [diagonal]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Diag", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Diag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.tensor_diag_part', v1=['linalg.tensor_diag_part', 'diag_part'])
@deprecated_endpoints('diag_part')
def diag_part(input, name=None):
  r"""Returns the diagonal part of the tensor.

  This operation returns a tensor with the `diagonal` part
  of the `input`. The `diagonal` part is computed as follows:

  Assume `input` has dimensions `[D1,..., Dk, D1,..., Dk]`, then the output is a
  tensor of rank `k` with dimensions `[D1,..., Dk]` where:

  `diagonal[i1,..., ik] = input[i1, ..., ik, i1,..., ik]`.

  For example:

  ```
  # 'input' is [[1, 0, 0, 0]
                [0, 2, 0, 0]
                [0, 0, 3, 0]
                [0, 0, 0, 4]]

  tf.diag_part(input) ==> [1, 2, 3, 4]
  ```

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int32`, `int64`, `complex64`, `complex128`.
      Rank k tensor where k is even and not zero.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DiagPart",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return diag_part_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              diag_part, input=input, name=name)
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
        "DiagPart", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          diag_part, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def diag_part_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function diag_part
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"DiagPart", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def edit_distance(hypothesis_indices, hypothesis_values, hypothesis_shape, truth_indices, truth_values, truth_shape, normalize=True, name=None):
  r"""Computes the (possibly normalized) Levenshtein Edit Distance.

  The inputs are variable-length sequences provided by SparseTensors
    (hypothesis_indices, hypothesis_values, hypothesis_shape)
  and
    (truth_indices, truth_values, truth_shape).

  The inputs are:

  Args:
    hypothesis_indices: A `Tensor` of type `int64`.
      The indices of the hypothesis list SparseTensor.
      This is an N x R int64 matrix.
    hypothesis_values: A `Tensor`.
      The values of the hypothesis list SparseTensor.
      This is an N-length vector.
    hypothesis_shape: A `Tensor` of type `int64`.
      The shape of the hypothesis list SparseTensor.
      This is an R-length vector.
    truth_indices: A `Tensor` of type `int64`.
      The indices of the truth list SparseTensor.
      This is an M x R int64 matrix.
    truth_values: A `Tensor`. Must have the same type as `hypothesis_values`.
      The values of the truth list SparseTensor.
      This is an M-length vector.
    truth_shape: A `Tensor` of type `int64`. truth indices, vector.
    normalize: An optional `bool`. Defaults to `True`.
      boolean (if true, edit distances are normalized by length of truth).

      The output is:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EditDistance",
        name, _ctx._post_execution_callbacks, hypothesis_indices,
        hypothesis_values, hypothesis_shape, truth_indices, truth_values,
        truth_shape, "normalize", normalize)
      return _result
    except _core._FallbackException:
      try:
        return edit_distance_eager_fallback(
            hypothesis_indices, hypothesis_values, hypothesis_shape,
            truth_indices, truth_values, truth_shape, normalize=normalize,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if normalize is None:
    normalize = True
  normalize = _execute.make_bool(normalize, "normalize")
  _, _, _op = _op_def_lib._apply_op_helper(
        "EditDistance", hypothesis_indices=hypothesis_indices,
                        hypothesis_values=hypothesis_values,
                        hypothesis_shape=hypothesis_shape,
                        truth_indices=truth_indices,
                        truth_values=truth_values, truth_shape=truth_shape,
                        normalize=normalize, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("normalize", _op.get_attr("normalize"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "EditDistance", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def edit_distance_eager_fallback(hypothesis_indices, hypothesis_values, hypothesis_shape, truth_indices, truth_values, truth_shape, normalize=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function edit_distance
  """
  _ctx = ctx if ctx else _context.context()
  if normalize is None:
    normalize = True
  normalize = _execute.make_bool(normalize, "normalize")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([hypothesis_values, truth_values], _ctx)
  (hypothesis_values, truth_values) = _inputs_T
  hypothesis_indices = _ops.convert_to_tensor(hypothesis_indices, _dtypes.int64)
  hypothesis_shape = _ops.convert_to_tensor(hypothesis_shape, _dtypes.int64)
  truth_indices = _ops.convert_to_tensor(truth_indices, _dtypes.int64)
  truth_shape = _ops.convert_to_tensor(truth_shape, _dtypes.int64)
  _inputs_flat = [hypothesis_indices, hypothesis_values, hypothesis_shape, truth_indices, truth_values, truth_shape]
  _attrs = ("normalize", normalize, "T", _attr_T)
  _result = _execute.execute(b"EditDistance", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EditDistance", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def empty(shape, dtype, init=False, name=None):
  r"""Creates a tensor with the given shape.

This operation creates a tensor of `shape` and `dtype`.

  Args:
    shape: A `Tensor` of type `int32`.
      1-D. Represents the shape of the output tensor.
    dtype: A `tf.DType`.
    init: An optional `bool`. Defaults to `False`.
      If True, initialize the returned tensor with the default value of dtype.  Otherwise, the implementation is free not to initializethe tensor's content.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Empty", name,
        _ctx._post_execution_callbacks, shape, "dtype", dtype, "init", init)
      return _result
    except _core._FallbackException:
      try:
        return empty_eager_fallback(
            shape, dtype=dtype, init=init, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if init is None:
    init = False
  init = _execute.make_bool(init, "init")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Empty", shape=shape, dtype=dtype, init=init, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "init", _op.get_attr("init"))
  _execute.record_gradient(
      "Empty", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def empty_eager_fallback(shape, dtype, init=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function empty
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if init is None:
    init = False
  init = _execute.make_bool(init, "init")
  shape = _ops.convert_to_tensor(shape, _dtypes.int32)
  _inputs_flat = [shape]
  _attrs = ("dtype", dtype, "init", init)
  _result = _execute.execute(b"Empty", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Empty", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def ensure_shape(input, shape, name=None):
  r"""Ensures that the tensor's shape matches the expected shape.

  Raises an error if the input tensor's shape does not match the specified shape.
  Returns the input tensor otherwise.

  Args:
    input: A `Tensor`. A tensor, whose shape is to be validated.
    shape: A `tf.TensorShape` or list of `ints`.
      The expected (possibly partially specified) shape of the input tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EnsureShape",
        name, _ctx._post_execution_callbacks, input, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return ensure_shape_eager_fallback(
            input, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "EnsureShape", input=input, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("shape", _op.get_attr("shape"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "EnsureShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ensure_shape_eager_fallback(input, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ensure_shape
  """
  _ctx = ctx if ctx else _context.context()
  shape = _execute.make_shape(shape, "shape")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("shape", shape, "T", _attr_T)
  _result = _execute.execute(b"EnsureShape", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EnsureShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def expand_dims(input, axis, name=None):
  r"""Inserts a dimension of 1 into a tensor's shape.

  Given a tensor `input`, this operation inserts a dimension of 1 at the
  dimension index `axis` of `input`'s shape. The dimension index `axis` starts at
  zero; if you specify a negative number for `axis` it is counted backward from
  the end.

  This operation is useful if you want to add a batch dimension to a single
  element. For example, if you have a single image of shape `[height, width,
  channels]`, you can make it a batch of 1 image with `expand_dims(image, 0)`,
  which will make the shape `[1, height, width, channels]`.

  Other examples:

  ```
  # 't' is a tensor of shape [2]
  shape(expand_dims(t, 0)) ==> [1, 2]
  shape(expand_dims(t, 1)) ==> [2, 1]
  shape(expand_dims(t, -1)) ==> [2, 1]

  # 't2' is a tensor of shape [2, 3, 5]
  shape(expand_dims(t2, 0)) ==> [1, 2, 3, 5]
  shape(expand_dims(t2, 2)) ==> [2, 3, 1, 5]
  shape(expand_dims(t2, 3)) ==> [2, 3, 5, 1]
  ```

  This operation requires that:

  `-1-input.dims() <= dim <= input.dims()`

  This operation is related to `squeeze()`, which removes dimensions of
  size 1.

  Args:
    input: A `Tensor`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D (scalar). Specifies the dimension index at which to
      expand the shape of `input`. Must be in the range
      `[-rank(input) - 1, rank(input)]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ExpandDims",
        name, _ctx._post_execution_callbacks, input, axis)
      return _result
    except _core._FallbackException:
      try:
        return expand_dims_eager_fallback(
            input, axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExpandDims", input=input, dim=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tdim", _op.get_attr("Tdim"))
  _execute.record_gradient(
      "ExpandDims", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def expand_dims_eager_fallback(input, axis, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function expand_dims
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tdim, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _inputs_flat = [input, axis]
  _attrs = ("T", _attr_T, "Tdim", _attr_Tdim)
  _result = _execute.execute(b"ExpandDims", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExpandDims", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def extract_image_patches(images, ksizes, strides, rates, padding, name=None):
  r"""Extract `patches` from `images` and put them in the "depth" output dimension.

  Args:
    images: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      4-D Tensor with shape `[batch, in_rows, in_cols, depth]`.
    ksizes: A list of `ints` that has length `>= 4`.
      The size of the sliding window for each dimension of `images`.
    strides: A list of `ints` that has length `>= 4`.
      1-D of length 4. How far the centers of two consecutive patches are in
      the images. Must be: `[1, stride_rows, stride_cols, 1]`.
    rates: A list of `ints` that has length `>= 4`.
      1-D of length 4. Must be: `[1, rate_rows, rate_cols, 1]`. This is the
      input stride, specifying how far two consecutive patch samples are in the
      input. Equivalent to extracting patches with
      `patch_sizes_eff = patch_sizes + (patch_sizes - 1) * (rates - 1)`, followed by
      subsampling them spatially by a factor of `rates`. This is equivalent to
      `rate` in dilated (a.k.a. Atrous) convolutions.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.

      We specify the size-related attributes as:

      ```python
            ksizes = [1, ksize_rows, ksize_cols, 1]
            strides = [1, strides_rows, strides_cols, 1]
            rates = [1, rates_rows, rates_cols, 1]
      ```
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExtractImagePatches", name, _ctx._post_execution_callbacks, images,
        "ksizes", ksizes, "strides", strides, "rates", rates, "padding",
        padding)
      return _result
    except _core._FallbackException:
      try:
        return extract_image_patches_eager_fallback(
            images, ksizes=ksizes, strides=strides, rates=rates,
            padding=padding, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(ksizes, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksizes' argument to "
        "'extract_image_patches' Op, not %r." % ksizes)
  ksizes = [_execute.make_int(_i, "ksizes") for _i in ksizes]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'extract_image_patches' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'extract_image_patches' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExtractImagePatches", images=images, ksizes=ksizes, strides=strides,
                               rates=rates, padding=padding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksizes", _op.get_attr("ksizes"), "strides",
            _op.get_attr("strides"), "rates", _op.get_attr("rates"), "T",
            _op.get_attr("T"), "padding", _op.get_attr("padding"))
  _execute.record_gradient(
      "ExtractImagePatches", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def extract_image_patches_eager_fallback(images, ksizes, strides, rates, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function extract_image_patches
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksizes, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksizes' argument to "
        "'extract_image_patches' Op, not %r." % ksizes)
  ksizes = [_execute.make_int(_i, "ksizes") for _i in ksizes]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'extract_image_patches' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  if not isinstance(rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'rates' argument to "
        "'extract_image_patches' Op, not %r." % rates)
  rates = [_execute.make_int(_i, "rates") for _i in rates]
  padding = _execute.make_str(padding, "padding")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  _inputs_flat = [images]
  _attrs = ("ksizes", ksizes, "strides", strides, "rates", rates, "T",
  _attr_T, "padding", padding)
  _result = _execute.execute(b"ExtractImagePatches", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExtractImagePatches", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('extract_volume_patches')
def extract_volume_patches(input, ksizes, strides, padding, name=None):
  r"""Extract `patches` from `input` and put them in the "depth" output dimension. 3D extension of `extract_image_patches`.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      5-D Tensor with shape `[batch, in_planes, in_rows, in_cols, depth]`.
    ksizes: A list of `ints` that has length `>= 5`.
      The size of the sliding window for each dimension of `input`.
    strides: A list of `ints` that has length `>= 5`.
      1-D of length 5. How far the centers of two consecutive patches are in
      `input`. Must be: `[1, stride_planes, stride_rows, stride_cols, 1]`.
    padding: A `string` from: `"SAME", "VALID"`.
      The type of padding algorithm to use.

      We specify the size-related attributes as:

      ```python
            ksizes = [1, ksize_planes, ksize_rows, ksize_cols, 1]
            strides = [1, stride_planes, strides_rows, strides_cols, 1]
      ```
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExtractVolumePatches", name, _ctx._post_execution_callbacks, input,
        "ksizes", ksizes, "strides", strides, "padding", padding)
      return _result
    except _core._FallbackException:
      try:
        return extract_volume_patches_eager_fallback(
            input, ksizes=ksizes, strides=strides, padding=padding, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              extract_volume_patches, input=input, ksizes=ksizes,
                                      strides=strides, padding=padding,
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
  if not isinstance(ksizes, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksizes' argument to "
        "'extract_volume_patches' Op, not %r." % ksizes)
  ksizes = [_execute.make_int(_i, "ksizes") for _i in ksizes]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'extract_volume_patches' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ExtractVolumePatches", input=input, ksizes=ksizes, strides=strides,
                                padding=padding, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          extract_volume_patches, input=input, ksizes=ksizes, strides=strides,
                                  padding=padding, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("ksizes", _op.get_attr("ksizes"), "strides",
            _op.get_attr("strides"), "T", _op.get_attr("T"), "padding",
            _op.get_attr("padding"))
  _execute.record_gradient(
      "ExtractVolumePatches", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def extract_volume_patches_eager_fallback(input, ksizes, strides, padding, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function extract_volume_patches
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(ksizes, (list, tuple)):
    raise TypeError(
        "Expected list for 'ksizes' argument to "
        "'extract_volume_patches' Op, not %r." % ksizes)
  ksizes = [_execute.make_int(_i, "ksizes") for _i in ksizes]
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'extract_volume_patches' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("ksizes", ksizes, "strides", strides, "T", _attr_T, "padding",
  padding)
  _result = _execute.execute(b"ExtractVolumePatches", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExtractVolumePatches", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_args', v1=['quantization.fake_quant_with_min_max_args', 'fake_quant_with_min_max_args'])
@deprecated_endpoints('fake_quant_with_min_max_args')
def fake_quant_with_min_max_args(inputs, min=-6, max=6, num_bits=8, narrow_range=False, name=None):
  r"""Fake-quantize the 'inputs' tensor, type float to 'outputs' tensor of same type.

  Attributes `[min; max]` define the clamping range for the `inputs` data.
  `inputs` values are quantized into the quantization range (`[0; 2^num_bits - 1]`
  when `narrow_range` is false and `[1; 2^num_bits - 1]` when it is true) and
  then de-quantized and output as floats in `[min; max]` interval.
  `num_bits` is the bitwidth of the quantization; between 2 and 16, inclusive.

  Quantization is called fake since the output is still in floating point.

  Args:
    inputs: A `Tensor` of type `float32`.
    min: An optional `float`. Defaults to `-6`.
    max: An optional `float`. Defaults to `6`.
    num_bits: An optional `int`. Defaults to `8`.
    narrow_range: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxArgs", name, _ctx._post_execution_callbacks,
        inputs, "min", min, "max", max, "num_bits", num_bits, "narrow_range",
        narrow_range)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_args_eager_fallback(
            inputs, min=min, max=max, num_bits=num_bits,
            narrow_range=narrow_range, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_args, inputs=inputs, min=min, max=max,
                                            num_bits=num_bits,
                                            narrow_range=narrow_range,
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
  if min is None:
    min = -6
  min = _execute.make_float(min, "min")
  if max is None:
    max = 6
  max = _execute.make_float(max, "max")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxArgs", inputs=inputs, min=min, max=max,
                                   num_bits=num_bits,
                                   narrow_range=narrow_range, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_args, inputs=inputs, min=min, max=max,
                                        num_bits=num_bits,
                                        narrow_range=narrow_range, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("min", _op.get_attr("min"), "max", _op.get_attr("max"),
            "num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxArgs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_quant_with_min_max_args_eager_fallback(inputs, min=-6, max=6, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_args
  """
  _ctx = ctx if ctx else _context.context()
  if min is None:
    min = -6
  min = _execute.make_float(min, "min")
  if max is None:
    max = 6
  max = _execute.make_float(max, "max")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  _inputs_flat = [inputs]
  _attrs = ("min", min, "max", max, "num_bits", num_bits, "narrow_range",
  narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxArgs", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxArgs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_args_gradient', v1=['quantization.fake_quant_with_min_max_args_gradient', 'fake_quant_with_min_max_args_gradient'])
@deprecated_endpoints('fake_quant_with_min_max_args_gradient')
def fake_quant_with_min_max_args_gradient(gradients, inputs, min=-6, max=6, num_bits=8, narrow_range=False, name=None):
  r"""Compute gradients for a FakeQuantWithMinMaxArgs operation.

  Args:
    gradients: A `Tensor` of type `float32`.
      Backpropagated gradients above the FakeQuantWithMinMaxArgs operation.
    inputs: A `Tensor` of type `float32`.
      Values passed as inputs to the FakeQuantWithMinMaxArgs operation.
    min: An optional `float`. Defaults to `-6`.
    max: An optional `float`. Defaults to `6`.
    num_bits: An optional `int`. Defaults to `8`.
    narrow_range: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxArgsGradient", name,
        _ctx._post_execution_callbacks, gradients, inputs, "min", min, "max",
        max, "num_bits", num_bits, "narrow_range", narrow_range)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_args_gradient_eager_fallback(
            gradients, inputs, min=min, max=max, num_bits=num_bits,
            narrow_range=narrow_range, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_args_gradient, gradients=gradients,
                                                     inputs=inputs, min=min,
                                                     max=max,
                                                     num_bits=num_bits,
                                                     narrow_range=narrow_range,
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
  if min is None:
    min = -6
  min = _execute.make_float(min, "min")
  if max is None:
    max = 6
  max = _execute.make_float(max, "max")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxArgsGradient", gradients=gradients, inputs=inputs,
                                           min=min, max=max,
                                           num_bits=num_bits,
                                           narrow_range=narrow_range,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_args_gradient, gradients=gradients,
                                                 inputs=inputs, min=min,
                                                 max=max, num_bits=num_bits,
                                                 narrow_range=narrow_range,
                                                 name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("min", _op.get_attr("min"), "max", _op.get_attr("max"),
            "num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxArgsGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_quant_with_min_max_args_gradient_eager_fallback(gradients, inputs, min=-6, max=6, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_args_gradient
  """
  _ctx = ctx if ctx else _context.context()
  if min is None:
    min = -6
  min = _execute.make_float(min, "min")
  if max is None:
    max = 6
  max = _execute.make_float(max, "max")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  _inputs_flat = [gradients, inputs]
  _attrs = ("min", min, "max", max, "num_bits", num_bits, "narrow_range",
  narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxArgsGradient", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxArgsGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_vars', v1=['quantization.fake_quant_with_min_max_vars', 'fake_quant_with_min_max_vars'])
@deprecated_endpoints('fake_quant_with_min_max_vars')
def fake_quant_with_min_max_vars(inputs, min, max, num_bits=8, narrow_range=False, name=None):
  r"""Fake-quantize the 'inputs' tensor of type float via global float scalars `min`

  and `max` to 'outputs' tensor of same shape as `inputs`.

  `[min; max]` define the clamping range for the `inputs` data.
  `inputs` values are quantized into the quantization range (`[0; 2^num_bits - 1]`
  when `narrow_range` is false and `[1; 2^num_bits - 1]` when it is true) and
  then de-quantized and output as floats in `[min; max]` interval.
  `num_bits` is the bitwidth of the quantization; between 2 and 16, inclusive.

  This operation has a gradient and thus allows for training `min` and `max`
  values.

  Args:
    inputs: A `Tensor` of type `float32`.
    min: A `Tensor` of type `float32`.
    max: A `Tensor` of type `float32`.
    num_bits: An optional `int`. Defaults to `8`.
    narrow_range: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxVars", name, _ctx._post_execution_callbacks,
        inputs, min, max, "num_bits", num_bits, "narrow_range", narrow_range)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_vars_eager_fallback(
            inputs, min, max, num_bits=num_bits, narrow_range=narrow_range,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_vars, inputs=inputs, min=min, max=max,
                                            num_bits=num_bits,
                                            narrow_range=narrow_range,
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
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxVars", inputs=inputs, min=min, max=max,
                                   num_bits=num_bits,
                                   narrow_range=narrow_range, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_vars, inputs=inputs, min=min, max=max,
                                        num_bits=num_bits,
                                        narrow_range=narrow_range, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxVars", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_quant_with_min_max_vars_eager_fallback(inputs, min, max, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_vars
  """
  _ctx = ctx if ctx else _context.context()
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  min = _ops.convert_to_tensor(min, _dtypes.float32)
  max = _ops.convert_to_tensor(max, _dtypes.float32)
  _inputs_flat = [inputs, min, max]
  _attrs = ("num_bits", num_bits, "narrow_range", narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxVars", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxVars", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_fake_quant_with_min_max_vars_gradient_outputs = ["backprops_wrt_input",
                                                 "backprop_wrt_min",
                                                 "backprop_wrt_max"]
_FakeQuantWithMinMaxVarsGradientOutput = _collections.namedtuple(
    "FakeQuantWithMinMaxVarsGradient",
    _fake_quant_with_min_max_vars_gradient_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_vars_gradient', v1=['quantization.fake_quant_with_min_max_vars_gradient', 'fake_quant_with_min_max_vars_gradient'])
@deprecated_endpoints('fake_quant_with_min_max_vars_gradient')
def fake_quant_with_min_max_vars_gradient(gradients, inputs, min, max, num_bits=8, narrow_range=False, name=None):
  r"""Compute gradients for a FakeQuantWithMinMaxVars operation.

  Args:
    gradients: A `Tensor` of type `float32`.
      Backpropagated gradients above the FakeQuantWithMinMaxVars operation.
    inputs: A `Tensor` of type `float32`.
      Values passed as inputs to the FakeQuantWithMinMaxVars operation.
      min, max: Quantization interval, scalar floats.
    min: A `Tensor` of type `float32`.
    max: A `Tensor` of type `float32`.
    num_bits: An optional `int`. Defaults to `8`.
      The bitwidth of the quantization; between 2 and 8, inclusive.
    narrow_range: An optional `bool`. Defaults to `False`.
      Whether to quantize into 2^num_bits - 1 distinct values.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (backprops_wrt_input, backprop_wrt_min, backprop_wrt_max).

    backprops_wrt_input: A `Tensor` of type `float32`.
    backprop_wrt_min: A `Tensor` of type `float32`.
    backprop_wrt_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxVarsGradient", name,
        _ctx._post_execution_callbacks, gradients, inputs, min, max,
        "num_bits", num_bits, "narrow_range", narrow_range)
      _result = _FakeQuantWithMinMaxVarsGradientOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_vars_gradient_eager_fallback(
            gradients, inputs, min, max, num_bits=num_bits,
            narrow_range=narrow_range, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_vars_gradient, gradients=gradients,
                                                     inputs=inputs, min=min,
                                                     max=max,
                                                     num_bits=num_bits,
                                                     narrow_range=narrow_range,
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
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxVarsGradient", gradients=gradients, inputs=inputs,
                                           min=min, max=max,
                                           num_bits=num_bits,
                                           narrow_range=narrow_range,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_vars_gradient, gradients=gradients,
                                                 inputs=inputs, min=min,
                                                 max=max, num_bits=num_bits,
                                                 narrow_range=narrow_range,
                                                 name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsGradient", _inputs_flat, _attrs, _result, name)
  _result = _FakeQuantWithMinMaxVarsGradientOutput._make(_result)
  return _result



def fake_quant_with_min_max_vars_gradient_eager_fallback(gradients, inputs, min, max, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_vars_gradient
  """
  _ctx = ctx if ctx else _context.context()
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  min = _ops.convert_to_tensor(min, _dtypes.float32)
  max = _ops.convert_to_tensor(max, _dtypes.float32)
  _inputs_flat = [gradients, inputs, min, max]
  _attrs = ("num_bits", num_bits, "narrow_range", narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxVarsGradient", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsGradient", _inputs_flat, _attrs, _result, name)
  _result = _FakeQuantWithMinMaxVarsGradientOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_vars_per_channel', v1=['quantization.fake_quant_with_min_max_vars_per_channel', 'fake_quant_with_min_max_vars_per_channel'])
@deprecated_endpoints('fake_quant_with_min_max_vars_per_channel')
def fake_quant_with_min_max_vars_per_channel(inputs, min, max, num_bits=8, narrow_range=False, name=None):
  r"""Fake-quantize the 'inputs' tensor of type float and one of the shapes: `[d]`,

  `[b, d]` `[b, h, w, d]` via per-channel floats `min` and `max` of shape `[d]`
  to 'outputs' tensor of same shape as `inputs`.

  `[min; max]` define the clamping range for the `inputs` data.
  `inputs` values are quantized into the quantization range (`[0; 2^num_bits - 1]`
  when `narrow_range` is false and `[1; 2^num_bits - 1]` when it is true) and
  then de-quantized and output as floats in `[min; max]` interval.
  `num_bits` is the bitwidth of the quantization; between 2 and 16, inclusive.

  This operation has a gradient and thus allows for training `min` and `max`
  values.

  Args:
    inputs: A `Tensor` of type `float32`.
    min: A `Tensor` of type `float32`.
    max: A `Tensor` of type `float32`.
    num_bits: An optional `int`. Defaults to `8`.
    narrow_range: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxVarsPerChannel", name,
        _ctx._post_execution_callbacks, inputs, min, max, "num_bits",
        num_bits, "narrow_range", narrow_range)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_vars_per_channel_eager_fallback(
            inputs, min, max, num_bits=num_bits, narrow_range=narrow_range,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_vars_per_channel, inputs=inputs,
                                                        min=min, max=max,
                                                        num_bits=num_bits,
                                                        narrow_range=narrow_range,
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
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxVarsPerChannel", inputs=inputs, min=min, max=max,
                                             num_bits=num_bits,
                                             narrow_range=narrow_range,
                                             name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_vars_per_channel, inputs=inputs, min=min,
                                                    max=max,
                                                    num_bits=num_bits,
                                                    narrow_range=narrow_range,
                                                    name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsPerChannel", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_quant_with_min_max_vars_per_channel_eager_fallback(inputs, min, max, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_vars_per_channel
  """
  _ctx = ctx if ctx else _context.context()
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  min = _ops.convert_to_tensor(min, _dtypes.float32)
  max = _ops.convert_to_tensor(max, _dtypes.float32)
  _inputs_flat = [inputs, min, max]
  _attrs = ("num_bits", num_bits, "narrow_range", narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxVarsPerChannel", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsPerChannel", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_fake_quant_with_min_max_vars_per_channel_gradient_outputs = ["backprops_wrt_input",
                                                             "backprop_wrt_min",
                                                             "backprop_wrt_max"]
_FakeQuantWithMinMaxVarsPerChannelGradientOutput = _collections.namedtuple(
    "FakeQuantWithMinMaxVarsPerChannelGradient",
    _fake_quant_with_min_max_vars_per_channel_gradient_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantization.fake_quant_with_min_max_vars_per_channel_gradient', v1=['quantization.fake_quant_with_min_max_vars_per_channel_gradient', 'fake_quant_with_min_max_vars_per_channel_gradient'])
@deprecated_endpoints('fake_quant_with_min_max_vars_per_channel_gradient')
def fake_quant_with_min_max_vars_per_channel_gradient(gradients, inputs, min, max, num_bits=8, narrow_range=False, name=None):
  r"""Compute gradients for a FakeQuantWithMinMaxVarsPerChannel operation.

  Args:
    gradients: A `Tensor` of type `float32`.
      Backpropagated gradients above the FakeQuantWithMinMaxVars operation,
      shape one of: `[d]`, `[b, d]`,  `[b, h, w, d]`.
    inputs: A `Tensor` of type `float32`.
      Values passed as inputs to the FakeQuantWithMinMaxVars operation, shape
        same as `gradients`.
      min, max: Quantization interval, floats of shape `[d]`.
    min: A `Tensor` of type `float32`.
    max: A `Tensor` of type `float32`.
    num_bits: An optional `int`. Defaults to `8`.
      The bitwidth of the quantization; between 2 and 16, inclusive.
    narrow_range: An optional `bool`. Defaults to `False`.
      Whether to quantize into 2^num_bits - 1 distinct values.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (backprops_wrt_input, backprop_wrt_min, backprop_wrt_max).

    backprops_wrt_input: A `Tensor` of type `float32`.
    backprop_wrt_min: A `Tensor` of type `float32`.
    backprop_wrt_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FakeQuantWithMinMaxVarsPerChannelGradient", name,
        _ctx._post_execution_callbacks, gradients, inputs, min, max,
        "num_bits", num_bits, "narrow_range", narrow_range)
      _result = _FakeQuantWithMinMaxVarsPerChannelGradientOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return fake_quant_with_min_max_vars_per_channel_gradient_eager_fallback(
            gradients, inputs, min, max, num_bits=num_bits,
            narrow_range=narrow_range, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fake_quant_with_min_max_vars_per_channel_gradient, gradients=gradients,
                                                                 inputs=inputs,
                                                                 min=min,
                                                                 max=max,
                                                                 num_bits=num_bits,
                                                                 narrow_range=narrow_range,
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
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQuantWithMinMaxVarsPerChannelGradient", gradients=gradients,
                                                     inputs=inputs, min=min,
                                                     max=max,
                                                     num_bits=num_bits,
                                                     narrow_range=narrow_range,
                                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fake_quant_with_min_max_vars_per_channel_gradient, gradients=gradients,
                                                             inputs=inputs,
                                                             min=min, max=max,
                                                             num_bits=num_bits,
                                                             narrow_range=narrow_range,
                                                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bits", _op.get_attr("num_bits"), "narrow_range",
            _op.get_attr("narrow_range"))
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsPerChannelGradient", _inputs_flat, _attrs, _result, name)
  _result = _FakeQuantWithMinMaxVarsPerChannelGradientOutput._make(_result)
  return _result



def fake_quant_with_min_max_vars_per_channel_gradient_eager_fallback(gradients, inputs, min, max, num_bits=8, narrow_range=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_quant_with_min_max_vars_per_channel_gradient
  """
  _ctx = ctx if ctx else _context.context()
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if narrow_range is None:
    narrow_range = False
  narrow_range = _execute.make_bool(narrow_range, "narrow_range")
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  inputs = _ops.convert_to_tensor(inputs, _dtypes.float32)
  min = _ops.convert_to_tensor(min, _dtypes.float32)
  max = _ops.convert_to_tensor(max, _dtypes.float32)
  _inputs_flat = [gradients, inputs, min, max]
  _attrs = ("num_bits", num_bits, "narrow_range", narrow_range)
  _result = _execute.execute(b"FakeQuantWithMinMaxVarsPerChannelGradient", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FakeQuantWithMinMaxVarsPerChannelGradient", _inputs_flat, _attrs, _result, name)
  _result = _FakeQuantWithMinMaxVarsPerChannelGradientOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('fill')
def fill(dims, value, name=None):
  r"""Creates a tensor filled with a scalar value.

  This operation creates a tensor of shape `dims` and fills it with `value`.

  For example:

  ```
  # Output tensor has shape [2, 3].
  fill([2, 3], 9) ==> [[9, 9, 9]
                       [9, 9, 9]]
  ```

  `tf.fill` differs from `tf.constant` in a few ways:

  *   `tf.fill` only supports scalar contents, whereas `tf.constant` supports
      Tensor values.
  *   `tf.fill` creates an Op in the computation graph that constructs the actual
      Tensor value at runtime. This is in contrast to `tf.constant` which embeds
      the entire Tensor into the graph with a `Const` node.
  *   Because `tf.fill` evaluates at graph runtime, it supports dynamic shapes
      based on other runtime Tensors, unlike `tf.constant`.

  Args:
    dims: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D. Represents the shape of the output tensor.
    value: A `Tensor`. 0-D (scalar). Value to fill the returned tensor.

      @compatibility(numpy)
      Equivalent to np.full
      @end_compatibility
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Fill", name,
        _ctx._post_execution_callbacks, dims, value)
      return _result
    except _core._FallbackException:
      try:
        return fill_eager_fallback(
            dims, value, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fill, dims=dims, value=value, name=name)
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
        "Fill", dims=dims, value=value, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fill, dims=dims, value=value, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "index_type", _op.get_attr("index_type"))
  _execute.record_gradient(
      "Fill", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fill_eager_fallback(dims, value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fill
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _attr_index_type, (dims,) = _execute.args_to_matching_eager([dims], _ctx, _dtypes.int32)
  _inputs_flat = [dims, value]
  _attrs = ("T", _attr_T, "index_type", _attr_index_type)
  _result = _execute.execute(b"Fill", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Fill", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def gather(params, indices, validate_indices=True, name=None):
  r"""Gather slices from `params` according to `indices`.

  `indices` must be an integer tensor of any dimension (usually 0-D or 1-D).
  Produces an output tensor with shape `indices.shape + params.shape[1:]` where:

  ```python
      # Scalar indices
      output[:, ..., :] = params[indices, :, ... :]

      # Vector indices
      output[i, :, ..., :] = params[indices[i], :, ... :]

      # Higher rank indices
      output[i, ..., j, :, ... :] = params[indices[i, ..., j], :, ..., :]
  ```

  If `indices` is a permutation and `len(indices) == params.shape[0]` then
  this operation will permute `params` accordingly.

  `validate_indices`: DEPRECATED. If this operation is assigned to CPU, values in
  `indices` are always validated to be within range. If assigned to GPU,
  out-of-bound indices result in safe but unspecified behavior, which may include
  raising an error.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/Gather.png" alt>
  </div>

  Args:
    params: A `Tensor`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `params`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Gather", name,
        _ctx._post_execution_callbacks, params, indices, "validate_indices",
        validate_indices)
      return _result
    except _core._FallbackException:
      try:
        return gather_eager_fallback(
            params, indices, validate_indices=validate_indices, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Gather", params=params, indices=indices,
                  validate_indices=validate_indices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("validate_indices", _op.get_attr("validate_indices"), "Tparams",
            _op.get_attr("Tparams"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "Gather", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def gather_eager_fallback(params, indices, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gather
  """
  _ctx = ctx if ctx else _context.context()
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_Tparams, (params,) = _execute.args_to_matching_eager([params], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [params, indices]
  _attrs = ("validate_indices", validate_indices, "Tparams", _attr_Tparams,
  "Tindices", _attr_Tindices)
  _result = _execute.execute(b"Gather", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Gather", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('gather_nd', v1=['gather_nd', 'manip.gather_nd'])
@deprecated_endpoints('manip.gather_nd')
def gather_nd(params, indices, name=None):
  r"""Gather slices from `params` into a Tensor with shape specified by `indices`.

  `indices` is an K-dimensional integer tensor, best thought of as a
  (K-1)-dimensional tensor of indices into `params`, where each element defines a
  slice of `params`:

      output[\\(i_0, ..., i_{K-2}\\)] = params[indices[\\(i_0, ..., i_{K-2}\\)]]

  Whereas in `tf.gather` `indices` defines slices into the first
  dimension of `params`, in `tf.gather_nd`, `indices` defines slices into the
  first `N` dimensions of `params`, where `N = indices.shape[-1]`.

  The last dimension of `indices` can be at most the rank of
  `params`:

      indices.shape[-1] <= params.rank

  The last dimension of `indices` corresponds to elements
  (if `indices.shape[-1] == params.rank`) or slices
  (if `indices.shape[-1] < params.rank`) along dimension `indices.shape[-1]`
  of `params`.  The output tensor has shape

      indices.shape[:-1] + params.shape[indices.shape[-1]:]

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, a 0 is stored in the
  corresponding output value.

  Some examples below.

  Simple indexing into a matrix:

  ```python
      indices = [[0, 0], [1, 1]]
      params = [['a', 'b'], ['c', 'd']]
      output = ['a', 'd']
  ```

  Slice indexing into a matrix:

  ```python
      indices = [[1], [0]]
      params = [['a', 'b'], ['c', 'd']]
      output = [['c', 'd'], ['a', 'b']]
  ```

  Indexing into a 3-tensor:

  ```python
      indices = [[1]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = [[['a1', 'b1'], ['c1', 'd1']]]


      indices = [[0, 1], [1, 0]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = [['c0', 'd0'], ['a1', 'b1']]


      indices = [[0, 0, 1], [1, 0, 1]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = ['b0', 'b1']
  ```

  Batched indexing into a matrix:

  ```python
      indices = [[[0, 0]], [[0, 1]]]
      params = [['a', 'b'], ['c', 'd']]
      output = [['a'], ['b']]
  ```

  Batched slice indexing into a matrix:

  ```python
      indices = [[[1]], [[0]]]
      params = [['a', 'b'], ['c', 'd']]
      output = [[['c', 'd']], [['a', 'b']]]
  ```

  Batched indexing into a 3-tensor:

  ```python
      indices = [[[1]], [[0]]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = [[[['a1', 'b1'], ['c1', 'd1']]],
                [[['a0', 'b0'], ['c0', 'd0']]]]

      indices = [[[0, 1], [1, 0]], [[0, 0], [1, 1]]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = [[['c0', 'd0'], ['a1', 'b1']],
                [['a0', 'b0'], ['c1', 'd1']]]


      indices = [[[0, 0, 1], [1, 0, 1]], [[0, 1, 1], [1, 1, 0]]]
      params = [[['a0', 'b0'], ['c0', 'd0']],
                [['a1', 'b1'], ['c1', 'd1']]]
      output = [['b0', 'b1'], ['d0', 'c1']]
  ```

  See also `tf.gather` and `tf.batch_gather`.

  Args:
    params: A `Tensor`. The tensor from which to gather values.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `params`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GatherNd",
        name, _ctx._post_execution_callbacks, params, indices)
      return _result
    except _core._FallbackException:
      try:
        return gather_nd_eager_fallback(
            params, indices, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gather_nd, params=params, indices=indices, name=name)
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
        "GatherNd", params=params, indices=indices, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gather_nd, params=params, indices=indices, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tparams", _op.get_attr("Tparams"), "Tindices",
            _op.get_attr("Tindices"))
  _execute.record_gradient(
      "GatherNd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def gather_nd_eager_fallback(params, indices, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gather_nd
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tparams, (params,) = _execute.args_to_matching_eager([params], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [params, indices]
  _attrs = ("Tparams", _attr_Tparams, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"GatherNd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GatherNd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def gather_v2(params, indices, axis, name=None):
  r"""Gather slices from `params` axis `axis` according to `indices`.

  `indices` must be an integer tensor of any dimension (usually 0-D or 1-D).
  Produces an output tensor with shape `params.shape[:axis] + indices.shape +
  params.shape[axis + 1:]` where:

  ```python
      # Scalar indices (output is rank(params) - 1).
      output[a_0, ..., a_n, b_0, ..., b_n] =
        params[a_0, ..., a_n, indices, b_0, ..., b_n]

      # Vector indices (output is rank(params)).
      output[a_0, ..., a_n, i, b_0, ..., b_n] =
        params[a_0, ..., a_n, indices[i], b_0, ..., b_n]

      # Higher rank indices (output is rank(params) + rank(indices) - 1).
      output[a_0, ..., a_n, i, ..., j, b_0, ... b_n] =
        params[a_0, ..., a_n, indices[i, ..., j], b_0, ..., b_n]
  ```

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/Gather.png" alt>
  </div>

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, a 0 is stored in the
  corresponding output value.

  See also `tf.batch_gather` and `tf.gather_nd`.

  Args:
    params: A `Tensor`.
      The tensor from which to gather values. Must be at least rank
      `axis + 1`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor. Must be in range `[0, params.shape[axis])`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The axis in `params` to gather `indices` from. Defaults to the first
      dimension. Supports negative indexes.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `params`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GatherV2",
        name, _ctx._post_execution_callbacks, params, indices, axis)
      return _result
    except _core._FallbackException:
      try:
        return gather_v2_eager_fallback(
            params, indices, axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "GatherV2", params=params, indices=indices, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tparams", _op.get_attr("Tparams"), "Tindices",
            _op.get_attr("Tindices"), "Taxis", _op.get_attr("Taxis"))
  _execute.record_gradient(
      "GatherV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def gather_v2_eager_fallback(params, indices, axis, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gather_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tparams, (params,) = _execute.args_to_matching_eager([params], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _attr_Taxis, (axis,) = _execute.args_to_matching_eager([axis], _ctx)
  _inputs_flat = [params, indices, axis]
  _attrs = ("Tparams", _attr_Tparams, "Tindices", _attr_Tindices, "Taxis",
  _attr_Taxis)
  _result = _execute.execute(b"GatherV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GatherV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('guarantee_const')
def guarantee_const(input, name=None):
  r"""Gives a guarantee to the TF runtime that the input tensor is a constant.

  The runtime is then free to make optimizations based on this.

  Only accepts value typed tensors as inputs and rejects resource variable handles
  as input.

  Returns the input tensor without modification.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GuaranteeConst", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return guarantee_const_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              guarantee_const, input=input, name=name)
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
        "GuaranteeConst", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          guarantee_const, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GuaranteeConst", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def guarantee_const_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function guarantee_const
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GuaranteeConst", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GuaranteeConst", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def identity(input, name=None):
  r"""Return a tensor with the same shape and contents as the input tensor or value.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Identity",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return identity_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Identity", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Identity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def identity_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function identity
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Identity", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Identity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('identity_n')
def identity_n(input, name=None):
  r"""Returns a list of tensors with the same shapes and contents as the input

  tensors.

  This op can be used to override the gradient for complicated functions. For
  example, suppose y = f(x) and we wish to apply a custom function g for backprop
  such that dx = g(dy). In Python,

  ```python
  with tf.get_default_graph().gradient_override_map(
      {'IdentityN': 'OverrideGradientWithG'}):
    y, _ = identity_n([f(x), x])

  @tf.RegisterGradient('OverrideGradientWithG')
  def ApplyG(op, dy, _):
    return [None, g(dy)]  # Do not backprop to f(x).
  ```

  Args:
    input: A list of `Tensor` objects.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "IdentityN",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return identity_n_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              identity_n, input=input, name=name)
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
        "IdentityN", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          identity_n, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "IdentityN", _inputs_flat, _attrs, _result, name)
  return _result



def identity_n_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function identity_n
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"IdentityN", len(input), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IdentityN", _inputs_flat, _attrs, _result, name)
  return _result


def immutable_const(dtype, shape, memory_region_name, name=None):
  r"""Returns immutable tensor from memory region.

  The current implementation memmaps the tensor from a file.

  Args:
    dtype: A `tf.DType`. Type of the returned tensor.
    shape: A `tf.TensorShape` or list of `ints`. Shape of the returned tensor.
    memory_region_name: A `string`.
      Name of readonly memory region used by the tensor, see
      NewReadOnlyMemoryRegionFromFile in tensorflow::Env.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ImmutableConst", name, _ctx._post_execution_callbacks, "dtype",
        dtype, "shape", shape, "memory_region_name", memory_region_name)
      return _result
    except _core._FallbackException:
      try:
        return immutable_const_eager_fallback(
            dtype=dtype, shape=shape, memory_region_name=memory_region_name,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  memory_region_name = _execute.make_str(memory_region_name, "memory_region_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ImmutableConst", dtype=dtype, shape=shape,
                          memory_region_name=memory_region_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"),
            "memory_region_name", _op.get_attr("memory_region_name"))
  _execute.record_gradient(
      "ImmutableConst", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def immutable_const_eager_fallback(dtype, shape, memory_region_name, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function immutable_const
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  memory_region_name = _execute.make_str(memory_region_name, "memory_region_name")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape, "memory_region_name",
  memory_region_name)
  _result = _execute.execute(b"ImmutableConst", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ImmutableConst", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def inplace_add(x, i, v, name=None):
  r"""    Adds v into specified rows of x.

    Computes y = x; y[i, :] += v; return y.

  Args:
    x: A `Tensor`. A `Tensor` of type T.
    i: A `Tensor` of type `int32`.
      A vector. Indices into the left-most dimension of `x`.
    v: A `Tensor`. Must have the same type as `x`.
      A `Tensor` of type T. Same dimension sizes as x except the first dimension, which must be the same as i's size.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "InplaceAdd",
        name, _ctx._post_execution_callbacks, x, i, v)
      return _result
    except _core._FallbackException:
      try:
        return inplace_add_eager_fallback(
            x, i, v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "InplaceAdd", x=x, i=i, v=v, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InplaceAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def inplace_add_eager_fallback(x, i, v, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function inplace_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, v], _ctx)
  (x, v) = _inputs_T
  i = _ops.convert_to_tensor(i, _dtypes.int32)
  _inputs_flat = [x, i, v]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InplaceAdd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InplaceAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def inplace_sub(x, i, v, name=None):
  r"""    Subtracts `v` into specified rows of `x`.

    Computes y = x; y[i, :] -= v; return y.

  Args:
    x: A `Tensor`. A `Tensor` of type T.
    i: A `Tensor` of type `int32`.
      A vector. Indices into the left-most dimension of `x`.
    v: A `Tensor`. Must have the same type as `x`.
      A `Tensor` of type T. Same dimension sizes as x except the first dimension, which must be the same as i's size.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "InplaceSub",
        name, _ctx._post_execution_callbacks, x, i, v)
      return _result
    except _core._FallbackException:
      try:
        return inplace_sub_eager_fallback(
            x, i, v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "InplaceSub", x=x, i=i, v=v, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InplaceSub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def inplace_sub_eager_fallback(x, i, v, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function inplace_sub
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, v], _ctx)
  (x, v) = _inputs_T
  i = _ops.convert_to_tensor(i, _dtypes.int32)
  _inputs_flat = [x, i, v]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InplaceSub", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InplaceSub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def inplace_update(x, i, v, name=None):
  r"""    Updates specified rows with values in `v`.

    Computes `x[i, :] = v; return x`.

  Args:
    x: A `Tensor`. A tensor of type `T`.
    i: A `Tensor` of type `int32`.
      A vector. Indices into the left-most dimension of `x`.
    v: A `Tensor`. Must have the same type as `x`.
      A `Tensor` of type T. Same dimension sizes as x except the first dimension, which must be the same as i's size.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InplaceUpdate", name, _ctx._post_execution_callbacks, x, i, v)
      return _result
    except _core._FallbackException:
      try:
        return inplace_update_eager_fallback(
            x, i, v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "InplaceUpdate", x=x, i=i, v=v, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InplaceUpdate", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def inplace_update_eager_fallback(x, i, v, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function inplace_update
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, v], _ctx)
  (x, v) = _inputs_T
  i = _ops.convert_to_tensor(i, _dtypes.int32)
  _inputs_flat = [x, i, v]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InplaceUpdate", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InplaceUpdate", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('math.invert_permutation', v1=['math.invert_permutation', 'invert_permutation'])
@deprecated_endpoints('invert_permutation')
def invert_permutation(x, name=None):
  r"""Computes the inverse permutation of a tensor.

  This operation computes the inverse of an index permutation. It takes a 1-D
  integer tensor `x`, which represents the indices of a zero-based array, and
  swaps each value with its index position. In other words, for an output tensor
  `y` and an input tensor `x`, this operation computes the following:

  `y[x[i]] = i for i in [0, 1, ..., len(x) - 1]`

  The values must include 0. There can be no duplicate values or negative values.

  For example:

  ```
  # tensor `x` is [3, 4, 0, 2, 1]
  invert_permutation(x) ==> [2, 4, 3, 0, 1]
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `int32`, `int64`. 1-D.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InvertPermutation", name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return invert_permutation_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              invert_permutation, x=x, name=name)
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
        "InvertPermutation", x=x, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          invert_permutation, x=x, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "InvertPermutation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def invert_permutation_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function invert_permutation
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx, _dtypes.int32)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"InvertPermutation", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InvertPermutation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_list_diff_outputs = ["out", "idx"]
_ListDiffOutput = _collections.namedtuple(
    "ListDiff", _list_diff_outputs)


def list_diff(x, y, out_idx=_dtypes.int32, name=None):
  r"""Computes the difference between two lists of numbers or strings.

  Given a list `x` and a list `y`, this operation returns a list `out` that
  represents all values that are in `x` but not in `y`. The returned list `out`
  is sorted in the same order that the numbers appear in `x` (duplicates are
  preserved). This operation also returns a list `idx` that represents the
  position of each `out` element in `x`. In other words:

  `out[i] = x[idx[i]] for i in [0, 1, ..., len(out) - 1]`

  For example, given this input:

  ```
  x = [1, 2, 3, 4, 5, 6]
  y = [1, 3, 5]
  ```

  This operation would return:

  ```
  out ==> [2, 4, 6]
  idx ==> [1, 3, 5]
  ```

  Args:
    x: A `Tensor`. 1-D. Values to keep.
    y: A `Tensor`. Must have the same type as `x`. 1-D. Values to remove.
    out_idx: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (out, idx).

    out: A `Tensor`. Has the same type as `x`.
    idx: A `Tensor` of type `out_idx`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ListDiff",
        name, _ctx._post_execution_callbacks, x, y, "out_idx", out_idx)
      _result = _ListDiffOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return list_diff_eager_fallback(
            x, y, out_idx=out_idx, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ListDiff", x=x, y=y, out_idx=out_idx, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_idx", _op.get_attr("out_idx"))
  _execute.record_gradient(
      "ListDiff", _inputs_flat, _attrs, _result, name)
  _result = _ListDiffOutput._make(_result)
  return _result



def list_diff_eager_fallback(x, y, out_idx=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function list_diff
  """
  _ctx = ctx if ctx else _context.context()
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, y], _ctx)
  (x, y) = _inputs_T
  _inputs_flat = [x, y]
  _attrs = ("T", _attr_T, "out_idx", out_idx)
  _result = _execute.execute(b"ListDiff", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ListDiff", _inputs_flat, _attrs, _result, name)
  _result = _ListDiffOutput._make(_result)
  return _result


def lower_bound(sorted_inputs, values, out_type=_dtypes.int32, name=None):
  r"""Applies lower_bound(sorted_search_values, values) along each row.

  Each set of rows with the same index in (sorted_inputs, values) is treated
  independently.  The resulting row is the equivalent of calling
  `np.searchsorted(sorted_inputs, values, side='left')`.

  The result is not a global index to the entire 
  `Tensor`, but rather just the index in the last dimension.

  A 2-D example:
    sorted_sequence = [[0, 3, 9, 9, 10],
                       [1, 2, 3, 4, 5]]
    values = [[2, 4, 9],
              [0, 2, 6]]

    result = LowerBound(sorted_sequence, values)

    result == [[1, 2, 2],
               [0, 1, 5]]

  Args:
    sorted_inputs: A `Tensor`. 2-D Tensor where each row is ordered.
    values: A `Tensor`. Must have the same type as `sorted_inputs`.
      2-D Tensor with the same numbers of rows as `sorted_search_values`. Contains
      the values that will be searched for in `sorted_search_values`.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "LowerBound",
        name, _ctx._post_execution_callbacks, sorted_inputs, values,
        "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return lower_bound_eager_fallback(
            sorted_inputs, values, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LowerBound", sorted_inputs=sorted_inputs, values=values,
                      out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "LowerBound", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lower_bound_eager_fallback(sorted_inputs, values, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lower_bound
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sorted_inputs, values], _ctx)
  (sorted_inputs, values) = _inputs_T
  _inputs_flat = [sorted_inputs, values]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"LowerBound", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LowerBound", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.band_part', v1=['linalg.band_part', 'matrix_band_part'])
@deprecated_endpoints('matrix_band_part')
def matrix_band_part(input, num_lower, num_upper, name=None):
  r"""Copy a tensor setting everything outside a central band in each innermost matrix

  to zero.

  The `band` part is computed as follows:
  Assume `input` has `k` dimensions `[I, J, K, ..., M, N]`, then the output is a
  tensor with the same shape where

  `band[i, j, k, ..., m, n] = in_band(m, n) * input[i, j, k, ..., m, n]`.

  The indicator function

  `in_band(m, n) = (num_lower < 0 || (m-n) <= num_lower)) &&
                   (num_upper < 0 || (n-m) <= num_upper)`.

  For example:

  ```
  # if 'input' is [[ 0,  1,  2, 3]
                   [-1,  0,  1, 2]
                   [-2, -1,  0, 1]
                   [-3, -2, -1, 0]],

  tf.matrix_band_part(input, 1, -1) ==> [[ 0,  1,  2, 3]
                                         [-1,  0,  1, 2]
                                         [ 0, -1,  0, 1]
                                         [ 0,  0, -1, 0]],

  tf.matrix_band_part(input, 2, 1) ==> [[ 0,  1,  0, 0]
                                        [-1,  0,  1, 0]
                                        [-2, -1,  0, 1]
                                        [ 0, -2, -1, 0]]
  ```

  Useful special cases:

  ```
   tf.matrix_band_part(input, 0, -1) ==> Upper triangular part.
   tf.matrix_band_part(input, -1, 0) ==> Lower triangular part.
   tf.matrix_band_part(input, 0, 0) ==> Diagonal.
  ```

  Args:
    input: A `Tensor`. Rank `k` tensor.
    num_lower: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D tensor. Number of subdiagonals to keep. If negative, keep entire
      lower triangle.
    num_upper: A `Tensor`. Must have the same type as `num_lower`.
      0-D tensor. Number of superdiagonals to keep. If negative, keep
      entire upper triangle.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixBandPart", name, _ctx._post_execution_callbacks, input,
        num_lower, num_upper)
      return _result
    except _core._FallbackException:
      try:
        return matrix_band_part_eager_fallback(
            input, num_lower, num_upper, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_band_part, input=input, num_lower=num_lower,
                                num_upper=num_upper, name=name)
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
        "MatrixBandPart", input=input, num_lower=num_lower,
                          num_upper=num_upper, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_band_part, input=input, num_lower=num_lower,
                            num_upper=num_upper, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindex", _op.get_attr("Tindex"))
  _execute.record_gradient(
      "MatrixBandPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_band_part_eager_fallback(input, num_lower, num_upper, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_band_part
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tindex, _inputs_Tindex = _execute.args_to_matching_eager([num_lower, num_upper], _ctx, _dtypes.int64)
  (num_lower, num_upper) = _inputs_Tindex
  _inputs_flat = [input, num_lower, num_upper]
  _attrs = ("T", _attr_T, "Tindex", _attr_Tindex)
  _result = _execute.execute(b"MatrixBandPart", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixBandPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.diag', v1=['linalg.diag', 'matrix_diag'])
@deprecated_endpoints('matrix_diag')
def matrix_diag(diagonal, name=None):
  r"""Returns a batched diagonal tensor with a given batched diagonal values.

  Given a `diagonal`, this operation returns a tensor with the `diagonal` and
  everything else padded with zeros. The diagonal is computed as follows:

  Assume `diagonal` has `k` dimensions `[I, J, K, ..., N]`, then the output is a
  tensor of rank `k+1` with dimensions [I, J, K, ..., N, N]` where:

  `output[i, j, k, ..., m, n] = 1{m=n} * diagonal[i, j, k, ..., n]`.

  For example:

  ```
  # 'diagonal' is [[1, 2, 3, 4], [5, 6, 7, 8]]

  and diagonal.shape = (2, 4)

  tf.matrix_diag(diagonal) ==> [[[1, 0, 0, 0]
                                       [0, 2, 0, 0]
                                       [0, 0, 3, 0]
                                       [0, 0, 0, 4]],
                                      [[5, 0, 0, 0]
                                       [0, 6, 0, 0]
                                       [0, 0, 7, 0]
                                       [0, 0, 0, 8]]]

  which has shape (2, 4, 4)
  ```

  Args:
    diagonal: A `Tensor`. Rank `k`, where `k >= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `diagonal`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MatrixDiag",
        name, _ctx._post_execution_callbacks, diagonal)
      return _result
    except _core._FallbackException:
      try:
        return matrix_diag_eager_fallback(
            diagonal, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_diag, diagonal=diagonal, name=name)
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
        "MatrixDiag", diagonal=diagonal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_diag, diagonal=diagonal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_diag_eager_fallback(diagonal, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_diag
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (diagonal,) = _execute.args_to_matching_eager([diagonal], _ctx)
  _inputs_flat = [diagonal]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixDiag", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.diag_part', v1=['linalg.diag_part', 'matrix_diag_part'])
@deprecated_endpoints('matrix_diag_part')
def matrix_diag_part(input, name=None):
  r"""Returns the batched diagonal part of a batched tensor.

  This operation returns a tensor with the `diagonal` part
  of the batched `input`. The `diagonal` part is computed as follows:

  Assume `input` has `k` dimensions `[I, J, K, ..., M, N]`, then the output is a
  tensor of rank `k - 1` with dimensions `[I, J, K, ..., min(M, N)]` where:

  `diagonal[i, j, k, ..., n] = input[i, j, k, ..., n, n]`.

  The input must be at least a matrix.

  For example:

  ```
  # 'input' is [[[1, 0, 0, 0]
                 [0, 2, 0, 0]
                 [0, 0, 3, 0]
                 [0, 0, 0, 4]],
                [[5, 0, 0, 0]
                 [0, 6, 0, 0]
                 [0, 0, 7, 0]
                 [0, 0, 0, 8]]]

  and input.shape = (2, 4, 4)

  tf.matrix_diag_part(input) ==> [[1, 2, 3, 4], [5, 6, 7, 8]]

  which has shape (2, 4)
  ```

  Args:
    input: A `Tensor`. Rank `k` tensor where `k >= 2`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixDiagPart", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return matrix_diag_part_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_diag_part, input=input, name=name)
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
        "MatrixDiagPart", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_diag_part, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixDiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_diag_part_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_diag_part
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixDiagPart", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixDiagPart", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('linalg.set_diag', v1=['linalg.set_diag', 'matrix_set_diag'])
@deprecated_endpoints('matrix_set_diag')
def matrix_set_diag(input, diagonal, name=None):
  r"""Returns a batched matrix tensor with new batched diagonal values.

  Given `input` and `diagonal`, this operation returns a tensor with the
  same shape and values as `input`, except for the main diagonal of the
  innermost matrices.  These will be overwritten by the values in `diagonal`.

  The output is computed as follows:

  Assume `input` has `k+1` dimensions `[I, J, K, ..., M, N]` and `diagonal` has
  `k` dimensions `[I, J, K, ..., min(M, N)]`.  Then the output is a
  tensor of rank `k+1` with dimensions `[I, J, K, ..., M, N]` where:

    * `output[i, j, k, ..., m, n] = diagonal[i, j, k, ..., n]` for `m == n`.
    * `output[i, j, k, ..., m, n] = input[i, j, k, ..., m, n]` for `m != n`.

  Args:
    input: A `Tensor`. Rank `k+1`, where `k >= 1`.
    diagonal: A `Tensor`. Must have the same type as `input`.
      Rank `k`, where `k >= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatrixSetDiag", name, _ctx._post_execution_callbacks, input,
        diagonal)
      return _result
    except _core._FallbackException:
      try:
        return matrix_set_diag_eager_fallback(
            input, diagonal, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matrix_set_diag, input=input, diagonal=diagonal, name=name)
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
        "MatrixSetDiag", input=input, diagonal=diagonal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matrix_set_diag, input=input, diagonal=diagonal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "MatrixSetDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matrix_set_diag_eager_fallback(input, diagonal, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matrix_set_diag
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, diagonal], _ctx)
  (input, diagonal) = _inputs_T
  _inputs_flat = [input, diagonal]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"MatrixSetDiag", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatrixSetDiag", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mirror_pad(input, paddings, mode, name=None):
  r"""Pads a tensor with mirrored values.

  This operation pads a `input` with mirrored values according to the `paddings`
  you specify. `paddings` is an integer tensor with shape `[n, 2]`, where n is
  the rank of `input`. For each dimension D of `input`, `paddings[D, 0]` indicates
  how many values to add before the contents of `input` in that dimension, and
  `paddings[D, 1]` indicates how many values to add after the contents of `input`
  in that dimension. Both `paddings[D, 0]` and `paddings[D, 1]` must be no greater
  than `input.dim_size(D)` (or `input.dim_size(D) - 1`) if `copy_border` is true
  (if false, respectively).

  The padded size of each dimension D of the output is:

  `paddings(D, 0) + input.dim_size(D) + paddings(D, 1)`

  For example:

  ```
  # 't' is [[1, 2, 3], [4, 5, 6]].
  # 'paddings' is [[1, 1]], [2, 2]].
  # 'mode' is SYMMETRIC.
  # rank of 't' is 2.
  pad(t, paddings) ==> [[2, 1, 1, 2, 3, 3, 2]
                        [2, 1, 1, 2, 3, 3, 2]
                        [5, 4, 4, 5, 6, 6, 5]
                        [5, 4, 4, 5, 6, 6, 5]]
  ```

  Args:
    input: A `Tensor`. The input tensor to be padded.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A two-column matrix specifying the padding sizes. The number of
      rows must be the same as the rank of `input`.
    mode: A `string` from: `"REFLECT", "SYMMETRIC"`.
      Either `REFLECT` or `SYMMETRIC`. In reflect mode the padded regions
      do not include the borders, while in symmetric mode the padded regions
      do include the borders. For example, if `input` is `[1, 2, 3]` and `paddings`
      is `[0, 2]`, then the output is `[1, 2, 3, 2, 1]` in reflect mode, and
      it is `[1, 2, 3, 3, 2]` in symmetric mode.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MirrorPad",
        name, _ctx._post_execution_callbacks, input, paddings, "mode", mode)
      return _result
    except _core._FallbackException:
      try:
        return mirror_pad_eager_fallback(
            input, paddings, mode=mode, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  mode = _execute.make_str(mode, "mode")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MirrorPad", input=input, paddings=paddings, mode=mode, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tpaddings", _op.get_attr("Tpaddings"),
            "mode", _op.get_attr("mode"))
  _execute.record_gradient(
      "MirrorPad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mirror_pad_eager_fallback(input, paddings, mode, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mirror_pad
  """
  _ctx = ctx if ctx else _context.context()
  mode = _execute.make_str(mode, "mode")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, paddings]
  _attrs = ("T", _attr_T, "Tpaddings", _attr_Tpaddings, "mode", mode)
  _result = _execute.execute(b"MirrorPad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MirrorPad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mirror_pad_grad(input, paddings, mode, name=None):
  r"""Gradient op for `MirrorPad` op. This op folds a mirror-padded tensor.

  This operation folds the padded areas of `input` by `MirrorPad` according to the
  `paddings` you specify. `paddings` must be the same as `paddings` argument
  given to the corresponding `MirrorPad` op.

  The folded size of each dimension D of the output is:

  `input.dim_size(D) - paddings(D, 0) - paddings(D, 1)`

  For example:

  ```
  # 't' is [[1, 2, 3], [4, 5, 6], [7, 8, 9]].
  # 'paddings' is [[0, 1]], [0, 1]].
  # 'mode' is SYMMETRIC.
  # rank of 't' is 2.
  pad(t, paddings) ==> [[ 1,  5]
                        [11, 28]]
  ```

  Args:
    input: A `Tensor`. The input tensor to be folded.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A two-column matrix specifying the padding sizes. The number of
      rows must be the same as the rank of `input`.
    mode: A `string` from: `"REFLECT", "SYMMETRIC"`.
      The mode used in the `MirrorPad` op.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MirrorPadGrad", name, _ctx._post_execution_callbacks, input,
        paddings, "mode", mode)
      return _result
    except _core._FallbackException:
      try:
        return mirror_pad_grad_eager_fallback(
            input, paddings, mode=mode, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  mode = _execute.make_str(mode, "mode")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MirrorPadGrad", input=input, paddings=paddings, mode=mode, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tpaddings", _op.get_attr("Tpaddings"),
            "mode", _op.get_attr("mode"))
  _execute.record_gradient(
      "MirrorPadGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mirror_pad_grad_eager_fallback(input, paddings, mode, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mirror_pad_grad
  """
  _ctx = ctx if ctx else _context.context()
  mode = _execute.make_str(mode, "mode")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, paddings]
  _attrs = ("T", _attr_T, "Tpaddings", _attr_Tpaddings, "mode", mode)
  _result = _execute.execute(b"MirrorPadGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MirrorPadGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def one_hot(indices, depth, on_value, off_value, axis=-1, name=None):
  r"""Returns a one-hot tensor.

  The locations represented by indices in `indices` take value `on_value`,
  while all other locations take value `off_value`.

  If the input `indices` is rank `N`, the output will have rank `N+1`,
  The new axis is created at dimension `axis` (default: the new axis is
  appended at the end).

  If `indices` is a scalar the output shape will be a vector of length `depth`.

  If `indices` is a vector of length `features`, the output shape will be:
  ```
    features x depth if axis == -1
    depth x features if axis == 0
  ```

  If `indices` is a matrix (batch) with shape `[batch, features]`,
  the output shape will be:
  ```
    batch x features x depth if axis == -1
    batch x depth x features if axis == 1
    depth x batch x features if axis == 0
  ```


  Examples
  =========

  Suppose that
  ```
    indices = [0, 2, -1, 1]
    depth = 3
    on_value = 5.0
    off_value = 0.0
    axis = -1
  ```

  Then output is `[4 x 3]`:
  ```
  output =
    [5.0 0.0 0.0]  // one_hot(0)
    [0.0 0.0 5.0]  // one_hot(2)
    [0.0 0.0 0.0]  // one_hot(-1)
    [0.0 5.0 0.0]  // one_hot(1)
  ```

  Suppose that
  ```
    indices = [0, 2, -1, 1]
    depth = 3
    on_value = 0.0
    off_value = 3.0
    axis = 0
  ```

  Then output is `[3 x 4]`:
  ```
  output =
    [0.0 3.0 3.0 3.0]
    [3.0 3.0 3.0 0.0]
    [3.0 3.0 3.0 3.0]
    [3.0 0.0 3.0 3.0]
  //  ^                one_hot(0)
  //      ^            one_hot(2)
  //          ^        one_hot(-1)
  //              ^    one_hot(1)
  ```

  Suppose that
  ```
    indices = [[0, 2], [1, -1]]
    depth = 3
    on_value = 1.0
    off_value = 0.0
    axis = -1
  ```

  Then output is `[2 x 2 x 3]`:
  ```
  output =
    [
      [1.0, 0.0, 0.0]  // one_hot(0)
      [0.0, 0.0, 1.0]  // one_hot(2)
    ][
      [0.0, 1.0, 0.0]  // one_hot(1)
      [0.0, 0.0, 0.0]  // one_hot(-1)
    ]
  ```

  Args:
    indices: A `Tensor`. Must be one of the following types: `uint8`, `int32`, `int64`.
      A tensor of indices.
    depth: A `Tensor` of type `int32`.
      A scalar defining the depth of the one hot dimension.
    on_value: A `Tensor`.
      A scalar defining the value to fill in output when `indices[j] = i`.
    off_value: A `Tensor`. Must have the same type as `on_value`.
      A scalar defining the value to fill in output when `indices[j] != i`.
    axis: An optional `int`. Defaults to `-1`.
      The axis to fill (default: -1, a new inner-most axis).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `on_value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "OneHot", name,
        _ctx._post_execution_callbacks, indices, depth, on_value, off_value,
        "axis", axis)
      return _result
    except _core._FallbackException:
      try:
        return one_hot_eager_fallback(
            indices, depth, on_value, off_value, axis=axis, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if axis is None:
    axis = -1
  axis = _execute.make_int(axis, "axis")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OneHot", indices=indices, depth=depth, on_value=on_value,
                  off_value=off_value, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("axis", _op.get_attr("axis"), "T", _op.get_attr("T"), "TI",
            _op.get_attr("TI"))
  _execute.record_gradient(
      "OneHot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def one_hot_eager_fallback(indices, depth, on_value, off_value, axis=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function one_hot
  """
  _ctx = ctx if ctx else _context.context()
  if axis is None:
    axis = -1
  axis = _execute.make_int(axis, "axis")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([on_value, off_value], _ctx)
  (on_value, off_value) = _inputs_T
  _attr_TI, (indices,) = _execute.args_to_matching_eager([indices], _ctx, _dtypes.int64)
  depth = _ops.convert_to_tensor(depth, _dtypes.int32)
  _inputs_flat = [indices, depth, on_value, off_value]
  _attrs = ("axis", axis, "T", _attr_T, "TI", _attr_TI)
  _result = _execute.execute(b"OneHot", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "OneHot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def ones_like(x, name=None):
  r"""Returns a tensor of ones with the same shape and type as x.

  Args:
    x: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`, `int8`, `uint8`, `int16`, `uint16`, `int32`, `int64`, `complex64`, `complex128`, `bool`.
      a tensor of type T.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "OnesLike",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return ones_like_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "OnesLike", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "OnesLike", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ones_like_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ones_like
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"OnesLike", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OnesLike", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def pack(values, axis=0, name=None):
  r"""Packs a list of `N` rank-`R` tensors into one rank-`(R+1)` tensor.

  Packs the `N` tensors in `values` into a tensor with rank one higher than each
  tensor in `values`, by packing them along the `axis` dimension.
  Given a list of tensors of shape `(A, B, C)`;

  if `axis == 0` then the `output` tensor will have the shape `(N, A, B, C)`.
  if `axis == 1` then the `output` tensor will have the shape `(A, N, B, C)`.
  Etc.

  For example:

  ```
  # 'x' is [1, 4]
  # 'y' is [2, 5]
  # 'z' is [3, 6]
  pack([x, y, z]) => [[1, 4], [2, 5], [3, 6]]  # Pack along first dim.
  pack([x, y, z], axis=1) => [[1, 2, 3], [4, 5, 6]]
  ```

  This is the opposite of `unpack`.

  Args:
    values: A list of at least 1 `Tensor` objects with the same type.
      Must be of same shape and type.
    axis: An optional `int`. Defaults to `0`.
      Dimension along which to pack.  Negative values wrap around, so the
      valid range is `[-(R+1), R+1)`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Pack", name,
        _ctx._post_execution_callbacks, values, "axis", axis)
      return _result
    except _core._FallbackException:
      try:
        return pack_eager_fallback(
            values, axis=axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'pack' Op, not %r." % values)
  _attr_N = len(values)
  if axis is None:
    axis = 0
  axis = _execute.make_int(axis, "axis")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Pack", values=values, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"), "axis",
            _op.get_attr("axis"))
  _execute.record_gradient(
      "Pack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def pack_eager_fallback(values, axis=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function pack
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'pack' Op, not %r." % values)
  _attr_N = len(values)
  if axis is None:
    axis = 0
  axis = _execute.make_int(axis, "axis")
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  _inputs_flat = list(values)
  _attrs = ("N", _attr_N, "T", _attr_T, "axis", axis)
  _result = _execute.execute(b"Pack", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Pack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def pad(input, paddings, name=None):
  r"""Pads a tensor with zeros.

  This operation pads a `input` with zeros according to the `paddings` you
  specify. `paddings` is an integer tensor with shape `[Dn, 2]`, where n is the
  rank of `input`. For each dimension D of `input`, `paddings[D, 0]` indicates
  how many zeros to add before the contents of `input` in that dimension, and
  `paddings[D, 1]` indicates how many zeros to add after the contents of `input`
  in that dimension.

  The padded size of each dimension D of the output is:

  `paddings(D, 0) + input.dim_size(D) + paddings(D, 1)`

  For example:

  ```
  # 't' is [[1, 1], [2, 2]]
  # 'paddings' is [[1, 1], [2, 2]]
  # rank of 't' is 2
  pad(t, paddings) ==> [[0, 0, 0, 0, 0, 0]
                        [0, 0, 1, 1, 0, 0]
                        [0, 0, 2, 2, 0, 0]
                        [0, 0, 0, 0, 0, 0]]
  ```

  Args:
    input: A `Tensor`.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Pad", name,
        _ctx._post_execution_callbacks, input, paddings)
      return _result
    except _core._FallbackException:
      try:
        return pad_eager_fallback(
            input, paddings, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Pad", input=input, paddings=paddings, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tpaddings", _op.get_attr("Tpaddings"))
  _execute.record_gradient(
      "Pad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def pad_eager_fallback(input, paddings, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function pad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, paddings]
  _attrs = ("T", _attr_T, "Tpaddings", _attr_Tpaddings)
  _result = _execute.execute(b"Pad", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Pad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def pad_v2(input, paddings, constant_values, name=None):
  r"""Pads a tensor.

  This operation pads `input` according to the `paddings` and `constant_values`
  you specify. `paddings` is an integer tensor with shape `[Dn, 2]`, where n is
  the rank of `input`. For each dimension D of `input`, `paddings[D, 0]` indicates
  how many padding values to add before the contents of `input` in that dimension,
  and `paddings[D, 1]` indicates how many padding values to add after the contents
  of `input` in that dimension. `constant_values` is a scalar tensor of the same
  type as `input` that indicates the value to use for padding `input`.

  The padded size of each dimension D of the output is:

  `paddings(D, 0) + input.dim_size(D) + paddings(D, 1)`

  For example:

  ```
  # 't' is [[1, 1], [2, 2]]
  # 'paddings' is [[1, 1], [2, 2]]
  # 'constant_values' is 0
  # rank of 't' is 2
  pad(t, paddings) ==> [[0, 0, 0, 0, 0, 0]
                        [0, 0, 1, 1, 0, 0]
                        [0, 0, 2, 2, 0, 0]
                        [0, 0, 0, 0, 0, 0]]
  ```

  Args:
    input: A `Tensor`.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    constant_values: A `Tensor`. Must have the same type as `input`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "PadV2", name,
        _ctx._post_execution_callbacks, input, paddings, constant_values)
      return _result
    except _core._FallbackException:
      try:
        return pad_v2_eager_fallback(
            input, paddings, constant_values, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "PadV2", input=input, paddings=paddings,
                 constant_values=constant_values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tpaddings", _op.get_attr("Tpaddings"))
  _execute.record_gradient(
      "PadV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def pad_v2_eager_fallback(input, paddings, constant_values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function pad_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, constant_values], _ctx)
  (input, constant_values) = _inputs_T
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, paddings, constant_values]
  _attrs = ("T", _attr_T, "Tpaddings", _attr_Tpaddings)
  _result = _execute.execute(b"PadV2", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "PadV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def parallel_concat(values, shape, name=None):
  r"""Concatenates a list of `N` tensors along the first dimension.

  The input tensors are all required to have size 1 in the first dimension.

  For example:

  ```
  # 'x' is [[1, 4]]
  # 'y' is [[2, 5]]
  # 'z' is [[3, 6]]
  parallel_concat([x, y, z]) => [[1, 4], [2, 5], [3, 6]]  # Pack along first dim.
  ```

  The difference between concat and parallel_concat is that concat requires all
  of the inputs be computed before the operation will begin but doesn't require
  that the input shapes be known during graph construction.  Parallel concat
  will copy pieces of the input into the output as they become available, in
  some situations this can provide a performance benefit.

  Args:
    values: A list of at least 1 `Tensor` objects with the same type.
      Tensors to be concatenated. All must have size 1 in the first dimension
      and same shape.
    shape: A `tf.TensorShape` or list of `ints`.
      the final shape of the result; should be equal to the shapes of any input
      but with the number of input values in the first dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `values`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ParallelConcat", name, _ctx._post_execution_callbacks, values,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return parallel_concat_eager_fallback(
            values, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'parallel_concat' Op, not %r." % values)
  _attr_N = len(values)
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ParallelConcat", values=values, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"), "shape",
            _op.get_attr("shape"))
  _execute.record_gradient(
      "ParallelConcat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def parallel_concat_eager_fallback(values, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function parallel_concat
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'parallel_concat' Op, not %r." % values)
  _attr_N = len(values)
  shape = _execute.make_shape(shape, "shape")
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  _inputs_flat = list(values)
  _attrs = ("N", _attr_N, "T", _attr_T, "shape", shape)
  _result = _execute.execute(b"ParallelConcat", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ParallelConcat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def placeholder(dtype, shape=None, name=None):
  r"""A placeholder op for a value that will be fed into the computation.

  N.B. This operation will fail with an error if it is executed. It is
  intended as a way to represent a value that will always be fed, and to
  provide attrs that enable the fed value to be checked at runtime.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
      (Optional) The shape of the tensor. If the shape has 0 dimensions, the
      shape is unconstrained.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Placeholder",
        name, _ctx._post_execution_callbacks, "dtype", dtype, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return placeholder_eager_fallback(
            dtype=dtype, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if shape is None:
    shape = None
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Placeholder", dtype=dtype, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "Placeholder", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def placeholder_eager_fallback(dtype, shape=None, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function placeholder
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if shape is None:
    shape = None
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"Placeholder", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Placeholder", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def placeholder_v2(dtype, shape, name=None):
  r"""A placeholder op for a value that will be fed into the computation.

  N.B. This operation will fail with an error if it is executed. It is
  intended as a way to represent a value that will always be fed, and to
  provide attrs that enable the fed value to be checked at runtime.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: A `tf.TensorShape` or list of `ints`.
      The shape of the tensor. The shape can be any partially-specified
      shape.  To be unconstrained, pass in a shape with unknown rank.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PlaceholderV2", name, _ctx._post_execution_callbacks, "dtype", dtype,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return placeholder_v2_eager_fallback(
            dtype=dtype, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PlaceholderV2", dtype=dtype, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "PlaceholderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def placeholder_v2_eager_fallback(dtype, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function placeholder_v2
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"PlaceholderV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PlaceholderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def placeholder_with_default(input, shape, name=None):
  r"""A placeholder op that passes through `input` when its output is not fed.

  Args:
    input: A `Tensor`. The default value to produce when `output` is not fed.
    shape: A `tf.TensorShape` or list of `ints`.
      The (possibly partial) shape of the tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PlaceholderWithDefault", name, _ctx._post_execution_callbacks, input,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return placeholder_with_default_eager_fallback(
            input, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  shape = _execute.make_shape(shape, "shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PlaceholderWithDefault", input=input, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "PlaceholderWithDefault", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def placeholder_with_default_eager_fallback(input, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function placeholder_with_default
  """
  _ctx = ctx if ctx else _context.context()
  shape = _execute.make_shape(shape, "shape")
  _attr_dtype, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("dtype", _attr_dtype, "shape", shape)
  _result = _execute.execute(b"PlaceholderWithDefault", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "PlaceholderWithDefault", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def prevent_gradient(input, message="", name=None):
  r"""An identity op that triggers an error if a gradient is requested.

  When executed in a graph, this op outputs its input tensor as-is.

  When building ops to compute gradients, the TensorFlow gradient system
  will return an error when trying to lookup the gradient of this op,
  because no gradient must ever be registered for this function.  This
  op exists to prevent subtle bugs from silently returning unimplemented
  gradients in some corner cases.

  Args:
    input: A `Tensor`. any tensor.
    message: An optional `string`. Defaults to `""`.
      Will be printed in the error when anyone tries to differentiate
      this operation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PreventGradient", name, _ctx._post_execution_callbacks, input,
        "message", message)
      return _result
    except _core._FallbackException:
      try:
        return prevent_gradient_eager_fallback(
            input, message=message, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if message is None:
    message = ""
  message = _execute.make_str(message, "message")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PreventGradient", input=input, message=message, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "message", _op.get_attr("message"))
  _execute.record_gradient(
      "PreventGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def prevent_gradient_eager_fallback(input, message="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function prevent_gradient
  """
  _ctx = ctx if ctx else _context.context()
  if message is None:
    message = ""
  message = _execute.make_str(message, "message")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "message", message)
  _result = _execute.execute(b"PreventGradient", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PreventGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def quantize_and_dequantize(input, signed_input=True, num_bits=8, range_given=False, input_min=0, input_max=0, name=None):
  r"""Use QuantizeAndDequantizeV2 instead.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    signed_input: An optional `bool`. Defaults to `True`.
    num_bits: An optional `int`. Defaults to `8`.
    range_given: An optional `bool`. Defaults to `False`.
    input_min: An optional `float`. Defaults to `0`.
    input_max: An optional `float`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizeAndDequantize", name, _ctx._post_execution_callbacks, input,
        "signed_input", signed_input, "num_bits", num_bits, "range_given",
        range_given, "input_min", input_min, "input_max", input_max)
      return _result
    except _core._FallbackException:
      try:
        return quantize_and_dequantize_eager_fallback(
            input, signed_input=signed_input, num_bits=num_bits,
            range_given=range_given, input_min=input_min, input_max=input_max,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if range_given is None:
    range_given = False
  range_given = _execute.make_bool(range_given, "range_given")
  if input_min is None:
    input_min = 0
  input_min = _execute.make_float(input_min, "input_min")
  if input_max is None:
    input_max = 0
  input_max = _execute.make_float(input_max, "input_max")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizeAndDequantize", input=input, signed_input=signed_input,
                                 num_bits=num_bits, range_given=range_given,
                                 input_min=input_min, input_max=input_max,
                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("signed_input", _op.get_attr("signed_input"), "num_bits",
            _op.get_attr("num_bits"), "range_given",
            _op.get_attr("range_given"), "input_min",
            _op.get_attr("input_min"), "input_max", _op.get_attr("input_max"),
            "T", _op.get_attr("T"))
  _execute.record_gradient(
      "QuantizeAndDequantize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantize_and_dequantize_eager_fallback(input, signed_input=True, num_bits=8, range_given=False, input_min=0, input_max=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantize_and_dequantize
  """
  _ctx = ctx if ctx else _context.context()
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if range_given is None:
    range_given = False
  range_given = _execute.make_bool(range_given, "range_given")
  if input_min is None:
    input_min = 0
  input_min = _execute.make_float(input_min, "input_min")
  if input_max is None:
    input_max = 0
  input_max = _execute.make_float(input_max, "input_max")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("signed_input", signed_input, "num_bits", num_bits, "range_given",
  range_given, "input_min", input_min, "input_max", input_max, "T", _attr_T)
  _result = _execute.execute(b"QuantizeAndDequantize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizeAndDequantize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('quantization.quantize_and_dequantize')
def quantize_and_dequantize_v2(input, input_min, input_max, signed_input=True, num_bits=8, range_given=False, round_mode="HALF_TO_EVEN", name=None):
  r"""Quantizes then dequantizes a tensor.

  This op simulates the precision loss from the quantized forward pass by:

  1. Quantizing the tensor to fixed point numbers, which should match the target
     quantization method when it is used in inference.
  2. Dequantizing it back to floating point numbers for the following ops, most
     likely matmul.

  There are different ways to quantize. This version uses only scaling, so 0.0
  maps to 0.

  From the specified 'num_bits' in the quantized output type, it determines
  minimum and maximum representable quantized values.

  e.g.

  *   [-128, 127] for signed, num_bits = 8, or
  *   [0, 255] for unsigned, num_bits = 8.

  If range_given == False, the initial input_min, input_max will be determined
  automatically as the minimum and maximum values in the input tensor, otherwise
  the specified values of input_min, input_max are used.

  Note: If the input_min, input_max are specified, they do not need to equal the
  actual minimum and maximum values in the tensor. e.g. in some cases it may be
  beneficial to specify these values such that the low probability extremes of the
  input distribution are clipped.

  This op determines the maximum scale_factor that would map the initial
  [input_min, input_max] range to a range that lies within the representable
  quantized range.

  It determines the scale from one of input_min and input_max, then updates the
  other one to maximize the respresentable range.

  e.g.

  *   if the output is signed, num_bits = 8, [input_min, input_max] = [-10.0,
      5.0]: it would use a scale_factor of -128 / -10.0 = 12.8 In this case, it
      would update input_max to be 127 / 12.8 = 9.921875
  *   if the output is signed, num_bits = 8, [input_min, input_max] = [-10.0,
      10.0]: it would use a scale_factor of 127 / 10.0 = 12.7 In this case, it
      would update input_min to be 128.0 / 12.7 = -10.07874
  *   if the output is unsigned, input_min is forced to be 0, and only the
      specified input_max is used.

  After determining the scale_factor and updating the input range, it applies the
  following to each value in the 'input' tensor.

  output = round(clamp(value, input_min, input_max) * scale_factor) / scale_factor.

  The above round function rounds the value based on the given round_mode.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
      Tensor to quantize and then dequantize.
    input_min: A `Tensor`. Must have the same type as `input`.
      If `range_given == True`, this specifies the minimum input value that needs to
      be represented, otherwise it is determined from the min value of the `input`
      tensor.
    input_max: A `Tensor`. Must have the same type as `input`.
      If `range_given == True`, this specifies the maximum input value that needs to
      be represented, otherwise it is determined from the max value of the `input`
      tensor.
    signed_input: An optional `bool`. Defaults to `True`.
      Whether the quantization is signed or unsigned. (actually this parameter should
      have been called <b>`signed_output`</b>)
    num_bits: An optional `int`. Defaults to `8`.
      The bitwidth of the quantization.
    range_given: An optional `bool`. Defaults to `False`.
      Whether the range is given or should be determined from the `input` tensor.
    round_mode: An optional `string` from: `"HALF_TO_EVEN", "HALF_UP"`. Defaults to `"HALF_TO_EVEN"`.
      The 'round_mode' attribute controls which rounding tie-breaking algorithm is
      used when rounding float values to their quantized equivalents. The following
      rounding modes are currently supported:

      *   HALF_TO_EVEN: this is the default round_mode.
      *   HALF_UP: round towards positive. In this mode 7.5 rounds up to 8 and -7.5
          rounds up to -7.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizeAndDequantizeV2", name, _ctx._post_execution_callbacks,
        input, input_min, input_max, "signed_input", signed_input, "num_bits",
        num_bits, "range_given", range_given, "round_mode", round_mode)
      return _result
    except _core._FallbackException:
      try:
        return quantize_and_dequantize_v2_eager_fallback(
            input, input_min, input_max, signed_input=signed_input,
            num_bits=num_bits, range_given=range_given, round_mode=round_mode,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantize_and_dequantize_v2, input=input, input_min=input_min,
                                          input_max=input_max,
                                          signed_input=signed_input,
                                          num_bits=num_bits,
                                          range_given=range_given,
                                          round_mode=round_mode, name=name)
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
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if range_given is None:
    range_given = False
  range_given = _execute.make_bool(range_given, "range_given")
  if round_mode is None:
    round_mode = "HALF_TO_EVEN"
  round_mode = _execute.make_str(round_mode, "round_mode")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizeAndDequantizeV2", input=input, input_min=input_min,
                                   input_max=input_max,
                                   signed_input=signed_input,
                                   num_bits=num_bits, range_given=range_given,
                                   round_mode=round_mode, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantize_and_dequantize_v2, input=input, input_min=input_min,
                                      input_max=input_max,
                                      signed_input=signed_input,
                                      num_bits=num_bits,
                                      range_given=range_given,
                                      round_mode=round_mode, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("signed_input", _op.get_attr("signed_input"), "num_bits",
            _op.get_attr("num_bits"), "range_given",
            _op.get_attr("range_given"), "T", _op.get_attr("T"), "round_mode",
            _op.get_attr("round_mode"))
  _execute.record_gradient(
      "QuantizeAndDequantizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantize_and_dequantize_v2_eager_fallback(input, input_min, input_max, signed_input=True, num_bits=8, range_given=False, round_mode="HALF_TO_EVEN", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantize_and_dequantize_v2
  """
  _ctx = ctx if ctx else _context.context()
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if num_bits is None:
    num_bits = 8
  num_bits = _execute.make_int(num_bits, "num_bits")
  if range_given is None:
    range_given = False
  range_given = _execute.make_bool(range_given, "range_given")
  if round_mode is None:
    round_mode = "HALF_TO_EVEN"
  round_mode = _execute.make_str(round_mode, "round_mode")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, input_min, input_max], _ctx)
  (input, input_min, input_max) = _inputs_T
  _inputs_flat = [input, input_min, input_max]
  _attrs = ("signed_input", signed_input, "num_bits", num_bits, "range_given",
  range_given, "T", _attr_T, "round_mode", round_mode)
  _result = _execute.execute(b"QuantizeAndDequantizeV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantizeAndDequantizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def quantize_and_dequantize_v3(input, input_min, input_max, num_bits, signed_input=True, range_given=True, name=None):
  r"""Quantizes then dequantizes a tensor.

  This is almost identical to QuantizeAndDequantizeV2, except that num_bits is a
  tensor, so its value can change during training.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `half`, `float32`, `float64`.
    input_min: A `Tensor`. Must have the same type as `input`.
    input_max: A `Tensor`. Must have the same type as `input`.
    num_bits: A `Tensor` of type `int32`.
    signed_input: An optional `bool`. Defaults to `True`.
    range_given: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizeAndDequantizeV3", name, _ctx._post_execution_callbacks,
        input, input_min, input_max, num_bits, "signed_input", signed_input,
        "range_given", range_given)
      return _result
    except _core._FallbackException:
      try:
        return quantize_and_dequantize_v3_eager_fallback(
            input, input_min, input_max, num_bits, signed_input=signed_input,
            range_given=range_given, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if range_given is None:
    range_given = True
  range_given = _execute.make_bool(range_given, "range_given")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizeAndDequantizeV3", input=input, input_min=input_min,
                                   input_max=input_max, num_bits=num_bits,
                                   signed_input=signed_input,
                                   range_given=range_given, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("signed_input", _op.get_attr("signed_input"), "range_given",
            _op.get_attr("range_given"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "QuantizeAndDequantizeV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantize_and_dequantize_v3_eager_fallback(input, input_min, input_max, num_bits, signed_input=True, range_given=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantize_and_dequantize_v3
  """
  _ctx = ctx if ctx else _context.context()
  if signed_input is None:
    signed_input = True
  signed_input = _execute.make_bool(signed_input, "signed_input")
  if range_given is None:
    range_given = True
  range_given = _execute.make_bool(range_given, "range_given")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, input_min, input_max], _ctx)
  (input, input_min, input_max) = _inputs_T
  num_bits = _ops.convert_to_tensor(num_bits, _dtypes.int32)
  _inputs_flat = [input, input_min, input_max, num_bits]
  _attrs = ("signed_input", signed_input, "range_given", range_given, "T",
  _attr_T)
  _result = _execute.execute(b"QuantizeAndDequantizeV3", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantizeAndDequantizeV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_quantize_v2_outputs = ["output", "output_min", "output_max"]
_QuantizeV2Output = _collections.namedtuple(
    "QuantizeV2", _quantize_v2_outputs)


def quantize_v2(input, min_range, max_range, T, mode="MIN_COMBINED", round_mode="HALF_AWAY_FROM_ZERO", name=None):
  r"""Quantize the 'input' tensor of type float to 'output' tensor of type 'T'.

  [min_range, max_range] are scalar floats that specify the range for
  the 'input' data. The 'mode' attribute controls exactly which calculations are
  used to convert the float values to their quantized equivalents.  The
  'round_mode' attribute controls which rounding tie-breaking algorithm is used
  when rounding float values to their quantized equivalents.

  In 'MIN_COMBINED' mode, each value of the tensor will undergo the following:

  ```
  out[i] = (in[i] - min_range) * range(T) / (max_range - min_range)
  if T == qint8: out[i] -= (range(T) + 1) / 2.0
  ```

  here `range(T) = numeric_limits<T>::max() - numeric_limits<T>::min()`

  *MIN_COMBINED Mode Example*

  Assume the input is type float and has a possible range of [0.0, 6.0] and the
  output type is quint8 ([0, 255]). The min_range and max_range values should be
  specified as 0.0 and 6.0. Quantizing from float to quint8 will multiply each
  value of the input by 255/6 and cast to quint8.

  If the output type was qint8 ([-128, 127]), the operation will additionally
  subtract each value by 128 prior to casting, so that the range of values aligns
  with the range of qint8.

  If the mode is 'MIN_FIRST', then this approach is used:

  ```
  num_discrete_values = 1 << (# of bits in T)
  range_adjust = num_discrete_values / (num_discrete_values - 1)
  range = (range_max - range_min) * range_adjust
  range_scale = num_discrete_values / range
  quantized = round(input * range_scale) - round(range_min * range_scale) +
    numeric_limits<T>::min()
  quantized = max(quantized, numeric_limits<T>::min())
  quantized = min(quantized, numeric_limits<T>::max())
  ```

  The biggest difference between this and MIN_COMBINED is that the minimum range
  is rounded first, before it's subtracted from the rounded value. With
  MIN_COMBINED, a small bias is introduced where repeated iterations of quantizing
  and dequantizing will introduce a larger and larger error.

  *SCALED mode Example*

  `SCALED` mode matches the quantization approach used in
  `QuantizeAndDequantize{V2|V3}`.

  If the mode is `SCALED`, we do not use the full range of the output type,
  choosing to elide the lowest possible value for symmetry (e.g., output range is
  -127 to 127, not -128 to 127 for signed 8 bit quantization), so that 0.0 maps to
  0.

  We first find the range of values in our tensor. The
  range we use is always centered on 0, so we find m such that

  ```c++
    m = max(abs(input_min), abs(input_max))
  ```

  Our input tensor range is then `[-m, m]`.

  Next, we choose our fixed-point quantization buckets, `[min_fixed, max_fixed]`.
  If T is signed, this is

  ```
    num_bits = sizeof(T) * 8
    [min_fixed, max_fixed] =
        [-(1 << (num_bits - 1) - 1), (1 << (num_bits - 1)) - 1]
  ```

  Otherwise, if T is unsigned, the fixed-point range is

  ```
    [min_fixed, max_fixed] = [0, (1 << num_bits) - 1]
  ```

  From this we compute our scaling factor, s:

  ```c++
    s = (max_fixed - min_fixed) / (2 * m)
  ```

  Now we can quantize the elements of our tensor:

  ```c++
  result = round(input * s)
  ```

  One thing to watch out for is that the operator may choose to adjust the
  requested minimum and maximum values slightly during the quantization process,
  so you should always use the output ports as the range for further calculations.
  For example, if the requested minimum and maximum values are close to equal,
  they will be separated by a small epsilon value to prevent ill-formed quantized
  buffers from being created. Otherwise, you can end up with buffers where all the
  quantized values map to the same float value, which causes problems for
  operations that have to perform further calculations on them.

  Args:
    input: A `Tensor` of type `float32`.
    min_range: A `Tensor` of type `float32`.
      The minimum scalar value possibly produced for the input.
    max_range: A `Tensor` of type `float32`.
      The maximum scalar value possibly produced for the input.
    T: A `tf.DType` from: `tf.qint8, tf.quint8, tf.qint32, tf.qint16, tf.quint16`.
    mode: An optional `string` from: `"MIN_COMBINED", "MIN_FIRST", "SCALED"`. Defaults to `"MIN_COMBINED"`.
    round_mode: An optional `string` from: `"HALF_AWAY_FROM_ZERO", "HALF_TO_EVEN"`. Defaults to `"HALF_AWAY_FROM_ZERO"`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, output_min, output_max).

    output: A `Tensor` of type `T`.
    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "QuantizeV2",
        name, _ctx._post_execution_callbacks, input, min_range, max_range,
        "T", T, "mode", mode, "round_mode", round_mode)
      _result = _QuantizeV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantize_v2_eager_fallback(
            input, min_range, max_range, T=T, mode=mode,
            round_mode=round_mode, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  T = _execute.make_type(T, "T")
  if mode is None:
    mode = "MIN_COMBINED"
  mode = _execute.make_str(mode, "mode")
  if round_mode is None:
    round_mode = "HALF_AWAY_FROM_ZERO"
  round_mode = _execute.make_str(round_mode, "round_mode")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizeV2", input=input, min_range=min_range, max_range=max_range,
                      T=T, mode=mode, round_mode=round_mode, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "mode", _op.get_attr("mode"),
            "round_mode", _op.get_attr("round_mode"))
  _execute.record_gradient(
      "QuantizeV2", _inputs_flat, _attrs, _result, name)
  _result = _QuantizeV2Output._make(_result)
  return _result



def quantize_v2_eager_fallback(input, min_range, max_range, T, mode="MIN_COMBINED", round_mode="HALF_AWAY_FROM_ZERO", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantize_v2
  """
  _ctx = ctx if ctx else _context.context()
  T = _execute.make_type(T, "T")
  if mode is None:
    mode = "MIN_COMBINED"
  mode = _execute.make_str(mode, "mode")
  if round_mode is None:
    round_mode = "HALF_AWAY_FROM_ZERO"
  round_mode = _execute.make_str(round_mode, "round_mode")
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  min_range = _ops.convert_to_tensor(min_range, _dtypes.float32)
  max_range = _ops.convert_to_tensor(max_range, _dtypes.float32)
  _inputs_flat = [input, min_range, max_range]
  _attrs = ("T", T, "mode", mode, "round_mode", round_mode)
  _result = _execute.execute(b"QuantizeV2", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizeV2", _inputs_flat, _attrs, _result, name)
  _result = _QuantizeV2Output._make(_result)
  return _result


_quantized_concat_outputs = ["output", "output_min", "output_max"]
_QuantizedConcatOutput = _collections.namedtuple(
    "QuantizedConcat", _quantized_concat_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantization.quantized_concat', v1=['quantization.quantized_concat', 'quantized_concat'])
@deprecated_endpoints('quantized_concat')
def quantized_concat(concat_dim, values, input_mins, input_maxes, name=None):
  r"""Concatenates quantized tensors along one dimension.

  Args:
    concat_dim: A `Tensor` of type `int32`.
      0-D.  The dimension along which to concatenate.  Must be in the
      range [0, rank(values)).
    values: A list of at least 2 `Tensor` objects with the same type.
      The `N` Tensors to concatenate. Their ranks and types must match,
      and their sizes must match in all dimensions except `concat_dim`.
    input_mins: A list with the same length as `values` of `Tensor` objects with type `float32`.
      The minimum scalar values for each of the input tensors.
    input_maxes: A list with the same length as `values` of `Tensor` objects with type `float32`.
      The maximum scalar values for each of the input tensors.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, output_min, output_max).

    output: A `Tensor`. Has the same type as `values`.
    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedConcat", name, _ctx._post_execution_callbacks, concat_dim,
        values, input_mins, input_maxes)
      _result = _QuantizedConcatOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_concat_eager_fallback(
            concat_dim, values, input_mins, input_maxes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantized_concat, concat_dim=concat_dim, values=values,
                                input_mins=input_mins,
                                input_maxes=input_maxes, name=name)
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
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'quantized_concat' Op, not %r." % values)
  _attr_N = len(values)
  if not isinstance(input_mins, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_mins' argument to "
        "'quantized_concat' Op, not %r." % input_mins)
  if len(input_mins) != _attr_N:
    raise ValueError(
        "List argument 'input_mins' to 'quantized_concat' Op with length %d "
        "must match length %d of argument 'values'." %
        (len(input_mins), _attr_N))
  if not isinstance(input_maxes, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_maxes' argument to "
        "'quantized_concat' Op, not %r." % input_maxes)
  if len(input_maxes) != _attr_N:
    raise ValueError(
        "List argument 'input_maxes' to 'quantized_concat' Op with length %d "
        "must match length %d of argument 'values'." %
        (len(input_maxes), _attr_N))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedConcat", concat_dim=concat_dim, values=values,
                           input_mins=input_mins, input_maxes=input_maxes,
                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantized_concat, concat_dim=concat_dim, values=values,
                            input_mins=input_mins, input_maxes=input_maxes,
                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "QuantizedConcat", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedConcatOutput._make(_result)
  return _result



def quantized_concat_eager_fallback(concat_dim, values, input_mins, input_maxes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_concat
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(values, (list, tuple)):
    raise TypeError(
        "Expected list for 'values' argument to "
        "'quantized_concat' Op, not %r." % values)
  _attr_N = len(values)
  if not isinstance(input_mins, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_mins' argument to "
        "'quantized_concat' Op, not %r." % input_mins)
  if len(input_mins) != _attr_N:
    raise ValueError(
        "List argument 'input_mins' to 'quantized_concat' Op with length %d "
        "must match length %d of argument 'values'." %
        (len(input_mins), _attr_N))
  if not isinstance(input_maxes, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_maxes' argument to "
        "'quantized_concat' Op, not %r." % input_maxes)
  if len(input_maxes) != _attr_N:
    raise ValueError(
        "List argument 'input_maxes' to 'quantized_concat' Op with length %d "
        "must match length %d of argument 'values'." %
        (len(input_maxes), _attr_N))
  _attr_T, values = _execute.args_to_matching_eager(list(values), _ctx)
  concat_dim = _ops.convert_to_tensor(concat_dim, _dtypes.int32)
  input_mins = _ops.convert_n_to_tensor(input_mins, _dtypes.float32)
  input_maxes = _ops.convert_n_to_tensor(input_maxes, _dtypes.float32)
  _inputs_flat = [concat_dim] + list(values) + list(input_mins) + list(input_maxes)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"QuantizedConcat", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedConcat", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedConcatOutput._make(_result)
  return _result


_quantized_instance_norm_outputs = ["y", "y_min", "y_max"]
_QuantizedInstanceNormOutput = _collections.namedtuple(
    "QuantizedInstanceNorm", _quantized_instance_norm_outputs)


def quantized_instance_norm(x, x_min, x_max, output_range_given=False, given_y_min=0, given_y_max=0, variance_epsilon=1e-05, min_separation=0.001, name=None):
  r"""Quantized Instance normalization.

  Args:
    x: A `Tensor`. Must be one of the following types: `qint8`, `quint8`, `qint32`, `qint16`, `quint16`.
      A 4D input Tensor.
    x_min: A `Tensor` of type `float32`.
      The value represented by the lowest quantized input.
    x_max: A `Tensor` of type `float32`.
      The value represented by the highest quantized input.
    output_range_given: An optional `bool`. Defaults to `False`.
      If True, `given_y_min` and `given_y_min`
      and `given_y_max` are used as the output range. Otherwise,
      the implementation computes the output range.
    given_y_min: An optional `float`. Defaults to `0`.
      Output in `y_min` if `output_range_given` is True.
    given_y_max: An optional `float`. Defaults to `0`.
      Output in `y_max` if `output_range_given` is True.
    variance_epsilon: An optional `float`. Defaults to `1e-05`.
      A small float number to avoid dividing by 0.
    min_separation: An optional `float`. Defaults to `0.001`.
      Minimum value of `y_max - y_min`
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, y_min, y_max).

    y: A `Tensor`. Has the same type as `x`.
    y_min: A `Tensor` of type `float32`.
    y_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedInstanceNorm", name, _ctx._post_execution_callbacks, x,
        x_min, x_max, "output_range_given", output_range_given, "given_y_min",
        given_y_min, "given_y_max", given_y_max, "variance_epsilon",
        variance_epsilon, "min_separation", min_separation)
      _result = _QuantizedInstanceNormOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_instance_norm_eager_fallback(
            x, x_min, x_max, output_range_given=output_range_given,
            given_y_min=given_y_min, given_y_max=given_y_max,
            variance_epsilon=variance_epsilon, min_separation=min_separation,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if output_range_given is None:
    output_range_given = False
  output_range_given = _execute.make_bool(output_range_given, "output_range_given")
  if given_y_min is None:
    given_y_min = 0
  given_y_min = _execute.make_float(given_y_min, "given_y_min")
  if given_y_max is None:
    given_y_max = 0
  given_y_max = _execute.make_float(given_y_max, "given_y_max")
  if variance_epsilon is None:
    variance_epsilon = 1e-05
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  if min_separation is None:
    min_separation = 0.001
  min_separation = _execute.make_float(min_separation, "min_separation")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedInstanceNorm", x=x, x_min=x_min, x_max=x_max,
                                 output_range_given=output_range_given,
                                 given_y_min=given_y_min,
                                 given_y_max=given_y_max,
                                 variance_epsilon=variance_epsilon,
                                 min_separation=min_separation, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "output_range_given",
            _op.get_attr("output_range_given"), "given_y_min",
            _op.get_attr("given_y_min"), "given_y_max",
            _op.get_attr("given_y_max"), "variance_epsilon",
            _op.get_attr("variance_epsilon"), "min_separation",
            _op.get_attr("min_separation"))
  _execute.record_gradient(
      "QuantizedInstanceNorm", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedInstanceNormOutput._make(_result)
  return _result



def quantized_instance_norm_eager_fallback(x, x_min, x_max, output_range_given=False, given_y_min=0, given_y_max=0, variance_epsilon=1e-05, min_separation=0.001, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_instance_norm
  """
  _ctx = ctx if ctx else _context.context()
  if output_range_given is None:
    output_range_given = False
  output_range_given = _execute.make_bool(output_range_given, "output_range_given")
  if given_y_min is None:
    given_y_min = 0
  given_y_min = _execute.make_float(given_y_min, "given_y_min")
  if given_y_max is None:
    given_y_max = 0
  given_y_max = _execute.make_float(given_y_max, "given_y_max")
  if variance_epsilon is None:
    variance_epsilon = 1e-05
  variance_epsilon = _execute.make_float(variance_epsilon, "variance_epsilon")
  if min_separation is None:
    min_separation = 0.001
  min_separation = _execute.make_float(min_separation, "min_separation")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  x_min = _ops.convert_to_tensor(x_min, _dtypes.float32)
  x_max = _ops.convert_to_tensor(x_max, _dtypes.float32)
  _inputs_flat = [x, x_min, x_max]
  _attrs = ("T", _attr_T, "output_range_given", output_range_given,
  "given_y_min", given_y_min, "given_y_max", given_y_max, "variance_epsilon",
  variance_epsilon, "min_separation", min_separation)
  _result = _execute.execute(b"QuantizedInstanceNorm", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedInstanceNorm", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedInstanceNormOutput._make(_result)
  return _result


_quantized_reshape_outputs = ["output", "output_min", "output_max"]
_QuantizedReshapeOutput = _collections.namedtuple(
    "QuantizedReshape", _quantized_reshape_outputs)


def quantized_reshape(tensor, shape, input_min, input_max, name=None):
  r"""Reshapes a quantized tensor as per the Reshape op.

  ```

  Args:
    tensor: A `Tensor`.
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Defines the shape of the output tensor.
    input_min: A `Tensor` of type `float32`. The minimum value of the input.
    input_max: A `Tensor` of type `float32`. The maximum value of the input.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output, output_min, output_max).

    output: A `Tensor`. Has the same type as `tensor`.
    output_min: A `Tensor` of type `float32`.
    output_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedReshape", name, _ctx._post_execution_callbacks, tensor,
        shape, input_min, input_max)
      _result = _QuantizedReshapeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_reshape_eager_fallback(
            tensor, shape, input_min, input_max, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedReshape", tensor=tensor, shape=shape, input_min=input_min,
                            input_max=input_max, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tshape", _op.get_attr("Tshape"))
  _execute.record_gradient(
      "QuantizedReshape", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReshapeOutput._make(_result)
  return _result



def quantized_reshape_eager_fallback(tensor, shape, input_min, input_max, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_reshape
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _attr_Tshape, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  input_min = _ops.convert_to_tensor(input_min, _dtypes.float32)
  input_max = _ops.convert_to_tensor(input_max, _dtypes.float32)
  _inputs_flat = [tensor, shape, input_min, input_max]
  _attrs = ("T", _attr_T, "Tshape", _attr_Tshape)
  _result = _execute.execute(b"QuantizedReshape", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantizedReshape", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedReshapeOutput._make(_result)
  return _result


def rank(input, name=None):
  r"""Returns the rank of a tensor.

  This operation returns an integer representing the rank of `input`.

  For example:

  ```
  # 't' is [[[1, 1, 1], [2, 2, 2]], [[3, 3, 3], [4, 4, 4]]]
  # shape of tensor 't' is [2, 2, 3]
  rank(t) ==> 3
  ```

  **Note**: The rank of a tensor is not the same as the rank of a matrix. The rank
  of a tensor is the number of indices required to uniquely select each element
  of the tensor. Rank is also known as "order", "degree", or "ndims."

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Rank", name,
        _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return rank_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Rank", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Rank", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rank_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rank
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Rank", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Rank", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def ref_identity(input, name=None):
  r"""Return the same ref tensor as the input ref tensor.

  Args:
    input: A mutable `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("ref_identity op does not support eager execution. Arg 'output' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "RefIdentity", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RefIdentity", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ref_identity_eager_fallback(input, name=None, ctx=None):
  raise RuntimeError("ref_identity op does not support eager execution. Arg 'output' is a ref.")

@_dispatch.add_dispatch_list
@tf_export('reshape', v1=['reshape', 'manip.reshape'])
@deprecated_endpoints('manip.reshape')
def reshape(tensor, shape, name=None):
  r"""Reshapes a tensor.

  Given `tensor`, this operation returns a tensor that has the same values
  as `tensor` with shape `shape`.

  If one component of `shape` is the special value -1, the size of that dimension
  is computed so that the total size remains constant.  In particular, a `shape`
  of `[-1]` flattens into 1-D.  At most one component of `shape` can be -1.

  If `shape` is 1-D or higher, then the operation returns a tensor with shape
  `shape` filled with the values of `tensor`. In this case, the number of elements
  implied by `shape` must be the same as the number of elements in `tensor`.

  For example:

  ```
  # tensor 't' is [1, 2, 3, 4, 5, 6, 7, 8, 9]
  # tensor 't' has shape [9]
  reshape(t, [3, 3]) ==> [[1, 2, 3],
                          [4, 5, 6],
                          [7, 8, 9]]

  # tensor 't' is [[[1, 1], [2, 2]],
  #                [[3, 3], [4, 4]]]
  # tensor 't' has shape [2, 2, 2]
  reshape(t, [2, 4]) ==> [[1, 1, 2, 2],
                          [3, 3, 4, 4]]

  # tensor 't' is [[[1, 1, 1],
  #                 [2, 2, 2]],
  #                [[3, 3, 3],
  #                 [4, 4, 4]],
  #                [[5, 5, 5],
  #                 [6, 6, 6]]]
  # tensor 't' has shape [3, 2, 3]
  # pass '[-1]' to flatten 't'
  reshape(t, [-1]) ==> [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6]

  # -1 can also be used to infer the shape

  # -1 is inferred to be 9:
  reshape(t, [2, -1]) ==> [[1, 1, 1, 2, 2, 2, 3, 3, 3],
                           [4, 4, 4, 5, 5, 5, 6, 6, 6]]
  # -1 is inferred to be 2:
  reshape(t, [-1, 9]) ==> [[1, 1, 1, 2, 2, 2, 3, 3, 3],
                           [4, 4, 4, 5, 5, 5, 6, 6, 6]]
  # -1 is inferred to be 3:
  reshape(t, [ 2, -1, 3]) ==> [[[1, 1, 1],
                                [2, 2, 2],
                                [3, 3, 3]],
                               [[4, 4, 4],
                                [5, 5, 5],
                                [6, 6, 6]]]

  # tensor 't' is [7]
  # shape `[]` reshapes to a scalar
  reshape(t, []) ==> 7
  ```

  Args:
    tensor: A `Tensor`.
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Defines the shape of the output tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Reshape",
        name, _ctx._post_execution_callbacks, tensor, shape)
      return _result
    except _core._FallbackException:
      try:
        return reshape_eager_fallback(
            tensor, shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              reshape, tensor=tensor, shape=shape, name=name)
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
        "Reshape", tensor=tensor, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          reshape, tensor=tensor, shape=shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tshape", _op.get_attr("Tshape"))
  _execute.record_gradient(
      "Reshape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reshape_eager_fallback(tensor, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reshape
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _attr_Tshape, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  _inputs_flat = [tensor, shape]
  _attrs = ("T", _attr_T, "Tshape", _attr_Tshape)
  _result = _execute.execute(b"Reshape", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Reshape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resource_strided_slice_assign(ref, begin, end, strides, value, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None):
  r"""Assign `value` to the sliced l-value reference of `ref`.

  The values of `value` are assigned to the positions in the variable
  `ref` that are selected by the slice parameters. The slice parameters
  `begin, `end`, `strides`, etc. work exactly as in `StridedSlice`.

  NOTE this op currently does not support broadcasting and so `value`'s
  shape must be exactly the shape produced by the slice of `ref`.

  Args:
    ref: A `Tensor` of type `resource`.
    begin: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    end: A `Tensor`. Must have the same type as `begin`.
    strides: A `Tensor`. Must have the same type as `begin`.
    value: A `Tensor`.
    begin_mask: An optional `int`. Defaults to `0`.
    end_mask: An optional `int`. Defaults to `0`.
    ellipsis_mask: An optional `int`. Defaults to `0`.
    new_axis_mask: An optional `int`. Defaults to `0`.
    shrink_axis_mask: An optional `int`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceStridedSliceAssign", name, _ctx._post_execution_callbacks,
        ref, begin, end, strides, value, "begin_mask", begin_mask, "end_mask",
        end_mask, "ellipsis_mask", ellipsis_mask, "new_axis_mask",
        new_axis_mask, "shrink_axis_mask", shrink_axis_mask)
      return _result
    except _core._FallbackException:
      try:
        return resource_strided_slice_assign_eager_fallback(
            ref, begin, end, strides, value, begin_mask=begin_mask,
            end_mask=end_mask, ellipsis_mask=ellipsis_mask,
            new_axis_mask=new_axis_mask, shrink_axis_mask=shrink_axis_mask,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceStridedSliceAssign", ref=ref, begin=begin, end=end,
                                      strides=strides, value=value,
                                      begin_mask=begin_mask,
                                      end_mask=end_mask,
                                      ellipsis_mask=ellipsis_mask,
                                      new_axis_mask=new_axis_mask,
                                      shrink_axis_mask=shrink_axis_mask,
                                      name=name)
  return _op
  _result = None
  return _result



def resource_strided_slice_assign_eager_fallback(ref, begin, end, strides, value, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_strided_slice_assign
  """
  _ctx = ctx if ctx else _context.context()
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _attr_Index, _inputs_Index = _execute.args_to_matching_eager([begin, end, strides], _ctx)
  (begin, end, strides) = _inputs_Index
  ref = _ops.convert_to_tensor(ref, _dtypes.resource)
  _inputs_flat = [ref, begin, end, strides, value]
  _attrs = ("T", _attr_T, "Index", _attr_Index, "begin_mask", begin_mask,
  "end_mask", end_mask, "ellipsis_mask", ellipsis_mask, "new_axis_mask",
  new_axis_mask, "shrink_axis_mask", shrink_axis_mask)
  _result = _execute.execute(b"ResourceStridedSliceAssign", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def reverse(tensor, dims, name=None):
  r"""Reverses specific dimensions of a tensor.

  Given a `tensor`, and a `bool` tensor `dims` representing the dimensions
  of `tensor`, this operation reverses each dimension i of `tensor` where
  `dims[i]` is `True`.

  `tensor` can have up to 8 dimensions. The number of dimensions
  of `tensor` must equal the number of elements in `dims`. In other words:

  `rank(tensor) = size(dims)`

  For example:

  ```
  # tensor 't' is [[[[ 0,  1,  2,  3],
  #                  [ 4,  5,  6,  7],
  #                  [ 8,  9, 10, 11]],
  #                 [[12, 13, 14, 15],
  #                  [16, 17, 18, 19],
  #                  [20, 21, 22, 23]]]]
  # tensor 't' shape is [1, 2, 3, 4]

  # 'dims' is [False, False, False, True]
  reverse(t, dims) ==> [[[[ 3,  2,  1,  0],
                          [ 7,  6,  5,  4],
                          [ 11, 10, 9, 8]],
                         [[15, 14, 13, 12],
                          [19, 18, 17, 16],
                          [23, 22, 21, 20]]]]

  # 'dims' is [False, True, False, False]
  reverse(t, dims) ==> [[[[12, 13, 14, 15],
                          [16, 17, 18, 19],
                          [20, 21, 22, 23]
                         [[ 0,  1,  2,  3],
                          [ 4,  5,  6,  7],
                          [ 8,  9, 10, 11]]]]

  # 'dims' is [False, False, True, False]
  reverse(t, dims) ==> [[[[8, 9, 10, 11],
                          [4, 5, 6, 7],
                          [0, 1, 2, 3]]
                         [[20, 21, 22, 23],
                          [16, 17, 18, 19],
                          [12, 13, 14, 15]]]]
  ```

  Args:
    tensor: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `bool`, `half`, `float32`, `float64`, `complex64`, `complex128`, `string`.
      Up to 8-D.
    dims: A `Tensor` of type `bool`. 1-D. The dimensions to reverse.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Reverse",
        name, _ctx._post_execution_callbacks, tensor, dims)
      return _result
    except _core._FallbackException:
      try:
        return reverse_eager_fallback(
            tensor, dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Reverse", tensor=tensor, dims=dims, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Reverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reverse_eager_fallback(tensor, dims, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reverse
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  dims = _ops.convert_to_tensor(dims, _dtypes.bool)
  _inputs_flat = [tensor, dims]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Reverse", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Reverse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reverse_sequence(input, seq_lengths, seq_dim, batch_dim=0, name=None):
  r"""Reverses variable length slices.

  This op first slices `input` along the dimension `batch_dim`, and for each
  slice `i`, reverses the first `seq_lengths[i]` elements along
  the dimension `seq_dim`.

  The elements of `seq_lengths` must obey `seq_lengths[i] <= input.dims[seq_dim]`,
  and `seq_lengths` must be a vector of length `input.dims[batch_dim]`.

  The output slice `i` along dimension `batch_dim` is then given by input
  slice `i`, with the first `seq_lengths[i]` slices along dimension
  `seq_dim` reversed.

  For example:

  ```
  # Given this:
  batch_dim = 0
  seq_dim = 1
  input.dims = (4, 8, ...)
  seq_lengths = [7, 2, 3, 5]

  # then slices of input are reversed on seq_dim, but only up to seq_lengths:
  output[0, 0:7, :, ...] = input[0, 7:0:-1, :, ...]
  output[1, 0:2, :, ...] = input[1, 2:0:-1, :, ...]
  output[2, 0:3, :, ...] = input[2, 3:0:-1, :, ...]
  output[3, 0:5, :, ...] = input[3, 5:0:-1, :, ...]

  # while entries past seq_lens are copied through:
  output[0, 7:, :, ...] = input[0, 7:, :, ...]
  output[1, 2:, :, ...] = input[1, 2:, :, ...]
  output[2, 3:, :, ...] = input[2, 3:, :, ...]
  output[3, 2:, :, ...] = input[3, 2:, :, ...]
  ```

  In contrast, if:

  ```
  # Given this:
  batch_dim = 2
  seq_dim = 0
  input.dims = (8, ?, 4, ...)
  seq_lengths = [7, 2, 3, 5]

  # then slices of input are reversed on seq_dim, but only up to seq_lengths:
  output[0:7, :, 0, :, ...] = input[7:0:-1, :, 0, :, ...]
  output[0:2, :, 1, :, ...] = input[2:0:-1, :, 1, :, ...]
  output[0:3, :, 2, :, ...] = input[3:0:-1, :, 2, :, ...]
  output[0:5, :, 3, :, ...] = input[5:0:-1, :, 3, :, ...]

  # while entries past seq_lens are copied through:
  output[7:, :, 0, :, ...] = input[7:, :, 0, :, ...]
  output[2:, :, 1, :, ...] = input[2:, :, 1, :, ...]
  output[3:, :, 2, :, ...] = input[3:, :, 2, :, ...]
  output[2:, :, 3, :, ...] = input[2:, :, 3, :, ...]
  ```

  Args:
    input: A `Tensor`. The input to reverse.
    seq_lengths: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D with length `input.dims(batch_dim)` and
      `max(seq_lengths) <= input.dims(seq_dim)`
    seq_dim: An `int`. The dimension which is partially reversed.
    batch_dim: An optional `int`. Defaults to `0`.
      The dimension along which reversal is performed.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReverseSequence", name, _ctx._post_execution_callbacks, input,
        seq_lengths, "seq_dim", seq_dim, "batch_dim", batch_dim)
      return _result
    except _core._FallbackException:
      try:
        return reverse_sequence_eager_fallback(
            input, seq_lengths, seq_dim=seq_dim, batch_dim=batch_dim,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  seq_dim = _execute.make_int(seq_dim, "seq_dim")
  if batch_dim is None:
    batch_dim = 0
  batch_dim = _execute.make_int(batch_dim, "batch_dim")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReverseSequence", input=input, seq_lengths=seq_lengths,
                           seq_dim=seq_dim, batch_dim=batch_dim, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seq_dim", _op.get_attr("seq_dim"), "batch_dim",
            _op.get_attr("batch_dim"), "T", _op.get_attr("T"), "Tlen",
            _op.get_attr("Tlen"))
  _execute.record_gradient(
      "ReverseSequence", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reverse_sequence_eager_fallback(input, seq_lengths, seq_dim, batch_dim=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reverse_sequence
  """
  _ctx = ctx if ctx else _context.context()
  seq_dim = _execute.make_int(seq_dim, "seq_dim")
  if batch_dim is None:
    batch_dim = 0
  batch_dim = _execute.make_int(batch_dim, "batch_dim")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tlen, (seq_lengths,) = _execute.args_to_matching_eager([seq_lengths], _ctx, _dtypes.int64)
  _inputs_flat = [input, seq_lengths]
  _attrs = ("seq_dim", seq_dim, "batch_dim", batch_dim, "T", _attr_T, "Tlen",
  _attr_Tlen)
  _result = _execute.execute(b"ReverseSequence", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReverseSequence", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('reverse', v1=['reverse', 'manip.reverse', 'reverse_v2'])
@deprecated_endpoints('manip.reverse', 'reverse_v2')
def reverse_v2(tensor, axis, name=None):
  r"""Reverses specific dimensions of a tensor.

  NOTE `tf.reverse` has now changed behavior in preparation for 1.0.
  `tf.reverse_v2` is currently an alias that will be deprecated before TF 1.0.

  Given a `tensor`, and a `int32` tensor `axis` representing the set of
  dimensions of `tensor` to reverse. This operation reverses each dimension
  `i` for which there exists `j` s.t. `axis[j] == i`.

  `tensor` can have up to 8 dimensions. The number of dimensions specified
  in `axis` may be 0 or more entries. If an index is specified more than
  once, a InvalidArgument error is raised.

  For example:

  ```
  # tensor 't' is [[[[ 0,  1,  2,  3],
  #                  [ 4,  5,  6,  7],
  #                  [ 8,  9, 10, 11]],
  #                 [[12, 13, 14, 15],
  #                  [16, 17, 18, 19],
  #                  [20, 21, 22, 23]]]]
  # tensor 't' shape is [1, 2, 3, 4]

  # 'dims' is [3] or 'dims' is [-1]
  reverse(t, dims) ==> [[[[ 3,  2,  1,  0],
                          [ 7,  6,  5,  4],
                          [ 11, 10, 9, 8]],
                         [[15, 14, 13, 12],
                          [19, 18, 17, 16],
                          [23, 22, 21, 20]]]]

  # 'dims' is '[1]' (or 'dims' is '[-3]')
  reverse(t, dims) ==> [[[[12, 13, 14, 15],
                          [16, 17, 18, 19],
                          [20, 21, 22, 23]
                         [[ 0,  1,  2,  3],
                          [ 4,  5,  6,  7],
                          [ 8,  9, 10, 11]]]]

  # 'dims' is '[2]' (or 'dims' is '[-2]')
  reverse(t, dims) ==> [[[[8, 9, 10, 11],
                          [4, 5, 6, 7],
                          [0, 1, 2, 3]]
                         [[20, 21, 22, 23],
                          [16, 17, 18, 19],
                          [12, 13, 14, 15]]]]
  ```

  Args:
    tensor: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `uint16`, `int16`, `int32`, `int64`, `bool`, `bfloat16`, `half`, `float32`, `float64`, `complex64`, `complex128`, `string`.
      Up to 8-D.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D. The indices of the dimensions to reverse. Must be in the range
      `[-rank(tensor), rank(tensor))`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ReverseV2",
        name, _ctx._post_execution_callbacks, tensor, axis)
      return _result
    except _core._FallbackException:
      try:
        return reverse_v2_eager_fallback(
            tensor, axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              reverse_v2, tensor=tensor, axis=axis, name=name)
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
        "ReverseV2", tensor=tensor, axis=axis, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          reverse_v2, tensor=tensor, axis=axis, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tidx", _op.get_attr("Tidx"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "ReverseV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reverse_v2_eager_fallback(tensor, axis, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reverse_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tidx, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int32)
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _inputs_flat = [tensor, axis]
  _attrs = ("Tidx", _attr_Tidx, "T", _attr_T)
  _result = _execute.execute(b"ReverseV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReverseV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('scatter_nd', v1=['scatter_nd', 'manip.scatter_nd'])
@deprecated_endpoints('manip.scatter_nd')
def scatter_nd(indices, updates, shape, name=None):
  r"""Scatter `updates` into a new tensor according to `indices`.

  Creates a new tensor by applying sparse `updates` to individual values or
  slices within a tensor (initially zero for numeric, empty for string) of
  the given `shape` according to indices.  This operator is the inverse of the
  `tf.gather_nd` operator which extracts values or slices from a given tensor.

  This operation is similar to tensor_scatter_add, except that the tensor is
  zero-initialized. Calling `tf.scatter_nd(indices, values, shape)` is identical
  to `tensor_scatter_add(tf.zeros(shape, values.dtype), indices, values)`

  If `indices` contains duplicates, then their updates are accumulated (summed).

  **WARNING**: The order in which updates are applied is nondeterministic, so the
  output will be nondeterministic if `indices` contains duplicates -- because
  of some numerical approximation issues, numbers summed in different order
  may yield different results.

  `indices` is an integer tensor containing indices into a new tensor of shape
  `shape`.  The last dimension of `indices` can be at most the rank of `shape`:

      indices.shape[-1] <= shape.rank

  The last dimension of `indices` corresponds to indices into elements
  (if `indices.shape[-1] = shape.rank`) or slices
  (if `indices.shape[-1] < shape.rank`) along dimension `indices.shape[-1]` of
  `shape`.  `updates` is a tensor with shape

      indices.shape[:-1] + shape[indices.shape[-1]:]

  The simplest form of scatter is to insert individual elements in a tensor by
  index. For example, say we want to insert 4 scattered elements in a rank-1
  tensor with 8 elements.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/ScatterNd1.png" alt>
  </div>

  In Python, this scatter operation would look like this:

  ```python
      indices = tf.constant([[4], [3], [1], [7]])
      updates = tf.constant([9, 10, 11, 12])
      shape = tf.constant([8])
      scatter = tf.scatter_nd(indices, updates, shape)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [0, 11, 0, 10, 9, 0, 0, 12]

  We can also, insert entire slices of a higher rank tensor all at once. For
  example, if we wanted to insert two slices in the first dimension of a
  rank-3 tensor with two matrices of new values.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/ScatterNd2.png" alt>
  </div>

  In Python, this scatter operation would look like this:

  ```python
      indices = tf.constant([[0], [2]])
      updates = tf.constant([[[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]],
                             [[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]]])
      shape = tf.constant([4, 4, 4])
      scatter = tf.scatter_nd(indices, updates, shape)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [[[5, 5, 5, 5], [6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8]],
       [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]],
       [[5, 5, 5, 5], [6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8]],
       [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]]

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, the index is ignored.

  Args:
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor.
    updates: A `Tensor`. Updates to scatter into output.
    shape: A `Tensor`. Must have the same type as `indices`.
      1-D. The shape of the resulting tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `updates`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ScatterNd",
        name, _ctx._post_execution_callbacks, indices, updates, shape)
      return _result
    except _core._FallbackException:
      try:
        return scatter_nd_eager_fallback(
            indices, updates, shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              scatter_nd, indices=indices, updates=updates, shape=shape,
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
        "ScatterNd", indices=indices, updates=updates, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          scatter_nd, indices=indices, updates=updates, shape=shape,
                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "ScatterNd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def scatter_nd_eager_fallback(indices, updates, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function scatter_nd
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, _inputs_Tindices = _execute.args_to_matching_eager([indices, shape], _ctx)
  (indices, shape) = _inputs_Tindices
  _inputs_flat = [indices, updates, shape]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ScatterNd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ScatterNd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def scatter_nd_non_aliasing_add(input, indices, updates, name=None):
  r"""Applies sparse addition to `input` using individual values or slices

  from `updates` according to indices `indices`.  The updates are non-aliasing:
  `input` is only modified in-place if no other operations will use it.
  Otherwise, a copy of `input` is made.  This operation has a gradient with
  respect to both `input` and `updates`.

  `input` is a `Tensor` with rank `P` and `indices` is a `Tensor` of rank `Q`.

  `indices` must be integer tensor, containing indices into `input`.
  It must be shape \\([d_0, ..., d_{Q-2}, K]\\) where `0 < K <= P`.

  The innermost dimension of `indices` (with length `K`) corresponds to
  indices into elements (if `K = P`) or `(P-K)`-dimensional slices
  (if `K < P`) along the `K`th dimension of `input`.

  `updates` is `Tensor` of rank `Q-1+P-K` with shape:

  $$[d_0, ..., d_{Q-2}, input.shape[K], ..., input.shape[P-1]].$$

  For example, say we want to add 4 scattered elements to a rank-1 tensor to 8
  elements. In Python, that addition would look like this:

      input = tf.constant([1, 2, 3, 4, 5, 6, 7, 8])
      indices = tf.constant([[4], [3], [1], [7]])
      updates = tf.constant([9, 10, 11, 12])
      output = tf.scatter_nd_non_aliasing_add(input, indices, updates)
      with tf.Session() as sess:
        print(sess.run(output))

  The resulting value `output` would look like this:

      [1, 13, 3, 14, 14, 6, 7, 20]

  See `tf.scatter_nd` for more details about how to make updates to slices.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`, `bool`.
      A Tensor.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A Tensor. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into `input`.
    updates: A `Tensor`. Must have the same type as `input`.
      A Tensor. Must have the same type as ref. A tensor of updated values
      to add to `input`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ScatterNdNonAliasingAdd", name, _ctx._post_execution_callbacks,
        input, indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return scatter_nd_non_aliasing_add_eager_fallback(
            input, indices, updates, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ScatterNdNonAliasingAdd", input=input, indices=indices,
                                   updates=updates, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "ScatterNdNonAliasingAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def scatter_nd_non_aliasing_add_eager_fallback(input, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function scatter_nd_non_aliasing_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([input, updates], _ctx)
  (input, updates) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [input, indices, updates]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ScatterNdNonAliasingAdd", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ScatterNdNonAliasingAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def shape(input, out_type=_dtypes.int32, name=None):
  r"""Returns the shape of a tensor.

  This operation returns a 1-D integer tensor representing the shape of `input`.

  For example:

  ```
  # 't' is [[[1, 1, 1], [2, 2, 2]], [[3, 3, 3], [4, 4, 4]]]
  shape(t) ==> [2, 2, 3]
  ```

  Args:
    input: A `Tensor`.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Shape", name,
        _ctx._post_execution_callbacks, input, "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return shape_eager_fallback(
            input, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Shape", input=input, out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "Shape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def shape_eager_fallback(input, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shape
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"Shape", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Shape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def shape_n(input, out_type=_dtypes.int32, name=None):
  r"""Returns shape of tensors.

  This operation returns N 1-D integer tensors representing shape of `input[i]s`.

  Args:
    input: A list of at least 1 `Tensor` objects with the same type.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A list with the same length as `input` of `Tensor` objects with type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ShapeN", name,
        _ctx._post_execution_callbacks, input, "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return shape_n_eager_fallback(
            input, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(input, (list, tuple)):
    raise TypeError(
        "Expected list for 'input' argument to "
        "'shape_n' Op, not %r." % input)
  _attr_N = len(input)
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ShapeN", input=input, out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"), "out_type",
            _op.get_attr("out_type"))
  _execute.record_gradient(
      "ShapeN", _inputs_flat, _attrs, _result, name)
  return _result



def shape_n_eager_fallback(input, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shape_n
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(input, (list, tuple)):
    raise TypeError(
        "Expected list for 'input' argument to "
        "'shape_n' Op, not %r." % input)
  _attr_N = len(input)
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, input = _execute.args_to_matching_eager(list(input), _ctx)
  _inputs_flat = list(input)
  _attrs = ("N", _attr_N, "T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"ShapeN", _attr_N, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ShapeN", _inputs_flat, _attrs, _result, name)
  return _result


def size(input, out_type=_dtypes.int32, name=None):
  r"""Returns the size of a tensor.

  This operation returns an integer representing the number of elements in
  `input`.

  For example:

  ```
  # 't' is [[[1, 1,, 1], [2, 2, 2]], [[3, 3, 3], [4, 4, 4]]]]
  size(t) ==> 12
  ```

  Args:
    input: A `Tensor`.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Size", name,
        _ctx._post_execution_callbacks, input, "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return size_eager_fallback(
            input, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Size", input=input, out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "Size", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def size_eager_fallback(input, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function size
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"Size", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Size", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _slice(input, begin, size, name=None):
  r"""Return a slice from 'input'.

  The output tensor is a tensor with dimensions described by 'size'
  whose values are extracted from 'input' starting at the offsets in
  'begin'.

  *Requirements*:
    0 <= begin[i] <= begin[i] + size[i] <= Di  for i in [0, n)

  Args:
    input: A `Tensor`.
    begin: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      begin[i] specifies the offset into the 'i'th dimension of
      'input' to slice from.
    size: A `Tensor`. Must have the same type as `begin`.
      size[i] specifies the number of elements of the 'i'th dimension
      of 'input' to slice. If size[i] is -1, all remaining elements in dimension
      i are included in the slice (i.e. this is equivalent to setting
      size[i] = input.dim_size(i) - begin[i]).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Slice", name,
        _ctx._post_execution_callbacks, input, begin, size)
      return _result
    except _core._FallbackException:
      try:
        return _slice_eager_fallback(
            input, begin, size, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Slice", input=input, begin=begin, size=size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Index", _op.get_attr("Index"))
  _execute.record_gradient(
      "Slice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _slice_eager_fallback(input, begin, size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _slice
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Index, _inputs_Index = _execute.args_to_matching_eager([begin, size], _ctx)
  (begin, size) = _inputs_Index
  _inputs_flat = [input, begin, size]
  _attrs = ("T", _attr_T, "Index", _attr_Index)
  _result = _execute.execute(b"Slice", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Slice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def snapshot(input, name=None):
  r"""Returns a copy of the input tensor.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Snapshot",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return snapshot_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Snapshot", input=input, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Snapshot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def snapshot_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function snapshot
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Snapshot", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Snapshot", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def space_to_batch(input, paddings, block_size, name=None):
  r"""SpaceToBatch for 4-D tensors of type T.

  This is a legacy version of the more general SpaceToBatchND.

  Zero-pads and then rearranges (permutes) blocks of spatial data into batch.
  More specifically, this op outputs a copy of the input tensor where values from
  the `height` and `width` dimensions are moved to the `batch` dimension. After
  the zero-padding, both `height` and `width` of the input must be divisible by the
  block size.

  Args:
    input: A `Tensor`. 4-D with shape `[batch, height, width, depth]`.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D tensor of non-negative integers with shape `[2, 2]`. It specifies
        the padding of the input with zeros across the spatial dimensions as follows:

            paddings = [[pad_top, pad_bottom], [pad_left, pad_right]]

        The effective spatial dimensions of the zero-padded input tensor will be:

            height_pad = pad_top + height + pad_bottom
            width_pad = pad_left + width + pad_right

      The attr `block_size` must be greater than one. It indicates the block size.

        * Non-overlapping blocks of size `block_size x block size` in the height and
          width dimensions are rearranged into the batch dimension at each location.
        * The batch of the output tensor is `batch * block_size * block_size`.
        * Both height_pad and width_pad must be divisible by block_size.

      The shape of the output will be:

          [batch*block_size*block_size, height_pad/block_size, width_pad/block_size,
           depth]

      Some examples:

      (1) For the following input of shape `[1, 2, 2, 1]` and block_size of 2:

      ```
      x = [[[[1], [2]], [[3], [4]]]]
      ```

      The output tensor has shape `[4, 1, 1, 1]` and value:

      ```
      [[[[1]]], [[[2]]], [[[3]]], [[[4]]]]
      ```

      (2) For the following input of shape `[1, 2, 2, 3]` and block_size of 2:

      ```
      x = [[[[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]]]]
      ```

      The output tensor has shape `[4, 1, 1, 3]` and value:

      ```
      [[[1, 2, 3]], [[4, 5, 6]], [[7, 8, 9]], [[10, 11, 12]]]
      ```

      (3) For the following input of shape `[1, 4, 4, 1]` and block_size of 2:

      ```
      x = [[[[1],   [2],  [3],  [4]],
            [[5],   [6],  [7],  [8]],
            [[9],  [10], [11],  [12]],
            [[13], [14], [15],  [16]]]]
      ```

      The output tensor has shape `[4, 2, 2, 1]` and value:

      ```
      x = [[[[1], [3]], [[9], [11]]],
           [[[2], [4]], [[10], [12]]],
           [[[5], [7]], [[13], [15]]],
           [[[6], [8]], [[14], [16]]]]
      ```

      (4) For the following input of shape `[2, 2, 4, 1]` and block_size of 2:

      ```
      x = [[[[1],   [2],  [3],  [4]],
            [[5],   [6],  [7],  [8]]],
           [[[9],  [10], [11],  [12]],
            [[13], [14], [15],  [16]]]]
      ```

      The output tensor has shape `[8, 1, 2, 1]` and value:

      ```
      x = [[[[1], [3]]], [[[9], [11]]], [[[2], [4]]], [[[10], [12]]],
           [[[5], [7]]], [[[13], [15]]], [[[6], [8]]], [[[14], [16]]]]
      ```

      Among others, this operation is useful for reducing atrous convolution into
      regular convolution.
    block_size: An `int` that is `>= 2`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SpaceToBatch",
        name, _ctx._post_execution_callbacks, input, paddings, "block_size",
        block_size)
      return _result
    except _core._FallbackException:
      try:
        return space_to_batch_eager_fallback(
            input, paddings, block_size=block_size, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  block_size = _execute.make_int(block_size, "block_size")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SpaceToBatch", input=input, paddings=paddings, block_size=block_size,
                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tpaddings", _op.get_attr("Tpaddings"),
            "block_size", _op.get_attr("block_size"))
  _execute.record_gradient(
      "SpaceToBatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def space_to_batch_eager_fallback(input, paddings, block_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function space_to_batch
  """
  _ctx = ctx if ctx else _context.context()
  block_size = _execute.make_int(block_size, "block_size")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, paddings]
  _attrs = ("T", _attr_T, "Tpaddings", _attr_Tpaddings, "block_size",
  block_size)
  _result = _execute.execute(b"SpaceToBatch", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SpaceToBatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('space_to_batch_nd', v1=['space_to_batch_nd', 'manip.space_to_batch_nd'])
@deprecated_endpoints('manip.space_to_batch_nd')
def space_to_batch_nd(input, block_shape, paddings, name=None):
  r"""SpaceToBatch for N-D tensors of type T.

  This operation divides "spatial" dimensions `[1, ..., M]` of the input into a
  grid of blocks of shape `block_shape`, and interleaves these blocks with the
  "batch" dimension (0) such that in the output, the spatial dimensions
  `[1, ..., M]` correspond to the position within the grid, and the batch
  dimension combines both the position within a spatial block and the original
  batch position.  Prior to division into blocks, the spatial dimensions of the
  input are optionally zero padded according to `paddings`.  See below for a
  precise description.

  Args:
    input: A `Tensor`.
      N-D with shape `input_shape = [batch] + spatial_shape + remaining_shape`,
      where spatial_shape has `M` dimensions.
    block_shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D with shape `[M]`, all values must be >= 1.
    paddings: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2-D with shape `[M, 2]`, all values must be >= 0.
        `paddings[i] = [pad_start, pad_end]` specifies the padding for input dimension
        `i + 1`, which corresponds to spatial dimension `i`.  It is required that
        `block_shape[i]` divides `input_shape[i + 1] + pad_start + pad_end`.

      This operation is equivalent to the following steps:

      1. Zero-pad the start and end of dimensions `[1, ..., M]` of the
         input according to `paddings` to produce `padded` of shape `padded_shape`.

      2. Reshape `padded` to `reshaped_padded` of shape:

           [batch] +
           [padded_shape[1] / block_shape[0],
             block_shape[0],
            ...,
            padded_shape[M] / block_shape[M-1],
            block_shape[M-1]] +
           remaining_shape

      3. Permute dimensions of `reshaped_padded` to produce
         `permuted_reshaped_padded` of shape:

           block_shape +
           [batch] +
           [padded_shape[1] / block_shape[0],
            ...,
            padded_shape[M] / block_shape[M-1]] +
           remaining_shape

      4. Reshape `permuted_reshaped_padded` to flatten `block_shape` into the batch
         dimension, producing an output tensor of shape:

           [batch * prod(block_shape)] +
           [padded_shape[1] / block_shape[0],
            ...,
            padded_shape[M] / block_shape[M-1]] +
           remaining_shape

      Some examples:

      (1) For the following input of shape `[1, 2, 2, 1]`, `block_shape = [2, 2]`, and
          `paddings = [[0, 0], [0, 0]]`:

      ```
      x = [[[[1], [2]], [[3], [4]]]]
      ```

      The output tensor has shape `[4, 1, 1, 1]` and value:

      ```
      [[[[1]]], [[[2]]], [[[3]]], [[[4]]]]
      ```

      (2) For the following input of shape `[1, 2, 2, 3]`, `block_shape = [2, 2]`, and
          `paddings = [[0, 0], [0, 0]]`:

      ```
      x = [[[[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]]]]
      ```

      The output tensor has shape `[4, 1, 1, 3]` and value:

      ```
      [[[1, 2, 3]], [[4, 5, 6]], [[7, 8, 9]], [[10, 11, 12]]]
      ```

      (3) For the following input of shape `[1, 4, 4, 1]`, `block_shape = [2, 2]`, and
          `paddings = [[0, 0], [0, 0]]`:

      ```
      x = [[[[1],   [2],  [3],  [4]],
            [[5],   [6],  [7],  [8]],
            [[9],  [10], [11],  [12]],
            [[13], [14], [15],  [16]]]]
      ```

      The output tensor has shape `[4, 2, 2, 1]` and value:

      ```
      x = [[[[1], [3]], [[9], [11]]],
           [[[2], [4]], [[10], [12]]],
           [[[5], [7]], [[13], [15]]],
           [[[6], [8]], [[14], [16]]]]
      ```

      (4) For the following input of shape `[2, 2, 4, 1]`, block_shape = `[2, 2]`, and
          paddings = `[[0, 0], [2, 0]]`:

      ```
      x = [[[[1],   [2],  [3],  [4]],
            [[5],   [6],  [7],  [8]]],
           [[[9],  [10], [11],  [12]],
            [[13], [14], [15],  [16]]]]
      ```

      The output tensor has shape `[8, 1, 3, 1]` and value:

      ```
      x = [[[[0], [1], [3]]], [[[0], [9], [11]]],
           [[[0], [2], [4]]], [[[0], [10], [12]]],
           [[[0], [5], [7]]], [[[0], [13], [15]]],
           [[[0], [6], [8]]], [[[0], [14], [16]]]]
      ```

      Among others, this operation is useful for reducing atrous convolution into
      regular convolution.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SpaceToBatchND", name, _ctx._post_execution_callbacks, input,
        block_shape, paddings)
      return _result
    except _core._FallbackException:
      try:
        return space_to_batch_nd_eager_fallback(
            input, block_shape, paddings, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              space_to_batch_nd, input=input, block_shape=block_shape,
                                 paddings=paddings, name=name)
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
        "SpaceToBatchND", input=input, block_shape=block_shape,
                          paddings=paddings, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          space_to_batch_nd, input=input, block_shape=block_shape,
                             paddings=paddings, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tblock_shape",
            _op.get_attr("Tblock_shape"), "Tpaddings",
            _op.get_attr("Tpaddings"))
  _execute.record_gradient(
      "SpaceToBatchND", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def space_to_batch_nd_eager_fallback(input, block_shape, paddings, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function space_to_batch_nd
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tblock_shape, (block_shape,) = _execute.args_to_matching_eager([block_shape], _ctx, _dtypes.int32)
  _attr_Tpaddings, (paddings,) = _execute.args_to_matching_eager([paddings], _ctx, _dtypes.int32)
  _inputs_flat = [input, block_shape, paddings]
  _attrs = ("T", _attr_T, "Tblock_shape", _attr_Tblock_shape, "Tpaddings",
  _attr_Tpaddings)
  _result = _execute.execute(b"SpaceToBatchND", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SpaceToBatchND", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def space_to_depth(input, block_size, data_format="NHWC", name=None):
  r"""SpaceToDepth for tensors of type T.

  Rearranges blocks of spatial data, into depth. More specifically,
  this op outputs a copy of the input tensor where values from the `height`
  and `width` dimensions are moved to the `depth` dimension.
  The attr `block_size` indicates the input block size.

    * Non-overlapping blocks of size `block_size x block size` are rearranged
      into depth at each location.
    * The depth of the output tensor is `block_size * block_size * input_depth`.
    * The Y, X coordinates within each block of the input become the high order
      component of the output channel index.
    * The input tensor's height and width must be divisible by block_size.

  The `data_format` attr specifies the layout of the input and output tensors
  with the following options:
    "NHWC": `[ batch, height, width, channels ]`
    "NCHW": `[ batch, channels, height, width ]`
    "NCHW_VECT_C":
        `qint8 [ batch, channels / 4, height, width, 4 ]`

  It is useful to consider the operation as transforming a 6-D Tensor.
  e.g. for data_format = NHWC,
       Each element in the input tensor can be specified via 6 coordinates,
       ordered by decreasing memory layout significance as:
       n,oY,bY,oX,bX,iC  (where n=batch index, oX, oY means X or Y coordinates
                          within the output image, bX, bY means coordinates
                          within the input block, iC means input channels).
       The output would be a transpose to the following layout:
       n,oY,oX,bY,bX,iC

  This operation is useful for resizing the activations between convolutions
  (but keeping all data), e.g. instead of pooling. It is also useful for training
  purely convolutional models.

  For example, given an input of shape `[1, 2, 2, 1]`, data_format = "NHWC" and
  block_size = 2:

  ```
  x = [[[[1], [2]],
        [[3], [4]]]]
  ```

  This operation will output a tensor of shape `[1, 1, 1, 4]`:

  ```
  [[[[1, 2, 3, 4]]]]
  ```

  Here, the input has a batch of 1 and each batch element has shape `[2, 2, 1]`,
  the corresponding output will have a single element (i.e. width and height are
  both 1) and will have a depth of 4 channels (1 * block_size * block_size).
  The output element shape is `[1, 1, 4]`.

  For an input tensor with larger depth, here of shape `[1, 2, 2, 3]`, e.g.

  ```
  x = [[[[1, 2, 3], [4, 5, 6]],
        [[7, 8, 9], [10, 11, 12]]]]
  ```

  This operation, for block_size of 2, will return the following tensor of shape
  `[1, 1, 1, 12]`

  ```
  [[[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]]]
  ```

  Similarly, for the following input of shape `[1 4 4 1]`, and a block size of 2:

  ```
  x = [[[[1],   [2],  [5],  [6]],
        [[3],   [4],  [7],  [8]],
        [[9],  [10], [13],  [14]],
        [[11], [12], [15],  [16]]]]
  ```

  the operator will return the following tensor of shape `[1 2 2 4]`:

  ```
  x = [[[[1, 2, 3, 4],
         [5, 6, 7, 8]],
        [[9, 10, 11, 12],
         [13, 14, 15, 16]]]]
  ```

  Args:
    input: A `Tensor`.
    block_size: An `int` that is `>= 2`. The size of the spatial block.
    data_format: An optional `string` from: `"NHWC", "NCHW", "NCHW_VECT_C"`. Defaults to `"NHWC"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SpaceToDepth",
        name, _ctx._post_execution_callbacks, input, "block_size", block_size,
        "data_format", data_format)
      return _result
    except _core._FallbackException:
      try:
        return space_to_depth_eager_fallback(
            input, block_size=block_size, data_format=data_format, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  block_size = _execute.make_int(block_size, "block_size")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SpaceToDepth", input=input, block_size=block_size,
                        data_format=data_format, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "block_size", _op.get_attr("block_size"),
            "data_format", _op.get_attr("data_format"))
  _execute.record_gradient(
      "SpaceToDepth", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def space_to_depth_eager_fallback(input, block_size, data_format="NHWC", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function space_to_depth
  """
  _ctx = ctx if ctx else _context.context()
  block_size = _execute.make_int(block_size, "block_size")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "block_size", block_size, "data_format",
  data_format)
  _result = _execute.execute(b"SpaceToDepth", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SpaceToDepth", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def split(axis, value, num_split, name=None):
  r"""Splits a tensor into `num_split` tensors along one dimension.

  Args:
    axis: A `Tensor` of type `int32`.
      0-D.  The dimension along which to split.  Must be in the range
      `[-rank(value), rank(value))`.
    value: A `Tensor`. The tensor to split.
    num_split: An `int` that is `>= 1`.
      The number of ways to split.  Must evenly divide
      `value.shape[split_dim]`.
    name: A name for the operation (optional).

  Returns:
    A list of `num_split` `Tensor` objects with the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Split", name,
        _ctx._post_execution_callbacks, axis, value, "num_split", num_split)
      return _result
    except _core._FallbackException:
      try:
        return split_eager_fallback(
            axis, value, num_split=num_split, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_split = _execute.make_int(num_split, "num_split")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Split", split_dim=axis, value=value, num_split=num_split, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_split", _op.get_attr("num_split"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "Split", _inputs_flat, _attrs, _result, name)
  return _result



def split_eager_fallback(axis, value, num_split, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function split
  """
  _ctx = ctx if ctx else _context.context()
  num_split = _execute.make_int(num_split, "num_split")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  axis = _ops.convert_to_tensor(axis, _dtypes.int32)
  _inputs_flat = [axis, value]
  _attrs = ("num_split", num_split, "T", _attr_T)
  _result = _execute.execute(b"Split", num_split, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Split", _inputs_flat, _attrs, _result, name)
  return _result


def split_v(value, size_splits, axis, num_split, name=None):
  r"""Splits a tensor into `num_split` tensors along one dimension.

  Args:
    value: A `Tensor`. The tensor to split.
    size_splits: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      list containing the sizes of each output tensor along the split
      dimension. Must sum to the dimension of value along split_dim.
      Can contain one -1 indicating that dimension is to be inferred.
    axis: A `Tensor` of type `int32`.
      0-D.  The dimension along which to split.  Must be in the range
      `[-rank(value), rank(value))`.
    num_split: An `int` that is `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A list of `num_split` `Tensor` objects with the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SplitV", name,
        _ctx._post_execution_callbacks, value, size_splits, axis, "num_split",
        num_split)
      return _result
    except _core._FallbackException:
      try:
        return split_v_eager_fallback(
            value, size_splits, axis, num_split=num_split, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_split = _execute.make_int(num_split, "num_split")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SplitV", value=value, size_splits=size_splits, split_dim=axis,
                  num_split=num_split, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_split", _op.get_attr("num_split"), "T", _op.get_attr("T"),
            "Tlen", _op.get_attr("Tlen"))
  _execute.record_gradient(
      "SplitV", _inputs_flat, _attrs, _result, name)
  return _result



def split_v_eager_fallback(value, size_splits, axis, num_split, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function split_v
  """
  _ctx = ctx if ctx else _context.context()
  num_split = _execute.make_int(num_split, "num_split")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _attr_Tlen, (size_splits,) = _execute.args_to_matching_eager([size_splits], _ctx, _dtypes.int64)
  axis = _ops.convert_to_tensor(axis, _dtypes.int32)
  _inputs_flat = [value, size_splits, axis]
  _attrs = ("num_split", num_split, "T", _attr_T, "Tlen", _attr_Tlen)
  _result = _execute.execute(b"SplitV", num_split, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SplitV", _inputs_flat, _attrs, _result, name)
  return _result


def squeeze(input, axis=[], name=None):
  r"""Removes dimensions of size 1 from the shape of a tensor.

  Given a tensor `input`, this operation returns a tensor of the same type with
  all dimensions of size 1 removed. If you don't want to remove all size 1
  dimensions, you can remove specific size 1 dimensions by specifying
  `axis`.

  For example:

  ```
  # 't' is a tensor of shape [1, 2, 1, 3, 1, 1]
  shape(squeeze(t)) ==> [2, 3]
  ```

  Or, to remove specific size 1 dimensions:

  ```
  # 't' is a tensor of shape [1, 2, 1, 3, 1, 1]
  shape(squeeze(t, [2, 4])) ==> [1, 2, 3, 1]
  ```

  Args:
    input: A `Tensor`. The `input` to squeeze.
    axis: An optional list of `ints`. Defaults to `[]`.
      If specified, only squeezes the dimensions listed. The dimension
      index starts at 0. It is an error to squeeze a dimension that is not 1. Must
      be in the range `[-rank(input), rank(input))`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Squeeze",
        name, _ctx._post_execution_callbacks, input, "squeeze_dims", axis)
      return _result
    except _core._FallbackException:
      try:
        return squeeze_eager_fallback(
            input, axis=axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if axis is None:
    axis = []
  if not isinstance(axis, (list, tuple)):
    raise TypeError(
        "Expected list for 'axis' argument to "
        "'squeeze' Op, not %r." % axis)
  axis = [_execute.make_int(_i, "axis") for _i in axis]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Squeeze", input=input, squeeze_dims=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "squeeze_dims",
            _op.get_attr("squeeze_dims"))
  _execute.record_gradient(
      "Squeeze", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def squeeze_eager_fallback(input, axis=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function squeeze
  """
  _ctx = ctx if ctx else _context.context()
  if axis is None:
    axis = []
  if not isinstance(axis, (list, tuple)):
    raise TypeError(
        "Expected list for 'axis' argument to "
        "'squeeze' Op, not %r." % axis)
  axis = [_execute.make_int(_i, "axis") for _i in axis]
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "squeeze_dims", axis)
  _result = _execute.execute(b"Squeeze", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Squeeze", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('stop_gradient')
def stop_gradient(input, name=None):
  r"""Stops gradient computation.

  When executed in a graph, this op outputs its input tensor as-is.

  When building ops to compute gradients, this op prevents the contribution of
  its inputs to be taken into account.  Normally, the gradient generator adds ops
  to a graph to compute the derivatives of a specified 'loss' by recursively
  finding out inputs that contributed to its computation.  If you insert this op
  in the graph it inputs are masked from the gradient generator.  They are not
  taken into account for computing gradients.

  This is useful any time you want to compute a value with TensorFlow but need
  to pretend that the value was a constant. Some examples include:

  *  The *EM* algorithm where the *M-step* should not involve backpropagation
     through the output of the *E-step*.
  *  Contrastive divergence training of Boltzmann machines where, when
     differentiating the energy function, the training must not backpropagate
     through the graph that generated the samples from the model.
  *  Adversarial training, where no backprop should happen through the adversarial
     example generation process.

  Args:
    input: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StopGradient",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return stop_gradient_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              stop_gradient, input=input, name=name)
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
        "StopGradient", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          stop_gradient, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "StopGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stop_gradient_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stop_gradient
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"StopGradient", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StopGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def strided_slice(input, begin, end, strides, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None):
  r"""Return a strided slice from `input`.

  Note, most python users will want to use the Python `Tensor.__getitem__`
  or `Variable.__getitem__` rather than this op directly.

  The goal of this op is to produce a new tensor with a subset of
  the elements from the `n` dimensional `input` tensor. The subset is chosen using
  a sequence of `m` sparse range specifications encoded into the arguments
  of this function. Note, in some cases
  `m` could be equal to `n`, but this need not be the case. Each
  range specification entry can be one of the following:

  - An ellipsis (...). Ellipses are used to imply zero or more
    dimensions of full-dimension selection and are produced using
    `ellipsis_mask`. For example, `foo[...]` is the identity slice.

  - A new axis. This is used to insert a new shape=1 dimension and is
    produced using `new_axis_mask`. For example, `foo[:, ...]` where
    `foo` is shape `(3, 4)` produces a `(1, 3, 4)` tensor.


  - A range `begin:end:stride`. This is used to specify how much to choose from
    a given dimension. `stride` can be any integer but 0.  `begin` is an integer
    which represents the index of the first value to select while `end` represents
    the index of the last value to select. The number of values selected in each
    dimension is `end - begin` if `stride > 0` and `begin - end` if `stride < 0`.
    `begin` and `end` can be negative where `-1` is the last element, `-2` is
    the second to last. `begin_mask` controls whether to replace the explicitly
    given `begin` with an implicit effective value of `0` if `stride > 0` and
    `-1` if `stride < 0`. `end_mask` is analogous but produces the number
    required to create the largest open interval. For example, given a shape
    `(3,)` tensor `foo[:]`, the effective `begin` and `end` are `0` and `3`. Do
    not assume this is equivalent to `foo[0:-1]` which has an effective `begin`
    and `end` of `0` and `2`. Another example is `foo[-2::-1]` which reverses the
    first dimension of a tensor while dropping the last two (in the original
    order elements). For example `foo = [1,2,3,4]; foo[-2::-1]` is `[4,3]`.

  - A single index. This is used to keep only elements that have a given
    index. For example (`foo[2, :]` on a shape `(5,6)` tensor produces a
    shape `(6,)` tensor. This is encoded in `begin` and `end` and
    `shrink_axis_mask`.

  Each conceptual range specification is encoded in the op's argument. This
  encoding is best understand by considering a non-trivial example. In
  particular,
  `foo[1, 2:4, None, ..., :-3:-1, :]` will be encoded as

  ```
  begin = [1, 2, x, x, 0, x] # x denotes don't care (usually 0)
  end = [2, 4, x, x, -3, x]
  strides = [1, 1, x, x, -1, 1]
  begin_mask = 1<<4 | 1 << 5 = 48
  end_mask = 1<<5 = 32
  ellipsis_mask = 1<<3 = 8
  new_axis_mask = 1<<2 4
  shrink_axis_mask = 1<<0
  ```

  In this case if `foo.shape` is (5, 5, 5, 5, 5, 5) the final shape of
  the slice becomes (2, 1, 5, 5, 2, 5).
  Let us walk step by step through each argument specification.

  1.  The first argument in the example slice is turned into `begin = 1` and
  `end = begin + 1 = 2`. To disambiguate from the original spec `2:4` we
  also set the appropriate bit in `shrink_axis_mask`.

  2. `2:4` is contributes 2, 4, 1 to begin, end, and stride. All masks have
  zero bits contributed.

  3. None is a synonym for `tf.newaxis`. This means insert a dimension of size 1
  dimension in the final shape. Dummy values are contributed to begin,
  end and stride, while the new_axis_mask bit is set.

  4. `...` grab the full ranges from as many dimensions as needed to
  fully specify a slice for every dimension of the input shape.

  5. `:-3:-1` shows the use of negative indices. A negative index `i` associated
  with a dimension that has shape `s` is converted to a positive index
  `s + i`. So `-1` becomes `s-1` (i.e. the last element). This conversion
  is done internally so begin, end and strides receive x, -3, and -1.
  The appropriate begin_mask bit is set to indicate the start range is the
  full range (ignoring the x).

  6. `:` indicates that the entire contents of the corresponding dimension
  is selected. This is equivalent to `::` or `0::1`. begin, end, and strides
  receive 0, 0, and 1, respectively. The appropriate bits in `begin_mask` and
  `end_mask` are also set.

  *Requirements*:
    `0 != strides[i] for i in [0, m)`
    `ellipsis_mask must be a power of two (only one ellipsis)`

  Args:
    input: A `Tensor`.
    begin: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      `begin[k]` specifies the offset into the `k`th range specification.
      The exact dimension this corresponds to will be determined by context.
      Out-of-bounds values will be silently clamped. If the `k`th bit of
      `begin_mask` then `begin[k]` is ignored and the full range of the
      appropriate dimension is used instead. Negative values causes indexing
      to start from the highest element e.g. If `foo==[1,2,3]` then `foo[-1]==3`.
    end: A `Tensor`. Must have the same type as `begin`.
      `end[i]` is like `begin` with the exception that `end_mask` is
      used to determine full ranges.
    strides: A `Tensor`. Must have the same type as `begin`.
      `strides[i]` specifies the increment in the `i`th specification
      after extracting a given element. Negative indices will reverse
      the original order. Out or range values are
      clamped to `[0,dim[i]) if slice[i]>0` or `[-1,dim[i]-1] if slice[i] < 0`
    begin_mask: An optional `int`. Defaults to `0`.
      a bitmask where a bit i being 1 means to ignore the begin
      value and instead use the largest interval possible. At runtime
      begin[i] will be replaced with `[0, n-1)` if `stride[i] > 0` or
      `[-1, n-1]` if `stride[i] < 0`
    end_mask: An optional `int`. Defaults to `0`. analogous to `begin_mask`
    ellipsis_mask: An optional `int`. Defaults to `0`.
      a bitmask where bit `i` being 1 means the `i`th
      position is actually an ellipsis. One bit at most can be 1.
      If `ellipsis_mask == 0`, then an implicit ellipsis mask of `1 << (m+1)`
      is provided. This means that `foo[3:5] == foo[3:5, ...]`. An ellipsis
      implicitly creates as many range specifications as necessary to fully
      specify the sliced range for every dimension. For example for a 4-dimensional
      tensor `foo` the slice `foo[2, ..., 5:8]` implies `foo[2, :, :, 5:8]`.
    new_axis_mask: An optional `int`. Defaults to `0`.
      a bitmask where bit `i` being 1 means the `i`th
      specification creates a new shape 1 dimension. For example
      `foo[:4, tf.newaxis, :2]` would produce a shape `(4, 1, 2)` tensor.
    shrink_axis_mask: An optional `int`. Defaults to `0`.
      a bitmask where bit `i` implies that the `i`th
      specification should shrink the dimensionality. begin and end
      must imply a slice of size 1 in the dimension. For example in
      python one might do `foo[:, 3, :]` which would result in
      `shrink_axis_mask` being 2.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StridedSlice",
        name, _ctx._post_execution_callbacks, input, begin, end, strides,
        "begin_mask", begin_mask, "end_mask", end_mask, "ellipsis_mask",
        ellipsis_mask, "new_axis_mask", new_axis_mask, "shrink_axis_mask",
        shrink_axis_mask)
      return _result
    except _core._FallbackException:
      try:
        return strided_slice_eager_fallback(
            input, begin, end, strides, begin_mask=begin_mask,
            end_mask=end_mask, ellipsis_mask=ellipsis_mask,
            new_axis_mask=new_axis_mask, shrink_axis_mask=shrink_axis_mask,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StridedSlice", input=input, begin=begin, end=end, strides=strides,
                        begin_mask=begin_mask, end_mask=end_mask,
                        ellipsis_mask=ellipsis_mask,
                        new_axis_mask=new_axis_mask,
                        shrink_axis_mask=shrink_axis_mask, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Index", _op.get_attr("Index"),
            "begin_mask", _op.get_attr("begin_mask"), "end_mask",
            _op.get_attr("end_mask"), "ellipsis_mask",
            _op.get_attr("ellipsis_mask"), "new_axis_mask",
            _op.get_attr("new_axis_mask"), "shrink_axis_mask",
            _op.get_attr("shrink_axis_mask"))
  _execute.record_gradient(
      "StridedSlice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def strided_slice_eager_fallback(input, begin, end, strides, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function strided_slice
  """
  _ctx = ctx if ctx else _context.context()
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Index, _inputs_Index = _execute.args_to_matching_eager([begin, end, strides], _ctx)
  (begin, end, strides) = _inputs_Index
  _inputs_flat = [input, begin, end, strides]
  _attrs = ("T", _attr_T, "Index", _attr_Index, "begin_mask", begin_mask,
  "end_mask", end_mask, "ellipsis_mask", ellipsis_mask, "new_axis_mask",
  new_axis_mask, "shrink_axis_mask", shrink_axis_mask)
  _result = _execute.execute(b"StridedSlice", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StridedSlice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def strided_slice_assign(ref, begin, end, strides, value, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None):
  r"""Assign `value` to the sliced l-value reference of `ref`.

  The values of `value` are assigned to the positions in the variable
  `ref` that are selected by the slice parameters. The slice parameters
  `begin, `end`, `strides`, etc. work exactly as in `StridedSlice`.

  NOTE this op currently does not support broadcasting and so `value`'s
  shape must be exactly the shape produced by the slice of `ref`.

  Args:
    ref: A mutable `Tensor`.
    begin: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    end: A `Tensor`. Must have the same type as `begin`.
    strides: A `Tensor`. Must have the same type as `begin`.
    value: A `Tensor`. Must have the same type as `ref`.
    begin_mask: An optional `int`. Defaults to `0`.
    end_mask: An optional `int`. Defaults to `0`.
    ellipsis_mask: An optional `int`. Defaults to `0`.
    new_axis_mask: An optional `int`. Defaults to `0`.
    shrink_axis_mask: An optional `int`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `ref`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("strided_slice_assign op does not support eager execution. Arg 'output_ref' is a ref.")
  # Add nodes to the TensorFlow graph.
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StridedSliceAssign", ref=ref, begin=begin, end=end, strides=strides,
                              value=value, begin_mask=begin_mask,
                              end_mask=end_mask, ellipsis_mask=ellipsis_mask,
                              new_axis_mask=new_axis_mask,
                              shrink_axis_mask=shrink_axis_mask, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Index", _op.get_attr("Index"),
            "begin_mask", _op.get_attr("begin_mask"), "end_mask",
            _op.get_attr("end_mask"), "ellipsis_mask",
            _op.get_attr("ellipsis_mask"), "new_axis_mask",
            _op.get_attr("new_axis_mask"), "shrink_axis_mask",
            _op.get_attr("shrink_axis_mask"))
  _execute.record_gradient(
      "StridedSliceAssign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def strided_slice_assign_eager_fallback(ref, begin, end, strides, value, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None, ctx=None):
  raise RuntimeError("strided_slice_assign op does not support eager execution. Arg 'output_ref' is a ref.")

def strided_slice_grad(shape, begin, end, strides, dy, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None):
  r"""Returns the gradient of `StridedSlice`.

  Since `StridedSlice` cuts out pieces of its `input` which is size
  `shape`, its gradient will have the same shape (which is passed here
  as `shape`). The gradient will be zero in any element that the slice
  does not select.

  Arguments are the same as StridedSliceGrad with the exception that
  `dy` is the input gradient to be propagated and `shape` is the
  shape of `StridedSlice`'s `input`.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    begin: A `Tensor`. Must have the same type as `shape`.
    end: A `Tensor`. Must have the same type as `shape`.
    strides: A `Tensor`. Must have the same type as `shape`.
    dy: A `Tensor`.
    begin_mask: An optional `int`. Defaults to `0`.
    end_mask: An optional `int`. Defaults to `0`.
    ellipsis_mask: An optional `int`. Defaults to `0`.
    new_axis_mask: An optional `int`. Defaults to `0`.
    shrink_axis_mask: An optional `int`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `dy`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StridedSliceGrad", name, _ctx._post_execution_callbacks, shape,
        begin, end, strides, dy, "begin_mask", begin_mask, "end_mask",
        end_mask, "ellipsis_mask", ellipsis_mask, "new_axis_mask",
        new_axis_mask, "shrink_axis_mask", shrink_axis_mask)
      return _result
    except _core._FallbackException:
      try:
        return strided_slice_grad_eager_fallback(
            shape, begin, end, strides, dy, begin_mask=begin_mask,
            end_mask=end_mask, ellipsis_mask=ellipsis_mask,
            new_axis_mask=new_axis_mask, shrink_axis_mask=shrink_axis_mask,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StridedSliceGrad", shape=shape, begin=begin, end=end,
                            strides=strides, dy=dy, begin_mask=begin_mask,
                            end_mask=end_mask, ellipsis_mask=ellipsis_mask,
                            new_axis_mask=new_axis_mask,
                            shrink_axis_mask=shrink_axis_mask, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Index", _op.get_attr("Index"),
            "begin_mask", _op.get_attr("begin_mask"), "end_mask",
            _op.get_attr("end_mask"), "ellipsis_mask",
            _op.get_attr("ellipsis_mask"), "new_axis_mask",
            _op.get_attr("new_axis_mask"), "shrink_axis_mask",
            _op.get_attr("shrink_axis_mask"))
  _execute.record_gradient(
      "StridedSliceGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def strided_slice_grad_eager_fallback(shape, begin, end, strides, dy, begin_mask=0, end_mask=0, ellipsis_mask=0, new_axis_mask=0, shrink_axis_mask=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function strided_slice_grad
  """
  _ctx = ctx if ctx else _context.context()
  if begin_mask is None:
    begin_mask = 0
  begin_mask = _execute.make_int(begin_mask, "begin_mask")
  if end_mask is None:
    end_mask = 0
  end_mask = _execute.make_int(end_mask, "end_mask")
  if ellipsis_mask is None:
    ellipsis_mask = 0
  ellipsis_mask = _execute.make_int(ellipsis_mask, "ellipsis_mask")
  if new_axis_mask is None:
    new_axis_mask = 0
  new_axis_mask = _execute.make_int(new_axis_mask, "new_axis_mask")
  if shrink_axis_mask is None:
    shrink_axis_mask = 0
  shrink_axis_mask = _execute.make_int(shrink_axis_mask, "shrink_axis_mask")
  _attr_T, (dy,) = _execute.args_to_matching_eager([dy], _ctx)
  _attr_Index, _inputs_Index = _execute.args_to_matching_eager([shape, begin, end, strides], _ctx)
  (shape, begin, end, strides) = _inputs_Index
  _inputs_flat = [shape, begin, end, strides, dy]
  _attrs = ("T", _attr_T, "Index", _attr_Index, "begin_mask", begin_mask,
  "end_mask", end_mask, "ellipsis_mask", ellipsis_mask, "new_axis_mask",
  new_axis_mask, "shrink_axis_mask", shrink_axis_mask)
  _result = _execute.execute(b"StridedSliceGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StridedSliceGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('tensor_scatter_add')
def tensor_scatter_add(tensor, indices, updates, name=None):
  r"""Adds sparse `updates` to an existing tensor according to `indices`.

  This operation creates a new tensor by adding sparse `updates` to the passed
  in `tensor`.
  This operation is very similar to `tf.scatter_nd_add`, except that the updates
  are added onto an existing tensor (as opposed to a variable). If the memory
  for the existing tensor cannot be re-used, a copy is made and updated.

  `indices` is an integer tensor containing indices into a new tensor of shape
  `shape`.  The last dimension of `indices` can be at most the rank of `shape`:

      indices.shape[-1] <= shape.rank

  The last dimension of `indices` corresponds to indices into elements
  (if `indices.shape[-1] = shape.rank`) or slices
  (if `indices.shape[-1] < shape.rank`) along dimension `indices.shape[-1]` of
  `shape`.  `updates` is a tensor with shape

      indices.shape[:-1] + shape[indices.shape[-1]:]

  The simplest form of tensor_scatter_add is to add individual elements to a
  tensor by index. For example, say we want to add 4 elements in a rank-1
  tensor with 8 elements.

  In Python, this scatter add operation would look like this:

  ```python
      indices = tf.constant([[4], [3], [1], [7]])
      updates = tf.constant([9, 10, 11, 12])
      tensor = tf.ones([8], dtype=tf.int32)
      updated = tf.tensor_scatter_add(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [1, 12, 1, 11, 10, 1, 1, 13]

  We can also, insert entire slices of a higher rank tensor all at once. For
  example, if we wanted to insert two slices in the first dimension of a
  rank-3 tensor with two matrices of new values.

  In Python, this scatter add operation would look like this:

  ```python
      indices = tf.constant([[0], [2]])
      updates = tf.constant([[[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]],
                             [[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]]])
      tensor = tf.ones([4, 4, 4])
      updated = tf.tensor_scatter_add(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [[[6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8], [9, 9, 9, 9]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]],
       [[6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8], [9, 9, 9, 9]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]]]

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, the index is ignored.

  Args:
    tensor: A `Tensor`. Tensor to copy/update.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor.
    updates: A `Tensor`. Must have the same type as `tensor`.
      Updates to scatter into output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorScatterAdd", name, _ctx._post_execution_callbacks, tensor,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return tensor_scatter_add_eager_fallback(
            tensor, indices, updates, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tensor_scatter_add, tensor=tensor, indices=indices,
                                  updates=updates, name=name)
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
        "TensorScatterAdd", tensor=tensor, indices=indices, updates=updates,
                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tensor_scatter_add, tensor=tensor, indices=indices, updates=updates,
                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "TensorScatterAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_scatter_add_eager_fallback(tensor, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_scatter_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([tensor, updates], _ctx)
  (tensor, updates) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [tensor, indices, updates]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"TensorScatterAdd", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorScatterAdd", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('tensor_scatter_sub')
def tensor_scatter_sub(tensor, indices, updates, name=None):
  r"""Subtracts sparse `updates` from an existing tensor according to `indices`.

  This operation creates a new tensor by subtracting sparse `updates` from the
  passed in `tensor`.
  This operation is very similar to `tf.scatter_nd_sub`, except that the updates
  are subtracted from an existing tensor (as opposed to a variable). If the memory
  for the existing tensor cannot be re-used, a copy is made and updated.

  `indices` is an integer tensor containing indices into a new tensor of shape
  `shape`.  The last dimension of `indices` can be at most the rank of `shape`:

      indices.shape[-1] <= shape.rank

  The last dimension of `indices` corresponds to indices into elements
  (if `indices.shape[-1] = shape.rank`) or slices
  (if `indices.shape[-1] < shape.rank`) along dimension `indices.shape[-1]` of
  `shape`.  `updates` is a tensor with shape

      indices.shape[:-1] + shape[indices.shape[-1]:]

  The simplest form of tensor_scatter_sub is to subtract individual elements
  from a tensor by index. For example, say we want to insert 4 scattered elements
  in a rank-1 tensor with 8 elements.

  In Python, this scatter subtract operation would look like this:

  ```python
      indices = tf.constant([[4], [3], [1], [7]])
      updates = tf.constant([9, 10, 11, 12])
      tensor = tf.ones([8], dtype=tf.int32)
      updated = tf.tensor_scatter_sub(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [1, -10, 1, -9, -8, 1, 1, -11]

  We can also, insert entire slices of a higher rank tensor all at once. For
  example, if we wanted to insert two slices in the first dimension of a
  rank-3 tensor with two matrices of new values.

  In Python, this scatter add operation would look like this:

  ```python
      indices = tf.constant([[0], [2]])
      updates = tf.constant([[[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]],
                             [[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]]])
      tensor = tf.ones([4, 4, 4])
      updated = tf.tensor_scatter_sub(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [[[-4, -4, -4, -4], [-5, -5, -5, -5], [-6, -6, -6, -6], [-7, -7, -7, -7]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]],
       [[-4, -4, -4, -4], [-5, -5, -5, -5], [-6, -6, -6, -6], [-7, -7, -7, -7]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]]]

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, the index is ignored.

  Args:
    tensor: A `Tensor`. Tensor to copy/update.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor.
    updates: A `Tensor`. Must have the same type as `tensor`.
      Updates to scatter into output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorScatterSub", name, _ctx._post_execution_callbacks, tensor,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return tensor_scatter_sub_eager_fallback(
            tensor, indices, updates, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tensor_scatter_sub, tensor=tensor, indices=indices,
                                  updates=updates, name=name)
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
        "TensorScatterSub", tensor=tensor, indices=indices, updates=updates,
                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tensor_scatter_sub, tensor=tensor, indices=indices, updates=updates,
                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "TensorScatterSub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_scatter_sub_eager_fallback(tensor, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_scatter_sub
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([tensor, updates], _ctx)
  (tensor, updates) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [tensor, indices, updates]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"TensorScatterSub", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorScatterSub", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('tensor_scatter_update')
def tensor_scatter_update(tensor, indices, updates, name=None):
  r"""Scatter `updates` into an existing tensor according to `indices`.

  This operation creates a new tensor by applying sparse `updates` to the passed
  in `tensor`.
  This operation is very similar to `tf.scatter_nd`, except that the updates are
  scattered onto an existing tensor (as opposed to a zero-tensor). If the memory
  for the existing tensor cannot be re-used, a copy is made and updated.

  If `indices` contains duplicates, then their updates are accumulated (summed).

  **WARNING**: The order in which updates are applied is nondeterministic, so the
  output will be nondeterministic if `indices` contains duplicates -- because
  of some numerical approximation issues, numbers summed in different order
  may yield different results.

  `indices` is an integer tensor containing indices into a new tensor of shape
  `shape`.  The last dimension of `indices` can be at most the rank of `shape`:

      indices.shape[-1] <= shape.rank

  The last dimension of `indices` corresponds to indices into elements
  (if `indices.shape[-1] = shape.rank`) or slices
  (if `indices.shape[-1] < shape.rank`) along dimension `indices.shape[-1]` of
  `shape`.  `updates` is a tensor with shape

      indices.shape[:-1] + shape[indices.shape[-1]:]

  The simplest form of scatter is to insert individual elements in a tensor by
  index. For example, say we want to insert 4 scattered elements in a rank-1
  tensor with 8 elements.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/ScatterNd1.png" alt>
  </div>

  In Python, this scatter operation would look like this:

  ```python
      indices = tf.constant([[4], [3], [1], [7]])
      updates = tf.constant([9, 10, 11, 12])
      tensor = tf.ones([8], dtype=tf.int32)
      updated = tf.tensor_scatter_update(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [1, 11, 1, 10, 9, 1, 1, 12]

  We can also, insert entire slices of a higher rank tensor all at once. For
  example, if we wanted to insert two slices in the first dimension of a
  rank-3 tensor with two matrices of new values.

  In Python, this scatter operation would look like this:

  ```python
      indices = tf.constant([[0], [2]])
      updates = tf.constant([[[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]],
                             [[5, 5, 5, 5], [6, 6, 6, 6],
                              [7, 7, 7, 7], [8, 8, 8, 8]]])
      tensor = tf.ones([4, 4, 4])
      updated = tf.tensor_scatter_update(tensor, indices, updates)
      with tf.Session() as sess:
        print(sess.run(scatter))
  ```

  The resulting tensor would look like this:

      [[[5, 5, 5, 5], [6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]],
       [[5, 5, 5, 5], [6, 6, 6, 6], [7, 7, 7, 7], [8, 8, 8, 8]],
       [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]]]

  Note that on CPU, if an out of bound index is found, an error is returned.
  On GPU, if an out of bound index is found, the index is ignored.

  Args:
    tensor: A `Tensor`. Tensor to copy/update.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Index tensor.
    updates: A `Tensor`. Must have the same type as `tensor`.
      Updates to scatter into output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `tensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorScatterUpdate", name, _ctx._post_execution_callbacks, tensor,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return tensor_scatter_update_eager_fallback(
            tensor, indices, updates, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tensor_scatter_update, tensor=tensor, indices=indices,
                                     updates=updates, name=name)
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
        "TensorScatterUpdate", tensor=tensor, indices=indices,
                               updates=updates, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tensor_scatter_update, tensor=tensor, indices=indices,
                                 updates=updates, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "TensorScatterUpdate", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_scatter_update_eager_fallback(tensor, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_scatter_update
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([tensor, updates], _ctx)
  (tensor, updates) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  _inputs_flat = [tensor, indices, updates]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"TensorScatterUpdate", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorScatterUpdate", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('tile', v1=['tile', 'manip.tile'])
@deprecated_endpoints('manip.tile')
def tile(input, multiples, name=None):
  r"""Constructs a tensor by tiling a given tensor.

  This operation creates a new tensor by replicating `input` `multiples` times.
  The output tensor's i'th dimension has `input.dims(i) * multiples[i]` elements,
  and the values of `input` are replicated `multiples[i]` times along the 'i'th
  dimension. For example, tiling `[a b c d]` by `[2]` produces
  `[a b c d a b c d]`.

  Args:
    input: A `Tensor`. 1-D or higher.
    multiples: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D. Length must be the same as the number of dimensions in `input`
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Tile", name,
        _ctx._post_execution_callbacks, input, multiples)
      return _result
    except _core._FallbackException:
      try:
        return tile_eager_fallback(
            input, multiples, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tile, input=input, multiples=multiples, name=name)
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
        "Tile", input=input, multiples=multiples, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tile, input=input, multiples=multiples, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tmultiples", _op.get_attr("Tmultiples"))
  _execute.record_gradient(
      "Tile", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tile_eager_fallback(input, multiples, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tile
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_Tmultiples, (multiples,) = _execute.args_to_matching_eager([multiples], _ctx, _dtypes.int32)
  _inputs_flat = [input, multiples]
  _attrs = ("T", _attr_T, "Tmultiples", _attr_Tmultiples)
  _result = _execute.execute(b"Tile", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Tile", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tile_grad(input, multiples, name=None):
  r"""Returns the gradient of `Tile`.

  Since `Tile` takes an input and repeats the input `multiples` times
  along each dimension, `TileGrad` takes in `multiples` and aggregates
  each repeated tile of `input` into `output`.

  Args:
    input: A `Tensor`.
    multiples: A `Tensor` of type `int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TileGrad",
        name, _ctx._post_execution_callbacks, input, multiples)
      return _result
    except _core._FallbackException:
      try:
        return tile_grad_eager_fallback(
            input, multiples, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TileGrad", input=input, multiples=multiples, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TileGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tile_grad_eager_fallback(input, multiples, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tile_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  multiples = _ops.convert_to_tensor(multiples, _dtypes.int32)
  _inputs_flat = [input, multiples]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TileGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TileGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def transpose(x, perm, name=None):
  r"""Shuffle dimensions of x according to a permutation.

  The output `y` has the same rank as `x`. The shapes of `x` and `y` satisfy:
    `y.shape[i] == x.shape[perm[i]] for i in [0, 1, ..., rank(x) - 1]`

  Args:
    x: A `Tensor`.
    perm: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Transpose",
        name, _ctx._post_execution_callbacks, x, perm)
      return _result
    except _core._FallbackException:
      try:
        return transpose_eager_fallback(
            x, perm, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Transpose", x=x, perm=perm, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tperm", _op.get_attr("Tperm"))
  _execute.record_gradient(
      "Transpose", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def transpose_eager_fallback(x, perm, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function transpose
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Tperm, (perm,) = _execute.args_to_matching_eager([perm], _ctx, _dtypes.int32)
  _inputs_flat = [x, perm]
  _attrs = ("T", _attr_T, "Tperm", _attr_Tperm)
  _result = _execute.execute(b"Transpose", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Transpose", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_unique_outputs = ["y", "idx"]
_UniqueOutput = _collections.namedtuple(
    "Unique", _unique_outputs)


def unique(x, out_idx=_dtypes.int32, name=None):
  r"""Finds unique elements in a 1-D tensor.

  This operation returns a tensor `y` containing all of the unique elements of `x`
  sorted in the same order that they occur in `x`. This operation also returns a
  tensor `idx` the same size as `x` that contains the index of each value of `x`
  in the unique output `y`. In other words:

  `y[idx[i]] = x[i] for i in [0, 1,...,rank(x) - 1]`

  For example:

  ```
  # tensor 'x' is [1, 1, 2, 4, 4, 4, 7, 8, 8]
  y, idx = unique(x)
  y ==> [1, 2, 4, 7, 8]
  idx ==> [0, 0, 1, 2, 2, 2, 3, 4, 4]
  ```

  Args:
    x: A `Tensor`. 1-D.
    out_idx: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, idx).

    y: A `Tensor`. Has the same type as `x`.
    idx: A `Tensor` of type `out_idx`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Unique", name,
        _ctx._post_execution_callbacks, x, "out_idx", out_idx)
      _result = _UniqueOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unique_eager_fallback(
            x, out_idx=out_idx, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Unique", x=x, out_idx=out_idx, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_idx", _op.get_attr("out_idx"))
  _execute.record_gradient(
      "Unique", _inputs_flat, _attrs, _result, name)
  _result = _UniqueOutput._make(_result)
  return _result



def unique_eager_fallback(x, out_idx=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unique
  """
  _ctx = ctx if ctx else _context.context()
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T, "out_idx", out_idx)
  _result = _execute.execute(b"Unique", 2, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Unique", _inputs_flat, _attrs, _result, name)
  _result = _UniqueOutput._make(_result)
  return _result


_unique_v2_outputs = ["y", "idx"]
_UniqueV2Output = _collections.namedtuple(
    "UniqueV2", _unique_v2_outputs)


def unique_v2(x, axis, out_idx=_dtypes.int32, name=None):
  r"""Finds unique elements along an axis of a tensor.

  This operation either returns a tensor `y` containing unique elements
  along the `axis` of a tensor. The returned unique elements is sorted
  in the same order as they occur along `axis` in `x`.
  This operation also returns a tensor `idx` that is the same size as
  the number of the elements in `x` along the `axis` dimension. It
  contains the index in the unique output `y`.
  In other words, for an `1-D` tensor `x` with `axis = None:

  `y[idx[i]] = x[i] for i in [0, 1,...,rank(x) - 1]`

  For example:

  ```
  # tensor 'x' is [1, 1, 2, 4, 4, 4, 7, 8, 8]
  y, idx = unique(x)
  y ==> [1, 2, 4, 7, 8]
  idx ==> [0, 0, 1, 2, 2, 2, 3, 4, 4]
  ```

  For an `2-D` tensor `x` with `axis = 0`:

  ```
  # tensor 'x' is [[1, 0, 0],
  #                [1, 0, 0],
  #                [2, 0, 0]]
  y, idx = unique(x, axis=0)
  y ==> [[1, 0, 0],
         [2, 0, 0]]
  idx ==> [0, 0, 1]
  ```

  For an `2-D` tensor `x` with `axis = 1`:

  ```
  # tensor 'x' is [[1, 0, 0],
  #                [1, 0, 0],
  #                [2, 0, 0]]
  y, idx = unique(x, axis=1)
  y ==> [[1, 0],
         [1, 0],
         [2, 0]]
  idx ==> [0, 1, 1]
  ```

  Args:
    x: A `Tensor`. A `Tensor`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `Tensor` of type `int32` (default: None). The axis of the Tensor to
      find the unique elements.
    out_idx: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, idx).

    y: A `Tensor`. Has the same type as `x`.
    idx: A `Tensor` of type `out_idx`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "UniqueV2",
        name, _ctx._post_execution_callbacks, x, axis, "out_idx", out_idx)
      _result = _UniqueV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unique_v2_eager_fallback(
            x, axis, out_idx=out_idx, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UniqueV2", x=x, axis=axis, out_idx=out_idx, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Taxis", _op.get_attr("Taxis"), "out_idx",
            _op.get_attr("out_idx"))
  _execute.record_gradient(
      "UniqueV2", _inputs_flat, _attrs, _result, name)
  _result = _UniqueV2Output._make(_result)
  return _result



def unique_v2_eager_fallback(x, axis, out_idx=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unique_v2
  """
  _ctx = ctx if ctx else _context.context()
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Taxis, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int64)
  _inputs_flat = [x, axis]
  _attrs = ("T", _attr_T, "Taxis", _attr_Taxis, "out_idx", out_idx)
  _result = _execute.execute(b"UniqueV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UniqueV2", _inputs_flat, _attrs, _result, name)
  _result = _UniqueV2Output._make(_result)
  return _result


_unique_with_counts_outputs = ["y", "idx", "count"]
_UniqueWithCountsOutput = _collections.namedtuple(
    "UniqueWithCounts", _unique_with_counts_outputs)


def unique_with_counts(x, out_idx=_dtypes.int32, name=None):
  r"""Finds unique elements in a 1-D tensor.

  This operation returns a tensor `y` containing all of the unique elements of `x`
  sorted in the same order that they occur in `x`. This operation also returns a
  tensor `idx` the same size as `x` that contains the index of each value of `x`
  in the unique output `y`. Finally, it returns a third tensor `count` that
  contains the count of each element of `y` in `x`. In other words:

  `y[idx[i]] = x[i] for i in [0, 1,...,rank(x) - 1]`

  For example:

  ```
  # tensor 'x' is [1, 1, 2, 4, 4, 4, 7, 8, 8]
  y, idx, count = unique_with_counts(x)
  y ==> [1, 2, 4, 7, 8]
  idx ==> [0, 0, 1, 2, 2, 2, 3, 4, 4]
  count ==> [2, 1, 3, 1, 2]
  ```

  Args:
    x: A `Tensor`. 1-D.
    out_idx: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, idx, count).

    y: A `Tensor`. Has the same type as `x`.
    idx: A `Tensor` of type `out_idx`.
    count: A `Tensor` of type `out_idx`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UniqueWithCounts", name, _ctx._post_execution_callbacks, x,
        "out_idx", out_idx)
      _result = _UniqueWithCountsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unique_with_counts_eager_fallback(
            x, out_idx=out_idx, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UniqueWithCounts", x=x, out_idx=out_idx, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_idx", _op.get_attr("out_idx"))
  _execute.record_gradient(
      "UniqueWithCounts", _inputs_flat, _attrs, _result, name)
  _result = _UniqueWithCountsOutput._make(_result)
  return _result



def unique_with_counts_eager_fallback(x, out_idx=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unique_with_counts
  """
  _ctx = ctx if ctx else _context.context()
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T, "out_idx", out_idx)
  _result = _execute.execute(b"UniqueWithCounts", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UniqueWithCounts", _inputs_flat, _attrs, _result, name)
  _result = _UniqueWithCountsOutput._make(_result)
  return _result


_unique_with_counts_v2_outputs = ["y", "idx", "count"]
_UniqueWithCountsV2Output = _collections.namedtuple(
    "UniqueWithCountsV2", _unique_with_counts_v2_outputs)


def unique_with_counts_v2(x, axis, out_idx=_dtypes.int32, name=None):
  r"""Finds unique elements along an axis of a tensor.

  This operation either returns a tensor `y` containing unique elements
  along the `axis` of a tensor. The returned unique elements is sorted
  in the same order as they occur along `axis` in `x`.
  This operation also returns a tensor `idx` and a tensor `count`
  that are the same size as the number of the elements in `x` along the
  `axis` dimension. The `idx` contains the index in the unique output `y`
  and the `count` contains the count in the unique output `y`.
  In other words, for an `1-D` tensor `x` with `axis = None:

  `y[idx[i]] = x[i] for i in [0, 1,...,rank(x) - 1]`

  For example:

  ```
  # tensor 'x' is [1, 1, 2, 4, 4, 4, 7, 8, 8]
  y, idx, count = unique_with_counts(x)
  y ==> [1, 2, 4, 7, 8]
  idx ==> [0, 0, 1, 2, 2, 2, 3, 4, 4]
  count ==> [2, 1, 3, 1, 2]
  ```

  For an `2-D` tensor `x` with `axis = 0`:

  ```
  # tensor 'x' is [[1, 0, 0],
  #                [1, 0, 0],
  #                [2, 0, 0]]
  y, idx, count = unique_with_counts(x, axis=0)
  y ==> [[1, 0, 0],
         [2, 0, 0]]
  idx ==> [0, 0, 1]
  count ==> [2, 1]
  ```

  For an `2-D` tensor `x` with `axis = 1`:

  ```
  # tensor 'x' is [[1, 0, 0],
  #                [1, 0, 0],
  #                [2, 0, 0]]
  y, idx, count = unique_with_counts(x, axis=1)
  y ==> [[1, 0],
         [1, 0],
         [2, 0]]
  idx ==> [0, 1, 1]
  count ==> [1, 2]
  ```

  Args:
    x: A `Tensor`. A `Tensor`.
    axis: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A `Tensor` of type `int32` (default: None). The axis of the Tensor to
      find the unique elements.
    out_idx: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (y, idx, count).

    y: A `Tensor`. Has the same type as `x`.
    idx: A `Tensor` of type `out_idx`.
    count: A `Tensor` of type `out_idx`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UniqueWithCountsV2", name, _ctx._post_execution_callbacks, x, axis,
        "out_idx", out_idx)
      _result = _UniqueWithCountsV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unique_with_counts_v2_eager_fallback(
            x, axis, out_idx=out_idx, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UniqueWithCountsV2", x=x, axis=axis, out_idx=out_idx, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Taxis", _op.get_attr("Taxis"), "out_idx",
            _op.get_attr("out_idx"))
  _execute.record_gradient(
      "UniqueWithCountsV2", _inputs_flat, _attrs, _result, name)
  _result = _UniqueWithCountsV2Output._make(_result)
  return _result



def unique_with_counts_v2_eager_fallback(x, axis, out_idx=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unique_with_counts_v2
  """
  _ctx = ctx if ctx else _context.context()
  if out_idx is None:
    out_idx = _dtypes.int32
  out_idx = _execute.make_type(out_idx, "out_idx")
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _attr_Taxis, (axis,) = _execute.args_to_matching_eager([axis], _ctx, _dtypes.int64)
  _inputs_flat = [x, axis]
  _attrs = ("T", _attr_T, "Taxis", _attr_Taxis, "out_idx", out_idx)
  _result = _execute.execute(b"UniqueWithCountsV2", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UniqueWithCountsV2", _inputs_flat, _attrs, _result, name)
  _result = _UniqueWithCountsV2Output._make(_result)
  return _result


def unpack(value, num, axis=0, name=None):
  r"""Unpacks a given dimension of a rank-`R` tensor into `num` rank-`(R-1)` tensors.

  Unpacks `num` tensors from `value` by chipping it along the `axis` dimension.
  For example, given a tensor of shape `(A, B, C, D)`;

  If `axis == 0` then the i'th tensor in `output` is the slice `value[i, :, :, :]`
    and each tensor in `output` will have shape `(B, C, D)`. (Note that the
    dimension unpacked along is gone, unlike `split`).

  If `axis == 1` then the i'th tensor in `output` is the slice `value[:, i, :, :]`
    and each tensor in `output` will have shape `(A, C, D)`.
  Etc.

  This is the opposite of `pack`.

  Args:
    value: A `Tensor`.
      1-D or higher, with `axis` dimension size equal to `num`.
    num: An `int` that is `>= 0`.
    axis: An optional `int`. Defaults to `0`.
      Dimension along which to unpack.  Negative values wrap around, so the
      valid range is `[-R, R)`.
    name: A name for the operation (optional).

  Returns:
    A list of `num` `Tensor` objects with the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Unpack", name,
        _ctx._post_execution_callbacks, value, "num", num, "axis", axis)
      return _result
    except _core._FallbackException:
      try:
        return unpack_eager_fallback(
            value, num=num, axis=axis, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num = _execute.make_int(num, "num")
  if axis is None:
    axis = 0
  axis = _execute.make_int(axis, "axis")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Unpack", value=value, num=num, axis=axis, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num", _op.get_attr("num"), "T", _op.get_attr("T"), "axis",
            _op.get_attr("axis"))
  _execute.record_gradient(
      "Unpack", _inputs_flat, _attrs, _result, name)
  return _result



def unpack_eager_fallback(value, num, axis=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unpack
  """
  _ctx = ctx if ctx else _context.context()
  num = _execute.make_int(num, "num")
  if axis is None:
    axis = 0
  axis = _execute.make_int(axis, "axis")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("num", num, "T", _attr_T, "axis", axis)
  _result = _execute.execute(b"Unpack", num, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Unpack", _inputs_flat, _attrs, _result, name)
  return _result


@_dispatch.add_dispatch_list
@tf_export('unravel_index')
def unravel_index(indices, dims, name=None):
  r"""Converts a flat index or array of flat indices into a tuple of

  coordinate arrays.

  @compatibility(numpy)
  Equivalent to np.unravel_index
  @end_compatibility

  Args:
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      An 0-D or 1-D `int` Tensor whose elements are indices into the
      flattened version of an array of dimensions dims.
    dims: A `Tensor`. Must have the same type as `indices`.
      An 1-D `int` Tensor. The shape of the array to use for unraveling
      indices.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `indices`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "UnravelIndex",
        name, _ctx._post_execution_callbacks, indices, dims)
      return _result
    except _core._FallbackException:
      try:
        return unravel_index_eager_fallback(
            indices, dims, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unravel_index, indices=indices, dims=dims, name=name)
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
        "UnravelIndex", indices=indices, dims=dims, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unravel_index, indices=indices, dims=dims, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tidx", _op.get_attr("Tidx"))
  _execute.record_gradient(
      "UnravelIndex", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unravel_index_eager_fallback(indices, dims, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unravel_index
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tidx, _inputs_Tidx = _execute.args_to_matching_eager([indices, dims], _ctx, _dtypes.int32)
  (indices, dims) = _inputs_Tidx
  _inputs_flat = [indices, dims]
  _attrs = ("Tidx", _attr_Tidx)
  _result = _execute.execute(b"UnravelIndex", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnravelIndex", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def upper_bound(sorted_inputs, values, out_type=_dtypes.int32, name=None):
  r"""Applies upper_bound(sorted_search_values, values) along each row.

  Each set of rows with the same index in (sorted_inputs, values) is treated
  independently.  The resulting row is the equivalent of calling
  `np.searchsorted(sorted_inputs, values, side='right')`.

  The result is not a global index to the entire 
  `Tensor`, but rather just the index in the last dimension.

  A 2-D example:
    sorted_sequence = [[0, 3, 9, 9, 10],
                       [1, 2, 3, 4, 5]]
    values = [[2, 4, 9],
              [0, 2, 6]]

    result = UpperBound(sorted_sequence, values)

    result == [[1, 2, 4],
               [0, 2, 5]]

  Args:
    sorted_inputs: A `Tensor`. 2-D Tensor where each row is ordered.
    values: A `Tensor`. Must have the same type as `sorted_inputs`.
      2-D Tensor with the same numbers of rows as `sorted_search_values`. Contains
      the values that will be searched for in `sorted_search_values`.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "UpperBound",
        name, _ctx._post_execution_callbacks, sorted_inputs, values,
        "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return upper_bound_eager_fallback(
            sorted_inputs, values, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UpperBound", sorted_inputs=sorted_inputs, values=values,
                      out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "UpperBound", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def upper_bound_eager_fallback(sorted_inputs, values, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function upper_bound
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([sorted_inputs, values], _ctx)
  (sorted_inputs, values) = _inputs_T
  _inputs_flat = [sorted_inputs, values]
  _attrs = ("T", _attr_T, "out_type", out_type)
  _result = _execute.execute(b"UpperBound", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UpperBound", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def where(condition, name=None):
  r"""Returns locations of nonzero / true values in a tensor.

  This operation returns the coordinates of true elements in `condition`. The
  coordinates are returned in a 2-D tensor where the first dimension (rows)
  represents the number of true elements, and the second dimension (columns)
  represents the coordinates of the true elements. Keep in mind, the shape of
  the output tensor can vary depending on how many true values there are in
  `condition`. Indices are output in row-major order.

  For example:

  ```
  # 'input' tensor is [[True, False]
  #                    [True, False]]
  # 'input' has two true values, so output has two coordinates.
  # 'input' has rank of 2, so coordinates have two indices.
  where(input) ==> [[0, 0],
                    [1, 0]]

  # `condition` tensor is [[[True, False]
  #                     [True, False]]
  #                    [[False, True]
  #                     [False, True]]
  #                    [[False, False]
  #                     [False, True]]]
  # 'input' has 5 true values, so output has 5 coordinates.
  # 'input' has rank of 3, so coordinates have three indices.
  where(input) ==> [[0, 0, 0],
                    [0, 1, 0],
                    [1, 0, 1],
                    [1, 1, 1],
                    [2, 1, 1]]

  # `condition` tensor is [[[1.5,  0.0]
  #                     [-0.5, 0.0]]
  #                    [[0.0,  0.25]
  #                     [0.0,  0.75]]
  #                    [[0.0,  0.0]
  #                     [0.0,  0.01]]]
  # 'input' has 5 nonzero values, so output has 5 coordinates.
  # 'input' has rank of 3, so coordinates have three indices.
  where(input) ==> [[0, 0, 0],
                    [0, 1, 0],
                    [1, 0, 1],
                    [1, 1, 1],
                    [2, 1, 1]]

  # `condition` tensor is [[[1.5 + 0.0j, 0.0  + 0.0j]
  #                     [0.0 + 0.5j, 0.0  + 0.0j]]
  #                    [[0.0 + 0.0j, 0.25 + 1.5j]
  #                     [0.0 + 0.0j, 0.75 + 0.0j]]
  #                    [[0.0 + 0.0j, 0.0  + 0.0j]
  #                     [0.0 + 0.0j, 0.01 + 0.0j]]]
  # 'input' has 5 nonzero magnitude values, so output has 5 coordinates.
  # 'input' has rank of 3, so coordinates have three indices.
  where(input) ==> [[0, 0, 0],
                    [0, 1, 0],
                    [1, 0, 1],
                    [1, 1, 1],
                    [2, 1, 1]]
  ```

  Args:
    condition: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`, `bool`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Where", name,
        _ctx._post_execution_callbacks, condition)
      return _result
    except _core._FallbackException:
      try:
        return where_eager_fallback(
            condition, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "Where", input=condition, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "Where", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def where_eager_fallback(condition, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function where
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (condition,) = _execute.args_to_matching_eager([condition], _ctx, _dtypes.bool)
  _inputs_flat = [condition]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Where", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Where", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def zeros_like(x, name=None):
  r"""Returns a tensor of zeros with the same shape and type as x.

  Args:
    x: A `Tensor`. a tensor of type T.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ZerosLike",
        name, _ctx._post_execution_callbacks, x)
      return _result
    except _core._FallbackException:
      try:
        return zeros_like_eager_fallback(
            x, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ZerosLike", x=x, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ZerosLike", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def zeros_like_eager_fallback(x, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function zeros_like
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (x,) = _execute.args_to_matching_eager([x], _ctx)
  _inputs_flat = [x]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ZerosLike", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ZerosLike", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BatchMatrixBandPart"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num_lower"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_upper"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "band"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 14
#     explanation: "Use MatrixBandPart"
#   }
# }
# op {
#   name: "BatchMatrixDiag"
#   input_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 14
#     explanation: "Use MatrixDiag"
#   }
# }
# op {
#   name: "BatchMatrixDiagPart"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 14
#     explanation: "Use MatrixDiagPart"
#   }
# }
# op {
#   name: "BatchMatrixSetDiag"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 14
#     explanation: "Use MatrixSetDiag"
#   }
# }
# op {
#   name: "BatchToSpace"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "crops"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "block_size"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "BatchToSpaceND"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "block_shape"
#     type_attr: "Tblock_shape"
#   }
#   input_arg {
#     name: "crops"
#     type_attr: "Tcrops"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tblock_shape"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tcrops"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Bitcast"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT64
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_UINT32
#         type: DT_UINT64
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT16
#         type: DT_QUINT16
#         type: DT_QINT32
#       }
#     }
#   }
#   attr {
#     name: "type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT64
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_UINT16
#         type: DT_UINT32
#         type: DT_UINT64
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT16
#         type: DT_QUINT16
#         type: DT_QINT32
#       }
#     }
#   }
# }
# op {
#   name: "BroadcastArgs"
#   input_arg {
#     name: "s0"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "s1"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "r0"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "BroadcastGradientArgs"
#   input_arg {
#     name: "s0"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "s1"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "r0"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "r1"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "BroadcastTo"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "CheckNumerics"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "message"
#     type: "string"
#   }
# }
# op {
#   name: "Concat"
#   input_arg {
#     name: "concat_dim"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "ConcatOffset"
#   input_arg {
#     name: "concat_dim"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "shape"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   output_arg {
#     name: "offset"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
# }
# op {
#   name: "ConcatV2"
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ConjugateTranspose"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "perm"
#     type_attr: "Tperm"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tperm"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Const"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "value"
#     type: "tensor"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
# }
# op {
#   name: "DebugGradientIdentity"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   allows_uninitialized_input: true
# }
# op {
#   name: "DebugGradientRefIdentity"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#     is_ref: true
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#     is_ref: true
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   allows_uninitialized_input: true
# }
# op {
#   name: "DeepCopy"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "DepthToSpace"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "block_size"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#         s: "NCHW_VECT_C"
#       }
#     }
#   }
# }
# op {
#   name: "Dequantize"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "min_range"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_range"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     default_value {
#       s: "MIN_COMBINED"
#     }
#     allowed_values {
#       list {
#         s: "MIN_COMBINED"
#         s: "MIN_FIRST"
#         s: "SCALED"
#       }
#     }
#   }
# }
# op {
#   name: "Diag"
#   input_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "DiagPart"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#       }
#     }
#   }
# }
# op {
#   name: "EditDistance"
#   input_arg {
#     name: "hypothesis_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "hypothesis_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "hypothesis_shape"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "truth_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "truth_values"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "truth_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "normalize"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "Empty"
#   input_arg {
#     name: "shape"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "init"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "EnsureShape"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "ExpandDims"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dim"
#     type_attr: "Tdim"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tdim"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ExtractImagePatches"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "patches"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksizes"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "rates"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 4
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "ExtractVolumePatches"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "patches"
#     type_attr: "T"
#   }
#   attr {
#     name: "ksizes"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#     has_minimum: true
#     minimum: 5
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxArgs"
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "outputs"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "min"
#     type: "float"
#     default_value {
#       f: -6
#     }
#   }
#   attr {
#     name: "max"
#     type: "float"
#     default_value {
#       f: 6
#     }
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxArgsGradient"
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprops"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "min"
#     type: "float"
#     default_value {
#       f: -6
#     }
#   }
#   attr {
#     name: "max"
#     type: "float"
#     default_value {
#       f: 6
#     }
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxVars"
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "outputs"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxVarsGradient"
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprops_wrt_input"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprop_wrt_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprop_wrt_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxVarsPerChannel"
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "outputs"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "FakeQuantWithMinMaxVarsPerChannelGradient"
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprops_wrt_input"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprop_wrt_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "backprop_wrt_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "narrow_range"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "Fill"
#   input_arg {
#     name: "dims"
#     type_attr: "index_type"
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "index_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Gather"
#   input_arg {
#     name: "params"
#     type_attr: "Tparams"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tparams"
#   }
#   attr {
#     name: "validate_indices"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "Tparams"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "GatherNd"
#   input_arg {
#     name: "params"
#     type_attr: "Tparams"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tparams"
#   }
#   attr {
#     name: "Tparams"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "GatherV2"
#   input_arg {
#     name: "params"
#     type_attr: "Tparams"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Taxis"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tparams"
#   }
#   attr {
#     name: "Tparams"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Taxis"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "GuaranteeConst"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "Identity"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "IdentityN"
#   input_arg {
#     name: "input"
#     type_list_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ImmutableConst"
#   output_arg {
#     name: "tensor"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   attr {
#     name: "memory_region_name"
#     type: "string"
#   }
# }
# op {
#   name: "InplaceAdd"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "i"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "InplaceSub"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "i"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "InplaceUpdate"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "i"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "InvertPermutation"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ListDiff"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "idx"
#     type_attr: "out_idx"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_idx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "LowerBound"
#   input_arg {
#     name: "sorted_inputs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "MatrixBandPart"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num_lower"
#     type_attr: "Tindex"
#   }
#   input_arg {
#     name: "num_upper"
#     type_attr: "Tindex"
#   }
#   output_arg {
#     name: "band"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindex"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "MatrixDiag"
#   input_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "MatrixDiagPart"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "MatrixSetDiag"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "diagonal"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "MirrorPad"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     allowed_values {
#       list {
#         s: "REFLECT"
#         s: "SYMMETRIC"
#       }
#     }
#   }
# }
# op {
#   name: "MirrorPadGrad"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     allowed_values {
#       list {
#         s: "REFLECT"
#         s: "SYMMETRIC"
#       }
#     }
#   }
# }
# op {
#   name: "OneHot"
#   input_arg {
#     name: "indices"
#     type_attr: "TI"
#   }
#   input_arg {
#     name: "depth"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "on_value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "off_value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "axis"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "TI"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "OnesLike"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT8
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_UINT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_BOOL
#       }
#     }
#   }
# }
# op {
#   name: "Pack"
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "axis"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "Pad"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "PadV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   input_arg {
#     name: "constant_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ParallelConcat"
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
# }
# op {
#   name: "Placeholder"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
# }
# op {
#   name: "PlaceholderV2"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   deprecation {
#     version: 23
#     explanation: "Placeholder now behaves the same as PlaceholderV2."
#   }
# }
# op {
#   name: "PlaceholderWithDefault"
#   input_arg {
#     name: "input"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
# }
# op {
#   name: "PreventGradient"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "message"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "QuantizeAndDequantize"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "signed_input"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "range_given"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "input_min"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "input_max"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   deprecation {
#     version: 22
#     explanation: "Replaced by QuantizeAndDequantizeV2"
#   }
# }
# op {
#   name: "QuantizeAndDequantizeV2"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_min"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_max"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "signed_input"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "num_bits"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "range_given"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "round_mode"
#     type: "string"
#     default_value {
#       s: "HALF_TO_EVEN"
#     }
#     allowed_values {
#       list {
#         s: "HALF_TO_EVEN"
#         s: "HALF_UP"
#       }
#     }
#   }
# }
# op {
#   name: "QuantizeAndDequantizeV3"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_min"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "input_max"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num_bits"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "signed_input"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "range_given"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "QuantizeV2"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_range"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_range"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "mode"
#     type: "string"
#     default_value {
#       s: "MIN_COMBINED"
#     }
#     allowed_values {
#       list {
#         s: "MIN_COMBINED"
#         s: "MIN_FIRST"
#         s: "SCALED"
#       }
#     }
#   }
#   attr {
#     name: "round_mode"
#     type: "string"
#     default_value {
#       s: "HALF_AWAY_FROM_ZERO"
#     }
#     allowed_values {
#       list {
#         s: "HALF_AWAY_FROM_ZERO"
#         s: "HALF_TO_EVEN"
#       }
#     }
#   }
# }
# op {
#   name: "QuantizedConcat"
#   input_arg {
#     name: "concat_dim"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   input_arg {
#     name: "input_mins"
#     type: DT_FLOAT
#     number_attr: "N"
#   }
#   input_arg {
#     name: "input_maxes"
#     type: DT_FLOAT
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "QuantizedInstanceNorm"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "x_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "x_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "y_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_QINT16
#         type: DT_QUINT16
#       }
#     }
#   }
#   attr {
#     name: "output_range_given"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "given_y_min"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "given_y_max"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "variance_epsilon"
#     type: "float"
#     default_value {
#       f: 1e-05
#     }
#   }
#   attr {
#     name: "min_separation"
#     type: "float"
#     default_value {
#       f: 0.001
#     }
#   }
# }
# op {
#   name: "QuantizedReshape"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type_attr: "Tshape"
#   }
#   input_arg {
#     name: "input_min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tshape"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Rank"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "RefIdentity"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#     is_ref: true
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#     is_ref: true
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   allows_uninitialized_input: true
# }
# op {
#   name: "Reshape"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type_attr: "Tshape"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tshape"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ResourceStridedSliceAssign"
#   input_arg {
#     name: "ref"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "begin"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "end"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "strides"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Index"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "begin_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "end_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ellipsis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "new_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "shrink_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "Reverse"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "dims"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_BOOL
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "ReverseSequence"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "seq_lengths"
#     type_attr: "Tlen"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "seq_dim"
#     type: "int"
#   }
#   attr {
#     name: "batch_dim"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tlen"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ReverseV2"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_BOOL
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "ScatterNd"
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "shape"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ScatterNdNonAliasingAdd"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#         type: DT_BOOL
#       }
#     }
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Shape"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "ShapeN"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#     number_attr: "N"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Size"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Slice"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "begin"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "size"
#     type_attr: "Index"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Index"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Snapshot"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SpaceToBatch"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "block_size"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
# }
# op {
#   name: "SpaceToBatchND"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "block_shape"
#     type_attr: "Tblock_shape"
#   }
#   input_arg {
#     name: "paddings"
#     type_attr: "Tpaddings"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tblock_shape"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tpaddings"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "SpaceToDepth"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "block_size"
#     type: "int"
#     has_minimum: true
#     minimum: 2
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#         s: "NCHW_VECT_C"
#       }
#     }
#   }
# }
# op {
#   name: "Split"
#   input_arg {
#     name: "split_dim"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#     number_attr: "num_split"
#   }
#   attr {
#     name: "num_split"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "SplitV"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size_splits"
#     type_attr: "Tlen"
#   }
#   input_arg {
#     name: "split_dim"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#     number_attr: "num_split"
#   }
#   attr {
#     name: "num_split"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tlen"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Squeeze"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "squeeze_dims"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
# }
# op {
#   name: "StopGradient"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "StridedSlice"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "begin"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "end"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "strides"
#     type_attr: "Index"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Index"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "begin_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "end_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ellipsis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "new_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "shrink_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "StridedSliceAssign"
#   input_arg {
#     name: "ref"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "begin"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "end"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "strides"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output_ref"
#     type_attr: "T"
#     is_ref: true
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Index"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "begin_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "end_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ellipsis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "new_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "shrink_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "StridedSliceGrad"
#   input_arg {
#     name: "shape"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "begin"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "end"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "strides"
#     type_attr: "Index"
#   }
#   input_arg {
#     name: "dy"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Index"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "begin_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "end_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ellipsis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "new_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "shrink_axis_mask"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "TensorScatterAdd"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorScatterSub"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TensorScatterUpdate"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Tile"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "multiples"
#     type_attr: "Tmultiples"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tmultiples"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "TileGrad"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "multiples"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 3
#     explanation: "TileGrad has been replaced with reduce_sum"
#   }
# }
# op {
#   name: "Transpose"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "perm"
#     type_attr: "Tperm"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Tperm"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Unique"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "idx"
#     type_attr: "out_idx"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_idx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "UniqueV2"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Taxis"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "idx"
#     type_attr: "out_idx"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Taxis"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "out_idx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "UniqueWithCounts"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "idx"
#     type_attr: "out_idx"
#   }
#   output_arg {
#     name: "count"
#     type_attr: "out_idx"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_idx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "UniqueWithCountsV2"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "axis"
#     type_attr: "Taxis"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "idx"
#     type_attr: "out_idx"
#   }
#   output_arg {
#     name: "count"
#     type_attr: "out_idx"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "Taxis"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "out_idx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Unpack"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#     number_attr: "num"
#   }
#   attr {
#     name: "num"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "axis"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "UnravelIndex"
#   input_arg {
#     name: "indices"
#     type_attr: "Tidx"
#   }
#   input_arg {
#     name: "dims"
#     type_attr: "Tidx"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tidx"
#   }
#   attr {
#     name: "Tidx"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "UpperBound"
#   input_arg {
#     name: "sorted_inputs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "Where"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "index"
#     type: DT_INT64
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_BOOL
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#         type: DT_BOOL
#       }
#     }
#   }
# }
# op {
#   name: "ZerosLike"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "y"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nm\n\023BatchMatrixBandPart\022\n\n\005input\"\001T\022\r\n\tnum_lower\030\t\022\r\n\tnum_upper\030\t\032\t\n\004band\"\001T\"\t\n\001T\022\004typeB\026\010\016\022\022Use MatrixBandPart\nL\n\017BatchMatrixDiag\022\r\n\010diagonal\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004typeB\022\010\016\022\016Use MatrixDiag\nS\n\023BatchMatrixDiagPart\022\n\n\005input\"\001T\032\r\n\010diagonal\"\001T\"\t\n\001T\022\004typeB\026\010\016\022\022Use MatrixDiagPart\n^\n\022BatchMatrixSetDiag\022\n\n\005input\"\001T\022\r\n\010diagonal\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004typeB\025\010\016\022\021Use MatrixSetDiag\nr\n\014BatchToSpace\022\n\n\005input\"\001T\022\r\n\005crops\"\004Tidx\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\nblock_size\022\003int(\0010\002\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\240\001\n\016BatchToSpaceND\022\n\n\005input\"\001T\022\033\n\013block_shape\"\014Tblock_shape\022\017\n\005crops\"\006Tcrops\032\013\n\006output\"\001T\"\t\n\001T\022\004type\" \n\014Tblock_shape\022\004type\032\0020\003:\006\n\0042\002\003\t\"\032\n\006Tcrops\022\004type\032\0020\003:\006\n\0042\002\003\t\np\n\007Bitcast\022\n\n\005input\"\001T\032\016\n\006output\"\004type\"\"\n\001T\022\004type:\027\n\0252\023\016\023\001\002\t\003\004\021\026\027\006\005\010\022\013\014\017\020\r\"%\n\004type\022\004type:\027\n\0252\023\016\023\001\002\t\003\004\021\026\027\006\005\010\022\013\014\017\020\r\nA\n\rBroadcastArgs\022\007\n\002s0\"\001T\022\007\n\002s1\"\001T\032\007\n\002r0\"\001T\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\nR\n\025BroadcastGradientArgs\022\007\n\002s0\"\001T\022\007\n\002s1\"\001T\032\007\n\002r0\"\001T\032\007\n\002r1\"\001T\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\nZ\n\013BroadcastTo\022\n\n\005input\"\001T\022\r\n\005shape\"\004Tidx\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\nQ\n\rCheckNumerics\022\013\n\006tensor\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\"\021\n\007message\022\006string\nN\n\006Concat\022\016\n\nconcat_dim\030\003\022\016\n\006values\"\001T*\001N\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\002\"\t\n\001T\022\004type\nI\n\014ConcatOffset\022\016\n\nconcat_dim\030\003\022\014\n\005shape\030\003*\001N\032\r\n\006offset\030\003*\001N\"\014\n\001N\022\003int(\0010\002\nh\n\010ConcatV2\022\016\n\006values\"\001T*\001N\022\014\n\004axis\"\004Tidx\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\002\"\t\n\001T\022\004type\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\nY\n\022ConjugateTranspose\022\006\n\001x\"\001T\022\r\n\004perm\"\005Tperm\032\006\n\001y\"\001T\"\t\n\001T\022\004type\"\031\n\005Tperm\022\004type\032\0020\003:\006\n\0042\002\003\t\n8\n\005Const\032\017\n\006output\"\005dtype\"\017\n\005value\022\006tensor\"\r\n\005dtype\022\004type\n>\n\025DebugGradientIdentity\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\230\001\001\nG\n\030DebugGradientRefIdentity\022\r\n\005input\"\001T\200\001\001\032\016\n\006output\"\001T\200\001\001\"\t\n\001T\022\004type\230\001\001\n(\n\010DeepCopy\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\t\n\001T\022\004type\210\001\001\n\205\001\n\014DepthToSpace\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\nblock_size\022\003int(\0010\002\":\n\013data_format\022\006string\032\006\022\004NHWC:\033\n\031\022\004NHWC\022\004NCHW\022\013NCHW_VECT_C\n\235\001\n\nDequantize\022\n\n\005input\"\001T\022\r\n\tmin_range\030\001\022\r\n\tmax_range\030\001\032\n\n\006output\030\001\"\024\n\001T\022\004type:\t\n\0072\005\013\014\r\017\020\"C\n\004mode\022\006string\032\016\022\014MIN_COMBINED:#\n!\022\014MIN_COMBINED\022\tMIN_FIRST\022\006SCALED\n;\n\004Diag\022\r\n\010diagonal\"\001T\032\013\n\006output\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n>\n\010DiagPart\022\n\n\005input\"\001T\032\r\n\010diagonal\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\016\023\001\002\003\t\010\022\n\271\001\n\014EditDistance\022\026\n\022hypothesis_indices\030\t\022\026\n\021hypothesis_values\"\001T\022\024\n\020hypothesis_shape\030\t\022\021\n\rtruth_indices\030\t\022\021\n\014truth_values\"\001T\022\017\n\013truth_shape\030\t\032\n\n\006output\030\001\"\025\n\tnormalize\022\004bool\032\002(\001\"\t\n\001T\022\004type\nG\n\005Empty\022\t\n\005shape\030\003\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\020\n\004init\022\004bool\032\002(\000\210\001\001\nA\n\013EnsureShape\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\016\n\005shape\022\005shape\"\t\n\001T\022\004type\nW\n\nExpandDims\022\n\n\005input\"\001T\022\013\n\003dim\"\004Tdim\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\004Tdim\022\004type\032\0020\003:\006\n\0042\002\003\t\n\274\001\n\023ExtractImagePatches\022\013\n\006images\"\001T\032\014\n\007patches\"\001T\"\027\n\006ksizes\022\tlist(int)(\0010\004\"\030\n\007strides\022\tlist(int)(\0010\004\"\026\n\005rates\022\tlist(int)(\0010\004\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\244\001\n\024ExtractVolumePatches\022\n\n\005input\"\001T\032\014\n\007patches\"\001T\"\027\n\006ksizes\022\tlist(int)(\0010\005\"\030\n\007strides\022\tlist(int)(\0010\005\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\n\213\001\n\027FakeQuantWithMinMaxArgs\022\n\n\006inputs\030\001\032\013\n\007outputs\030\001\"\023\n\003min\022\005float\032\005%\000\000\300\300\"\023\n\003max\022\005float\032\005%\000\000\300@\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\n\244\001\n\037FakeQuantWithMinMaxArgsGradient\022\r\n\tgradients\030\001\022\n\n\006inputs\030\001\032\r\n\tbackprops\030\001\"\023\n\003min\022\005float\032\005%\000\000\300\300\"\023\n\003max\022\005float\032\005%\000\000\300@\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\ns\n\027FakeQuantWithMinMaxVars\022\n\n\006inputs\030\001\022\007\n\003min\030\001\022\007\n\003max\030\001\032\013\n\007outputs\030\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\n\302\001\n\037FakeQuantWithMinMaxVarsGradient\022\r\n\tgradients\030\001\022\n\n\006inputs\030\001\022\007\n\003min\030\001\022\007\n\003max\030\001\032\027\n\023backprops_wrt_input\030\001\032\024\n\020backprop_wrt_min\030\001\032\024\n\020backprop_wrt_max\030\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\n}\n!FakeQuantWithMinMaxVarsPerChannel\022\n\n\006inputs\030\001\022\007\n\003min\030\001\022\007\n\003max\030\001\032\013\n\007outputs\030\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\n\314\001\n)FakeQuantWithMinMaxVarsPerChannelGradient\022\r\n\tgradients\030\001\022\n\n\006inputs\030\001\022\007\n\003min\030\001\022\007\n\003max\030\001\032\027\n\023backprops_wrt_input\030\001\032\024\n\020backprop_wrt_min\030\001\032\024\n\020backprop_wrt_max\030\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\030\n\014narrow_range\022\004bool\032\002(\000\n^\n\004Fill\022\022\n\004dims\"\nindex_type\022\n\n\005value\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\036\n\nindex_type\022\004type\032\0020\003:\006\n\0042\002\003\t\n\214\001\n\006Gather\022\021\n\006params\"\007Tparams\022\023\n\007indices\"\010Tindices\032\021\n\006output\"\007Tparams\"\034\n\020validate_indices\022\004bool\032\002(\001\"\017\n\007Tparams\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\np\n\010GatherNd\022\021\n\006params\"\007Tparams\022\023\n\007indices\"\010Tindices\032\021\n\006output\"\007Tparams\"\017\n\007Tparams\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\n\226\001\n\010GatherV2\022\021\n\006params\"\007Tparams\022\023\n\007indices\"\010Tindices\022\r\n\004axis\"\005Taxis\032\021\n\006output\"\007Tparams\"\017\n\007Tparams\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\025\n\005Taxis\022\004type:\006\n\0042\002\003\t\n7\n\016GuaranteeConst\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\210\001\001\n.\n\010Identity\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n9\n\tIdentityN\022\n\n\005input2\001T\032\013\n\006output2\001T\"\023\n\001T\022\nlist(type)(\0010\001\n^\n\016ImmutableConst\032\017\n\006tensor\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\"\034\n\022memory_region_name\022\006string\n6\n\nInplaceAdd\022\006\n\001x\"\001T\022\005\n\001i\030\003\022\006\n\001v\"\001T\032\006\n\001y\"\001T\"\t\n\001T\022\004type\n6\n\nInplaceSub\022\006\n\001x\"\001T\022\005\n\001i\030\003\022\006\n\001v\"\001T\032\006\n\001y\"\001T\"\t\n\001T\022\004type\n9\n\rInplaceUpdate\022\006\n\001x\"\001T\022\005\n\001i\030\003\022\006\n\001v\"\001T\032\006\n\001y\"\001T\"\t\n\001T\022\004type\n:\n\021InvertPermutation\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\n\\\n\010ListDiff\022\006\n\001x\"\001T\022\006\n\001y\"\001T\032\010\n\003out\"\001T\032\016\n\003idx\"\007out_idx\"\t\n\001T\022\004type\"\033\n\007out_idx\022\004type\032\0020\003:\006\n\0042\002\003\t\nj\n\nLowerBound\022\022\n\rsorted_inputs\"\001T\022\013\n\006values\"\001T\032\022\n\006output\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\nx\n\016MatrixBandPart\022\n\n\005input\"\001T\022\023\n\tnum_lower\"\006Tindex\022\023\n\tnum_upper\"\006Tindex\032\t\n\004band\"\001T\"\t\n\001T\022\004type\"\032\n\006Tindex\022\004type\032\0020\t:\006\n\0042\002\003\t\n3\n\nMatrixDiag\022\r\n\010diagonal\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n6\n\016MatrixDiagPart\022\n\n\005input\"\001T\032\r\n\010diagonal\"\001T\"\t\n\001T\022\004type\nB\n\rMatrixSetDiag\022\n\n\005input\"\001T\022\r\n\010diagonal\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n\215\001\n\tMirrorPad\022\n\n\005input\"\001T\022\025\n\010paddings\"\tTpaddings\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\"&\n\004mode\022\006string:\026\n\024\022\007REFLECT\022\tSYMMETRIC\n\221\001\n\rMirrorPadGrad\022\n\n\005input\"\001T\022\025\n\010paddings\"\tTpaddings\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\"&\n\004mode\022\006string:\026\n\024\022\007REFLECT\022\tSYMMETRIC\n\214\001\n\006OneHot\022\r\n\007indices\"\002TI\022\t\n\005depth\030\003\022\r\n\010on_value\"\001T\022\016\n\toff_value\"\001T\032\013\n\006output\"\001T\"\030\n\004axis\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\t\n\001T\022\004type\"\027\n\002TI\022\004type\032\0020\t:\007\n\0052\003\004\003\t\n8\n\010OnesLike\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\034\n\001T\022\004type:\021\n\0172\r\016\023\001\002\006\004\005\021\003\t\010\022\n\nM\n\004Pack\022\016\n\006values\"\001T*\001N\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\"\017\n\004axis\022\003int\032\002\030\000\n_\n\003Pad\022\n\n\005input\"\001T\022\025\n\010paddings\"\tTpaddings\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\nw\n\005PadV2\022\n\n\005input\"\001T\022\025\n\010paddings\"\tTpaddings\022\024\n\017constant_values\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\nV\n\016ParallelConcat\022\016\n\006values\"\001T*\001N\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\"\016\n\005shape\022\005shape\nC\n\013Placeholder\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\024\n\005shape\022\005shape\032\004:\002\030\001\nw\n\rPlaceholderV2\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shapeB6\010\027\0222Placeholder now behaves the same as PlaceholderV2.\nX\n\026PlaceholderWithDefault\022\016\n\005input\"\005dtype\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\nL\n\017PreventGradient\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\007message\022\006string\032\002\022\000\n\354\001\n\025QuantizeAndDequantize\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\030\n\014signed_input\022\004bool\032\002(\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\027\n\013range_given\022\004bool\032\002(\000\"\031\n\tinput_min\022\005float\032\005%\000\000\000\000\"\031\n\tinput_max\022\005float\032\005%\000\000\000\000\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002B\'\010\026\022#Replaced by QuantizeAndDequantizeV2\n\360\001\n\027QuantizeAndDequantizeV2\022\n\n\005input\"\001T\022\016\n\tinput_min\"\001T\022\016\n\tinput_max\"\001T\032\013\n\006output\"\001T\"\030\n\014signed_input\022\004bool\032\002(\001\"\023\n\010num_bits\022\003int\032\002\030\010\"\027\n\013range_given\022\004bool\032\002(\000\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\"?\n\nround_mode\022\006string\032\016\022\014HALF_TO_EVEN:\031\n\027\022\014HALF_TO_EVEN\022\007HALF_UP\n\250\001\n\027QuantizeAndDequantizeV3\022\n\n\005input\"\001T\022\016\n\tinput_min\"\001T\022\016\n\tinput_max\"\001T\022\014\n\010num_bits\030\003\032\013\n\006output\"\001T\"\030\n\014signed_input\022\004bool\032\002(\001\"\027\n\013range_given\022\004bool\032\002(\001\"\023\n\001T\022\004type:\010\n\0062\004\016\023\001\002\n\221\002\n\nQuantizeV2\022\t\n\005input\030\001\022\r\n\tmin_range\030\001\022\r\n\tmax_range\030\001\032\013\n\006output\"\001T\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\024\n\001T\022\004type:\t\n\0072\005\013\014\r\017\020\"C\n\004mode\022\006string\032\016\022\014MIN_COMBINED:#\n!\022\014MIN_COMBINED\022\tMIN_FIRST\022\006SCALED\"R\n\nround_mode\022\006string\032\025\022\023HALF_AWAY_FROM_ZERO:%\n#\022\023HALF_AWAY_FROM_ZERO\022\014HALF_TO_EVEN\n\236\001\n\017QuantizedConcat\022\016\n\nconcat_dim\030\003\022\016\n\006values\"\001T*\001N\022\021\n\ninput_mins\030\001*\001N\022\022\n\013input_maxes\030\001*\001N\032\013\n\006output\"\001T\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\014\n\001N\022\003int(\0010\002\"\t\n\001T\022\004type\n\205\002\n\025QuantizedInstanceNorm\022\006\n\001x\"\001T\022\t\n\005x_min\030\001\022\t\n\005x_max\030\001\032\006\n\001y\"\001T\032\t\n\005y_min\030\001\032\t\n\005y_max\030\001\"\024\n\001T\022\004type:\t\n\0072\005\013\014\r\017\020\"\036\n\022output_range_given\022\004bool\032\002(\000\"\033\n\013given_y_min\022\005float\032\005%\000\000\000\000\"\033\n\013given_y_max\022\005float\032\005%\000\000\000\000\" \n\020variance_epsilon\022\005float\032\005%\254\305\'7\"\036\n\016min_separation\022\005float\032\005%o\022\203:\n\242\001\n\020QuantizedReshape\022\013\n\006tensor\"\001T\022\017\n\005shape\"\006Tshape\022\r\n\tinput_min\030\001\022\r\n\tinput_max\030\001\032\013\n\006output\"\001T\032\016\n\noutput_min\030\001\032\016\n\noutput_max\030\001\"\t\n\001T\022\004type\"\032\n\006Tshape\022\004type\032\0020\003:\006\n\0042\002\003\t\n)\n\004Rank\022\n\n\005input\"\001T\032\n\n\006output\030\003\"\t\n\001T\022\004type\n:\n\013RefIdentity\022\r\n\005input\"\001T\200\001\001\032\016\n\006output\"\001T\200\001\001\"\t\n\001T\022\004type\230\001\001\n[\n\007Reshape\022\013\n\006tensor\"\001T\022\017\n\005shape\"\006Tshape\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\032\n\006Tshape\022\004type\032\0020\003:\006\n\0042\002\003\t\n\203\002\n\032ResourceStridedSliceAssign\022\007\n\003ref\030\024\022\016\n\005begin\"\005Index\022\014\n\003end\"\005Index\022\020\n\007strides\"\005Index\022\n\n\005value\"\001T\"\t\n\001T\022\004type\"\025\n\005Index\022\004type:\006\n\0042\002\003\t\"\025\n\nbegin_mask\022\003int\032\002\030\000\"\023\n\010end_mask\022\003int\032\002\030\000\"\030\n\rellipsis_mask\022\003int\032\002\030\000\"\030\n\rnew_axis_mask\022\003int\032\002\030\000\"\033\n\020shrink_axis_mask\022\003int\032\002\030\000\210\001\001\nK\n\007Reverse\022\013\n\006tensor\"\001T\022\010\n\004dims\030\n\032\013\n\006output\"\001T\"\034\n\001T\022\004type:\021\n\0172\r\004\006\021\005\003\t\n\023\001\002\010\022\007\n\212\001\n\017ReverseSequence\022\n\n\005input\"\001T\022\023\n\013seq_lengths\"\004Tlen\032\013\n\006output\"\001T\"\016\n\007seq_dim\022\003int\"\024\n\tbatch_dim\022\003int\032\002\030\000\"\t\n\001T\022\004type\"\030\n\004Tlen\022\004type\032\0020\t:\006\n\0042\002\003\t\nl\n\tReverseV2\022\013\n\006tensor\"\001T\022\014\n\004axis\"\004Tidx\032\013\n\006output\"\001T\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\"\035\n\001T\022\004type:\022\n\0202\016\004\006\021\005\003\t\n\016\023\001\002\010\022\007\ns\n\tScatterNd\022\023\n\007indices\"\010Tindices\022\014\n\007updates\"\001T\022\021\n\005shape\"\010Tindices\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\n\222\001\n\027ScatterNdNonAliasingAdd\022\n\n\005input\"\001T\022\023\n\007indices\"\010Tindices\022\014\n\007updates\"\001T\032\013\n\006output\"\001T\"!\n\001T\022\004type:\026\n\0242\022\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nP\n\005Shape\022\n\n\005input\"\001T\032\022\n\006output\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\ne\n\006ShapeN\022\r\n\005input\"\001T*\001N\032\025\n\006output\"\010out_type*\001N\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\nO\n\004Size\022\n\n\005input\"\001T\032\022\n\006output\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\na\n\005Slice\022\n\n\005input\"\001T\022\016\n\005begin\"\005Index\022\r\n\004size\"\005Index\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\005Index\022\004type:\006\n\0042\002\003\t\n.\n\010Snapshot\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n\177\n\014SpaceToBatch\022\n\n\005input\"\001T\022\025\n\010paddings\"\tTpaddings\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\"\025\n\nblock_size\022\003int(\0010\002\n\251\001\n\016SpaceToBatchND\022\n\n\005input\"\001T\022\033\n\013block_shape\"\014Tblock_shape\022\025\n\010paddings\"\tTpaddings\032\013\n\006output\"\001T\"\t\n\001T\022\004type\" \n\014Tblock_shape\022\004type\032\0020\003:\006\n\0042\002\003\t\"\035\n\tTpaddings\022\004type\032\0020\003:\006\n\0042\002\003\t\n\205\001\n\014SpaceToDepth\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\nblock_size\022\003int(\0010\002\":\n\013data_format\022\006string\032\006\022\004NHWC:\033\n\031\022\004NHWC\022\004NCHW\022\013NCHW_VECT_C\n[\n\005Split\022\r\n\tsplit_dim\030\003\022\n\n\005value\"\001T\032\026\n\006output\"\001T*\tnum_split\"\024\n\tnum_split\022\003int(\0010\001\"\t\n\001T\022\004type\n\213\001\n\006SplitV\022\n\n\005value\"\001T\022\023\n\013size_splits\"\004Tlen\022\r\n\tsplit_dim\030\003\032\026\n\006output\"\001T*\tnum_split\"\024\n\tnum_split\022\003int(\0010\001\"\t\n\001T\022\004type\"\030\n\004Tlen\022\004type\032\0020\t:\006\n\0042\002\003\t\nN\n\007Squeeze\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\037\n\014squeeze_dims\022\tlist(int)\032\002\n\000(\001\n2\n\014StopGradient\022\n\n\005input\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\n\366\001\n\014StridedSlice\022\n\n\005input\"\001T\022\016\n\005begin\"\005Index\022\014\n\003end\"\005Index\022\020\n\007strides\"\005Index\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\005Index\022\004type:\006\n\0042\002\003\t\"\025\n\nbegin_mask\022\003int\032\002\030\000\"\023\n\010end_mask\022\003int\032\002\030\000\"\030\n\rellipsis_mask\022\003int\032\002\030\000\"\030\n\rnew_axis_mask\022\003int\032\002\030\000\"\033\n\020shrink_axis_mask\022\003int\032\002\030\000\n\220\002\n\022StridedSliceAssign\022\013\n\003ref\"\001T\200\001\001\022\016\n\005begin\"\005Index\022\014\n\003end\"\005Index\022\020\n\007strides\"\005Index\022\n\n\005value\"\001T\032\022\n\noutput_ref\"\001T\200\001\001\"\t\n\001T\022\004type\"\025\n\005Index\022\004type:\006\n\0042\002\003\t\"\025\n\nbegin_mask\022\003int\032\002\030\000\"\023\n\010end_mask\022\003int\032\002\030\000\"\030\n\rellipsis_mask\022\003int\032\002\030\000\"\030\n\rnew_axis_mask\022\003int\032\002\030\000\"\033\n\020shrink_axis_mask\022\003int\032\002\030\000\n\207\002\n\020StridedSliceGrad\022\016\n\005shape\"\005Index\022\016\n\005begin\"\005Index\022\014\n\003end\"\005Index\022\020\n\007strides\"\005Index\022\007\n\002dy\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\025\n\005Index\022\004type:\006\n\0042\002\003\t\"\025\n\nbegin_mask\022\003int\032\002\030\000\"\023\n\010end_mask\022\003int\032\002\030\000\"\030\n\rellipsis_mask\022\003int\032\002\030\000\"\030\n\rnew_axis_mask\022\003int\032\002\030\000\"\033\n\020shrink_axis_mask\022\003int\032\002\030\000\nt\n\020TensorScatterAdd\022\013\n\006tensor\"\001T\022\023\n\007indices\"\010Tindices\022\014\n\007updates\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nt\n\020TensorScatterSub\022\013\n\006tensor\"\001T\022\023\n\007indices\"\010Tindices\022\014\n\007updates\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nw\n\023TensorScatterUpdate\022\013\n\006tensor\"\001T\022\023\n\007indices\"\010Tindices\022\014\n\007updates\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\nc\n\004Tile\022\n\n\005input\"\001T\022\027\n\tmultiples\"\nTmultiples\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\036\n\nTmultiples\022\004type\032\0020\003:\006\n\0042\002\003\t\nm\n\010TileGrad\022\n\n\005input\"\001T\022\r\n\tmultiples\030\003\032\013\n\006output\"\001T\"\t\n\001T\022\004typeB.\010\003\022*TileGrad has been replaced with reduce_sum\nP\n\tTranspose\022\006\n\001x\"\001T\022\r\n\004perm\"\005Tperm\032\006\n\001y\"\001T\"\t\n\001T\022\004type\"\031\n\005Tperm\022\004type\032\0020\003:\006\n\0042\002\003\t\nP\n\006Unique\022\006\n\001x\"\001T\032\006\n\001y\"\001T\032\016\n\003idx\"\007out_idx\"\t\n\001T\022\004type\"\033\n\007out_idx\022\004type\032\0020\003:\006\n\0042\002\003\t\n|\n\010UniqueV2\022\006\n\001x\"\001T\022\r\n\004axis\"\005Taxis\032\006\n\001y\"\001T\032\016\n\003idx\"\007out_idx\"\t\n\001T\022\004type\"\031\n\005Taxis\022\004type\032\0020\t:\006\n\0042\002\003\t\"\033\n\007out_idx\022\004type\032\0020\003:\006\n\0042\002\003\t\nl\n\020UniqueWithCounts\022\006\n\001x\"\001T\032\006\n\001y\"\001T\032\016\n\003idx\"\007out_idx\032\020\n\005count\"\007out_idx\"\t\n\001T\022\004type\"\033\n\007out_idx\022\004type\032\0020\003:\006\n\0042\002\003\t\n\230\001\n\022UniqueWithCountsV2\022\006\n\001x\"\001T\022\r\n\004axis\"\005Taxis\032\006\n\001y\"\001T\032\016\n\003idx\"\007out_idx\032\020\n\005count\"\007out_idx\"\t\n\001T\022\004type\"\031\n\005Taxis\022\004type\032\0020\t:\006\n\0042\002\003\t\"\033\n\007out_idx\022\004type\032\0020\003:\006\n\0042\002\003\t\nP\n\006Unpack\022\n\n\005value\"\001T\032\020\n\006output\"\001T*\003num\"\014\n\003num\022\003int(\001\"\t\n\001T\022\004type\"\017\n\004axis\022\003int\032\002\030\000\nW\n\014UnravelIndex\022\017\n\007indices\"\004Tidx\022\014\n\004dims\"\004Tidx\032\016\n\006output\"\004Tidx\"\030\n\004Tidx\022\004type\032\0020\003:\006\n\0042\002\003\t\nj\n\nUpperBound\022\022\n\rsorted_inputs\"\001T\022\013\n\006values\"\001T\032\022\n\006output\"\010out_type\"\t\n\001T\022\004type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\nE\n\005Where\022\n\n\005input\"\001T\032\t\n\005index\030\t\"%\n\001T\022\004type\032\0020\n:\026\n\0242\022\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\n&\n\tZerosLike\022\006\n\001x\"\001T\032\006\n\001y\"\001T\"\t\n\001T\022\004type")
