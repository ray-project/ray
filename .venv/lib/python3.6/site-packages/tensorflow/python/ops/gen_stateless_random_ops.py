"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: stateless_random_ops.cc
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


def stateless_multinomial(logits, num_samples, seed, output_dtype=_dtypes.int64, name=None):
  r"""Draws samples from a multinomial distribution.

  Args:
    logits: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      2-D Tensor with shape `[batch_size, num_classes]`.  Each slice `[i, :]`
      represents the unnormalized log probabilities for all classes.
    num_samples: A `Tensor` of type `int32`.
      0-D.  Number of independent samples to draw for each row slice.
    seed: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2 seeds (shape [2]).
    output_dtype: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `output_dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessMultinomial", name, _ctx._post_execution_callbacks, logits,
        num_samples, seed, "output_dtype", output_dtype)
      return _result
    except _core._FallbackException:
      try:
        return stateless_multinomial_eager_fallback(
            logits, num_samples, seed, output_dtype=output_dtype, name=name,
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
  if output_dtype is None:
    output_dtype = _dtypes.int64
  output_dtype = _execute.make_type(output_dtype, "output_dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatelessMultinomial", logits=logits, num_samples=num_samples,
                                seed=seed, output_dtype=output_dtype,
                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tseed", _op.get_attr("Tseed"),
            "output_dtype", _op.get_attr("output_dtype"))
  _execute.record_gradient(
      "StatelessMultinomial", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stateless_multinomial_eager_fallback(logits, num_samples, seed, output_dtype=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_multinomial
  """
  _ctx = ctx if ctx else _context.context()
  if output_dtype is None:
    output_dtype = _dtypes.int64
  output_dtype = _execute.make_type(output_dtype, "output_dtype")
  _attr_T, (logits,) = _execute.args_to_matching_eager([logits], _ctx)
  _attr_Tseed, (seed,) = _execute.args_to_matching_eager([seed], _ctx, _dtypes.int64)
  num_samples = _ops.convert_to_tensor(num_samples, _dtypes.int32)
  _inputs_flat = [logits, num_samples, seed]
  _attrs = ("T", _attr_T, "Tseed", _attr_Tseed, "output_dtype", output_dtype)
  _result = _execute.execute(b"StatelessMultinomial", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StatelessMultinomial", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stateless_random_normal(shape, seed, dtype=_dtypes.float32, name=None):
  r"""Outputs deterministic pseudorandom values from a normal distribution.

  The generated values will have mean 0 and standard deviation 1.

  The outputs are a deterministic function of `shape` and `seed`.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    seed: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2 seeds (shape [2]).
    dtype: An optional `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`. Defaults to `tf.float32`.
      The type of the output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessRandomNormal", name, _ctx._post_execution_callbacks, shape,
        seed, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return stateless_random_normal_eager_fallback(
            shape, seed, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatelessRandomNormal", shape=shape, seed=seed, dtype=dtype,
                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"), "Tseed",
            _op.get_attr("Tseed"))
  _execute.record_gradient(
      "StatelessRandomNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stateless_random_normal_eager_fallback(shape, seed, dtype=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_random_normal
  """
  _ctx = ctx if ctx else _context.context()
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  _attr_Tseed, (seed,) = _execute.args_to_matching_eager([seed], _ctx, _dtypes.int64)
  _inputs_flat = [shape, seed]
  _attrs = ("dtype", dtype, "T", _attr_T, "Tseed", _attr_Tseed)
  _result = _execute.execute(b"StatelessRandomNormal", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StatelessRandomNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stateless_random_uniform(shape, seed, dtype=_dtypes.float32, name=None):
  r"""Outputs deterministic pseudorandom random values from a uniform distribution.

  The generated values follow a uniform distribution in the range `[0, 1)`. The
  lower bound 0 is included in the range, while the upper bound 1 is excluded.

  The outputs are a deterministic function of `shape` and `seed`.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    seed: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2 seeds (shape [2]).
    dtype: An optional `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`. Defaults to `tf.float32`.
      The type of the output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessRandomUniform", name, _ctx._post_execution_callbacks, shape,
        seed, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return stateless_random_uniform_eager_fallback(
            shape, seed, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatelessRandomUniform", shape=shape, seed=seed, dtype=dtype,
                                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"), "Tseed",
            _op.get_attr("Tseed"))
  _execute.record_gradient(
      "StatelessRandomUniform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stateless_random_uniform_eager_fallback(shape, seed, dtype=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_random_uniform
  """
  _ctx = ctx if ctx else _context.context()
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  _attr_Tseed, (seed,) = _execute.args_to_matching_eager([seed], _ctx, _dtypes.int64)
  _inputs_flat = [shape, seed]
  _attrs = ("dtype", dtype, "T", _attr_T, "Tseed", _attr_Tseed)
  _result = _execute.execute(b"StatelessRandomUniform", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatelessRandomUniform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stateless_random_uniform_int(shape, seed, minval, maxval, name=None):
  r"""Outputs deterministic pseudorandom random integers from a uniform distribution.

  The generated values follow a uniform distribution in the range `[minval, maxval)`.

  The outputs are a deterministic function of `shape`, `seed`, `minval`, and `maxval`.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    seed: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2 seeds (shape [2]).
    minval: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Minimum value (inclusive, scalar).
    maxval: A `Tensor`. Must have the same type as `minval`.
      Maximum value (exclusive, scalar).
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `minval`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessRandomUniformInt", name, _ctx._post_execution_callbacks,
        shape, seed, minval, maxval)
      return _result
    except _core._FallbackException:
      try:
        return stateless_random_uniform_int_eager_fallback(
            shape, seed, minval, maxval, name=name, ctx=_ctx)
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
        "StatelessRandomUniformInt", shape=shape, seed=seed, minval=minval,
                                     maxval=maxval, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"), "Tseed",
            _op.get_attr("Tseed"))
  _execute.record_gradient(
      "StatelessRandomUniformInt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stateless_random_uniform_int_eager_fallback(shape, seed, minval, maxval, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_random_uniform_int
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, _inputs_dtype = _execute.args_to_matching_eager([minval, maxval], _ctx)
  (minval, maxval) = _inputs_dtype
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _attr_Tseed, (seed,) = _execute.args_to_matching_eager([seed], _ctx, _dtypes.int64)
  _inputs_flat = [shape, seed, minval, maxval]
  _attrs = ("dtype", _attr_dtype, "T", _attr_T, "Tseed", _attr_Tseed)
  _result = _execute.execute(b"StatelessRandomUniformInt", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatelessRandomUniformInt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stateless_truncated_normal(shape, seed, dtype=_dtypes.float32, name=None):
  r"""Outputs deterministic pseudorandom values from a truncated normal distribution.

  The generated values follow a normal distribution with mean 0 and standard
  deviation 1, except that values whose magnitude is more than 2 standard
  deviations from the mean are dropped and re-picked.

  The outputs are a deterministic function of `shape` and `seed`.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    seed: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      2 seeds (shape [2]).
    dtype: An optional `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`. Defaults to `tf.float32`.
      The type of the output.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessTruncatedNormal", name, _ctx._post_execution_callbacks,
        shape, seed, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return stateless_truncated_normal_eager_fallback(
            shape, seed, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatelessTruncatedNormal", shape=shape, seed=seed, dtype=dtype,
                                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"), "Tseed",
            _op.get_attr("Tseed"))
  _execute.record_gradient(
      "StatelessTruncatedNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stateless_truncated_normal_eager_fallback(shape, seed, dtype=_dtypes.float32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_truncated_normal
  """
  _ctx = ctx if ctx else _context.context()
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx, _dtypes.int32)
  _attr_Tseed, (seed,) = _execute.args_to_matching_eager([seed], _ctx, _dtypes.int64)
  _inputs_flat = [shape, seed]
  _attrs = ("dtype", dtype, "T", _attr_T, "Tseed", _attr_Tseed)
  _result = _execute.execute(b"StatelessTruncatedNormal", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatelessTruncatedNormal", _inputs_flat, _attrs, _result, name)
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
#   name: "StatelessMultinomial"
#   input_arg {
#     name: "logits"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num_samples"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "seed"
#     type_attr: "Tseed"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "output_dtype"
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
#     name: "Tseed"
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
#     name: "output_dtype"
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
#   name: "StatelessRandomNormal"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "seed"
#     type_attr: "Tseed"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
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
#   attr {
#     name: "Tseed"
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
#   name: "StatelessRandomUniform"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "seed"
#     type_attr: "Tseed"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
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
#   attr {
#     name: "Tseed"
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
#   name: "StatelessRandomUniformInt"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "seed"
#     type_attr: "Tseed"
#   }
#   input_arg {
#     name: "minval"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "maxval"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
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
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "Tseed"
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
#   name: "StatelessTruncatedNormal"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "seed"
#     type_attr: "Tseed"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
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
#   attr {
#     name: "Tseed"
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
_op_def_lib = _InitOpDefLibrary(b"\n\265\001\n\024StatelessMultinomial\022\013\n\006logits\"\001T\022\017\n\013num_samples\030\003\022\r\n\004seed\"\005Tseed\032\026\n\006output\"\014output_dtype\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\"\031\n\005Tseed\022\004type\032\0020\t:\006\n\0042\002\003\t\" \n\014output_dtype\022\004type\032\0020\t:\006\n\0042\002\003\t\n\222\001\n\025StatelessRandomNormal\022\n\n\005shape\"\001T\022\r\n\004seed\"\005Tseed\032\017\n\006output\"\005dtype\"\033\n\005dtype\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\"\031\n\005Tseed\022\004type\032\0020\t:\006\n\0042\002\003\t\n\223\001\n\026StatelessRandomUniform\022\n\n\005shape\"\001T\022\r\n\004seed\"\005Tseed\032\017\n\006output\"\005dtype\"\033\n\005dtype\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\"\031\n\005Tseed\022\004type\032\0020\t:\006\n\0042\002\003\t\n\256\001\n\031StatelessRandomUniformInt\022\n\n\005shape\"\001T\022\r\n\004seed\"\005Tseed\022\017\n\006minval\"\005dtype\022\017\n\006maxval\"\005dtype\032\017\n\006output\"\005dtype\"\025\n\005dtype\022\004type:\006\n\0042\002\003\t\"\021\n\001T\022\004type:\006\n\0042\002\003\t\"\031\n\005Tseed\022\004type\032\0020\t:\006\n\0042\002\003\t\n\225\001\n\030StatelessTruncatedNormal\022\n\n\005shape\"\001T\022\r\n\004seed\"\005Tseed\032\017\n\006output\"\005dtype\"\033\n\005dtype\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\"\025\n\001T\022\004type\032\0020\003:\006\n\0042\002\003\t\"\031\n\005Tseed\022\004type\032\0020\t:\006\n\0042\002\003\t")
