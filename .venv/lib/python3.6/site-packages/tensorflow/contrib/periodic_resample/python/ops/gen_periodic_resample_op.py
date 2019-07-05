"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_periodic_resample_op_py.cc
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
@tf_export('periodic_resample')
def periodic_resample(values, shape, name=None):
  r"""Periodically resample elements of a tensor to conform to `shape`.

  This function implements a slightly more generic version of the subpixel
  convolutions found in this [paper](https://arxiv.org/abs/1609.05158).

  The formula for computing the elements in the `output` tensor is as follows:

    `T` = `values` tensor of rank `R`

    `S` = desired `shape` of output tensor (vector of length `R`)

    `P` = `output` tensor of rank `R`

    \\((T_1,\\ldots,T_R)\\) = shape(`T`)

    \\([S_1,\\ldots,S_q,\\ldots,S_R]\\) = elements of vector `S`

    A single element in `S` is left unspecified (denoted \\(S_q=-1\\)).

    Let \\(f_i\\) denote the (possibly non-integer) factor that relates the original
    dimension to the desired dimensions, \\(S_i=f_i T_i\\), for \\(i\\neq q\\) where
    \\(f_i>0\\).

    Define the following:

    \\(g_i=\\lceil f_i\\rceil\\)

    \\(t=\\prod_i T_i\\)

    \\(s=\\prod_{i\\neq q} S_i\\)

    \\(S_q\\) can then be defined by \\(S_q=\\lfloor t/s\\rfloor\\).
    The elements of the resulting tensor are defined as

    \\(P_{s_1,\\ldots,s_R}=T_{h_1,\\ldots,h_q,\\ldots,h_R}\\).

    The \\(h_i\\) (\\(i\\neq q\\)) are defined by \\(h_i=\\lfloor s_i/g_i\\rfloor\\).

    \\(h_q=S_q\\sum_{j\\neq q}^{q-1}G_j \\mathrm{mod}(s_j,g_j) + s_q\\), where
    \\(G_j=\\prod_{i}^{j-1}g_i\\) (\\(G_0=1\\)).

  One drawback of this method is that whenever the output dimensions are slightly
  less than integer multiples of the input dimensions, many of the tensor elements
  are repeated in an inefficient way. This is resolved by specifying that all
  desired dimensions are integer multiples of the input tensor.

  For example:

  ```prettyprint
  `input` is [[ 0  1  2  3]
              [ 4  5  6  7]
              [ 8  9 10 11]]

  tf.periodic_resample(input, [6, None]) ==> [[ 0  1]
                                              [ 2  3]
                                              [ 4  5]
                                              [ 6  7]
                                              [ 8  9]
                                              [10 11]]
  ```

  Args:
    values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The tensor of rank `R` to periodic_resample
    shape: A `tf.TensorShape` or list of `ints`.
      A 1-D tensor representing the desired shape of the output tensor.
      Exactly one element of this tensor must have the value `None` which represents
      that this dimension of `values` can be adjusted downward in order to
      accommodate increases in other dimensions. The specified sizes of the
      non-adjustable dimensions must by at least as large as in the `values` tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `values`.
    Periodically resampled tensor that has dimensions specified as in
    `shape` except that the dimension specified as `None` will be minimally
    decreased as necessary.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PeriodicResample", name, _ctx._post_execution_callbacks, values,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return periodic_resample_eager_fallback(
            values, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              periodic_resample, values=values, shape=shape, name=name)
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
  shape = _execute.make_shape(shape, "shape")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "PeriodicResample", values=values, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          periodic_resample, values=values, shape=shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "PeriodicResample", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def periodic_resample_eager_fallback(values, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function periodic_resample
  """
  _ctx = ctx if ctx else _context.context()
  shape = _execute.make_shape(shape, "shape")
  _attr_T, (values,) = _execute.args_to_matching_eager([values], _ctx)
  _inputs_flat = [values]
  _attrs = ("T", _attr_T, "shape", shape)
  _result = _execute.execute(b"PeriodicResample", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PeriodicResample", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("PeriodicResample")(None)


@_dispatch.add_dispatch_list
@tf_export('periodic_resample_op_grad')
def periodic_resample_op_grad(grad, original_shape, desired_shape, name=None):
  r"""TODO: add doc.

  Args:
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    original_shape: A `tf.TensorShape` or list of `ints`.
    desired_shape: A `tf.TensorShape` or list of `ints`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grad`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PeriodicResampleOpGrad", name, _ctx._post_execution_callbacks, grad,
        "original_shape", original_shape, "desired_shape", desired_shape)
      return _result
    except _core._FallbackException:
      try:
        return periodic_resample_op_grad_eager_fallback(
            grad, original_shape=original_shape, desired_shape=desired_shape,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              periodic_resample_op_grad, grad=grad,
                                         original_shape=original_shape,
                                         desired_shape=desired_shape,
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
  original_shape = _execute.make_shape(original_shape, "original_shape")
  desired_shape = _execute.make_shape(desired_shape, "desired_shape")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "PeriodicResampleOpGrad", grad=grad, original_shape=original_shape,
                                  desired_shape=desired_shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          periodic_resample_op_grad, grad=grad, original_shape=original_shape,
                                     desired_shape=desired_shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "original_shape",
            _op.get_attr("original_shape"), "desired_shape",
            _op.get_attr("desired_shape"))
  _execute.record_gradient(
      "PeriodicResampleOpGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def periodic_resample_op_grad_eager_fallback(grad, original_shape, desired_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function periodic_resample_op_grad
  """
  _ctx = ctx if ctx else _context.context()
  original_shape = _execute.make_shape(original_shape, "original_shape")
  desired_shape = _execute.make_shape(desired_shape, "desired_shape")
  _attr_T, (grad,) = _execute.args_to_matching_eager([grad], _ctx)
  _inputs_flat = [grad]
  _attrs = ("T", _attr_T, "original_shape", original_shape, "desired_shape",
  desired_shape)
  _result = _execute.execute(b"PeriodicResampleOpGrad", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "PeriodicResampleOpGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("PeriodicResampleOpGrad")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "PeriodicResample"
#   input_arg {
#     name: "values"
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
#       }
#     }
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
# }
# op {
#   name: "PeriodicResampleOpGrad"
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "grad_values"
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
#       }
#     }
#   }
#   attr {
#     name: "original_shape"
#     type: "shape"
#   }
#   attr {
#     name: "desired_shape"
#     type: "shape"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n^\n\020PeriodicResample\022\013\n\006values\"\001T\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\016\n\005shape\022\005shape\n\210\001\n\026PeriodicResampleOpGrad\022\t\n\004grad\"\001T\032\020\n\013grad_values\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\016original_shape\022\005shape\"\026\n\rdesired_shape\022\005shape")
