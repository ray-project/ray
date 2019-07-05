"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: libsvm_ops.cc
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


_decode_libsvm_outputs = ["label", "feature_indices", "feature_values",
                         "feature_shape"]
_DecodeLibsvmOutput = _collections.namedtuple(
    "DecodeLibsvm", _decode_libsvm_outputs)


@_dispatch.add_dispatch_list
@tf_export('decode_libsvm')
def decode_libsvm(input, num_features, dtype=_dtypes.float32, label_dtype=_dtypes.int64, name=None):
  r"""Convert LibSVM input to tensors. The output consists of

  a label and a feature tensor. The shape of the label tensor
  is the same as input and the shape of the feature tensor is
  `[input_shape, num_features]`.

  Args:
    input: A `Tensor` of type `string`. Each string is a record in the LibSVM.
    num_features: An `int` that is `>= 1`. The number of features.
    dtype: An optional `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.int64`. Defaults to `tf.float32`.
    label_dtype: An optional `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (label, feature_indices, feature_values, feature_shape).

    label: A `Tensor` of type `label_dtype`. A tensor of the same shape as input.
    feature_indices: A `Tensor` of type `int64`. A 2-D int64 tensor of dense_shape [N, ndims].
    feature_values: A `Tensor` of type `dtype`. A 1-D tensor of any type and dense_shape [N].
    feature_shape: A `Tensor` of type `int64`. A 1-D int64 tensor of dense_shape [ndims].
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeLibsvm",
        name, _ctx._post_execution_callbacks, input, "dtype", dtype,
        "label_dtype", label_dtype, "num_features", num_features)
      _result = _DecodeLibsvmOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return decode_libsvm_eager_fallback(
            input, dtype=dtype, label_dtype=label_dtype,
            num_features=num_features, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decode_libsvm, input=input, num_features=num_features,
                             dtype=dtype, label_dtype=label_dtype, name=name)
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
  num_features = _execute.make_int(num_features, "num_features")
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  if label_dtype is None:
    label_dtype = _dtypes.int64
  label_dtype = _execute.make_type(label_dtype, "label_dtype")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeLibsvm", input=input, num_features=num_features, dtype=dtype,
                        label_dtype=label_dtype, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decode_libsvm, input=input, num_features=num_features, dtype=dtype,
                         label_dtype=label_dtype, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "label_dtype",
            _op.get_attr("label_dtype"), "num_features",
            _op.get_attr("num_features"))
  _execute.record_gradient(
      "DecodeLibsvm", _inputs_flat, _attrs, _result, name)
  _result = _DecodeLibsvmOutput._make(_result)
  return _result



def decode_libsvm_eager_fallback(input, num_features, dtype=_dtypes.float32, label_dtype=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_libsvm
  """
  _ctx = ctx if ctx else _context.context()
  num_features = _execute.make_int(num_features, "num_features")
  if dtype is None:
    dtype = _dtypes.float32
  dtype = _execute.make_type(dtype, "dtype")
  if label_dtype is None:
    label_dtype = _dtypes.int64
  label_dtype = _execute.make_type(label_dtype, "label_dtype")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("dtype", dtype, "label_dtype", label_dtype, "num_features",
  num_features)
  _result = _execute.execute(b"DecodeLibsvm", 4, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeLibsvm", _inputs_flat, _attrs, _result, name)
  _result = _DecodeLibsvmOutput._make(_result)
  return _result

_ops.RegisterShape("DecodeLibsvm")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "DecodeLibsvm"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "label"
#     type_attr: "label_dtype"
#   }
#   output_arg {
#     name: "feature_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "feature_values"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "feature_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "label_dtype"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\311\001\n\014DecodeLibsvm\022\t\n\005input\030\007\032\024\n\005label\"\013label_dtype\032\023\n\017feature_indices\030\t\032\027\n\016feature_values\"\005dtype\032\021\n\rfeature_shape\030\t\"\033\n\005dtype\022\004type\032\0020\001:\010\n\0062\004\001\002\003\t\"!\n\013label_dtype\022\004type\032\0020\t:\010\n\0062\004\001\002\003\t\"\027\n\014num_features\022\003int(\0010\001")
