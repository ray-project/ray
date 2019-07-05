"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: sparse_feature_cross_op.cc
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


_sparse_feature_cross_outputs = ["output_indices", "output_values",
                                "output_shape"]
_SparseFeatureCrossOutput = _collections.namedtuple(
    "SparseFeatureCross", _sparse_feature_cross_outputs)


@_dispatch.add_dispatch_list
@tf_export('sparse_feature_cross')
def sparse_feature_cross(indices, values, shapes, dense, hashed_output, num_buckets, out_type, internal_type, name=None):
  r"""Generates sparse cross form a list of sparse tensors.

  The op takes two lists, one of 2D `SparseTensor` and one of 2D `Tensor`, each
  representing features of one feature column. It outputs a 2D `SparseTensor` with
  the batchwise crosses of these features.

  For example, if the inputs are

      inputs[0]: SparseTensor with shape = [2, 2]
      [0, 0]: "a"
      [1, 0]: "b"
      [1, 1]: "c"

      inputs[1]: SparseTensor with shape = [2, 1]
      [0, 0]: "d"
      [1, 0]: "e"

      inputs[2]: Tensor [["f"], ["g"]]

  then the output will be

      shape = [2, 2]
      [0, 0]: "a_X_d_X_f"
      [1, 0]: "b_X_e_X_g"
      [1, 1]: "c_X_e_X_g"

  if hashed_output=true then the output will be

      shape = [2, 2]
      [0, 0]: HashCombine(
                  Fingerprint64("f"), HashCombine(
                      Fingerprint64("d"), Fingerprint64("a")))
      [1, 0]: HashCombine(
                  Fingerprint64("g"), HashCombine(
                      Fingerprint64("e"), Fingerprint64("b")))
      [1, 1]: HashCombine(
                  Fingerprint64("g"), HashCombine(
                      Fingerprint64("e"), Fingerprint64("c")))

  Args:
    indices: A list of `Tensor` objects with type `int64`.
      2-D.  Indices of each input `SparseTensor`.
    values: A list of `Tensor` objects with types from: `int64`, `string`.
      1-D.   values of each `SparseTensor`.
    shapes: A list with the same length as `indices` of `Tensor` objects with type `int64`.
      1-D.   Shapes of each `SparseTensor`.
    dense: A list of `Tensor` objects with types from: `int64`, `string`.
      2-D.    Columns represented by dense `Tensor`.
    hashed_output: A `bool`.
    num_buckets: An `int` that is `>= 0`.
    out_type: A `tf.DType` from: `tf.int64, tf.string`.
    internal_type: A `tf.DType` from: `tf.int64, tf.string`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`. 2-D.  Indices of the concatenated `SparseTensor`.
    output_values: A `Tensor` of type `out_type`. 1-D.  Non-empty values of the concatenated or hashed
      `SparseTensor`.
    output_shape: A `Tensor` of type `int64`. 1-D.  Shape of the concatenated `SparseTensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseFeatureCross", name, _ctx._post_execution_callbacks, indices,
        values, shapes, dense, "hashed_output", hashed_output, "num_buckets",
        num_buckets, "out_type", out_type, "internal_type", internal_type)
      _result = _SparseFeatureCrossOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_feature_cross_eager_fallback(
            indices, values, shapes, dense, hashed_output=hashed_output,
            num_buckets=num_buckets, out_type=out_type,
            internal_type=internal_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sparse_feature_cross, indices=indices, values=values,
                                    shapes=shapes, dense=dense,
                                    hashed_output=hashed_output,
                                    num_buckets=num_buckets,
                                    out_type=out_type,
                                    internal_type=internal_type, name=name)
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
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_feature_cross' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_feature_cross' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_feature_cross' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SparseFeatureCross", indices=indices, values=values, shapes=shapes,
                              dense=dense, hashed_output=hashed_output,
                              num_buckets=num_buckets, out_type=out_type,
                              internal_type=internal_type, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sparse_feature_cross, indices=indices, values=values, shapes=shapes,
                                dense=dense, hashed_output=hashed_output,
                                num_buckets=num_buckets, out_type=out_type,
                                internal_type=internal_type, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "hashed_output",
            _op.get_attr("hashed_output"), "num_buckets",
            _op.get_attr("num_buckets"), "sparse_types",
            _op.get_attr("sparse_types"), "dense_types",
            _op.get_attr("dense_types"), "out_type", _op.get_attr("out_type"),
            "internal_type", _op.get_attr("internal_type"))
  _execute.record_gradient(
      "SparseFeatureCross", _inputs_flat, _attrs, _result, name)
  _result = _SparseFeatureCrossOutput._make(_result)
  return _result



def sparse_feature_cross_eager_fallback(indices, values, shapes, dense, hashed_output, num_buckets, out_type, internal_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_feature_cross
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_feature_cross' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_feature_cross' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_feature_cross' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  _attr_sparse_types, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  _attr_dense_types, dense = _execute.convert_to_mixed_eager_tensors(dense, _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int64)
  shapes = _ops.convert_n_to_tensor(shapes, _dtypes.int64)
  _inputs_flat = list(indices) + list(values) + list(shapes) + list(dense)
  _attrs = ("N", _attr_N, "hashed_output", hashed_output, "num_buckets",
  num_buckets, "sparse_types", _attr_sparse_types, "dense_types",
  _attr_dense_types, "out_type", out_type, "internal_type", internal_type)
  _result = _execute.execute(b"SparseFeatureCross", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseFeatureCross", _inputs_flat, _attrs, _result, name)
  _result = _SparseFeatureCrossOutput._make(_result)
  return _result

_ops.RegisterShape("SparseFeatureCross")(None)


_sparse_feature_cross_v2_outputs = ["output_indices", "output_values",
                                   "output_shape"]
_SparseFeatureCrossV2Output = _collections.namedtuple(
    "SparseFeatureCrossV2", _sparse_feature_cross_v2_outputs)


@_dispatch.add_dispatch_list
@tf_export('sparse_feature_cross_v2')
def sparse_feature_cross_v2(indices, values, shapes, dense, hashed_output, num_buckets, hash_key, out_type, internal_type, name=None):
  r"""Generates sparse cross form a list of sparse tensors.

  The op takes two lists, one of 2D `SparseTensor` and one of 2D `Tensor`, each
  representing features of one feature column. It outputs a 2D `SparseTensor` with
  the batchwise crosses of these features.

  For example, if the inputs are

      inputs[0]: SparseTensor with shape = [2, 2]
      [0, 0]: "a"
      [1, 0]: "b"
      [1, 1]: "c"

      inputs[1]: SparseTensor with shape = [2, 1]
      [0, 0]: "d"
      [1, 0]: "e"

      inputs[2]: Tensor [["f"], ["g"]]

  then the output will be

      shape = [2, 2]
      [0, 0]: "a_X_d_X_f"
      [1, 0]: "b_X_e_X_g"
      [1, 1]: "c_X_e_X_g"

  if hashed_output=true then the output will be

      shape = [2, 2]
      [0, 0]: FingerprintCat64(
                  Fingerprint64("f"), FingerprintCat64(
                      Fingerprint64("d"), Fingerprint64("a")))
      [1, 0]: FingerprintCat64(
                  Fingerprint64("g"), FingerprintCat64(
                      Fingerprint64("e"), Fingerprint64("b")))
      [1, 1]: FingerprintCat64(
                  Fingerprint64("g"), FingerprintCat64(
                      Fingerprint64("e"), Fingerprint64("c")))

  Args:
    indices: A list of `Tensor` objects with type `int64`.
      2-D.  Indices of each input `SparseTensor`.
    values: A list of `Tensor` objects with types from: `int64`, `string`.
      1-D.   values of each `SparseTensor`.
    shapes: A list with the same length as `indices` of `Tensor` objects with type `int64`.
      1-D.   Shapes of each `SparseTensor`.
    dense: A list of `Tensor` objects with types from: `int64`, `string`.
      2-D.    Columns represented by dense `Tensor`.
    hashed_output: A `bool`.
    num_buckets: An `int` that is `>= 0`.
    hash_key: An `int`.
    out_type: A `tf.DType` from: `tf.int64, tf.string`.
    internal_type: A `tf.DType` from: `tf.int64, tf.string`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_indices, output_values, output_shape).

    output_indices: A `Tensor` of type `int64`. 2-D.  Indices of the concatenated `SparseTensor`.
    output_values: A `Tensor` of type `out_type`. 1-D.  Non-empty values of the concatenated or hashed
      `SparseTensor`.
    output_shape: A `Tensor` of type `int64`. 1-D.  Shape of the concatenated `SparseTensor`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseFeatureCrossV2", name, _ctx._post_execution_callbacks, indices,
        values, shapes, dense, "hashed_output", hashed_output, "num_buckets",
        num_buckets, "hash_key", hash_key, "out_type", out_type,
        "internal_type", internal_type)
      _result = _SparseFeatureCrossV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sparse_feature_cross_v2_eager_fallback(
            indices, values, shapes, dense, hashed_output=hashed_output,
            num_buckets=num_buckets, hash_key=hash_key, out_type=out_type,
            internal_type=internal_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sparse_feature_cross_v2, indices=indices, values=values,
                                       shapes=shapes, dense=dense,
                                       hashed_output=hashed_output,
                                       num_buckets=num_buckets,
                                       hash_key=hash_key, out_type=out_type,
                                       internal_type=internal_type, name=name)
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
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_feature_cross_v2' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_feature_cross_v2' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_feature_cross_v2' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  hash_key = _execute.make_int(hash_key, "hash_key")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SparseFeatureCrossV2", indices=indices, values=values, shapes=shapes,
                                dense=dense, hashed_output=hashed_output,
                                num_buckets=num_buckets, hash_key=hash_key,
                                out_type=out_type,
                                internal_type=internal_type, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sparse_feature_cross_v2, indices=indices, values=values,
                                   shapes=shapes, dense=dense,
                                   hashed_output=hashed_output,
                                   num_buckets=num_buckets, hash_key=hash_key,
                                   out_type=out_type,
                                   internal_type=internal_type, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "hashed_output",
            _op.get_attr("hashed_output"), "num_buckets",
            _op.get_attr("num_buckets"), "hash_key", _op.get_attr("hash_key"),
            "sparse_types", _op.get_attr("sparse_types"), "dense_types",
            _op.get_attr("dense_types"), "out_type", _op.get_attr("out_type"),
            "internal_type", _op.get_attr("internal_type"))
  _execute.record_gradient(
      "SparseFeatureCrossV2", _inputs_flat, _attrs, _result, name)
  _result = _SparseFeatureCrossV2Output._make(_result)
  return _result



def sparse_feature_cross_v2_eager_fallback(indices, values, shapes, dense, hashed_output, num_buckets, hash_key, out_type, internal_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_feature_cross_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'sparse_feature_cross_v2' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'sparse_feature_cross_v2' Op, not %r." % shapes)
  if len(shapes) != _attr_N:
    raise ValueError(
        "List argument 'shapes' to 'sparse_feature_cross_v2' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(shapes), _attr_N))
  hashed_output = _execute.make_bool(hashed_output, "hashed_output")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  hash_key = _execute.make_int(hash_key, "hash_key")
  out_type = _execute.make_type(out_type, "out_type")
  internal_type = _execute.make_type(internal_type, "internal_type")
  _attr_sparse_types, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  _attr_dense_types, dense = _execute.convert_to_mixed_eager_tensors(dense, _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int64)
  shapes = _ops.convert_n_to_tensor(shapes, _dtypes.int64)
  _inputs_flat = list(indices) + list(values) + list(shapes) + list(dense)
  _attrs = ("N", _attr_N, "hashed_output", hashed_output, "num_buckets",
  num_buckets, "hash_key", hash_key, "sparse_types", _attr_sparse_types,
  "dense_types", _attr_dense_types, "out_type", out_type, "internal_type",
  internal_type)
  _result = _execute.execute(b"SparseFeatureCrossV2", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SparseFeatureCrossV2", _inputs_flat, _attrs, _result, name)
  _result = _SparseFeatureCrossV2Output._make(_result)
  return _result

_ops.RegisterShape("SparseFeatureCrossV2")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "SparseFeatureCross"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "sparse_types"
#   }
#   input_arg {
#     name: "shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "dense"
#     type_list_attr: "dense_types"
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "hashed_output"
#     type: "bool"
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "sparse_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "dense_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "internal_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "SparseFeatureCrossV2"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "sparse_types"
#   }
#   input_arg {
#     name: "shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "dense"
#     type_list_attr: "dense_types"
#   }
#   output_arg {
#     name: "output_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output_values"
#     type_attr: "out_type"
#   }
#   output_arg {
#     name: "output_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "hashed_output"
#     type: "bool"
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "hash_key"
#     type: "int"
#   }
#   attr {
#     name: "sparse_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "dense_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "internal_type"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\337\002\n\022SparseFeatureCross\022\016\n\007indices\030\t*\001N\022\026\n\006values2\014sparse_types\022\r\n\006shapes\030\t*\001N\022\024\n\005dense2\013dense_types\032\022\n\016output_indices\030\t\032\031\n\routput_values\"\010out_type\032\020\n\014output_shape\030\t\"\n\n\001N\022\003int(\001\"\025\n\rhashed_output\022\004bool\"\024\n\013num_buckets\022\003int(\001\"$\n\014sparse_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"#\n\013dense_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"\030\n\010out_type\022\004type:\006\n\0042\002\t\007\"\035\n\rinternal_type\022\004type:\006\n\0042\002\t\007\n\362\002\n\024SparseFeatureCrossV2\022\016\n\007indices\030\t*\001N\022\026\n\006values2\014sparse_types\022\r\n\006shapes\030\t*\001N\022\024\n\005dense2\013dense_types\032\022\n\016output_indices\030\t\032\031\n\routput_values\"\010out_type\032\020\n\014output_shape\030\t\"\n\n\001N\022\003int(\001\"\025\n\rhashed_output\022\004bool\"\024\n\013num_buckets\022\003int(\001\"\017\n\010hash_key\022\003int\"$\n\014sparse_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"#\n\013dense_types\022\nlist(type)(\001:\006\n\0042\002\t\007\"\030\n\010out_type\022\004type:\006\n\0042\002\t\007\"\035\n\rinternal_type\022\004type:\006\n\0042\002\t\007")
