"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: tensor_forest_ops.cc
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


def tensor_forest_create_tree_variable(tree_handle, tree_config, name=None):
  r"""Creates a tree resource and returns a handle to it.

  Args:
    tree_handle: A `Tensor` of type `resource`.
      Handle to the tree resource to be created.
    tree_config: A `Tensor` of type `string`.
      Serialized proto string of the boosted_trees.Tree.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestCreateTreeVariable", name,
        _ctx._post_execution_callbacks, tree_handle, tree_config)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_create_tree_variable_eager_fallback(
            tree_handle, tree_config, name=name, ctx=_ctx)
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
        "TensorForestCreateTreeVariable", tree_handle=tree_handle,
                                          tree_config=tree_config, name=name)
  return _op
  _result = None
  return _result



def tensor_forest_create_tree_variable_eager_fallback(tree_handle, tree_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_create_tree_variable
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  tree_config = _ops.convert_to_tensor(tree_config, _dtypes.string)
  _inputs_flat = [tree_handle, tree_config]
  _attrs = None
  _result = _execute.execute(b"TensorForestCreateTreeVariable", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def tensor_forest_tree_deserialize(tree_handle, tree_config, name=None):
  r"""Deserializes a proto into the tree handle

  Args:
    tree_handle: A `Tensor` of type `resource`.
      Handle to the tree resource to be restored.
    tree_config: A `Tensor` of type `string`.
      Serialied proto string of the boosted_trees.Tree proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestTreeDeserialize", name, _ctx._post_execution_callbacks,
        tree_handle, tree_config)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_deserialize_eager_fallback(
            tree_handle, tree_config, name=name, ctx=_ctx)
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
        "TensorForestTreeDeserialize", tree_handle=tree_handle,
                                       tree_config=tree_config, name=name)
  return _op
  _result = None
  return _result



def tensor_forest_tree_deserialize_eager_fallback(tree_handle, tree_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  tree_config = _ops.convert_to_tensor(tree_config, _dtypes.string)
  _inputs_flat = [tree_handle, tree_config]
  _attrs = None
  _result = _execute.execute(b"TensorForestTreeDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def tensor_forest_tree_is_initialized_op(tree_handle, name=None):
  r"""Checks whether a tree has been initialized.

  Args:
    tree_handle: A `Tensor` of type `resource`. Handle to the tree.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestTreeIsInitializedOp", name,
        _ctx._post_execution_callbacks, tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_is_initialized_op_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
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
        "TensorForestTreeIsInitializedOp", tree_handle=tree_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorForestTreeIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_forest_tree_is_initialized_op_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_is_initialized_op
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TensorForestTreeIsInitializedOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorForestTreeIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_forest_tree_predict(tree_handle, dense_features, logits_dimension, name=None):
  r"""Output the logits for the given input data

  Args:
    tree_handle: A `Tensor` of type `resource`. Handle to the tree resource.
    dense_features: A `Tensor` of type `float32`.
      Rank 2 dense features tensor.
    logits_dimension: An `int`. Scalar, dimension of the logits.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestTreePredict", name, _ctx._post_execution_callbacks,
        tree_handle, dense_features, "logits_dimension", logits_dimension)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_predict_eager_fallback(
            tree_handle, dense_features, logits_dimension=logits_dimension,
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
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorForestTreePredict", tree_handle=tree_handle,
                                   dense_features=dense_features,
                                   logits_dimension=logits_dimension,
                                   name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("logits_dimension", _op.get_attr("logits_dimension"))
  _execute.record_gradient(
      "TensorForestTreePredict", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_forest_tree_predict_eager_fallback(tree_handle, dense_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_predict
  """
  _ctx = ctx if ctx else _context.context()
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  dense_features = _ops.convert_to_tensor(dense_features, _dtypes.float32)
  _inputs_flat = [tree_handle, dense_features]
  _attrs = ("logits_dimension", logits_dimension)
  _result = _execute.execute(b"TensorForestTreePredict", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorForestTreePredict", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_forest_tree_resource_handle_op(container="", shared_name="", name=None):
  r"""Creates a handle to a TensorForestTreeResource

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
        "TensorForestTreeResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorForestTreeResourceHandleOp", container=container,
                                            shared_name=shared_name,
                                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "TensorForestTreeResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_forest_tree_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_resource_handle_op
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
  _result = _execute.execute(b"TensorForestTreeResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorForestTreeResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_forest_tree_serialize(tree_handle, name=None):
  r"""Serializes the tree handle to a proto

  Args:
    tree_handle: A `Tensor` of type `resource`.
      Handle to the tree resource to be serialized.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestTreeSerialize", name, _ctx._post_execution_callbacks,
        tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_serialize_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
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
        "TensorForestTreeSerialize", tree_handle=tree_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorForestTreeSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_forest_tree_serialize_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_serialize
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TensorForestTreeSerialize", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorForestTreeSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_forest_tree_size(tree_handle, name=None):
  r"""Get the number of nodes in a tree

  Args:
    tree_handle: A `Tensor` of type `resource`. Handle to the tree resource.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorForestTreeSize", name, _ctx._post_execution_callbacks,
        tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tensor_forest_tree_size_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
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
        "TensorForestTreeSize", tree_handle=tree_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorForestTreeSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_forest_tree_size_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_forest_tree_size
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TensorForestTreeSize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorForestTreeSize", _inputs_flat, _attrs, _result, name)
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
#   name: "TensorForestCreateTreeVariable"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "tree_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorForestTreeDeserialize"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "tree_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorForestTreeIsInitializedOp"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorForestTreePredict"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "dense_features"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "logits"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorForestTreeResourceHandleOp"
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
#   name: "TensorForestTreeSerialize"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "tree_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorForestTreeSize"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "tree_size"
#     type: DT_INT32
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nE\n\036TensorForestCreateTreeVariable\022\017\n\013tree_handle\030\024\022\017\n\013tree_config\030\007\210\001\001\nB\n\033TensorForestTreeDeserialize\022\017\n\013tree_handle\030\024\022\017\n\013tree_config\030\007\210\001\001\nI\n\037TensorForestTreeIsInitializedOp\022\017\n\013tree_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\nf\n\027TensorForestTreePredict\022\017\n\013tree_handle\030\024\022\022\n\016dense_features\030\001\032\n\n\006logits\030\001\"\027\n\020logits_dimension\022\003int\210\001\001\ng\n TensorForestTreeResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n@\n\031TensorForestTreeSerialize\022\017\n\013tree_handle\030\024\032\017\n\013tree_config\030\007\210\001\001\n9\n\024TensorForestTreeSize\022\017\n\013tree_handle\030\024\032\r\n\ttree_size\030\003\210\001\001")
