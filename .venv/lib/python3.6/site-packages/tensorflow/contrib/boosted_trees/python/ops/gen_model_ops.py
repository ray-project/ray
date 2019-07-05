"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_model_ops_py.cc
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
@tf_export('create_tree_ensemble_variable')
def create_tree_ensemble_variable(tree_ensemble_handle, stamp_token, tree_ensemble_config, name=None):
  r"""Creates a tree ensemble model and returns a handle to it.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    tree_ensemble_config: A `Tensor` of type `string`.
      Serialized proto of the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CreateTreeEnsembleVariable", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, tree_ensemble_config)
      return _result
    except _core._FallbackException:
      try:
        return create_tree_ensemble_variable_eager_fallback(
            tree_ensemble_handle, stamp_token, tree_ensemble_config,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_tree_ensemble_variable, tree_ensemble_handle=tree_ensemble_handle,
                                             stamp_token=stamp_token,
                                             tree_ensemble_config=tree_ensemble_config,
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
        "CreateTreeEnsembleVariable", tree_ensemble_handle=tree_ensemble_handle,
                                      stamp_token=stamp_token,
                                      tree_ensemble_config=tree_ensemble_config,
                                      name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_tree_ensemble_variable, tree_ensemble_handle=tree_ensemble_handle,
                                         stamp_token=stamp_token,
                                         tree_ensemble_config=tree_ensemble_config,
                                         name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_tree_ensemble_variable_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_tree_ensemble_variable
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_config = _ops.convert_to_tensor(tree_ensemble_config, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_config]
  _attrs = None
  _result = _execute.execute(b"CreateTreeEnsembleVariable", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateTreeEnsembleVariable")(None)


@_dispatch.add_dispatch_list
@tf_export('decision_tree_ensemble_resource_handle_op')
def decision_tree_ensemble_resource_handle_op(container="", shared_name="", name=None):
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
        "DecisionTreeEnsembleResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return decision_tree_ensemble_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decision_tree_ensemble_resource_handle_op, container=container,
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
        "DecisionTreeEnsembleResourceHandleOp", container=container,
                                                shared_name=shared_name,
                                                name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decision_tree_ensemble_resource_handle_op, container=container,
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
      "DecisionTreeEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decision_tree_ensemble_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decision_tree_ensemble_resource_handle_op
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
  _result = _execute.execute(b"DecisionTreeEnsembleResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DecisionTreeEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("DecisionTreeEnsembleResourceHandleOp")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_deserialize')
def tree_ensemble_deserialize(tree_ensemble_handle, stamp_token, tree_ensemble_config, name=None):
  r"""Deserializes a serialized tree ensemble config and replaces current tree

  ensemble.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the new value of the resource stamp.
    tree_ensemble_config: A `Tensor` of type `string`.
      Serialized proto of the ensemble.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleDeserialize", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, tree_ensemble_config)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_deserialize_eager_fallback(
            tree_ensemble_handle, stamp_token, tree_ensemble_config,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_deserialize, tree_ensemble_handle=tree_ensemble_handle,
                                         stamp_token=stamp_token,
                                         tree_ensemble_config=tree_ensemble_config,
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
        "TreeEnsembleDeserialize", tree_ensemble_handle=tree_ensemble_handle,
                                   stamp_token=stamp_token,
                                   tree_ensemble_config=tree_ensemble_config,
                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_deserialize, tree_ensemble_handle=tree_ensemble_handle,
                                     stamp_token=stamp_token,
                                     tree_ensemble_config=tree_ensemble_config,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def tree_ensemble_deserialize_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_config = _ops.convert_to_tensor(tree_ensemble_config, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_config]
  _attrs = None
  _result = _execute.execute(b"TreeEnsembleDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("TreeEnsembleDeserialize")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_is_initialized_op')
def tree_ensemble_is_initialized_op(tree_ensemble_handle, name=None):
  r"""Checks whether a tree ensemble has been initialized.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleIsInitializedOp", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_is_initialized_op_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_is_initialized_op, tree_ensemble_handle=tree_ensemble_handle,
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
        "TreeEnsembleIsInitializedOp", tree_ensemble_handle=tree_ensemble_handle,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_is_initialized_op, tree_ensemble_handle=tree_ensemble_handle,
                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeEnsembleIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tree_ensemble_is_initialized_op_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_is_initialized_op
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"TreeEnsembleIsInitializedOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TreeEnsembleIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TreeEnsembleIsInitializedOp")(None)


_tree_ensemble_serialize_outputs = ["stamp_token", "tree_ensemble_config"]
_TreeEnsembleSerializeOutput = _collections.namedtuple(
    "TreeEnsembleSerialize", _tree_ensemble_serialize_outputs)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_serialize')
def tree_ensemble_serialize(tree_ensemble_handle, name=None):
  r"""Serializes the tree ensemble to a proto.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, tree_ensemble_config).

    stamp_token: A `Tensor` of type `int64`. Stamp token of the tree ensemble resource.
    tree_ensemble_config: A `Tensor` of type `string`. Serialized proto of the ensemble.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleSerialize", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      _result = _TreeEnsembleSerializeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_serialize_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_serialize, tree_ensemble_handle=tree_ensemble_handle,
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
        "TreeEnsembleSerialize", tree_ensemble_handle=tree_ensemble_handle,
                                 name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_serialize, tree_ensemble_handle=tree_ensemble_handle,
                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeEnsembleSerialize", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleSerializeOutput._make(_result)
  return _result



def tree_ensemble_serialize_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_serialize
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"TreeEnsembleSerialize", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreeEnsembleSerialize", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleSerializeOutput._make(_result)
  return _result

_ops.RegisterShape("TreeEnsembleSerialize")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_stamp_token')
def tree_ensemble_stamp_token(tree_ensemble_handle, name=None):
  r"""Retrieves the tree ensemble resource stamp token.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`. Stamp token of the tree ensemble resource.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleStampToken", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_stamp_token_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_stamp_token, tree_ensemble_handle=tree_ensemble_handle,
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
        "TreeEnsembleStampToken", tree_ensemble_handle=tree_ensemble_handle,
                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_stamp_token, tree_ensemble_handle=tree_ensemble_handle,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeEnsembleStampToken", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tree_ensemble_stamp_token_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_stamp_token
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"TreeEnsembleStampToken", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TreeEnsembleStampToken", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TreeEnsembleStampToken")(None)


_tree_ensemble_used_handlers_outputs = ["num_used_handlers",
                                       "used_handlers_mask"]
_TreeEnsembleUsedHandlersOutput = _collections.namedtuple(
    "TreeEnsembleUsedHandlers", _tree_ensemble_used_handlers_outputs)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_used_handlers')
def tree_ensemble_used_handlers(tree_ensemble_handle, stamp_token, num_all_handlers, name=None):
  r"""Returns the mask of used handlers along with the number of non-zero elements in

  this mask. Used in feature selection.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the new value of the resource stamp.
    num_all_handlers: An `int` that is `>= 0`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (num_used_handlers, used_handlers_mask).

    num_used_handlers: A `Tensor` of type `int64`. number of feature column handlers used in the model.
    used_handlers_mask: A `Tensor` of type `bool`. A boolean vector of showing which handlers are used in the
      model.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleUsedHandlers", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, "num_all_handlers",
        num_all_handlers)
      _result = _TreeEnsembleUsedHandlersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_used_handlers_eager_fallback(
            tree_ensemble_handle, stamp_token,
            num_all_handlers=num_all_handlers, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_used_handlers, tree_ensemble_handle=tree_ensemble_handle,
                                           stamp_token=stamp_token,
                                           num_all_handlers=num_all_handlers,
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
  num_all_handlers = _execute.make_int(num_all_handlers, "num_all_handlers")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TreeEnsembleUsedHandlers", tree_ensemble_handle=tree_ensemble_handle,
                                    stamp_token=stamp_token,
                                    num_all_handlers=num_all_handlers,
                                    name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_used_handlers, tree_ensemble_handle=tree_ensemble_handle,
                                       stamp_token=stamp_token,
                                       num_all_handlers=num_all_handlers,
                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_all_handlers", _op.get_attr("num_all_handlers"))
  _execute.record_gradient(
      "TreeEnsembleUsedHandlers", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleUsedHandlersOutput._make(_result)
  return _result



def tree_ensemble_used_handlers_eager_fallback(tree_ensemble_handle, stamp_token, num_all_handlers, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_used_handlers
  """
  _ctx = ctx if ctx else _context.context()
  num_all_handlers = _execute.make_int(num_all_handlers, "num_all_handlers")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  _inputs_flat = [tree_ensemble_handle, stamp_token]
  _attrs = ("num_all_handlers", num_all_handlers)
  _result = _execute.execute(b"TreeEnsembleUsedHandlers", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TreeEnsembleUsedHandlers", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleUsedHandlersOutput._make(_result)
  return _result

_ops.RegisterShape("TreeEnsembleUsedHandlers")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "CreateTreeEnsembleVariable"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "DecisionTreeEnsembleResourceHandleOp"
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
#   name: "TreeEnsembleDeserialize"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeEnsembleIsInitializedOp"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeEnsembleSerialize"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "tree_ensemble_config"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeEnsembleStampToken"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeEnsembleUsedHandlers"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_used_handlers"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "used_handlers_mask"
#     type: DT_BOOL
#   }
#   attr {
#     name: "num_all_handlers"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nd\n\032CreateTreeEnsembleVariable\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\030\n\024tree_ensemble_config\030\007\210\001\001\nk\n$DecisionTreeEnsembleResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\na\n\027TreeEnsembleDeserialize\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\030\n\024tree_ensemble_config\030\007\210\001\001\nN\n\033TreeEnsembleIsInitializedOp\022\030\n\024tree_ensemble_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\n_\n\025TreeEnsembleSerialize\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\032\030\n\024tree_ensemble_config\030\007\210\001\001\nF\n\026TreeEnsembleStampToken\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\210\001\001\n\222\001\n\030TreeEnsembleUsedHandlers\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\032\025\n\021num_used_handlers\030\t\032\026\n\022used_handlers_mask\030\n\"\031\n\020num_all_handlers\022\003int(\001\210\001\001")
