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
@tf_export('create_tree_variable')
def create_tree_variable(tree_handle, tree_config, params, name=None):
  r"""Creates a tree  model and returns a handle to it.

  Args:
    tree_handle: A `Tensor` of type `resource`.
      handle to the tree resource to be created.
    tree_config: A `Tensor` of type `string`. Serialized proto of the tree.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CreateTreeVariable", name, _ctx._post_execution_callbacks,
        tree_handle, tree_config, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return create_tree_variable_eager_fallback(
            tree_handle, tree_config, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_tree_variable, tree_handle=tree_handle,
                                    tree_config=tree_config, params=params,
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
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "CreateTreeVariable", tree_handle=tree_handle,
                              tree_config=tree_config, params=params,
                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_tree_variable, tree_handle=tree_handle,
                                tree_config=tree_config, params=params,
                                name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_tree_variable_eager_fallback(tree_handle, tree_config, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_tree_variable
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  tree_config = _ops.convert_to_tensor(tree_config, _dtypes.string)
  _inputs_flat = [tree_handle, tree_config]
  _attrs = ("params", params)
  _result = _execute.execute(b"CreateTreeVariable", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateTreeVariable")(None)


@_dispatch.add_dispatch_list
@tf_export('decision_tree_resource_handle_op')
def decision_tree_resource_handle_op(container="", shared_name="", name=None):
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
        "DecisionTreeResourceHandleOp", name, _ctx._post_execution_callbacks,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return decision_tree_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decision_tree_resource_handle_op, container=container,
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
        "DecisionTreeResourceHandleOp", container=container,
                                        shared_name=shared_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decision_tree_resource_handle_op, container=container,
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
      "DecisionTreeResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decision_tree_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decision_tree_resource_handle_op
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
  _result = _execute.execute(b"DecisionTreeResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DecisionTreeResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("DecisionTreeResourceHandleOp")(None)


@_dispatch.add_dispatch_list
@tf_export('feature_usage_counts')
def feature_usage_counts(tree_handle, params, name=None):
  r"""Outputs the number of times each feature was used in a split.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
    `feature_counts[i]` is the number of times feature i was used
    in a split.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FeatureUsageCounts", name, _ctx._post_execution_callbacks,
        tree_handle, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return feature_usage_counts_eager_fallback(
            tree_handle, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              feature_usage_counts, tree_handle=tree_handle, params=params,
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
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FeatureUsageCounts", tree_handle=tree_handle, params=params,
                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          feature_usage_counts, tree_handle=tree_handle, params=params,
                                name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("params", _op.get_attr("params"))
  _execute.record_gradient(
      "FeatureUsageCounts", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def feature_usage_counts_eager_fallback(tree_handle, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function feature_usage_counts
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = ("params", params)
  _result = _execute.execute(b"FeatureUsageCounts", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FeatureUsageCounts", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("FeatureUsageCounts")(None)


@_dispatch.add_dispatch_list
@tf_export('traverse_tree_v4')
def traverse_tree_v4(tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_spec, params, name=None):
  r"""Outputs the leaf ids for the given input data.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    input_data: A `Tensor` of type `float32`.
      The training batch's features as a 2-d tensor; `input_data[i][j]`
      gives the j-th feature of the i-th input.
    sparse_input_indices: A `Tensor` of type `int64`.
      The indices tensor from the SparseTensor input.
    sparse_input_values: A `Tensor` of type `float32`.
      The values tensor from the SparseTensor input.
    sparse_input_shape: A `Tensor` of type `int64`.
      The shape tensor from the SparseTensor input.
    input_spec: A `string`.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`. `leaf_ids[i]` is the leaf id for input i.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TraverseTreeV4", name, _ctx._post_execution_callbacks, tree_handle,
        input_data, sparse_input_indices, sparse_input_values,
        sparse_input_shape, "input_spec", input_spec, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return traverse_tree_v4_eager_fallback(
            tree_handle, input_data, sparse_input_indices,
            sparse_input_values, sparse_input_shape, input_spec=input_spec,
            params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              traverse_tree_v4, tree_handle=tree_handle,
                                input_data=input_data,
                                sparse_input_indices=sparse_input_indices,
                                sparse_input_values=sparse_input_values,
                                sparse_input_shape=sparse_input_shape,
                                input_spec=input_spec, params=params,
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
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TraverseTreeV4", tree_handle=tree_handle, input_data=input_data,
                          sparse_input_indices=sparse_input_indices,
                          sparse_input_values=sparse_input_values,
                          sparse_input_shape=sparse_input_shape,
                          input_spec=input_spec, params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          traverse_tree_v4, tree_handle=tree_handle, input_data=input_data,
                            sparse_input_indices=sparse_input_indices,
                            sparse_input_values=sparse_input_values,
                            sparse_input_shape=sparse_input_shape,
                            input_spec=input_spec, params=params, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("input_spec", _op.get_attr("input_spec"), "params",
            _op.get_attr("params"))
  _execute.record_gradient(
      "TraverseTreeV4", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def traverse_tree_v4_eager_fallback(tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_spec, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function traverse_tree_v4
  """
  _ctx = ctx if ctx else _context.context()
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  sparse_input_indices = _ops.convert_to_tensor(sparse_input_indices, _dtypes.int64)
  sparse_input_values = _ops.convert_to_tensor(sparse_input_values, _dtypes.float32)
  sparse_input_shape = _ops.convert_to_tensor(sparse_input_shape, _dtypes.int64)
  _inputs_flat = [tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape]
  _attrs = ("input_spec", input_spec, "params", params)
  _result = _execute.execute(b"TraverseTreeV4", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TraverseTreeV4", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TraverseTreeV4")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_deserialize')
def tree_deserialize(tree_handle, tree_config, params, name=None):
  r"""Deserializes a serialized tree config and replaces current tree.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree .
    tree_config: A `Tensor` of type `string`. Serialized proto of the .
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeDeserialize", name, _ctx._post_execution_callbacks, tree_handle,
        tree_config, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return tree_deserialize_eager_fallback(
            tree_handle, tree_config, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_deserialize, tree_handle=tree_handle,
                                tree_config=tree_config, params=params,
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
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TreeDeserialize", tree_handle=tree_handle, tree_config=tree_config,
                           params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_deserialize, tree_handle=tree_handle, tree_config=tree_config,
                            params=params, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def tree_deserialize_eager_fallback(tree_handle, tree_config, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  tree_config = _ops.convert_to_tensor(tree_config, _dtypes.string)
  _inputs_flat = [tree_handle, tree_config]
  _attrs = ("params", params)
  _result = _execute.execute(b"TreeDeserialize", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("TreeDeserialize")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_is_initialized_op')
def tree_is_initialized_op(tree_handle, name=None):
  r"""Checks whether a tree has been initialized.

  Args:
    tree_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeIsInitializedOp", name, _ctx._post_execution_callbacks,
        tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tree_is_initialized_op_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_is_initialized_op, tree_handle=tree_handle, name=name)
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
        "TreeIsInitializedOp", tree_handle=tree_handle, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_is_initialized_op, tree_handle=tree_handle, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tree_is_initialized_op_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_is_initialized_op
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TreeIsInitializedOp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreeIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TreeIsInitializedOp")(None)


_tree_predictions_v4_outputs = ["predictions", "tree_paths"]
_TreePredictionsV4Output = _collections.namedtuple(
    "TreePredictionsV4", _tree_predictions_v4_outputs)


@_dispatch.add_dispatch_list
@tf_export('tree_predictions_v4')
def tree_predictions_v4(tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_spec, params, name=None):
  r"""Outputs the predictions for the given input data.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    input_data: A `Tensor` of type `float32`.
      The training batch's features as a 2-d tensor; `input_data[i][j]`
      gives the j-th feature of the i-th input.
    sparse_input_indices: A `Tensor` of type `int64`.
      The indices tensor from the SparseTensor input.
    sparse_input_values: A `Tensor` of type `float32`.
      The values tensor from the SparseTensor input.
    sparse_input_shape: A `Tensor` of type `int64`.
      The shape tensor from the SparseTensor input.
    input_spec: A `string`.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (predictions, tree_paths).

    predictions: A `Tensor` of type `float32`. `predictions[i][j]` is the probability that input i is class j.
    tree_paths: A `Tensor` of type `string`. `tree_paths[i]` is a serialized TreePath proto for example i.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreePredictionsV4", name, _ctx._post_execution_callbacks,
        tree_handle, input_data, sparse_input_indices, sparse_input_values,
        sparse_input_shape, "input_spec", input_spec, "params", params)
      _result = _TreePredictionsV4Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tree_predictions_v4_eager_fallback(
            tree_handle, input_data, sparse_input_indices,
            sparse_input_values, sparse_input_shape, input_spec=input_spec,
            params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_predictions_v4, tree_handle=tree_handle,
                                   input_data=input_data,
                                   sparse_input_indices=sparse_input_indices,
                                   sparse_input_values=sparse_input_values,
                                   sparse_input_shape=sparse_input_shape,
                                   input_spec=input_spec, params=params,
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
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TreePredictionsV4", tree_handle=tree_handle, input_data=input_data,
                             sparse_input_indices=sparse_input_indices,
                             sparse_input_values=sparse_input_values,
                             sparse_input_shape=sparse_input_shape,
                             input_spec=input_spec, params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_predictions_v4, tree_handle=tree_handle, input_data=input_data,
                               sparse_input_indices=sparse_input_indices,
                               sparse_input_values=sparse_input_values,
                               sparse_input_shape=sparse_input_shape,
                               input_spec=input_spec, params=params,
                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("input_spec", _op.get_attr("input_spec"), "params",
            _op.get_attr("params"))
  _execute.record_gradient(
      "TreePredictionsV4", _inputs_flat, _attrs, _result, name)
  _result = _TreePredictionsV4Output._make(_result)
  return _result



def tree_predictions_v4_eager_fallback(tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_spec, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_predictions_v4
  """
  _ctx = ctx if ctx else _context.context()
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  sparse_input_indices = _ops.convert_to_tensor(sparse_input_indices, _dtypes.int64)
  sparse_input_values = _ops.convert_to_tensor(sparse_input_values, _dtypes.float32)
  sparse_input_shape = _ops.convert_to_tensor(sparse_input_shape, _dtypes.int64)
  _inputs_flat = [tree_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape]
  _attrs = ("input_spec", input_spec, "params", params)
  _result = _execute.execute(b"TreePredictionsV4", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreePredictionsV4", _inputs_flat, _attrs, _result, name)
  _result = _TreePredictionsV4Output._make(_result)
  return _result

_ops.RegisterShape("TreePredictionsV4")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_serialize')
def tree_serialize(tree_handle, name=None):
  r"""Serializes the tree  to a proto.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`. Serialized proto of the tree.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeSerialize", name, _ctx._post_execution_callbacks, tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tree_serialize_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_serialize, tree_handle=tree_handle, name=name)
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
        "TreeSerialize", tree_handle=tree_handle, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_serialize, tree_handle=tree_handle, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tree_serialize_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_serialize
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TreeSerialize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreeSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TreeSerialize")(None)


@_dispatch.add_dispatch_list
@tf_export('tree_size')
def tree_size(tree_handle, name=None):
  r"""Outputs the size of the tree, including leaves.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`. Size scalar.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TreeSize",
        name, _ctx._post_execution_callbacks, tree_handle)
      return _result
    except _core._FallbackException:
      try:
        return tree_size_eager_fallback(
            tree_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_size, tree_handle=tree_handle, name=name)
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
        "TreeSize", tree_handle=tree_handle, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_size, tree_handle=tree_handle, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tree_size_eager_fallback(tree_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_size
  """
  _ctx = ctx if ctx else _context.context()
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  _inputs_flat = [tree_handle]
  _attrs = None
  _result = _execute.execute(b"TreeSize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreeSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TreeSize")(None)


@_dispatch.add_dispatch_list
@tf_export('update_model_v4')
def update_model_v4(tree_handle, leaf_ids, input_labels, input_weights, params, name=None):
  r"""Updates the given leaves for each example with the new labels.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    leaf_ids: A `Tensor` of type `int32`.
      `leaf_ids[i]` is the leaf id for input i.
    input_labels: A `Tensor` of type `float32`.
      The training batch's labels as a 1 or 2-d tensor.
      'input_labels[i][j]' gives the j-th label/target for the i-th input.
    input_weights: A `Tensor` of type `float32`.
      The training batch's weights as a 1-d tensor.
      'input_weights[i]' gives the weight for the i-th input.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UpdateModelV4", name, _ctx._post_execution_callbacks, tree_handle,
        leaf_ids, input_labels, input_weights, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return update_model_v4_eager_fallback(
            tree_handle, leaf_ids, input_labels, input_weights, params=params,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              update_model_v4, tree_handle=tree_handle, leaf_ids=leaf_ids,
                               input_labels=input_labels,
                               input_weights=input_weights, params=params,
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
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "UpdateModelV4", tree_handle=tree_handle, leaf_ids=leaf_ids,
                         input_labels=input_labels,
                         input_weights=input_weights, params=params,
                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          update_model_v4, tree_handle=tree_handle, leaf_ids=leaf_ids,
                           input_labels=input_labels,
                           input_weights=input_weights, params=params,
                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def update_model_v4_eager_fallback(tree_handle, leaf_ids, input_labels, input_weights, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function update_model_v4
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  leaf_ids = _ops.convert_to_tensor(leaf_ids, _dtypes.int32)
  input_labels = _ops.convert_to_tensor(input_labels, _dtypes.float32)
  input_weights = _ops.convert_to_tensor(input_weights, _dtypes.float32)
  _inputs_flat = [tree_handle, leaf_ids, input_labels, input_weights]
  _attrs = ("params", params)
  _result = _execute.execute(b"UpdateModelV4", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("UpdateModelV4")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "CreateTreeVariable"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "tree_config"
#     type: DT_STRING
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "DecisionTreeResourceHandleOp"
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
#   name: "FeatureUsageCounts"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "feature_counts"
#     type: DT_INT32
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TraverseTreeV4"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_input_values"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_input_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "leaf_ids"
#     type: DT_INT32
#   }
#   attr {
#     name: "input_spec"
#     type: "string"
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeDeserialize"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "tree_config"
#     type: DT_STRING
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeIsInitializedOp"
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
#   name: "TreePredictionsV4"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "input_data"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_input_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sparse_input_values"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_input_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "predictions"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "tree_paths"
#     type: DT_STRING
#   }
#   attr {
#     name: "input_spec"
#     type: "string"
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeSerialize"
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
#   name: "TreeSize"
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
# op {
#   name: "UpdateModelV4"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "leaf_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "input_labels"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_weights"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nK\n\022CreateTreeVariable\022\017\n\013tree_handle\030\024\022\017\n\013tree_config\030\007\"\020\n\006params\022\006string\210\001\001\nc\n\034DecisionTreeResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nN\n\022FeatureUsageCounts\022\017\n\013tree_handle\030\024\032\022\n\016feature_counts\030\003\"\020\n\006params\022\006string\210\001\001\n\265\001\n\016TraverseTreeV4\022\017\n\013tree_handle\030\024\022\016\n\ninput_data\030\001\022\030\n\024sparse_input_indices\030\t\022\027\n\023sparse_input_values\030\001\022\026\n\022sparse_input_shape\030\t\032\014\n\010leaf_ids\030\003\"\024\n\ninput_spec\022\006string\"\020\n\006params\022\006string\210\001\001\nH\n\017TreeDeserialize\022\017\n\013tree_handle\030\024\022\017\n\013tree_config\030\007\"\020\n\006params\022\006string\210\001\001\n=\n\023TreeIsInitializedOp\022\017\n\013tree_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\n\313\001\n\021TreePredictionsV4\022\017\n\013tree_handle\030\024\022\016\n\ninput_data\030\001\022\030\n\024sparse_input_indices\030\t\022\027\n\023sparse_input_values\030\001\022\026\n\022sparse_input_shape\030\t\032\017\n\013predictions\030\001\032\016\n\ntree_paths\030\007\"\024\n\ninput_spec\022\006string\"\020\n\006params\022\006string\210\001\001\n4\n\rTreeSerialize\022\017\n\013tree_handle\030\024\032\017\n\013tree_config\030\007\210\001\001\n-\n\010TreeSize\022\017\n\013tree_handle\030\024\032\r\n\ttree_size\030\003\210\001\001\nh\n\rUpdateModelV4\022\017\n\013tree_handle\030\024\022\014\n\010leaf_ids\030\003\022\020\n\014input_labels\030\001\022\021\n\rinput_weights\030\001\"\020\n\006params\022\006string\210\001\001")
