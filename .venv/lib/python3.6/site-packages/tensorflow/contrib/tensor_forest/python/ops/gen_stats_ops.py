"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_stats_ops_py.cc
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
@tf_export('create_fertile_stats_variable')
def create_fertile_stats_variable(stats_handle, stats_config, params, name=None):
  r"""Creates a stats model and returns a handle to it.

  Args:
    stats_handle: A `Tensor` of type `resource`.
      handle to the stats resource to be created.
    stats_config: A `Tensor` of type `string`. Serialized proto of the stats.
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
        "CreateFertileStatsVariable", name, _ctx._post_execution_callbacks,
        stats_handle, stats_config, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return create_fertile_stats_variable_eager_fallback(
            stats_handle, stats_config, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_fertile_stats_variable, stats_handle=stats_handle,
                                             stats_config=stats_config,
                                             params=params, name=name)
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
        "CreateFertileStatsVariable", stats_handle=stats_handle,
                                      stats_config=stats_config,
                                      params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_fertile_stats_variable, stats_handle=stats_handle,
                                         stats_config=stats_config,
                                         params=params, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_fertile_stats_variable_eager_fallback(stats_handle, stats_config, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_fertile_stats_variable
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  stats_config = _ops.convert_to_tensor(stats_config, _dtypes.string)
  _inputs_flat = [stats_handle, stats_config]
  _attrs = ("params", params)
  _result = _execute.execute(b"CreateFertileStatsVariable", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateFertileStatsVariable")(None)


@_dispatch.add_dispatch_list
@tf_export('fertile_stats_deserialize')
def fertile_stats_deserialize(stats_handle, stats_config, params, name=None):
  r"""Deserializes a serialized stats config and replaces current stats.

  Args:
    stats_handle: A `Tensor` of type `resource`. The handle to the stats.
    stats_config: A `Tensor` of type `string`. Serialized proto of the stats.
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
        "FertileStatsDeserialize", name, _ctx._post_execution_callbacks,
        stats_handle, stats_config, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return fertile_stats_deserialize_eager_fallback(
            stats_handle, stats_config, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fertile_stats_deserialize, stats_handle=stats_handle,
                                         stats_config=stats_config,
                                         params=params, name=name)
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
        "FertileStatsDeserialize", stats_handle=stats_handle,
                                   stats_config=stats_config, params=params,
                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fertile_stats_deserialize, stats_handle=stats_handle,
                                     stats_config=stats_config, params=params,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def fertile_stats_deserialize_eager_fallback(stats_handle, stats_config, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fertile_stats_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  stats_config = _ops.convert_to_tensor(stats_config, _dtypes.string)
  _inputs_flat = [stats_handle, stats_config]
  _attrs = ("params", params)
  _result = _execute.execute(b"FertileStatsDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("FertileStatsDeserialize")(None)


@_dispatch.add_dispatch_list
@tf_export('fertile_stats_is_initialized_op')
def fertile_stats_is_initialized_op(stats_handle, name=None):
  r"""Checks whether a stats has been initialized.

  Args:
    stats_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FertileStatsIsInitializedOp", name, _ctx._post_execution_callbacks,
        stats_handle)
      return _result
    except _core._FallbackException:
      try:
        return fertile_stats_is_initialized_op_eager_fallback(
            stats_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fertile_stats_is_initialized_op, stats_handle=stats_handle,
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
        "FertileStatsIsInitializedOp", stats_handle=stats_handle, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fertile_stats_is_initialized_op, stats_handle=stats_handle,
                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "FertileStatsIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fertile_stats_is_initialized_op_eager_fallback(stats_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fertile_stats_is_initialized_op
  """
  _ctx = ctx if ctx else _context.context()
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  _inputs_flat = [stats_handle]
  _attrs = None
  _result = _execute.execute(b"FertileStatsIsInitializedOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FertileStatsIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("FertileStatsIsInitializedOp")(None)


@_dispatch.add_dispatch_list
@tf_export('fertile_stats_resource_handle_op')
def fertile_stats_resource_handle_op(container="", shared_name="", name=None):
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
        "FertileStatsResourceHandleOp", name, _ctx._post_execution_callbacks,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return fertile_stats_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fertile_stats_resource_handle_op, container=container,
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
        "FertileStatsResourceHandleOp", container=container,
                                        shared_name=shared_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fertile_stats_resource_handle_op, container=container,
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
      "FertileStatsResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fertile_stats_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fertile_stats_resource_handle_op
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
  _result = _execute.execute(b"FertileStatsResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FertileStatsResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("FertileStatsResourceHandleOp")(None)


@_dispatch.add_dispatch_list
@tf_export('fertile_stats_serialize')
def fertile_stats_serialize(stats_handle, params, name=None):
  r"""Serializes the stats to a proto.

  Args:
    stats_handle: A `Tensor` of type `resource`. The handle to the stats.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`. Serialized proto of the stats.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FertileStatsSerialize", name, _ctx._post_execution_callbacks,
        stats_handle, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return fertile_stats_serialize_eager_fallback(
            stats_handle, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fertile_stats_serialize, stats_handle=stats_handle,
                                       params=params, name=name)
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
        "FertileStatsSerialize", stats_handle=stats_handle, params=params,
                                 name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fertile_stats_serialize, stats_handle=stats_handle, params=params,
                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("params", _op.get_attr("params"))
  _execute.record_gradient(
      "FertileStatsSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fertile_stats_serialize_eager_fallback(stats_handle, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fertile_stats_serialize
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  _inputs_flat = [stats_handle]
  _attrs = ("params", params)
  _result = _execute.execute(b"FertileStatsSerialize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FertileStatsSerialize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("FertileStatsSerialize")(None)


@_dispatch.add_dispatch_list
@tf_export('finalize_tree')
def finalize_tree(tree_handle, stats_handle, params, name=None):
  r"""Puts the Leaf models inside the tree into their final form.

  If drop_final_class is true, the per-class probability prediction of the
  last class is not stored in the leaf models.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    stats_handle: A `Tensor` of type `resource`. The handle to the stats.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FinalizeTree",
        name, _ctx._post_execution_callbacks, tree_handle, stats_handle,
        "params", params)
      return _result
    except _core._FallbackException:
      try:
        return finalize_tree_eager_fallback(
            tree_handle, stats_handle, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              finalize_tree, tree_handle=tree_handle,
                             stats_handle=stats_handle, params=params,
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
        "FinalizeTree", tree_handle=tree_handle, stats_handle=stats_handle,
                        params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          finalize_tree, tree_handle=tree_handle, stats_handle=stats_handle,
                         params=params, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def finalize_tree_eager_fallback(tree_handle, stats_handle, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function finalize_tree
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  _inputs_flat = [tree_handle, stats_handle]
  _attrs = ("params", params)
  _result = _execute.execute(b"FinalizeTree", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("FinalizeTree")(None)


@_dispatch.add_dispatch_list
@tf_export('grow_tree_v4')
def grow_tree_v4(tree_handle, stats_handle, finished_nodes, params, name=None):
  r"""Grows the tree for finished nodes and allocates waiting nodes.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    stats_handle: A `Tensor` of type `resource`. The handle to the stats.
    finished_nodes: A `Tensor` of type `int32`.
      A 1-d Tensor of finished node ids from ProcessInput.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GrowTreeV4",
        name, _ctx._post_execution_callbacks, tree_handle, stats_handle,
        finished_nodes, "params", params)
      return _result
    except _core._FallbackException:
      try:
        return grow_tree_v4_eager_fallback(
            tree_handle, stats_handle, finished_nodes, params=params,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              grow_tree_v4, tree_handle=tree_handle,
                            stats_handle=stats_handle,
                            finished_nodes=finished_nodes, params=params,
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
        "GrowTreeV4", tree_handle=tree_handle, stats_handle=stats_handle,
                      finished_nodes=finished_nodes, params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          grow_tree_v4, tree_handle=tree_handle, stats_handle=stats_handle,
                        finished_nodes=finished_nodes, params=params,
                        name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def grow_tree_v4_eager_fallback(tree_handle, stats_handle, finished_nodes, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function grow_tree_v4
  """
  _ctx = ctx if ctx else _context.context()
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  finished_nodes = _ops.convert_to_tensor(finished_nodes, _dtypes.int32)
  _inputs_flat = [tree_handle, stats_handle, finished_nodes]
  _attrs = ("params", params)
  _result = _execute.execute(b"GrowTreeV4", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("GrowTreeV4")(None)


@_dispatch.add_dispatch_list
@tf_export('process_input_v4')
def process_input_v4(tree_handle, stats_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_labels, input_weights, leaf_ids, random_seed, input_spec, params, name=None):
  r"""Add labels to stats after traversing the tree for each example.

  Outputs node ids that are finished.

  Args:
    tree_handle: A `Tensor` of type `resource`. The handle to the tree.
    stats_handle: A `Tensor` of type `resource`. The handle to the stats.
    input_data: A `Tensor` of type `float32`.
      The training batch's features as a 2-d tensor; `input_data[i][j]`
      gives the j-th feature of the i-th input.
    sparse_input_indices: A `Tensor` of type `int64`.
      The indices tensor from the SparseTensor input.
    sparse_input_values: A `Tensor` of type `float32`.
      The values tensor from the SparseTensor input.
    sparse_input_shape: A `Tensor` of type `int64`.
      The shape tensor from the SparseTensor input.
    input_labels: A `Tensor` of type `float32`.
      The training batch's labels as a 1 or 2-d tensor.
      'input_labels[i][j]' gives the j-th label/target for the i-th input.
    input_weights: A `Tensor` of type `float32`.
      The training batch's weights as a 1-d tensor.
      'input_weights[i]' gives the weight for the i-th input.
    leaf_ids: A `Tensor` of type `int32`.
      `leaf_ids[i]` is the leaf id for input i.
    random_seed: An `int`.
    input_spec: A `string`.
    params: A `string`. A serialized TensorForestParams proto.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
    A 1-d tensor of node ids that have finished and are ready to
    grow.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ProcessInputV4", name, _ctx._post_execution_callbacks, tree_handle,
        stats_handle, input_data, sparse_input_indices, sparse_input_values,
        sparse_input_shape, input_labels, input_weights, leaf_ids,
        "random_seed", random_seed, "input_spec", input_spec, "params",
        params)
      return _result
    except _core._FallbackException:
      try:
        return process_input_v4_eager_fallback(
            tree_handle, stats_handle, input_data, sparse_input_indices,
            sparse_input_values, sparse_input_shape, input_labels,
            input_weights, leaf_ids, random_seed=random_seed,
            input_spec=input_spec, params=params, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              process_input_v4, tree_handle=tree_handle,
                                stats_handle=stats_handle,
                                input_data=input_data,
                                sparse_input_indices=sparse_input_indices,
                                sparse_input_values=sparse_input_values,
                                sparse_input_shape=sparse_input_shape,
                                input_labels=input_labels,
                                input_weights=input_weights,
                                leaf_ids=leaf_ids, random_seed=random_seed,
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
  random_seed = _execute.make_int(random_seed, "random_seed")
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ProcessInputV4", tree_handle=tree_handle, stats_handle=stats_handle,
                          input_data=input_data,
                          sparse_input_indices=sparse_input_indices,
                          sparse_input_values=sparse_input_values,
                          sparse_input_shape=sparse_input_shape,
                          input_labels=input_labels,
                          input_weights=input_weights, leaf_ids=leaf_ids,
                          random_seed=random_seed, input_spec=input_spec,
                          params=params, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          process_input_v4, tree_handle=tree_handle,
                            stats_handle=stats_handle, input_data=input_data,
                            sparse_input_indices=sparse_input_indices,
                            sparse_input_values=sparse_input_values,
                            sparse_input_shape=sparse_input_shape,
                            input_labels=input_labels,
                            input_weights=input_weights, leaf_ids=leaf_ids,
                            random_seed=random_seed, input_spec=input_spec,
                            params=params, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("random_seed", _op.get_attr("random_seed"), "input_spec",
            _op.get_attr("input_spec"), "params", _op.get_attr("params"))
  _execute.record_gradient(
      "ProcessInputV4", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def process_input_v4_eager_fallback(tree_handle, stats_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_labels, input_weights, leaf_ids, random_seed, input_spec, params, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function process_input_v4
  """
  _ctx = ctx if ctx else _context.context()
  random_seed = _execute.make_int(random_seed, "random_seed")
  input_spec = _execute.make_str(input_spec, "input_spec")
  params = _execute.make_str(params, "params")
  tree_handle = _ops.convert_to_tensor(tree_handle, _dtypes.resource)
  stats_handle = _ops.convert_to_tensor(stats_handle, _dtypes.resource)
  input_data = _ops.convert_to_tensor(input_data, _dtypes.float32)
  sparse_input_indices = _ops.convert_to_tensor(sparse_input_indices, _dtypes.int64)
  sparse_input_values = _ops.convert_to_tensor(sparse_input_values, _dtypes.float32)
  sparse_input_shape = _ops.convert_to_tensor(sparse_input_shape, _dtypes.int64)
  input_labels = _ops.convert_to_tensor(input_labels, _dtypes.float32)
  input_weights = _ops.convert_to_tensor(input_weights, _dtypes.float32)
  leaf_ids = _ops.convert_to_tensor(leaf_ids, _dtypes.int32)
  _inputs_flat = [tree_handle, stats_handle, input_data, sparse_input_indices, sparse_input_values, sparse_input_shape, input_labels, input_weights, leaf_ids]
  _attrs = ("random_seed", random_seed, "input_spec", input_spec, "params",
  params)
  _result = _execute.execute(b"ProcessInputV4", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ProcessInputV4", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ProcessInputV4")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "CreateFertileStatsVariable"
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stats_config"
#     type: DT_STRING
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "FertileStatsDeserialize"
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stats_config"
#     type: DT_STRING
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "FertileStatsIsInitializedOp"
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "FertileStatsResourceHandleOp"
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
#   name: "FertileStatsSerialize"
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stats_config"
#     type: DT_STRING
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "FinalizeTree"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "GrowTreeV4"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stats_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "finished_nodes"
#     type: DT_INT32
#   }
#   attr {
#     name: "params"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "ProcessInputV4"
#   input_arg {
#     name: "tree_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stats_handle"
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
#   input_arg {
#     name: "input_labels"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "input_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "leaf_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "finished_nodes"
#     type: DT_INT32
#   }
#   attr {
#     name: "random_seed"
#     type: "int"
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
_op_def_lib = _InitOpDefLibrary(b"\nU\n\032CreateFertileStatsVariable\022\020\n\014stats_handle\030\024\022\020\n\014stats_config\030\007\"\020\n\006params\022\006string\210\001\001\nR\n\027FertileStatsDeserialize\022\020\n\014stats_handle\030\024\022\020\n\014stats_config\030\007\"\020\n\006params\022\006string\210\001\001\nF\n\033FertileStatsIsInitializedOp\022\020\n\014stats_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\nc\n\034FertileStatsResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nP\n\025FertileStatsSerialize\022\020\n\014stats_handle\030\024\032\020\n\014stats_config\030\007\"\020\n\006params\022\006string\210\001\001\nF\n\014FinalizeTree\022\017\n\013tree_handle\030\024\022\020\n\014stats_handle\030\024\"\020\n\006params\022\006string\210\001\001\nX\n\nGrowTreeV4\022\017\n\013tree_handle\030\024\022\020\n\014stats_handle\030\024\022\022\n\016finished_nodes\030\003\"\020\n\006params\022\006string\210\001\001\n\224\002\n\016ProcessInputV4\022\017\n\013tree_handle\030\024\022\020\n\014stats_handle\030\024\022\016\n\ninput_data\030\001\022\030\n\024sparse_input_indices\030\t\022\027\n\023sparse_input_values\030\001\022\026\n\022sparse_input_shape\030\t\022\020\n\014input_labels\030\001\022\021\n\rinput_weights\030\001\022\014\n\010leaf_ids\030\003\032\022\n\016finished_nodes\030\003\"\022\n\013random_seed\022\003int\"\024\n\ninput_spec\022\006string\"\020\n\006params\022\006string\210\001\001")
