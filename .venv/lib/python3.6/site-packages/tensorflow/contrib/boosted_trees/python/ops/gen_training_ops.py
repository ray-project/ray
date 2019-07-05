"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_training_ops_py.cc
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
@tf_export('center_tree_ensemble_bias')
def center_tree_ensemble_bias(tree_ensemble_handle, stamp_token, next_stamp_token, delta_updates, learner_config, centering_epsilon=0.01, name=None):
  r"""Centers the tree ensemble bias before adding trees based on feature splits.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the ensemble variable.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for validating operation consistency.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token to be used for the next iteration.
    delta_updates: A `Tensor` of type `float32`.
      Rank 1 Tensor containing delta updates per bias dimension.
    learner_config: A `string`.
      Config for the learner of type LearnerConfig proto.
    centering_epsilon: An optional `float`. Defaults to `0.01`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
    Scalar indicating whether more centering is needed.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CenterTreeEnsembleBias", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, next_stamp_token, delta_updates,
        "learner_config", learner_config, "centering_epsilon",
        centering_epsilon)
      return _result
    except _core._FallbackException:
      try:
        return center_tree_ensemble_bias_eager_fallback(
            tree_ensemble_handle, stamp_token, next_stamp_token,
            delta_updates, learner_config=learner_config,
            centering_epsilon=centering_epsilon, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              center_tree_ensemble_bias, tree_ensemble_handle=tree_ensemble_handle,
                                         stamp_token=stamp_token,
                                         next_stamp_token=next_stamp_token,
                                         delta_updates=delta_updates,
                                         learner_config=learner_config,
                                         centering_epsilon=centering_epsilon,
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
  learner_config = _execute.make_str(learner_config, "learner_config")
  if centering_epsilon is None:
    centering_epsilon = 0.01
  centering_epsilon = _execute.make_float(centering_epsilon, "centering_epsilon")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "CenterTreeEnsembleBias", tree_ensemble_handle=tree_ensemble_handle,
                                  stamp_token=stamp_token,
                                  next_stamp_token=next_stamp_token,
                                  delta_updates=delta_updates,
                                  learner_config=learner_config,
                                  centering_epsilon=centering_epsilon,
                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          center_tree_ensemble_bias, tree_ensemble_handle=tree_ensemble_handle,
                                     stamp_token=stamp_token,
                                     next_stamp_token=next_stamp_token,
                                     delta_updates=delta_updates,
                                     learner_config=learner_config,
                                     centering_epsilon=centering_epsilon,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("learner_config", _op.get_attr("learner_config"),
            "centering_epsilon", _op.get_attr("centering_epsilon"))
  _execute.record_gradient(
      "CenterTreeEnsembleBias", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def center_tree_ensemble_bias_eager_fallback(tree_ensemble_handle, stamp_token, next_stamp_token, delta_updates, learner_config, centering_epsilon=0.01, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function center_tree_ensemble_bias
  """
  _ctx = ctx if ctx else _context.context()
  learner_config = _execute.make_str(learner_config, "learner_config")
  if centering_epsilon is None:
    centering_epsilon = 0.01
  centering_epsilon = _execute.make_float(centering_epsilon, "centering_epsilon")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  delta_updates = _ops.convert_to_tensor(delta_updates, _dtypes.float32)
  _inputs_flat = [tree_ensemble_handle, stamp_token, next_stamp_token, delta_updates]
  _attrs = ("learner_config", learner_config, "centering_epsilon",
  centering_epsilon)
  _result = _execute.execute(b"CenterTreeEnsembleBias", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "CenterTreeEnsembleBias", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("CenterTreeEnsembleBias")(None)


@_dispatch.add_dispatch_list
@tf_export('grow_tree_ensemble')
def grow_tree_ensemble(tree_ensemble_handle, stamp_token, next_stamp_token, learning_rate, dropout_seed, max_tree_depth, weak_learner_type, partition_ids, gains, splits, learner_config, center_bias, name=None):
  r"""Grows the tree ensemble by either adding a layer to the last tree being grown

  or by starting a new tree.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the ensemble variable.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for validating operation consistency.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token to be used for the next iteration.
    learning_rate: A `Tensor` of type `float32`. Scalar learning rate.
    dropout_seed: A `Tensor` of type `int64`.
    max_tree_depth: A `Tensor` of type `int32`.
    weak_learner_type: A `Tensor` of type `int32`.
      The type of weak learner to use.
    partition_ids: A list of `Tensor` objects with type `int32`.
      List of Rank 1 Tensors containing partition Id per candidate.
    gains: A list with the same length as `partition_ids` of `Tensor` objects with type `float32`.
      List of Rank 1 Tensors containing gains per candidate.
    splits: A list with the same length as `partition_ids` of `Tensor` objects with type `string`.
      List of Rank 1 Tensors containing serialized SplitInfo protos per candidate.
    learner_config: A `string`.
      Config for the learner of type LearnerConfig proto.
    center_bias: A `bool`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GrowTreeEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, next_stamp_token, learning_rate,
        dropout_seed, max_tree_depth, weak_learner_type, partition_ids, gains,
        splits, "learner_config", learner_config, "center_bias", center_bias)
      return _result
    except _core._FallbackException:
      try:
        return grow_tree_ensemble_eager_fallback(
            tree_ensemble_handle, stamp_token, next_stamp_token,
            learning_rate, dropout_seed, max_tree_depth, weak_learner_type,
            partition_ids, gains, splits, learner_config=learner_config,
            center_bias=center_bias, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              grow_tree_ensemble, tree_ensemble_handle=tree_ensemble_handle,
                                  stamp_token=stamp_token,
                                  next_stamp_token=next_stamp_token,
                                  learning_rate=learning_rate,
                                  dropout_seed=dropout_seed,
                                  max_tree_depth=max_tree_depth,
                                  weak_learner_type=weak_learner_type,
                                  partition_ids=partition_ids, gains=gains,
                                  splits=splits,
                                  learner_config=learner_config,
                                  center_bias=center_bias, name=name)
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
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'grow_tree_ensemble' Op, not %r." % partition_ids)
  _attr_num_handlers = len(partition_ids)
  if not isinstance(gains, (list, tuple)):
    raise TypeError(
        "Expected list for 'gains' argument to "
        "'grow_tree_ensemble' Op, not %r." % gains)
  if len(gains) != _attr_num_handlers:
    raise ValueError(
        "List argument 'gains' to 'grow_tree_ensemble' Op with length %d "
        "must match length %d of argument 'partition_ids'." %
        (len(gains), _attr_num_handlers))
  if not isinstance(splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'splits' argument to "
        "'grow_tree_ensemble' Op, not %r." % splits)
  if len(splits) != _attr_num_handlers:
    raise ValueError(
        "List argument 'splits' to 'grow_tree_ensemble' Op with length %d "
        "must match length %d of argument 'partition_ids'." %
        (len(splits), _attr_num_handlers))
  learner_config = _execute.make_str(learner_config, "learner_config")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "GrowTreeEnsemble", tree_ensemble_handle=tree_ensemble_handle,
                            stamp_token=stamp_token,
                            next_stamp_token=next_stamp_token,
                            learning_rate=learning_rate,
                            dropout_seed=dropout_seed,
                            max_tree_depth=max_tree_depth,
                            weak_learner_type=weak_learner_type,
                            partition_ids=partition_ids, gains=gains,
                            splits=splits, learner_config=learner_config,
                            center_bias=center_bias, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          grow_tree_ensemble, tree_ensemble_handle=tree_ensemble_handle,
                              stamp_token=stamp_token,
                              next_stamp_token=next_stamp_token,
                              learning_rate=learning_rate,
                              dropout_seed=dropout_seed,
                              max_tree_depth=max_tree_depth,
                              weak_learner_type=weak_learner_type,
                              partition_ids=partition_ids, gains=gains,
                              splits=splits, learner_config=learner_config,
                              center_bias=center_bias, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def grow_tree_ensemble_eager_fallback(tree_ensemble_handle, stamp_token, next_stamp_token, learning_rate, dropout_seed, max_tree_depth, weak_learner_type, partition_ids, gains, splits, learner_config, center_bias, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function grow_tree_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(partition_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'partition_ids' argument to "
        "'grow_tree_ensemble' Op, not %r." % partition_ids)
  _attr_num_handlers = len(partition_ids)
  if not isinstance(gains, (list, tuple)):
    raise TypeError(
        "Expected list for 'gains' argument to "
        "'grow_tree_ensemble' Op, not %r." % gains)
  if len(gains) != _attr_num_handlers:
    raise ValueError(
        "List argument 'gains' to 'grow_tree_ensemble' Op with length %d "
        "must match length %d of argument 'partition_ids'." %
        (len(gains), _attr_num_handlers))
  if not isinstance(splits, (list, tuple)):
    raise TypeError(
        "Expected list for 'splits' argument to "
        "'grow_tree_ensemble' Op, not %r." % splits)
  if len(splits) != _attr_num_handlers:
    raise ValueError(
        "List argument 'splits' to 'grow_tree_ensemble' Op with length %d "
        "must match length %d of argument 'partition_ids'." %
        (len(splits), _attr_num_handlers))
  learner_config = _execute.make_str(learner_config, "learner_config")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  learning_rate = _ops.convert_to_tensor(learning_rate, _dtypes.float32)
  dropout_seed = _ops.convert_to_tensor(dropout_seed, _dtypes.int64)
  max_tree_depth = _ops.convert_to_tensor(max_tree_depth, _dtypes.int32)
  weak_learner_type = _ops.convert_to_tensor(weak_learner_type, _dtypes.int32)
  partition_ids = _ops.convert_n_to_tensor(partition_ids, _dtypes.int32)
  gains = _ops.convert_n_to_tensor(gains, _dtypes.float32)
  splits = _ops.convert_n_to_tensor(splits, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, next_stamp_token, learning_rate, dropout_seed, max_tree_depth, weak_learner_type] + list(partition_ids) + list(gains) + list(splits)
  _attrs = ("learner_config", learner_config, "num_handlers",
  _attr_num_handlers, "center_bias", center_bias)
  _result = _execute.execute(b"GrowTreeEnsemble", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("GrowTreeEnsemble")(None)


_tree_ensemble_stats_outputs = ["num_trees", "num_layers", "active_tree",
                               "active_layer", "attempted_trees",
                               "attempted_layers"]
_TreeEnsembleStatsOutput = _collections.namedtuple(
    "TreeEnsembleStats", _tree_ensemble_stats_outputs)


@_dispatch.add_dispatch_list
@tf_export('tree_ensemble_stats')
def tree_ensemble_stats(tree_ensemble_handle, stamp_token, name=None):
  r"""Retrieves stats related to the tree ensemble.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the ensemble variable.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for validating operation consistency.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (num_trees, num_layers, active_tree, active_layer, attempted_trees, attempted_layers).

    num_trees: A `Tensor` of type `int64`. Scalar indicating the number of finalized trees in the ensemble.
    num_layers: A `Tensor` of type `int64`. Scalar indicating the number of layers in the ensemble.
    active_tree: A `Tensor` of type `int64`. Scalar indicating the active tree being trained.
    active_layer: A `Tensor` of type `int64`. Scalar indicating the active layer being trained.
    attempted_trees: A `Tensor` of type `int64`.
    attempted_layers: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TreeEnsembleStats", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token)
      _result = _TreeEnsembleStatsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tree_ensemble_stats_eager_fallback(
            tree_ensemble_handle, stamp_token, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tree_ensemble_stats, tree_ensemble_handle=tree_ensemble_handle,
                                   stamp_token=stamp_token, name=name)
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
        "TreeEnsembleStats", tree_ensemble_handle=tree_ensemble_handle,
                             stamp_token=stamp_token, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tree_ensemble_stats, tree_ensemble_handle=tree_ensemble_handle,
                               stamp_token=stamp_token, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TreeEnsembleStats", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleStatsOutput._make(_result)
  return _result



def tree_ensemble_stats_eager_fallback(tree_ensemble_handle, stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tree_ensemble_stats
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  _inputs_flat = [tree_ensemble_handle, stamp_token]
  _attrs = None
  _result = _execute.execute(b"TreeEnsembleStats", 6, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TreeEnsembleStats", _inputs_flat, _attrs, _result, name)
  _result = _TreeEnsembleStatsOutput._make(_result)
  return _result

_ops.RegisterShape("TreeEnsembleStats")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "CenterTreeEnsembleBias"
#   input_arg {
#     name: "tree_ensemble_handle"
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
#   input_arg {
#     name: "delta_updates"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "continue_centering"
#     type: DT_BOOL
#   }
#   attr {
#     name: "learner_config"
#     type: "string"
#   }
#   attr {
#     name: "centering_epsilon"
#     type: "float"
#     default_value {
#       f: 0.01
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "GrowTreeEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
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
#   input_arg {
#     name: "learning_rate"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "dropout_seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "max_tree_depth"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "weak_learner_type"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#     number_attr: "num_handlers"
#   }
#   input_arg {
#     name: "gains"
#     type: DT_FLOAT
#     number_attr: "num_handlers"
#   }
#   input_arg {
#     name: "splits"
#     type: DT_STRING
#     number_attr: "num_handlers"
#   }
#   attr {
#     name: "learner_config"
#     type: "string"
#   }
#   attr {
#     name: "num_handlers"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "center_bias"
#     type: "bool"
#   }
#   is_stateful: true
# }
# op {
#   name: "TreeEnsembleStats"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_trees"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_layers"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "active_tree"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "active_layer"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "attempted_trees"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "attempted_layers"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\304\001\n\026CenterTreeEnsembleBias\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\022\021\n\rdelta_updates\030\001\032\026\n\022continue_centering\030\n\"\030\n\016learner_config\022\006string\"!\n\021centering_epsilon\022\005float\032\005%\n\327#<\210\001\001\n\300\002\n\020GrowTreeEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\022\021\n\rlearning_rate\030\001\022\020\n\014dropout_seed\030\t\022\022\n\016max_tree_depth\030\003\022\025\n\021weak_learner_type\030\003\022\037\n\rpartition_ids\030\003*\014num_handlers\022\027\n\005gains\030\001*\014num_handlers\022\030\n\006splits\030\007*\014num_handlers\"\030\n\016learner_config\022\006string\"\025\n\014num_handlers\022\003int(\001\"\023\n\013center_bias\022\004bool\210\001\001\n\256\001\n\021TreeEnsembleStats\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\032\r\n\tnum_trees\030\t\032\016\n\nnum_layers\030\t\032\017\n\013active_tree\030\t\032\020\n\014active_layer\030\t\032\023\n\017attempted_trees\030\t\032\024\n\020attempted_layers\030\t\210\001\001")
