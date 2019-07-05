"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_prediction_ops_py.cc
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
@tf_export('gradient_trees_partition_examples')
def gradient_trees_partition_examples(tree_ensemble_handle, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, use_locking=False, name=None):
  r"""Splits input examples into the leaves of the tree.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      The handle to the tree ensemble.
    dense_float_features: A list of `Tensor` objects with type `float32`.
      Rank 2 Tensors containing dense float feature values.
    sparse_float_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse float indices.
    sparse_float_feature_values: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`.
      Rank 1 Tensors containing sparse float values.
    sparse_float_feature_shapes: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse float shapes.
    sparse_int_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse int indices.
    sparse_int_feature_values: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int values.
    sparse_int_feature_shapes: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int shapes.
    use_locking: An optional `bool`. Defaults to `False`.
      Whether to use locking.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
    Rank 1 Tensor containing partition ids per example.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GradientTreesPartitionExamples", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle,
        dense_float_features, sparse_float_feature_indices,
        sparse_float_feature_values, sparse_float_feature_shapes,
        sparse_int_feature_indices, sparse_int_feature_values,
        sparse_int_feature_shapes, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return gradient_trees_partition_examples_eager_fallback(
            tree_ensemble_handle, dense_float_features,
            sparse_float_feature_indices, sparse_float_feature_values,
            sparse_float_feature_shapes, sparse_int_feature_indices,
            sparse_int_feature_values, sparse_int_feature_shapes,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gradient_trees_partition_examples, tree_ensemble_handle=tree_ensemble_handle,
                                                 dense_float_features=dense_float_features,
                                                 sparse_float_feature_indices=sparse_float_feature_indices,
                                                 sparse_float_feature_values=sparse_float_feature_values,
                                                 sparse_float_feature_shapes=sparse_float_feature_shapes,
                                                 sparse_int_feature_indices=sparse_int_feature_indices,
                                                 sparse_int_feature_values=sparse_int_feature_values,
                                                 sparse_int_feature_shapes=sparse_int_feature_shapes,
                                                 use_locking=use_locking,
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
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "GradientTreesPartitionExamples", tree_ensemble_handle=tree_ensemble_handle,
                                          dense_float_features=dense_float_features,
                                          sparse_float_feature_indices=sparse_float_feature_indices,
                                          sparse_float_feature_values=sparse_float_feature_values,
                                          sparse_float_feature_shapes=sparse_float_feature_shapes,
                                          sparse_int_feature_indices=sparse_int_feature_indices,
                                          sparse_int_feature_values=sparse_int_feature_values,
                                          sparse_int_feature_shapes=sparse_int_feature_shapes,
                                          use_locking=use_locking, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gradient_trees_partition_examples, tree_ensemble_handle=tree_ensemble_handle,
                                             dense_float_features=dense_float_features,
                                             sparse_float_feature_indices=sparse_float_feature_indices,
                                             sparse_float_feature_values=sparse_float_feature_values,
                                             sparse_float_feature_shapes=sparse_float_feature_shapes,
                                             sparse_int_feature_indices=sparse_int_feature_indices,
                                             sparse_int_feature_values=sparse_int_feature_values,
                                             sparse_int_feature_shapes=sparse_int_feature_shapes,
                                             use_locking=use_locking,
                                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_dense_float_features",
            _op.get_attr("num_dense_float_features"),
            "num_sparse_float_features",
            _op.get_attr("num_sparse_float_features"),
            "num_sparse_int_features",
            _op.get_attr("num_sparse_int_features"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "GradientTreesPartitionExamples", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def gradient_trees_partition_examples_eager_fallback(tree_ensemble_handle, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gradient_trees_partition_examples
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_partition_examples' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_partition_examples' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  dense_float_features = _ops.convert_n_to_tensor(dense_float_features, _dtypes.float32)
  sparse_float_feature_indices = _ops.convert_n_to_tensor(sparse_float_feature_indices, _dtypes.int64)
  sparse_float_feature_values = _ops.convert_n_to_tensor(sparse_float_feature_values, _dtypes.float32)
  sparse_float_feature_shapes = _ops.convert_n_to_tensor(sparse_float_feature_shapes, _dtypes.int64)
  sparse_int_feature_indices = _ops.convert_n_to_tensor(sparse_int_feature_indices, _dtypes.int64)
  sparse_int_feature_values = _ops.convert_n_to_tensor(sparse_int_feature_values, _dtypes.int64)
  sparse_int_feature_shapes = _ops.convert_n_to_tensor(sparse_int_feature_shapes, _dtypes.int64)
  _inputs_flat = [tree_ensemble_handle] + list(dense_float_features) + list(sparse_float_feature_indices) + list(sparse_float_feature_values) + list(sparse_float_feature_shapes) + list(sparse_int_feature_indices) + list(sparse_int_feature_values) + list(sparse_int_feature_shapes)
  _attrs = ("num_dense_float_features", _attr_num_dense_float_features,
  "num_sparse_float_features", _attr_num_sparse_float_features,
  "num_sparse_int_features", _attr_num_sparse_int_features, "use_locking",
  use_locking)
  _result = _execute.execute(b"GradientTreesPartitionExamples", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "GradientTreesPartitionExamples", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("GradientTreesPartitionExamples")(None)


_gradient_trees_prediction_outputs = ["predictions",
                                     "drop_out_tree_indices_weights"]
_GradientTreesPredictionOutput = _collections.namedtuple(
    "GradientTreesPrediction", _gradient_trees_prediction_outputs)


@_dispatch.add_dispatch_list
@tf_export('gradient_trees_prediction')
def gradient_trees_prediction(tree_ensemble_handle, seed, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, learner_config, apply_dropout, apply_averaging, center_bias, reduce_dim, use_locking=False, name=None):
  r"""Runs multiple additive regression forests predictors on input instances

  and computes the final prediction for each class.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      The handle to the tree ensemble.
    seed: A `Tensor` of type `int64`. random seed to be used for dropout.
    dense_float_features: A list of `Tensor` objects with type `float32`.
      Rank 2 Tensors containing dense float feature values.
    sparse_float_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse float indices.
    sparse_float_feature_values: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`.
      Rank 1 Tensors containing sparse float values.
    sparse_float_feature_shapes: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse float shapes.
    sparse_int_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse int indices.
    sparse_int_feature_values: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int values.
    sparse_int_feature_shapes: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int shapes.
    learner_config: A `string`.
      Config for the learner of type LearnerConfig proto. Prediction
      ops for now uses only LearningRateDropoutDrivenConfig config from the learner.
    apply_dropout: A `bool`. whether to apply dropout during prediction.
    apply_averaging: A `bool`.
      whether averaging of tree ensembles should take place. If set
      to true, will be based on AveragingConfig from learner_config.
    center_bias: A `bool`.
    reduce_dim: A `bool`.
      whether to reduce the dimension (legacy impl) or not.
    use_locking: An optional `bool`. Defaults to `False`.
      Whether to use locking.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (predictions, drop_out_tree_indices_weights).

    predictions: A `Tensor` of type `float32`. Rank 2 Tensor containing predictions per example per class.
    drop_out_tree_indices_weights: A `Tensor` of type `float32`. Tensor of Rank 2 containing dropped trees indices
      and original weights of those trees during prediction.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GradientTreesPrediction", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, seed, dense_float_features,
        sparse_float_feature_indices, sparse_float_feature_values,
        sparse_float_feature_shapes, sparse_int_feature_indices,
        sparse_int_feature_values, sparse_int_feature_shapes,
        "learner_config", learner_config, "use_locking", use_locking,
        "apply_dropout", apply_dropout, "apply_averaging", apply_averaging,
        "center_bias", center_bias, "reduce_dim", reduce_dim)
      _result = _GradientTreesPredictionOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return gradient_trees_prediction_eager_fallback(
            tree_ensemble_handle, seed, dense_float_features,
            sparse_float_feature_indices, sparse_float_feature_values,
            sparse_float_feature_shapes, sparse_int_feature_indices,
            sparse_int_feature_values, sparse_int_feature_shapes,
            learner_config=learner_config, use_locking=use_locking,
            apply_dropout=apply_dropout, apply_averaging=apply_averaging,
            center_bias=center_bias, reduce_dim=reduce_dim, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gradient_trees_prediction, tree_ensemble_handle=tree_ensemble_handle,
                                         seed=seed,
                                         dense_float_features=dense_float_features,
                                         sparse_float_feature_indices=sparse_float_feature_indices,
                                         sparse_float_feature_values=sparse_float_feature_values,
                                         sparse_float_feature_shapes=sparse_float_feature_shapes,
                                         sparse_int_feature_indices=sparse_int_feature_indices,
                                         sparse_int_feature_values=sparse_int_feature_values,
                                         sparse_int_feature_shapes=sparse_int_feature_shapes,
                                         learner_config=learner_config,
                                         apply_dropout=apply_dropout,
                                         apply_averaging=apply_averaging,
                                         center_bias=center_bias,
                                         reduce_dim=reduce_dim,
                                         use_locking=use_locking, name=name)
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
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_prediction' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  learner_config = _execute.make_str(learner_config, "learner_config")
  apply_dropout = _execute.make_bool(apply_dropout, "apply_dropout")
  apply_averaging = _execute.make_bool(apply_averaging, "apply_averaging")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  reduce_dim = _execute.make_bool(reduce_dim, "reduce_dim")
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "GradientTreesPrediction", tree_ensemble_handle=tree_ensemble_handle,
                                   seed=seed,
                                   dense_float_features=dense_float_features,
                                   sparse_float_feature_indices=sparse_float_feature_indices,
                                   sparse_float_feature_values=sparse_float_feature_values,
                                   sparse_float_feature_shapes=sparse_float_feature_shapes,
                                   sparse_int_feature_indices=sparse_int_feature_indices,
                                   sparse_int_feature_values=sparse_int_feature_values,
                                   sparse_int_feature_shapes=sparse_int_feature_shapes,
                                   learner_config=learner_config,
                                   apply_dropout=apply_dropout,
                                   apply_averaging=apply_averaging,
                                   center_bias=center_bias,
                                   reduce_dim=reduce_dim,
                                   use_locking=use_locking, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gradient_trees_prediction, tree_ensemble_handle=tree_ensemble_handle,
                                     seed=seed,
                                     dense_float_features=dense_float_features,
                                     sparse_float_feature_indices=sparse_float_feature_indices,
                                     sparse_float_feature_values=sparse_float_feature_values,
                                     sparse_float_feature_shapes=sparse_float_feature_shapes,
                                     sparse_int_feature_indices=sparse_int_feature_indices,
                                     sparse_int_feature_values=sparse_int_feature_values,
                                     sparse_int_feature_shapes=sparse_int_feature_shapes,
                                     learner_config=learner_config,
                                     apply_dropout=apply_dropout,
                                     apply_averaging=apply_averaging,
                                     center_bias=center_bias,
                                     reduce_dim=reduce_dim,
                                     use_locking=use_locking, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("learner_config", _op.get_attr("learner_config"),
            "num_dense_float_features",
            _op.get_attr("num_dense_float_features"),
            "num_sparse_float_features",
            _op.get_attr("num_sparse_float_features"),
            "num_sparse_int_features",
            _op.get_attr("num_sparse_int_features"), "use_locking",
            _op.get_attr("use_locking"), "apply_dropout",
            _op.get_attr("apply_dropout"), "apply_averaging",
            _op.get_attr("apply_averaging"), "center_bias",
            _op.get_attr("center_bias"), "reduce_dim",
            _op.get_attr("reduce_dim"))
  _execute.record_gradient(
      "GradientTreesPrediction", _inputs_flat, _attrs, _result, name)
  _result = _GradientTreesPredictionOutput._make(_result)
  return _result



def gradient_trees_prediction_eager_fallback(tree_ensemble_handle, seed, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, learner_config, apply_dropout, apply_averaging, center_bias, reduce_dim, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gradient_trees_prediction
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_prediction' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_prediction' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_prediction' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  learner_config = _execute.make_str(learner_config, "learner_config")
  apply_dropout = _execute.make_bool(apply_dropout, "apply_dropout")
  apply_averaging = _execute.make_bool(apply_averaging, "apply_averaging")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  reduce_dim = _execute.make_bool(reduce_dim, "reduce_dim")
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  dense_float_features = _ops.convert_n_to_tensor(dense_float_features, _dtypes.float32)
  sparse_float_feature_indices = _ops.convert_n_to_tensor(sparse_float_feature_indices, _dtypes.int64)
  sparse_float_feature_values = _ops.convert_n_to_tensor(sparse_float_feature_values, _dtypes.float32)
  sparse_float_feature_shapes = _ops.convert_n_to_tensor(sparse_float_feature_shapes, _dtypes.int64)
  sparse_int_feature_indices = _ops.convert_n_to_tensor(sparse_int_feature_indices, _dtypes.int64)
  sparse_int_feature_values = _ops.convert_n_to_tensor(sparse_int_feature_values, _dtypes.int64)
  sparse_int_feature_shapes = _ops.convert_n_to_tensor(sparse_int_feature_shapes, _dtypes.int64)
  _inputs_flat = [tree_ensemble_handle, seed] + list(dense_float_features) + list(sparse_float_feature_indices) + list(sparse_float_feature_values) + list(sparse_float_feature_shapes) + list(sparse_int_feature_indices) + list(sparse_int_feature_values) + list(sparse_int_feature_shapes)
  _attrs = ("learner_config", learner_config, "num_dense_float_features",
  _attr_num_dense_float_features, "num_sparse_float_features",
  _attr_num_sparse_float_features, "num_sparse_int_features",
  _attr_num_sparse_int_features, "use_locking", use_locking, "apply_dropout",
  apply_dropout, "apply_averaging", apply_averaging, "center_bias",
  center_bias, "reduce_dim", reduce_dim)
  _result = _execute.execute(b"GradientTreesPrediction", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "GradientTreesPrediction", _inputs_flat, _attrs, _result, name)
  _result = _GradientTreesPredictionOutput._make(_result)
  return _result

_ops.RegisterShape("GradientTreesPrediction")(None)


_gradient_trees_prediction_verbose_outputs = ["predictions",
                                             "drop_out_tree_indices_weights",
                                             "leaf_index"]
_GradientTreesPredictionVerboseOutput = _collections.namedtuple(
    "GradientTreesPredictionVerbose",
    _gradient_trees_prediction_verbose_outputs)


@_dispatch.add_dispatch_list
@tf_export('gradient_trees_prediction_verbose')
def gradient_trees_prediction_verbose(tree_ensemble_handle, seed, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, learner_config, apply_dropout, apply_averaging, center_bias, reduce_dim, use_locking=False, name=None):
  r"""Runs multiple additive regression forests predictors on input instances

  and computes the final prediction for each class, and outputs a matrix of
  leaf ids per each tree in an ensemble.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      The handle to the tree ensemble.
    seed: A `Tensor` of type `int64`. random seed to be used for dropout.
    dense_float_features: A list of `Tensor` objects with type `float32`.
      Rank 2 Tensors containing dense float feature values.
    sparse_float_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse float indices.
    sparse_float_feature_values: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`.
      Rank 1 Tensors containing sparse float values.
    sparse_float_feature_shapes: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse float shapes.
    sparse_int_feature_indices: A list of `Tensor` objects with type `int64`.
      Rank 2 Tensors containing sparse int indices.
    sparse_int_feature_values: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int values.
    sparse_int_feature_shapes: A list with the same length as `sparse_int_feature_indices` of `Tensor` objects with type `int64`.
      Rank 1 Tensors containing sparse int shapes.
    learner_config: A `string`.
      Config for the learner of type LearnerConfig proto. Prediction
      ops for now uses only LearningRateDropoutDrivenConfig config from the learner.
    apply_dropout: A `bool`. whether to apply dropout during prediction.
    apply_averaging: A `bool`.
      whether averaging of tree ensembles should take place. If set
      to true, will be based on AveragingConfig from learner_config.
    center_bias: A `bool`.
    reduce_dim: A `bool`.
      whether to reduce the dimension (legacy impl) or not.
    use_locking: An optional `bool`. Defaults to `False`.
      Whether to use locking.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (predictions, drop_out_tree_indices_weights, leaf_index).

    predictions: A `Tensor` of type `float32`. Rank 2 Tensor containing predictions per example per class.
    drop_out_tree_indices_weights: A `Tensor` of type `float32`. Tensor of Rank 2 containing dropped trees indices
    leaf_index: A `Tensor` of type `int32`. tensor of rank 2 containing leaf ids for each tree where an instance ended up.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GradientTreesPredictionVerbose", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle, seed,
        dense_float_features, sparse_float_feature_indices,
        sparse_float_feature_values, sparse_float_feature_shapes,
        sparse_int_feature_indices, sparse_int_feature_values,
        sparse_int_feature_shapes, "learner_config", learner_config,
        "use_locking", use_locking, "apply_dropout", apply_dropout,
        "apply_averaging", apply_averaging, "center_bias", center_bias,
        "reduce_dim", reduce_dim)
      _result = _GradientTreesPredictionVerboseOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return gradient_trees_prediction_verbose_eager_fallback(
            tree_ensemble_handle, seed, dense_float_features,
            sparse_float_feature_indices, sparse_float_feature_values,
            sparse_float_feature_shapes, sparse_int_feature_indices,
            sparse_int_feature_values, sparse_int_feature_shapes,
            learner_config=learner_config, use_locking=use_locking,
            apply_dropout=apply_dropout, apply_averaging=apply_averaging,
            center_bias=center_bias, reduce_dim=reduce_dim, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gradient_trees_prediction_verbose, tree_ensemble_handle=tree_ensemble_handle,
                                                 seed=seed,
                                                 dense_float_features=dense_float_features,
                                                 sparse_float_feature_indices=sparse_float_feature_indices,
                                                 sparse_float_feature_values=sparse_float_feature_values,
                                                 sparse_float_feature_shapes=sparse_float_feature_shapes,
                                                 sparse_int_feature_indices=sparse_int_feature_indices,
                                                 sparse_int_feature_values=sparse_int_feature_values,
                                                 sparse_int_feature_shapes=sparse_int_feature_shapes,
                                                 learner_config=learner_config,
                                                 apply_dropout=apply_dropout,
                                                 apply_averaging=apply_averaging,
                                                 center_bias=center_bias,
                                                 reduce_dim=reduce_dim,
                                                 use_locking=use_locking,
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
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  learner_config = _execute.make_str(learner_config, "learner_config")
  apply_dropout = _execute.make_bool(apply_dropout, "apply_dropout")
  apply_averaging = _execute.make_bool(apply_averaging, "apply_averaging")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  reduce_dim = _execute.make_bool(reduce_dim, "reduce_dim")
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "GradientTreesPredictionVerbose", tree_ensemble_handle=tree_ensemble_handle,
                                          seed=seed,
                                          dense_float_features=dense_float_features,
                                          sparse_float_feature_indices=sparse_float_feature_indices,
                                          sparse_float_feature_values=sparse_float_feature_values,
                                          sparse_float_feature_shapes=sparse_float_feature_shapes,
                                          sparse_int_feature_indices=sparse_int_feature_indices,
                                          sparse_int_feature_values=sparse_int_feature_values,
                                          sparse_int_feature_shapes=sparse_int_feature_shapes,
                                          learner_config=learner_config,
                                          apply_dropout=apply_dropout,
                                          apply_averaging=apply_averaging,
                                          center_bias=center_bias,
                                          reduce_dim=reduce_dim,
                                          use_locking=use_locking, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gradient_trees_prediction_verbose, tree_ensemble_handle=tree_ensemble_handle,
                                             seed=seed,
                                             dense_float_features=dense_float_features,
                                             sparse_float_feature_indices=sparse_float_feature_indices,
                                             sparse_float_feature_values=sparse_float_feature_values,
                                             sparse_float_feature_shapes=sparse_float_feature_shapes,
                                             sparse_int_feature_indices=sparse_int_feature_indices,
                                             sparse_int_feature_values=sparse_int_feature_values,
                                             sparse_int_feature_shapes=sparse_int_feature_shapes,
                                             learner_config=learner_config,
                                             apply_dropout=apply_dropout,
                                             apply_averaging=apply_averaging,
                                             center_bias=center_bias,
                                             reduce_dim=reduce_dim,
                                             use_locking=use_locking,
                                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("learner_config", _op.get_attr("learner_config"),
            "num_dense_float_features",
            _op.get_attr("num_dense_float_features"),
            "num_sparse_float_features",
            _op.get_attr("num_sparse_float_features"),
            "num_sparse_int_features",
            _op.get_attr("num_sparse_int_features"), "use_locking",
            _op.get_attr("use_locking"), "apply_dropout",
            _op.get_attr("apply_dropout"), "apply_averaging",
            _op.get_attr("apply_averaging"), "center_bias",
            _op.get_attr("center_bias"), "reduce_dim",
            _op.get_attr("reduce_dim"))
  _execute.record_gradient(
      "GradientTreesPredictionVerbose", _inputs_flat, _attrs, _result, name)
  _result = _GradientTreesPredictionVerboseOutput._make(_result)
  return _result



def gradient_trees_prediction_verbose_eager_fallback(tree_ensemble_handle, seed, dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, sparse_int_feature_indices, sparse_int_feature_values, sparse_int_feature_shapes, learner_config, apply_dropout, apply_averaging, center_bias, reduce_dim, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gradient_trees_prediction_verbose
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % dense_float_features)
  _attr_num_dense_float_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_float_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_float_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_float_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_float_features))
  if not isinstance(sparse_int_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_indices' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_indices)
  _attr_num_sparse_int_features = len(sparse_int_feature_indices)
  if not isinstance(sparse_int_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_values' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_values)
  if len(sparse_int_feature_values) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_values' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_values), _attr_num_sparse_int_features))
  if not isinstance(sparse_int_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_int_feature_shapes' argument to "
        "'gradient_trees_prediction_verbose' Op, not %r." % sparse_int_feature_shapes)
  if len(sparse_int_feature_shapes) != _attr_num_sparse_int_features:
    raise ValueError(
        "List argument 'sparse_int_feature_shapes' to 'gradient_trees_prediction_verbose' Op with length %d "
        "must match length %d of argument 'sparse_int_feature_indices'." %
        (len(sparse_int_feature_shapes), _attr_num_sparse_int_features))
  learner_config = _execute.make_str(learner_config, "learner_config")
  apply_dropout = _execute.make_bool(apply_dropout, "apply_dropout")
  apply_averaging = _execute.make_bool(apply_averaging, "apply_averaging")
  center_bias = _execute.make_bool(center_bias, "center_bias")
  reduce_dim = _execute.make_bool(reduce_dim, "reduce_dim")
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  dense_float_features = _ops.convert_n_to_tensor(dense_float_features, _dtypes.float32)
  sparse_float_feature_indices = _ops.convert_n_to_tensor(sparse_float_feature_indices, _dtypes.int64)
  sparse_float_feature_values = _ops.convert_n_to_tensor(sparse_float_feature_values, _dtypes.float32)
  sparse_float_feature_shapes = _ops.convert_n_to_tensor(sparse_float_feature_shapes, _dtypes.int64)
  sparse_int_feature_indices = _ops.convert_n_to_tensor(sparse_int_feature_indices, _dtypes.int64)
  sparse_int_feature_values = _ops.convert_n_to_tensor(sparse_int_feature_values, _dtypes.int64)
  sparse_int_feature_shapes = _ops.convert_n_to_tensor(sparse_int_feature_shapes, _dtypes.int64)
  _inputs_flat = [tree_ensemble_handle, seed] + list(dense_float_features) + list(sparse_float_feature_indices) + list(sparse_float_feature_values) + list(sparse_float_feature_shapes) + list(sparse_int_feature_indices) + list(sparse_int_feature_values) + list(sparse_int_feature_shapes)
  _attrs = ("learner_config", learner_config, "num_dense_float_features",
  _attr_num_dense_float_features, "num_sparse_float_features",
  _attr_num_sparse_float_features, "num_sparse_int_features",
  _attr_num_sparse_int_features, "use_locking", use_locking, "apply_dropout",
  apply_dropout, "apply_averaging", apply_averaging, "center_bias",
  center_bias, "reduce_dim", reduce_dim)
  _result = _execute.execute(b"GradientTreesPredictionVerbose", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "GradientTreesPredictionVerbose", _inputs_flat, _attrs, _result, name)
  _result = _GradientTreesPredictionVerboseOutput._make(_result)
  return _result

_ops.RegisterShape("GradientTreesPredictionVerbose")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "GradientTreesPartitionExamples"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "dense_float_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_values"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   output_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   attr {
#     name: "num_dense_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_int_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "GradientTreesPrediction"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense_float_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_values"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   output_arg {
#     name: "predictions"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "drop_out_tree_indices_weights"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "learner_config"
#     type: "string"
#   }
#   attr {
#     name: "num_dense_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_int_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "apply_dropout"
#     type: "bool"
#   }
#   attr {
#     name: "apply_averaging"
#     type: "bool"
#   }
#   attr {
#     name: "center_bias"
#     type: "bool"
#   }
#   attr {
#     name: "reduce_dim"
#     type: "bool"
#   }
#   is_stateful: true
# }
# op {
#   name: "GradientTreesPredictionVerbose"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense_float_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_float_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_values"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   input_arg {
#     name: "sparse_int_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_int_features"
#   }
#   output_arg {
#     name: "predictions"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "drop_out_tree_indices_weights"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "leaf_index"
#     type: DT_INT32
#   }
#   attr {
#     name: "learner_config"
#     type: "string"
#   }
#   attr {
#     name: "num_dense_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_float_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_int_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "apply_dropout"
#     type: "bool"
#   }
#   attr {
#     name: "apply_averaging"
#     type: "bool"
#   }
#   attr {
#     name: "center_bias"
#     type: "bool"
#   }
#   attr {
#     name: "reduce_dim"
#     type: "bool"
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\344\004\n\036GradientTreesPartitionExamples\022\030\n\024tree_ensemble_handle\030\024\0222\n\024dense_float_features\030\001*\030num_dense_float_features\022;\n\034sparse_float_feature_indices\030\t*\031num_sparse_float_features\022:\n\033sparse_float_feature_values\030\001*\031num_sparse_float_features\022:\n\033sparse_float_feature_shapes\030\t*\031num_sparse_float_features\0227\n\032sparse_int_feature_indices\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_values\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_shapes\030\t*\027num_sparse_int_features\032\021\n\rpartition_ids\030\003\"!\n\030num_dense_float_features\022\003int(\001\"\"\n\031num_sparse_float_features\022\003int(\001\" \n\027num_sparse_int_features\022\003int(\001\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\373\005\n\027GradientTreesPrediction\022\030\n\024tree_ensemble_handle\030\024\022\010\n\004seed\030\t\0222\n\024dense_float_features\030\001*\030num_dense_float_features\022;\n\034sparse_float_feature_indices\030\t*\031num_sparse_float_features\022:\n\033sparse_float_feature_values\030\001*\031num_sparse_float_features\022:\n\033sparse_float_feature_shapes\030\t*\031num_sparse_float_features\0227\n\032sparse_int_feature_indices\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_values\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_shapes\030\t*\027num_sparse_int_features\032\017\n\013predictions\030\001\032!\n\035drop_out_tree_indices_weights\030\001\"\030\n\016learner_config\022\006string\"!\n\030num_dense_float_features\022\003int(\001\"\"\n\031num_sparse_float_features\022\003int(\001\" \n\027num_sparse_int_features\022\003int(\001\"\027\n\013use_locking\022\004bool\032\002(\000\"\025\n\rapply_dropout\022\004bool\"\027\n\017apply_averaging\022\004bool\"\023\n\013center_bias\022\004bool\"\022\n\nreduce_dim\022\004bool\210\001\001\n\222\006\n\036GradientTreesPredictionVerbose\022\030\n\024tree_ensemble_handle\030\024\022\010\n\004seed\030\t\0222\n\024dense_float_features\030\001*\030num_dense_float_features\022;\n\034sparse_float_feature_indices\030\t*\031num_sparse_float_features\022:\n\033sparse_float_feature_values\030\001*\031num_sparse_float_features\022:\n\033sparse_float_feature_shapes\030\t*\031num_sparse_float_features\0227\n\032sparse_int_feature_indices\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_values\030\t*\027num_sparse_int_features\0226\n\031sparse_int_feature_shapes\030\t*\027num_sparse_int_features\032\017\n\013predictions\030\001\032!\n\035drop_out_tree_indices_weights\030\001\032\016\n\nleaf_index\030\003\"\030\n\016learner_config\022\006string\"!\n\030num_dense_float_features\022\003int(\001\"\"\n\031num_sparse_float_features\022\003int(\001\" \n\027num_sparse_int_features\022\003int(\001\"\027\n\013use_locking\022\004bool\032\002(\000\"\025\n\rapply_dropout\022\004bool\"\027\n\017apply_averaging\022\004bool\"\023\n\013center_bias\022\004bool\"\022\n\nreduce_dim\022\004bool\210\001\001")
