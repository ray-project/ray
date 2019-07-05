"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_split_handler_ops_py.cc
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


_build_categorical_equality_splits_outputs = ["output_partition_ids", "gains",
                                             "split_infos"]
_BuildCategoricalEqualitySplitsOutput = _collections.namedtuple(
    "BuildCategoricalEqualitySplits",
    _build_categorical_equality_splits_outputs)


@_dispatch.add_dispatch_list
@tf_export('build_categorical_equality_splits')
def build_categorical_equality_splits(num_minibatches, partition_ids, feature_ids, gradients, hessians, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type, name=None):
  r"""Find the split that has the best gain for the accumulated stats.

  Args:
    num_minibatches: A `Tensor` of type `int64`.
      A scalar, the number of times per example gradients & hessians
      were accumulated. The stats are divided by this to get per example stats.
    partition_ids: A `Tensor` of type `int32`.
      A rank 1 tensor of partition IDs.
    feature_ids: A `Tensor` of type `int64`.
      A rank 2 tensor of feature IDs and dimensions.
    gradients: A `Tensor` of type `float32`. A rank 1 tensor of gradients.
    hessians: A `Tensor` of type `float32`. A rank 1 tensor of hessians.
    class_id: A `Tensor` of type `int32`.
      A scalar, the class id for which we're building the splits.
    feature_column_group_id: A `Tensor` of type `int32`.
      A scalar, the index of the feature we are spiltting on.
    bias_feature_id: A `Tensor` of type `int64`.
    l1_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l1 regularization term.
    l2_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l2 regularization term.
    tree_complexity_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the tree complexity
      regularization term.
    min_node_weight: A `Tensor` of type `float32`.
      A scalar, minimum sum of example hessian needed in a child.
      If a split results in a leaf node with a smaller value, the split will not
      be considered.
    multiclass_strategy: A `Tensor` of type `int32`.
      A scalar, specifying the multiclass handling strategy.
      See LearnerConfig.MultiClassStrategy for valid values.
    weak_learner_type: A `Tensor` of type `int32`.
      A scalar, specifying the weak learner type to use.
      See LearnerConfig.WeakLearnerType for valid values.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_partition_ids, gains, split_infos).

    output_partition_ids: A `Tensor` of type `int32`. A rank 1 tensor, the partition IDs that we created splits
      for.
    gains: A `Tensor` of type `float32`. A rank 1 tensor, for the computed gain for the created splits.
    split_infos: A `Tensor` of type `string`. A rank 1 tensor of serialized protos which contains the
      `SplitInfo`s.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BuildCategoricalEqualitySplits", name,
        _ctx._post_execution_callbacks, num_minibatches, partition_ids,
        feature_ids, gradients, hessians, class_id, feature_column_group_id,
        bias_feature_id, l1_regularization, l2_regularization,
        tree_complexity_regularization, min_node_weight, multiclass_strategy,
        weak_learner_type)
      _result = _BuildCategoricalEqualitySplitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return build_categorical_equality_splits_eager_fallback(
            num_minibatches, partition_ids, feature_ids, gradients, hessians,
            class_id, feature_column_group_id, bias_feature_id,
            l1_regularization, l2_regularization,
            tree_complexity_regularization, min_node_weight,
            multiclass_strategy, weak_learner_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              build_categorical_equality_splits, num_minibatches=num_minibatches,
                                                 partition_ids=partition_ids,
                                                 feature_ids=feature_ids,
                                                 gradients=gradients,
                                                 hessians=hessians,
                                                 class_id=class_id,
                                                 feature_column_group_id=feature_column_group_id,
                                                 bias_feature_id=bias_feature_id,
                                                 l1_regularization=l1_regularization,
                                                 l2_regularization=l2_regularization,
                                                 tree_complexity_regularization=tree_complexity_regularization,
                                                 min_node_weight=min_node_weight,
                                                 multiclass_strategy=multiclass_strategy,
                                                 weak_learner_type=weak_learner_type,
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
        "BuildCategoricalEqualitySplits", num_minibatches=num_minibatches,
                                          partition_ids=partition_ids,
                                          feature_ids=feature_ids,
                                          gradients=gradients,
                                          hessians=hessians,
                                          class_id=class_id,
                                          feature_column_group_id=feature_column_group_id,
                                          bias_feature_id=bias_feature_id,
                                          l1_regularization=l1_regularization,
                                          l2_regularization=l2_regularization,
                                          tree_complexity_regularization=tree_complexity_regularization,
                                          min_node_weight=min_node_weight,
                                          multiclass_strategy=multiclass_strategy,
                                          weak_learner_type=weak_learner_type,
                                          name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          build_categorical_equality_splits, num_minibatches=num_minibatches,
                                             partition_ids=partition_ids,
                                             feature_ids=feature_ids,
                                             gradients=gradients,
                                             hessians=hessians,
                                             class_id=class_id,
                                             feature_column_group_id=feature_column_group_id,
                                             bias_feature_id=bias_feature_id,
                                             l1_regularization=l1_regularization,
                                             l2_regularization=l2_regularization,
                                             tree_complexity_regularization=tree_complexity_regularization,
                                             min_node_weight=min_node_weight,
                                             multiclass_strategy=multiclass_strategy,
                                             weak_learner_type=weak_learner_type,
                                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BuildCategoricalEqualitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildCategoricalEqualitySplitsOutput._make(_result)
  return _result



def build_categorical_equality_splits_eager_fallback(num_minibatches, partition_ids, feature_ids, gradients, hessians, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function build_categorical_equality_splits
  """
  _ctx = ctx if ctx else _context.context()
  num_minibatches = _ops.convert_to_tensor(num_minibatches, _dtypes.int64)
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  class_id = _ops.convert_to_tensor(class_id, _dtypes.int32)
  feature_column_group_id = _ops.convert_to_tensor(feature_column_group_id, _dtypes.int32)
  bias_feature_id = _ops.convert_to_tensor(bias_feature_id, _dtypes.int64)
  l1_regularization = _ops.convert_to_tensor(l1_regularization, _dtypes.float32)
  l2_regularization = _ops.convert_to_tensor(l2_regularization, _dtypes.float32)
  tree_complexity_regularization = _ops.convert_to_tensor(tree_complexity_regularization, _dtypes.float32)
  min_node_weight = _ops.convert_to_tensor(min_node_weight, _dtypes.float32)
  multiclass_strategy = _ops.convert_to_tensor(multiclass_strategy, _dtypes.int32)
  weak_learner_type = _ops.convert_to_tensor(weak_learner_type, _dtypes.int32)
  _inputs_flat = [num_minibatches, partition_ids, feature_ids, gradients, hessians, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type]
  _attrs = None
  _result = _execute.execute(b"BuildCategoricalEqualitySplits", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BuildCategoricalEqualitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildCategoricalEqualitySplitsOutput._make(_result)
  return _result

_ops.RegisterShape("BuildCategoricalEqualitySplits")(None)


_build_dense_inequality_splits_outputs = ["output_partition_ids", "gains",
                                         "split_infos"]
_BuildDenseInequalitySplitsOutput = _collections.namedtuple(
    "BuildDenseInequalitySplits", _build_dense_inequality_splits_outputs)


@_dispatch.add_dispatch_list
@tf_export('build_dense_inequality_splits')
def build_dense_inequality_splits(num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type, name=None):
  r"""Find the split that has the best gain for the accumulated stats.

  Args:
    num_minibatches: A `Tensor` of type `int64`.
      A scalar, the number of times per example gradients & hessians
      were accumulated. The stats are divided by this to get per example stats.
    partition_ids: A `Tensor` of type `int32`.
      A rank 1 tensor of partition IDs.
    bucket_ids: A `Tensor` of type `int64`.
      A rank 2 tensor of buckets IDs and dimensions.
    gradients: A `Tensor` of type `float32`. A rank 1 tensor of gradients.
    hessians: A `Tensor` of type `float32`. A rank 1 tensor of hessians.
    bucket_boundaries: A `Tensor` of type `float32`.
      A rank 1 tensor, thresholds that were used for bucketization.
    class_id: A `Tensor` of type `int32`.
      A scalar, the class id for which we're building the splits.
    feature_column_group_id: A `Tensor` of type `int32`.
      A scalar, the index of the feature we are spiltting on.
    l1_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l1 regularization term.
    l2_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l2 regularization term.
    tree_complexity_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the tree complexity
      regularization term.
    min_node_weight: A `Tensor` of type `float32`.
      A scalar, minimum sum of example hessian needed in a child.
      If a split results in a leaf node with a smaller value, the split will not
      be considered.
    multiclass_strategy: A `Tensor` of type `int32`.
      A scalar, specifying the multiclass handling strategy.
      See LearnerConfig.MultiClassStrategy for valid values.
    weak_learner_type: A `Tensor` of type `int32`.
      A scalar, specifying the weak learner type to use.
      See LearnerConfig.WeakLearnerType for valid values.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_partition_ids, gains, split_infos).

    output_partition_ids: A `Tensor` of type `int32`. A rank 1 tensor, the partition IDs that we created splits
      for.
    gains: A `Tensor` of type `float32`. A rank 1 tensor, for the computed gain for the created splits.
    split_infos: A `Tensor` of type `string`. A rank 1 tensor of serialized protos which contains the
      `SplitInfo`s.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BuildDenseInequalitySplits", name, _ctx._post_execution_callbacks,
        num_minibatches, partition_ids, bucket_ids, gradients, hessians,
        bucket_boundaries, class_id, feature_column_group_id,
        l1_regularization, l2_regularization, tree_complexity_regularization,
        min_node_weight, multiclass_strategy, weak_learner_type)
      _result = _BuildDenseInequalitySplitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return build_dense_inequality_splits_eager_fallback(
            num_minibatches, partition_ids, bucket_ids, gradients, hessians,
            bucket_boundaries, class_id, feature_column_group_id,
            l1_regularization, l2_regularization,
            tree_complexity_regularization, min_node_weight,
            multiclass_strategy, weak_learner_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              build_dense_inequality_splits, num_minibatches=num_minibatches,
                                             partition_ids=partition_ids,
                                             bucket_ids=bucket_ids,
                                             gradients=gradients,
                                             hessians=hessians,
                                             bucket_boundaries=bucket_boundaries,
                                             class_id=class_id,
                                             feature_column_group_id=feature_column_group_id,
                                             l1_regularization=l1_regularization,
                                             l2_regularization=l2_regularization,
                                             tree_complexity_regularization=tree_complexity_regularization,
                                             min_node_weight=min_node_weight,
                                             multiclass_strategy=multiclass_strategy,
                                             weak_learner_type=weak_learner_type,
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
        "BuildDenseInequalitySplits", num_minibatches=num_minibatches,
                                      partition_ids=partition_ids,
                                      bucket_ids=bucket_ids,
                                      gradients=gradients, hessians=hessians,
                                      bucket_boundaries=bucket_boundaries,
                                      class_id=class_id,
                                      feature_column_group_id=feature_column_group_id,
                                      l1_regularization=l1_regularization,
                                      l2_regularization=l2_regularization,
                                      tree_complexity_regularization=tree_complexity_regularization,
                                      min_node_weight=min_node_weight,
                                      multiclass_strategy=multiclass_strategy,
                                      weak_learner_type=weak_learner_type,
                                      name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          build_dense_inequality_splits, num_minibatches=num_minibatches,
                                         partition_ids=partition_ids,
                                         bucket_ids=bucket_ids,
                                         gradients=gradients,
                                         hessians=hessians,
                                         bucket_boundaries=bucket_boundaries,
                                         class_id=class_id,
                                         feature_column_group_id=feature_column_group_id,
                                         l1_regularization=l1_regularization,
                                         l2_regularization=l2_regularization,
                                         tree_complexity_regularization=tree_complexity_regularization,
                                         min_node_weight=min_node_weight,
                                         multiclass_strategy=multiclass_strategy,
                                         weak_learner_type=weak_learner_type,
                                         name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BuildDenseInequalitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildDenseInequalitySplitsOutput._make(_result)
  return _result



def build_dense_inequality_splits_eager_fallback(num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function build_dense_inequality_splits
  """
  _ctx = ctx if ctx else _context.context()
  num_minibatches = _ops.convert_to_tensor(num_minibatches, _dtypes.int64)
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  bucket_ids = _ops.convert_to_tensor(bucket_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  bucket_boundaries = _ops.convert_to_tensor(bucket_boundaries, _dtypes.float32)
  class_id = _ops.convert_to_tensor(class_id, _dtypes.int32)
  feature_column_group_id = _ops.convert_to_tensor(feature_column_group_id, _dtypes.int32)
  l1_regularization = _ops.convert_to_tensor(l1_regularization, _dtypes.float32)
  l2_regularization = _ops.convert_to_tensor(l2_regularization, _dtypes.float32)
  tree_complexity_regularization = _ops.convert_to_tensor(tree_complexity_regularization, _dtypes.float32)
  min_node_weight = _ops.convert_to_tensor(min_node_weight, _dtypes.float32)
  multiclass_strategy = _ops.convert_to_tensor(multiclass_strategy, _dtypes.int32)
  weak_learner_type = _ops.convert_to_tensor(weak_learner_type, _dtypes.int32)
  _inputs_flat = [num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, weak_learner_type]
  _attrs = None
  _result = _execute.execute(b"BuildDenseInequalitySplits", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BuildDenseInequalitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildDenseInequalitySplitsOutput._make(_result)
  return _result

_ops.RegisterShape("BuildDenseInequalitySplits")(None)


_build_sparse_inequality_splits_outputs = ["output_partition_ids", "gains",
                                          "split_infos"]
_BuildSparseInequalitySplitsOutput = _collections.namedtuple(
    "BuildSparseInequalitySplits", _build_sparse_inequality_splits_outputs)


@_dispatch.add_dispatch_list
@tf_export('build_sparse_inequality_splits')
def build_sparse_inequality_splits(num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, name=None):
  r"""Find the split that has the best gain for the accumulated stats for a particular

  feature column.

  Args:
    num_minibatches: A `Tensor` of type `int64`.
      A scalar, the number of times per example gradients & hessians
      were accumulated. The stats are divided by this to get per example stats.
    partition_ids: A `Tensor` of type `int32`.
      A rank 2 tensor of partition IDs for each dimension of feature column.
    bucket_ids: A `Tensor` of type `int64`.
      A rank 2 tensor of buckets IDs and dimensions.
    gradients: A `Tensor` of type `float32`. A rank 1 tensor of gradients.
    hessians: A `Tensor` of type `float32`. A rank 1 tensor of hessians.
    bucket_boundaries: A `Tensor` of type `float32`.
      A rank 1 tensor, thresholds that were used for bucketization.
    class_id: A `Tensor` of type `int32`.
      A scalar, the class id for which we're building the splits.
    feature_column_group_id: A `Tensor` of type `int32`.
      A scalar, the index of the feature we are spiltting on.
    bias_feature_id: A `Tensor` of type `int64`.
    l1_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l1 regularization term.
    l2_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the l2 regularization term.
    tree_complexity_regularization: A `Tensor` of type `float32`.
      A scalar, which specifies the tree complexity
      regularization term.
    min_node_weight: A `Tensor` of type `float32`.
      A scalar, minimum sum of example hessian needed in a child.
      If a split results in a leaf node with a smaller value, the split will not
      be considered.
    multiclass_strategy: A `Tensor` of type `int32`.
      A scalar, specifying the multiclass handling strategy.
      See LearnerConfig.MultiClassStrategy for valid values.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (output_partition_ids, gains, split_infos).

    output_partition_ids: A `Tensor` of type `int32`. A rank 1 tensor, the partition IDs that we created splits
      for.
    gains: A `Tensor` of type `float32`. A rank 1 tensor, for the computed gain for the created splits.
    split_infos: A `Tensor` of type `string`. A rank 1 tensor of serialized protos which contains the
      `SplitInfo`s.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BuildSparseInequalitySplits", name, _ctx._post_execution_callbacks,
        num_minibatches, partition_ids, bucket_ids, gradients, hessians,
        bucket_boundaries, class_id, feature_column_group_id, bias_feature_id,
        l1_regularization, l2_regularization, tree_complexity_regularization,
        min_node_weight, multiclass_strategy)
      _result = _BuildSparseInequalitySplitsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return build_sparse_inequality_splits_eager_fallback(
            num_minibatches, partition_ids, bucket_ids, gradients, hessians,
            bucket_boundaries, class_id, feature_column_group_id,
            bias_feature_id, l1_regularization, l2_regularization,
            tree_complexity_regularization, min_node_weight,
            multiclass_strategy, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              build_sparse_inequality_splits, num_minibatches=num_minibatches,
                                              partition_ids=partition_ids,
                                              bucket_ids=bucket_ids,
                                              gradients=gradients,
                                              hessians=hessians,
                                              bucket_boundaries=bucket_boundaries,
                                              class_id=class_id,
                                              feature_column_group_id=feature_column_group_id,
                                              bias_feature_id=bias_feature_id,
                                              l1_regularization=l1_regularization,
                                              l2_regularization=l2_regularization,
                                              tree_complexity_regularization=tree_complexity_regularization,
                                              min_node_weight=min_node_weight,
                                              multiclass_strategy=multiclass_strategy,
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
        "BuildSparseInequalitySplits", num_minibatches=num_minibatches,
                                       partition_ids=partition_ids,
                                       bucket_ids=bucket_ids,
                                       gradients=gradients, hessians=hessians,
                                       bucket_boundaries=bucket_boundaries,
                                       class_id=class_id,
                                       feature_column_group_id=feature_column_group_id,
                                       bias_feature_id=bias_feature_id,
                                       l1_regularization=l1_regularization,
                                       l2_regularization=l2_regularization,
                                       tree_complexity_regularization=tree_complexity_regularization,
                                       min_node_weight=min_node_weight,
                                       multiclass_strategy=multiclass_strategy,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          build_sparse_inequality_splits, num_minibatches=num_minibatches,
                                          partition_ids=partition_ids,
                                          bucket_ids=bucket_ids,
                                          gradients=gradients,
                                          hessians=hessians,
                                          bucket_boundaries=bucket_boundaries,
                                          class_id=class_id,
                                          feature_column_group_id=feature_column_group_id,
                                          bias_feature_id=bias_feature_id,
                                          l1_regularization=l1_regularization,
                                          l2_regularization=l2_regularization,
                                          tree_complexity_regularization=tree_complexity_regularization,
                                          min_node_weight=min_node_weight,
                                          multiclass_strategy=multiclass_strategy,
                                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BuildSparseInequalitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildSparseInequalitySplitsOutput._make(_result)
  return _result



def build_sparse_inequality_splits_eager_fallback(num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function build_sparse_inequality_splits
  """
  _ctx = ctx if ctx else _context.context()
  num_minibatches = _ops.convert_to_tensor(num_minibatches, _dtypes.int64)
  partition_ids = _ops.convert_to_tensor(partition_ids, _dtypes.int32)
  bucket_ids = _ops.convert_to_tensor(bucket_ids, _dtypes.int64)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  bucket_boundaries = _ops.convert_to_tensor(bucket_boundaries, _dtypes.float32)
  class_id = _ops.convert_to_tensor(class_id, _dtypes.int32)
  feature_column_group_id = _ops.convert_to_tensor(feature_column_group_id, _dtypes.int32)
  bias_feature_id = _ops.convert_to_tensor(bias_feature_id, _dtypes.int64)
  l1_regularization = _ops.convert_to_tensor(l1_regularization, _dtypes.float32)
  l2_regularization = _ops.convert_to_tensor(l2_regularization, _dtypes.float32)
  tree_complexity_regularization = _ops.convert_to_tensor(tree_complexity_regularization, _dtypes.float32)
  min_node_weight = _ops.convert_to_tensor(min_node_weight, _dtypes.float32)
  multiclass_strategy = _ops.convert_to_tensor(multiclass_strategy, _dtypes.int32)
  _inputs_flat = [num_minibatches, partition_ids, bucket_ids, gradients, hessians, bucket_boundaries, class_id, feature_column_group_id, bias_feature_id, l1_regularization, l2_regularization, tree_complexity_regularization, min_node_weight, multiclass_strategy]
  _attrs = None
  _result = _execute.execute(b"BuildSparseInequalitySplits", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BuildSparseInequalitySplits", _inputs_flat, _attrs, _result, name)
  _result = _BuildSparseInequalitySplitsOutput._make(_result)
  return _result

_ops.RegisterShape("BuildSparseInequalitySplits")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BuildCategoricalEqualitySplits"
#   input_arg {
#     name: "num_minibatches"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "class_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_column_group_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bias_feature_id"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "l1_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_complexity_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_node_weight"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "multiclass_strategy"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "weak_learner_type"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "gains"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "split_infos"
#     type: DT_STRING
#   }
# }
# op {
#   name: "BuildDenseInequalitySplits"
#   input_arg {
#     name: "num_minibatches"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bucket_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "bucket_boundaries"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "class_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_column_group_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "l1_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_complexity_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_node_weight"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "multiclass_strategy"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "weak_learner_type"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "gains"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "split_infos"
#     type: DT_STRING
#   }
# }
# op {
#   name: "BuildSparseInequalitySplits"
#   input_arg {
#     name: "num_minibatches"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "partition_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bucket_ids"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "bucket_boundaries"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "class_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "feature_column_group_id"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bias_feature_id"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "l1_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_complexity_regularization"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_node_weight"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "multiclass_strategy"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output_partition_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "gains"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "split_infos"
#     type: DT_STRING
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\203\003\n\036BuildCategoricalEqualitySplits\022\023\n\017num_minibatches\030\t\022\021\n\rpartition_ids\030\003\022\017\n\013feature_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\022\014\n\010class_id\030\003\022\033\n\027feature_column_group_id\030\003\022\023\n\017bias_feature_id\030\t\022\025\n\021l1_regularization\030\001\022\025\n\021l2_regularization\030\001\022\"\n\036tree_complexity_regularization\030\001\022\023\n\017min_node_weight\030\001\022\027\n\023multiclass_strategy\030\003\022\025\n\021weak_learner_type\030\003\032\030\n\024output_partition_ids\030\003\032\t\n\005gains\030\001\032\017\n\013split_infos\030\007\n\200\003\n\032BuildDenseInequalitySplits\022\023\n\017num_minibatches\030\t\022\021\n\rpartition_ids\030\003\022\016\n\nbucket_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\022\025\n\021bucket_boundaries\030\001\022\014\n\010class_id\030\003\022\033\n\027feature_column_group_id\030\003\022\025\n\021l1_regularization\030\001\022\025\n\021l2_regularization\030\001\022\"\n\036tree_complexity_regularization\030\001\022\023\n\017min_node_weight\030\001\022\027\n\023multiclass_strategy\030\003\022\025\n\021weak_learner_type\030\003\032\030\n\024output_partition_ids\030\003\032\t\n\005gains\030\001\032\017\n\013split_infos\030\007\n\377\002\n\033BuildSparseInequalitySplits\022\023\n\017num_minibatches\030\t\022\021\n\rpartition_ids\030\003\022\016\n\nbucket_ids\030\t\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\022\025\n\021bucket_boundaries\030\001\022\014\n\010class_id\030\003\022\033\n\027feature_column_group_id\030\003\022\023\n\017bias_feature_id\030\t\022\025\n\021l1_regularization\030\001\022\025\n\021l2_regularization\030\001\022\"\n\036tree_complexity_regularization\030\001\022\023\n\017min_node_weight\030\001\022\027\n\023multiclass_strategy\030\003\032\030\n\024output_partition_ids\030\003\032\t\n\005gains\030\001\032\017\n\013split_infos\030\007")
