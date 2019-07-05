"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: boosted_trees_ops.cc
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


def boosted_trees_bucketize(float_values, bucket_boundaries, name=None):
  r"""Bucketize each feature based on bucket boundaries.

  An op that returns a list of float tensors, where each tensor represents the
  bucketized values for a single feature.

  Args:
    float_values: A list of `Tensor` objects with type `float32`.
      float; List of Rank 1 Tensor each containing float values for a single feature.
    bucket_boundaries: A list with the same length as `float_values` of `Tensor` objects with type `float32`.
      float; List of Rank 1 Tensors each containing the bucket boundaries for a single
      feature.
    name: A name for the operation (optional).

  Returns:
    A list with the same length as `float_values` of `Tensor` objects with type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesBucketize", name, _ctx._post_execution_callbacks,
        float_values, bucket_boundaries)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_bucketize_eager_fallback(
            float_values, bucket_boundaries, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(float_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'float_values' argument to "
        "'boosted_trees_bucketize' Op, not %r." % float_values)
  _attr_num_features = len(float_values)
  if not isinstance(bucket_boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucket_boundaries' argument to "
        "'boosted_trees_bucketize' Op, not %r." % bucket_boundaries)
  if len(bucket_boundaries) != _attr_num_features:
    raise ValueError(
        "List argument 'bucket_boundaries' to 'boosted_trees_bucketize' Op with length %d "
        "must match length %d of argument 'float_values'." %
        (len(bucket_boundaries), _attr_num_features))
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesBucketize", float_values=float_values,
                                 bucket_boundaries=bucket_boundaries,
                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_features", _op.get_attr("num_features"))
  _execute.record_gradient(
      "BoostedTreesBucketize", _inputs_flat, _attrs, _result, name)
  return _result



def boosted_trees_bucketize_eager_fallback(float_values, bucket_boundaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_bucketize
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(float_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'float_values' argument to "
        "'boosted_trees_bucketize' Op, not %r." % float_values)
  _attr_num_features = len(float_values)
  if not isinstance(bucket_boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucket_boundaries' argument to "
        "'boosted_trees_bucketize' Op, not %r." % bucket_boundaries)
  if len(bucket_boundaries) != _attr_num_features:
    raise ValueError(
        "List argument 'bucket_boundaries' to 'boosted_trees_bucketize' Op with length %d "
        "must match length %d of argument 'float_values'." %
        (len(bucket_boundaries), _attr_num_features))
  float_values = _ops.convert_n_to_tensor(float_values, _dtypes.float32)
  bucket_boundaries = _ops.convert_n_to_tensor(bucket_boundaries, _dtypes.float32)
  _inputs_flat = list(float_values) + list(bucket_boundaries)
  _attrs = ("num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesBucketize", _attr_num_features,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesBucketize", _inputs_flat, _attrs, _result, name)
  return _result


_boosted_trees_calculate_best_gains_per_feature_outputs = ["node_ids_list",
                                                          "gains_list",
                                                          "thresholds_list",
                                                          "left_node_contribs_list",
                                                          "right_node_contribs_list"]
_BoostedTreesCalculateBestGainsPerFeatureOutput = _collections.namedtuple(
    "BoostedTreesCalculateBestGainsPerFeature",
    _boosted_trees_calculate_best_gains_per_feature_outputs)


def boosted_trees_calculate_best_gains_per_feature(node_id_range, stats_summary_list, l1, l2, tree_complexity, min_node_weight, max_splits, name=None):
  r"""Calculates gains for each feature and returns the best possible split information for the feature.

  The split information is the best threshold (bucket id), gains and left/right node contributions per node for each feature.

  It is possible that not all nodes can be split on each feature. Hence, the list of possible nodes can differ between the features. Therefore, we return `node_ids_list` for each feature, containing the list of nodes that this feature can be used to split.

  In this manner, the output is the best split per features and per node, so that it needs to be combined later to produce the best split for each node (among all possible features).

  The length of output lists are all of the same length, `num_features`.
  The output shapes are compatible in a way that the first dimension of all tensors of all lists are the same and equal to the number of possible split nodes for each feature.

  Args:
    node_id_range: A `Tensor` of type `int32`.
      A Rank 1 tensor (shape=[2]) to specify the range [first, last) of node ids to process within `stats_summary_list`. The nodes are iterated between the two nodes specified by the tensor, as like `for node_id in range(node_id_range[0], node_id_range[1])` (Note that the last index node_id_range[1] is exclusive).
    stats_summary_list: A list of at least 1 `Tensor` objects with type `float32`.
      A list of Rank 3 tensor (#shape=[max_splits, bucket, 2]) for accumulated stats summary (gradient/hessian) per node per buckets for each feature. The first dimension of the tensor is the maximum number of splits, and thus not all elements of it will be used, but only the indexes specified by node_ids will be used.
    l1: A `Tensor` of type `float32`.
      l1 regularization factor on leaf weights, per instance based.
    l2: A `Tensor` of type `float32`.
      l2 regularization factor on leaf weights, per instance based.
    tree_complexity: A `Tensor` of type `float32`.
      adjustment to the gain, per leaf based.
    min_node_weight: A `Tensor` of type `float32`.
      mininum avg of hessians in a node before required for the node to be considered for splitting.
    max_splits: An `int` that is `>= 1`.
      the number of nodes that can be split in the whole tree. Used as a dimension of output tensors.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (node_ids_list, gains_list, thresholds_list, left_node_contribs_list, right_node_contribs_list).

    node_ids_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `int32`.
    gains_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
    thresholds_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `int32`.
    left_node_contribs_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
    right_node_contribs_list: A list with the same length as `stats_summary_list` of `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCalculateBestGainsPerFeature", name,
        _ctx._post_execution_callbacks, node_id_range, stats_summary_list, l1,
        l2, tree_complexity, min_node_weight, "max_splits", max_splits)
      _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_calculate_best_gains_per_feature_eager_fallback(
            node_id_range, stats_summary_list, l1, l2, tree_complexity,
            min_node_weight, max_splits=max_splits, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(stats_summary_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_summary_list' argument to "
        "'boosted_trees_calculate_best_gains_per_feature' Op, not %r." % stats_summary_list)
  _attr_num_features = len(stats_summary_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCalculateBestGainsPerFeature", node_id_range=node_id_range,
                                                    stats_summary_list=stats_summary_list,
                                                    l1=l1, l2=l2,
                                                    tree_complexity=tree_complexity,
                                                    min_node_weight=min_node_weight,
                                                    max_splits=max_splits,
                                                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_splits", _op.get_attr("max_splits"), "num_features",
            _op.get_attr("num_features"))
  _execute.record_gradient(
      "BoostedTreesCalculateBestGainsPerFeature", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_features]] + _result[_attr_num_features:]
  _result = _result[:1] + [_result[1:1 + _attr_num_features]] + _result[1 + _attr_num_features:]
  _result = _result[:2] + [_result[2:2 + _attr_num_features]] + _result[2 + _attr_num_features:]
  _result = _result[:3] + [_result[3:3 + _attr_num_features]] + _result[3 + _attr_num_features:]
  _result = _result[:4] + [_result[4:]]
  _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
  return _result



def boosted_trees_calculate_best_gains_per_feature_eager_fallback(node_id_range, stats_summary_list, l1, l2, tree_complexity, min_node_weight, max_splits, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_calculate_best_gains_per_feature
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(stats_summary_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'stats_summary_list' argument to "
        "'boosted_trees_calculate_best_gains_per_feature' Op, not %r." % stats_summary_list)
  _attr_num_features = len(stats_summary_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  node_id_range = _ops.convert_to_tensor(node_id_range, _dtypes.int32)
  stats_summary_list = _ops.convert_n_to_tensor(stats_summary_list, _dtypes.float32)
  l1 = _ops.convert_to_tensor(l1, _dtypes.float32)
  l2 = _ops.convert_to_tensor(l2, _dtypes.float32)
  tree_complexity = _ops.convert_to_tensor(tree_complexity, _dtypes.float32)
  min_node_weight = _ops.convert_to_tensor(min_node_weight, _dtypes.float32)
  _inputs_flat = [node_id_range] + list(stats_summary_list) + [l1, l2, tree_complexity, min_node_weight]
  _attrs = ("max_splits", max_splits, "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesCalculateBestGainsPerFeature",
                             _attr_num_features + _attr_num_features +
                             _attr_num_features + _attr_num_features +
                             _attr_num_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesCalculateBestGainsPerFeature", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_features]] + _result[_attr_num_features:]
  _result = _result[:1] + [_result[1:1 + _attr_num_features]] + _result[1 + _attr_num_features:]
  _result = _result[:2] + [_result[2:2 + _attr_num_features]] + _result[2 + _attr_num_features:]
  _result = _result[:3] + [_result[3:3 + _attr_num_features]] + _result[3 + _attr_num_features:]
  _result = _result[:4] + [_result[4:]]
  _result = _BoostedTreesCalculateBestGainsPerFeatureOutput._make(_result)
  return _result


def boosted_trees_center_bias(tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2, name=None):
  r"""Calculates the prior from the training data (the bias) and fills in the first node with the logits' prior. Returns a boolean indicating whether to continue centering.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    mean_gradients: A `Tensor` of type `float32`.
      A tensor with shape=[logits_dimension] with mean of gradients for a first node.
    mean_hessians: A `Tensor` of type `float32`.
      A tensor with shape=[logits_dimension] mean of hessians for a first node.
    l1: A `Tensor` of type `float32`.
      l1 regularization factor on leaf weights, per instance based.
    l2: A `Tensor` of type `float32`.
      l2 regularization factor on leaf weights, per instance based.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCenterBias", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_center_bias_eager_fallback(
            tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCenterBias", tree_ensemble_handle=tree_ensemble_handle,
                                  mean_gradients=mean_gradients,
                                  mean_hessians=mean_hessians, l1=l1, l2=l2,
                                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BoostedTreesCenterBias", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_center_bias_eager_fallback(tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_center_bias
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  mean_gradients = _ops.convert_to_tensor(mean_gradients, _dtypes.float32)
  mean_hessians = _ops.convert_to_tensor(mean_hessians, _dtypes.float32)
  l1 = _ops.convert_to_tensor(l1, _dtypes.float32)
  l2 = _ops.convert_to_tensor(l2, _dtypes.float32)
  _inputs_flat = [tree_ensemble_handle, mean_gradients, mean_hessians, l1, l2]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesCenterBias", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesCenterBias", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_create_ensemble(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None):
  r"""Creates a tree ensemble model and returns a handle to it.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble resource to be created.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    tree_ensemble_serialized: A `Tensor` of type `string`.
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
        "BoostedTreesCreateEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, stamp_token, tree_ensemble_serialized)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_create_ensemble_eager_fallback(
            tree_ensemble_handle, stamp_token, tree_ensemble_serialized,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCreateEnsemble", tree_ensemble_handle=tree_ensemble_handle,
                                      stamp_token=stamp_token,
                                      tree_ensemble_serialized=tree_ensemble_serialized,
                                      name=name)
  return _op
  _result = None
  return _result



def boosted_trees_create_ensemble_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_create_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_serialized = _ops.convert_to_tensor(tree_ensemble_serialized, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_serialized]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesCreateEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_create_quantile_stream_resource(quantile_stream_resource_handle, epsilon, num_streams, max_elements=1099511627776, name=None):
  r"""Create the Resource for Quantile Streams.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource; Handle to quantile stream resource.
    epsilon: A `Tensor` of type `float32`.
      float; The required approximation error of the stream resource.
    num_streams: A `Tensor` of type `int64`.
      int; The number of streams managed by the resource that shares the same epsilon.
    max_elements: An optional `int`. Defaults to `1099511627776`.
      int; The maximum number of data points that can be fed to the stream.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesCreateQuantileStreamResource", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle,
        epsilon, num_streams, "max_elements", max_elements)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_create_quantile_stream_resource_eager_fallback(
            quantile_stream_resource_handle, epsilon, num_streams,
            max_elements=max_elements, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if max_elements is None:
    max_elements = 1099511627776
  max_elements = _execute.make_int(max_elements, "max_elements")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesCreateQuantileStreamResource", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                    epsilon=epsilon,
                                                    num_streams=num_streams,
                                                    max_elements=max_elements,
                                                    name=name)
  return _op
  _result = None
  return _result



def boosted_trees_create_quantile_stream_resource_eager_fallback(quantile_stream_resource_handle, epsilon, num_streams, max_elements=1099511627776, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_create_quantile_stream_resource
  """
  _ctx = ctx if ctx else _context.context()
  if max_elements is None:
    max_elements = 1099511627776
  max_elements = _execute.make_int(max_elements, "max_elements")
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  epsilon = _ops.convert_to_tensor(epsilon, _dtypes.float32)
  num_streams = _ops.convert_to_tensor(num_streams, _dtypes.int64)
  _inputs_flat = [quantile_stream_resource_handle, epsilon, num_streams]
  _attrs = ("max_elements", max_elements)
  _result = _execute.execute(b"BoostedTreesCreateQuantileStreamResource", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_deserialize_ensemble(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None):
  r"""Deserializes a serialized tree ensemble config and replaces current tree

  ensemble.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the new value of the resource stamp.
    tree_ensemble_serialized: A `Tensor` of type `string`.
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
        "BoostedTreesDeserializeEnsemble", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle, stamp_token,
        tree_ensemble_serialized)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_deserialize_ensemble_eager_fallback(
            tree_ensemble_handle, stamp_token, tree_ensemble_serialized,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesDeserializeEnsemble", tree_ensemble_handle=tree_ensemble_handle,
                                           stamp_token=stamp_token,
                                           tree_ensemble_serialized=tree_ensemble_serialized,
                                           name=name)
  return _op
  _result = None
  return _result



def boosted_trees_deserialize_ensemble_eager_fallback(tree_ensemble_handle, stamp_token, tree_ensemble_serialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_deserialize_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  tree_ensemble_serialized = _ops.convert_to_tensor(tree_ensemble_serialized, _dtypes.string)
  _inputs_flat = [tree_ensemble_handle, stamp_token, tree_ensemble_serialized]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesDeserializeEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_ensemble_resource_handle_op(container="", shared_name="", name=None):
  r"""Creates a handle to a BoostedTreesEnsembleResource

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
        "BoostedTreesEnsembleResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_ensemble_resource_handle_op_eager_fallback(
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
        "BoostedTreesEnsembleResourceHandleOp", container=container,
                                                shared_name=shared_name,
                                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "BoostedTreesEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_ensemble_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_ensemble_resource_handle_op
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
  _result = _execute.execute(b"BoostedTreesEnsembleResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesEnsembleResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_example_debug_outputs(tree_ensemble_handle, bucketized_features, logits_dimension, name=None):
  r"""Debugging/model interpretability outputs for each example.

  It traverses all the trees and computes debug metrics for individual examples, 
  such as getting split feature ids and logits after each split along the decision
  path used to compute directional feature contributions.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for constructing the protos in
      examples_debug_outputs_serialized.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesExampleDebugOutputs", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle,
        bucketized_features, "logits_dimension", logits_dimension)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_example_debug_outputs_eager_fallback(
            tree_ensemble_handle, bucketized_features,
            logits_dimension=logits_dimension, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_example_debug_outputs' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesExampleDebugOutputs", tree_ensemble_handle=tree_ensemble_handle,
                                           bucketized_features=bucketized_features,
                                           logits_dimension=logits_dimension,
                                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bucketized_features",
            _op.get_attr("num_bucketized_features"), "logits_dimension",
            _op.get_attr("logits_dimension"))
  _execute.record_gradient(
      "BoostedTreesExampleDebugOutputs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_example_debug_outputs_eager_fallback(tree_ensemble_handle, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_example_debug_outputs
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_example_debug_outputs' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesExampleDebugOutputs", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesExampleDebugOutputs", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_boosted_trees_get_ensemble_states_outputs = ["stamp_token", "num_trees",
                                             "num_finalized_trees",
                                             "num_attempted_layers",
                                             "last_layer_nodes_range"]
_BoostedTreesGetEnsembleStatesOutput = _collections.namedtuple(
    "BoostedTreesGetEnsembleStates",
    _boosted_trees_get_ensemble_states_outputs)


def boosted_trees_get_ensemble_states(tree_ensemble_handle, name=None):
  r"""Retrieves the tree ensemble resource stamp token, number of trees and growing statistics.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, num_trees, num_finalized_trees, num_attempted_layers, last_layer_nodes_range).

    stamp_token: A `Tensor` of type `int64`.
    num_trees: A `Tensor` of type `int32`.
    num_finalized_trees: A `Tensor` of type `int32`.
    num_attempted_layers: A `Tensor` of type `int32`.
    last_layer_nodes_range: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesGetEnsembleStates", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_get_ensemble_states_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
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
        "BoostedTreesGetEnsembleStates", tree_ensemble_handle=tree_ensemble_handle,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BoostedTreesGetEnsembleStates", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
  return _result



def boosted_trees_get_ensemble_states_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_get_ensemble_states
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesGetEnsembleStates", 5,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesGetEnsembleStates", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesGetEnsembleStatesOutput._make(_result)
  return _result


def boosted_trees_make_quantile_summaries(float_values, example_weights, epsilon, name=None):
  r"""Makes the summary of quantiles for the batch.

  An op that takes a list of tensors (one tensor per feature) and outputs the
  quantile summaries for each tensor.

  Args:
    float_values: A list of `Tensor` objects with type `float32`.
      float; List of Rank 1 Tensors each containing values for a single feature.
    example_weights: A `Tensor` of type `float32`.
      float; Rank 1 Tensor with weights per instance.
    epsilon: A `Tensor` of type `float32`.
      float; The required maximum approximation error.
    name: A name for the operation (optional).

  Returns:
    A list with the same length as `float_values` of `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesMakeQuantileSummaries", name,
        _ctx._post_execution_callbacks, float_values, example_weights,
        epsilon)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_make_quantile_summaries_eager_fallback(
            float_values, example_weights, epsilon, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(float_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'float_values' argument to "
        "'boosted_trees_make_quantile_summaries' Op, not %r." % float_values)
  _attr_num_features = len(float_values)
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesMakeQuantileSummaries", float_values=float_values,
                                             example_weights=example_weights,
                                             epsilon=epsilon, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_features", _op.get_attr("num_features"))
  _execute.record_gradient(
      "BoostedTreesMakeQuantileSummaries", _inputs_flat, _attrs, _result, name)
  return _result



def boosted_trees_make_quantile_summaries_eager_fallback(float_values, example_weights, epsilon, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_make_quantile_summaries
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(float_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'float_values' argument to "
        "'boosted_trees_make_quantile_summaries' Op, not %r." % float_values)
  _attr_num_features = len(float_values)
  float_values = _ops.convert_n_to_tensor(float_values, _dtypes.float32)
  example_weights = _ops.convert_to_tensor(example_weights, _dtypes.float32)
  epsilon = _ops.convert_to_tensor(epsilon, _dtypes.float32)
  _inputs_flat = list(float_values) + [example_weights, epsilon]
  _attrs = ("num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesMakeQuantileSummaries",
                             _attr_num_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesMakeQuantileSummaries", _inputs_flat, _attrs, _result, name)
  return _result


def boosted_trees_make_stats_summary(node_ids, gradients, hessians, bucketized_features_list, max_splits, num_buckets, name=None):
  r"""Makes the summary of accumulated stats for the batch.

  The summary stats contains gradients and hessians accumulated into the corresponding node and bucket for each example.

  Args:
    node_ids: A `Tensor` of type `int32`.
      int32 Rank 1 Tensor containing node ids, which each example falls into for the requested layer.
    gradients: A `Tensor` of type `float32`.
      float32; Rank 2 Tensor (shape=[#examples, 1]) for gradients.
    hessians: A `Tensor` of type `float32`.
      float32; Rank 2 Tensor (shape=[#examples, 1]) for hessians.
    bucketized_features_list: A list of at least 1 `Tensor` objects with type `int32`.
      int32 list of Rank 1 Tensors, each containing the bucketized feature (for each feature column).
    max_splits: An `int` that is `>= 1`.
      int; the maximum number of splits possible in the whole tree.
    num_buckets: An `int` that is `>= 1`.
      int; equals to the maximum possible value of bucketized feature.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesMakeStatsSummary", name, _ctx._post_execution_callbacks,
        node_ids, gradients, hessians, bucketized_features_list, "max_splits",
        max_splits, "num_buckets", num_buckets)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_make_stats_summary_eager_fallback(
            node_ids, gradients, hessians, bucketized_features_list,
            max_splits=max_splits, num_buckets=num_buckets, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(bucketized_features_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features_list' argument to "
        "'boosted_trees_make_stats_summary' Op, not %r." % bucketized_features_list)
  _attr_num_features = len(bucketized_features_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesMakeStatsSummary", node_ids=node_ids,
                                        gradients=gradients,
                                        hessians=hessians,
                                        bucketized_features_list=bucketized_features_list,
                                        max_splits=max_splits,
                                        num_buckets=num_buckets, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_splits", _op.get_attr("max_splits"), "num_buckets",
            _op.get_attr("num_buckets"), "num_features",
            _op.get_attr("num_features"))
  _execute.record_gradient(
      "BoostedTreesMakeStatsSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_make_stats_summary_eager_fallback(node_ids, gradients, hessians, bucketized_features_list, max_splits, num_buckets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_make_stats_summary
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features_list, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features_list' argument to "
        "'boosted_trees_make_stats_summary' Op, not %r." % bucketized_features_list)
  _attr_num_features = len(bucketized_features_list)
  max_splits = _execute.make_int(max_splits, "max_splits")
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  node_ids = _ops.convert_to_tensor(node_ids, _dtypes.int32)
  gradients = _ops.convert_to_tensor(gradients, _dtypes.float32)
  hessians = _ops.convert_to_tensor(hessians, _dtypes.float32)
  bucketized_features_list = _ops.convert_n_to_tensor(bucketized_features_list, _dtypes.int32)
  _inputs_flat = [node_ids, gradients, hessians] + list(bucketized_features_list)
  _attrs = ("max_splits", max_splits, "num_buckets", num_buckets,
  "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesMakeStatsSummary", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesMakeStatsSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_predict(tree_ensemble_handle, bucketized_features, logits_dimension, name=None):
  r"""Runs multiple additive regression ensemble predictors on input instances and

  computes the logits. It is designed to be used during prediction.
  It traverses all the trees and calculates the final score for each instance.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for partial logits
      shape.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesPredict", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, bucketized_features, "logits_dimension",
        logits_dimension)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_predict_eager_fallback(
            tree_ensemble_handle, bucketized_features,
            logits_dimension=logits_dimension, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesPredict", tree_ensemble_handle=tree_ensemble_handle,
                               bucketized_features=bucketized_features,
                               logits_dimension=logits_dimension, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bucketized_features",
            _op.get_attr("num_bucketized_features"), "logits_dimension",
            _op.get_attr("logits_dimension"))
  _execute.record_gradient(
      "BoostedTreesPredict", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_predict_eager_fallback(tree_ensemble_handle, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_predict
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesPredict", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesPredict", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def boosted_trees_quantile_stream_resource_add_summaries(quantile_stream_resource_handle, summaries, name=None):
  r"""Add the quantile summaries to each quantile stream resource.

  An op that adds a list of quantile summaries to a quantile stream resource. Each
  summary Tensor is rank 2, containing summaries (value, weight, min_rank, max_rank)
  for a single feature.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource handle referring to a QuantileStreamResource.
    summaries: A list of `Tensor` objects with type `float32`.
      string; List of Rank 2 Tensor each containing the summaries for a single feature.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesQuantileStreamResourceAddSummaries", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle,
        summaries)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_quantile_stream_resource_add_summaries_eager_fallback(
            quantile_stream_resource_handle, summaries, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(summaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'summaries' argument to "
        "'boosted_trees_quantile_stream_resource_add_summaries' Op, not %r." % summaries)
  _attr_num_features = len(summaries)
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesQuantileStreamResourceAddSummaries", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                          summaries=summaries,
                                                          name=name)
  return _op
  _result = None
  return _result



def boosted_trees_quantile_stream_resource_add_summaries_eager_fallback(quantile_stream_resource_handle, summaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_quantile_stream_resource_add_summaries
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(summaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'summaries' argument to "
        "'boosted_trees_quantile_stream_resource_add_summaries' Op, not %r." % summaries)
  _attr_num_features = len(summaries)
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  summaries = _ops.convert_n_to_tensor(summaries, _dtypes.float32)
  _inputs_flat = [quantile_stream_resource_handle] + list(summaries)
  _attrs = ("num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesQuantileStreamResourceAddSummaries",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_quantile_stream_resource_deserialize(quantile_stream_resource_handle, bucket_boundaries, name=None):
  r"""Deserialize bucket boundaries and ready flag into current QuantileAccumulator.

  An op that deserializes bucket boundaries and are boundaries ready flag into current QuantileAccumulator.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource handle referring to a QuantileStreamResource.
    bucket_boundaries: A list of at least 1 `Tensor` objects with type `float32`.
      float; List of Rank 1 Tensors each containing the bucket boundaries for a feature.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesQuantileStreamResourceDeserialize", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle,
        bucket_boundaries)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_quantile_stream_resource_deserialize_eager_fallback(
            quantile_stream_resource_handle, bucket_boundaries, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(bucket_boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucket_boundaries' argument to "
        "'boosted_trees_quantile_stream_resource_deserialize' Op, not %r." % bucket_boundaries)
  _attr_num_streams = len(bucket_boundaries)
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesQuantileStreamResourceDeserialize", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                         bucket_boundaries=bucket_boundaries,
                                                         name=name)
  return _op
  _result = None
  return _result



def boosted_trees_quantile_stream_resource_deserialize_eager_fallback(quantile_stream_resource_handle, bucket_boundaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_quantile_stream_resource_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucket_boundaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucket_boundaries' argument to "
        "'boosted_trees_quantile_stream_resource_deserialize' Op, not %r." % bucket_boundaries)
  _attr_num_streams = len(bucket_boundaries)
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  bucket_boundaries = _ops.convert_n_to_tensor(bucket_boundaries, _dtypes.float32)
  _inputs_flat = [quantile_stream_resource_handle] + list(bucket_boundaries)
  _attrs = ("num_streams", _attr_num_streams)
  _result = _execute.execute(b"BoostedTreesQuantileStreamResourceDeserialize",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_quantile_stream_resource_flush(quantile_stream_resource_handle, num_buckets, generate_quantiles=False, name=None):
  r"""Flush the summaries for a quantile stream resource.

  An op that flushes the summaries for a quantile stream resource.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource handle referring to a QuantileStreamResource.
    num_buckets: A `Tensor` of type `int64`.
      int; approximate number of buckets unless using generate_quantiles.
    generate_quantiles: An optional `bool`. Defaults to `False`.
      bool; If True, the output will be the num_quantiles for each stream where the ith
      entry is the ith quantile of the input with an approximation error of epsilon.
      Duplicate values may be present.
      If False, the output will be the points in the histogram that we got which roughly
      translates to 1/epsilon boundaries and without any duplicates.
      Default to False.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesQuantileStreamResourceFlush", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle,
        num_buckets, "generate_quantiles", generate_quantiles)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_quantile_stream_resource_flush_eager_fallback(
            quantile_stream_resource_handle, num_buckets,
            generate_quantiles=generate_quantiles, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if generate_quantiles is None:
    generate_quantiles = False
  generate_quantiles = _execute.make_bool(generate_quantiles, "generate_quantiles")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesQuantileStreamResourceFlush", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                   num_buckets=num_buckets,
                                                   generate_quantiles=generate_quantiles,
                                                   name=name)
  return _op
  _result = None
  return _result



def boosted_trees_quantile_stream_resource_flush_eager_fallback(quantile_stream_resource_handle, num_buckets, generate_quantiles=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_quantile_stream_resource_flush
  """
  _ctx = ctx if ctx else _context.context()
  if generate_quantiles is None:
    generate_quantiles = False
  generate_quantiles = _execute.make_bool(generate_quantiles, "generate_quantiles")
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  num_buckets = _ops.convert_to_tensor(num_buckets, _dtypes.int64)
  _inputs_flat = [quantile_stream_resource_handle, num_buckets]
  _attrs = ("generate_quantiles", generate_quantiles)
  _result = _execute.execute(b"BoostedTreesQuantileStreamResourceFlush", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def boosted_trees_quantile_stream_resource_get_bucket_boundaries(quantile_stream_resource_handle, num_features, name=None):
  r"""Generate the bucket boundaries for each feature based on accumulated summaries.

  An op that returns a list of float tensors for a quantile stream resource. Each
  tensor is Rank 1 containing bucket boundaries for a single feature.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource handle referring to a QuantileStreamResource.
    num_features: An `int` that is `>= 0`.
      inferred int; number of features to get bucket boundaries for.
    name: A name for the operation (optional).

  Returns:
    A list of `num_features` `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesQuantileStreamResourceGetBucketBoundaries", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle,
        "num_features", num_features)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_quantile_stream_resource_get_bucket_boundaries_eager_fallback(
            quantile_stream_resource_handle, num_features=num_features,
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
  num_features = _execute.make_int(num_features, "num_features")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesQuantileStreamResourceGetBucketBoundaries", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                                 num_features=num_features,
                                                                 name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("num_features", _op.get_attr("num_features"))
  _execute.record_gradient(
      "BoostedTreesQuantileStreamResourceGetBucketBoundaries", _inputs_flat, _attrs, _result, name)
  return _result



def boosted_trees_quantile_stream_resource_get_bucket_boundaries_eager_fallback(quantile_stream_resource_handle, num_features, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_quantile_stream_resource_get_bucket_boundaries
  """
  _ctx = ctx if ctx else _context.context()
  num_features = _execute.make_int(num_features, "num_features")
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  _inputs_flat = [quantile_stream_resource_handle]
  _attrs = ("num_features", num_features)
  _result = _execute.execute(b"BoostedTreesQuantileStreamResourceGetBucketBoundaries",
                             num_features, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "BoostedTreesQuantileStreamResourceGetBucketBoundaries", _inputs_flat, _attrs, _result, name)
  return _result


def boosted_trees_quantile_stream_resource_handle_op(container="", shared_name="", name=None):
  r"""Creates a handle to a BoostedTreesQuantileStreamResource.

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
        "BoostedTreesQuantileStreamResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_quantile_stream_resource_handle_op_eager_fallback(
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
        "BoostedTreesQuantileStreamResourceHandleOp", container=container,
                                                      shared_name=shared_name,
                                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "BoostedTreesQuantileStreamResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def boosted_trees_quantile_stream_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_quantile_stream_resource_handle_op
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
  _result = _execute.execute(b"BoostedTreesQuantileStreamResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesQuantileStreamResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_boosted_trees_serialize_ensemble_outputs = ["stamp_token",
                                            "tree_ensemble_serialized"]
_BoostedTreesSerializeEnsembleOutput = _collections.namedtuple(
    "BoostedTreesSerializeEnsemble",
    _boosted_trees_serialize_ensemble_outputs)


def boosted_trees_serialize_ensemble(tree_ensemble_handle, name=None):
  r"""Serializes the tree ensemble to a proto.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, tree_ensemble_serialized).

    stamp_token: A `Tensor` of type `int64`.
    tree_ensemble_serialized: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesSerializeEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle)
      _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_serialize_ensemble_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
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
        "BoostedTreesSerializeEnsemble", tree_ensemble_handle=tree_ensemble_handle,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BoostedTreesSerializeEnsemble", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
  return _result



def boosted_trees_serialize_ensemble_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_serialize_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"BoostedTreesSerializeEnsemble", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesSerializeEnsemble", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesSerializeEnsembleOutput._make(_result)
  return _result


_boosted_trees_training_predict_outputs = ["partial_logits", "tree_ids",
                                          "node_ids"]
_BoostedTreesTrainingPredictOutput = _collections.namedtuple(
    "BoostedTreesTrainingPredict", _boosted_trees_training_predict_outputs)


def boosted_trees_training_predict(tree_ensemble_handle, cached_tree_ids, cached_node_ids, bucketized_features, logits_dimension, name=None):
  r"""Runs multiple additive regression ensemble predictors on input instances and

  computes the update to cached logits. It is designed to be used during training.
  It traverses the trees starting from cached tree id and cached node id and
  calculates the updates to be pushed to the cache.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
    cached_tree_ids: A `Tensor` of type `int32`.
      Rank 1 Tensor containing cached tree ids which is the starting
      tree of prediction.
    cached_node_ids: A `Tensor` of type `int32`.
      Rank 1 Tensor containing cached node id which is the starting
      node of prediction.
    bucketized_features: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors containing bucket id for each
      feature.
    logits_dimension: An `int`.
      scalar, dimension of the logits, to be used for partial logits
      shape.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (partial_logits, tree_ids, node_ids).

    partial_logits: A `Tensor` of type `float32`.
    tree_ids: A `Tensor` of type `int32`.
    node_ids: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesTrainingPredict", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, cached_tree_ids, cached_node_ids,
        bucketized_features, "logits_dimension", logits_dimension)
      _result = _BoostedTreesTrainingPredictOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_training_predict_eager_fallback(
            tree_ensemble_handle, cached_tree_ids, cached_node_ids,
            bucketized_features, logits_dimension=logits_dimension, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_training_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesTrainingPredict", tree_ensemble_handle=tree_ensemble_handle,
                                       cached_tree_ids=cached_tree_ids,
                                       cached_node_ids=cached_node_ids,
                                       bucketized_features=bucketized_features,
                                       logits_dimension=logits_dimension,
                                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_bucketized_features",
            _op.get_attr("num_bucketized_features"), "logits_dimension",
            _op.get_attr("logits_dimension"))
  _execute.record_gradient(
      "BoostedTreesTrainingPredict", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesTrainingPredictOutput._make(_result)
  return _result



def boosted_trees_training_predict_eager_fallback(tree_ensemble_handle, cached_tree_ids, cached_node_ids, bucketized_features, logits_dimension, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_training_predict
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(bucketized_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'bucketized_features' argument to "
        "'boosted_trees_training_predict' Op, not %r." % bucketized_features)
  _attr_num_bucketized_features = len(bucketized_features)
  logits_dimension = _execute.make_int(logits_dimension, "logits_dimension")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  cached_tree_ids = _ops.convert_to_tensor(cached_tree_ids, _dtypes.int32)
  cached_node_ids = _ops.convert_to_tensor(cached_node_ids, _dtypes.int32)
  bucketized_features = _ops.convert_n_to_tensor(bucketized_features, _dtypes.int32)
  _inputs_flat = [tree_ensemble_handle, cached_tree_ids, cached_node_ids] + list(bucketized_features)
  _attrs = ("num_bucketized_features", _attr_num_bucketized_features,
  "logits_dimension", logits_dimension)
  _result = _execute.execute(b"BoostedTreesTrainingPredict", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BoostedTreesTrainingPredict", _inputs_flat, _attrs, _result, name)
  _result = _BoostedTreesTrainingPredictOutput._make(_result)
  return _result


def boosted_trees_update_ensemble(tree_ensemble_handle, feature_ids, node_ids, gains, thresholds, left_node_contribs, right_node_contribs, max_depth, learning_rate, pruning_mode, name=None):
  r"""Updates the tree ensemble by either adding a layer to the last tree being grown

  or by starting a new tree.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the ensemble variable.
    feature_ids: A `Tensor` of type `int32`.
      Rank 1 tensor with ids for each feature. This is the real id of
      the feature that will be used in the split.
    node_ids: A list of `Tensor` objects with type `int32`.
      List of rank 1 tensors representing the nodes for which this feature
      has a split.
    gains: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 1 tensors representing the gains for each of the feature's
      split.
    thresholds: A list with the same length as `node_ids` of `Tensor` objects with type `int32`.
      List of rank 1 tensors representing the thesholds for each of the
      feature's split.
    left_node_contribs: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 2 tensors with left leaf contribs for each of
      the feature's splits. Will be added to the previous node values to constitute
      the values of the left nodes.
    right_node_contribs: A list with the same length as `node_ids` of `Tensor` objects with type `float32`.
      List of rank 2 tensors with right leaf contribs for each
      of the feature's splits. Will be added to the previous node values to constitute
      the values of the right nodes.
    max_depth: A `Tensor` of type `int32`. Max depth of the tree to build.
    learning_rate: A `Tensor` of type `float32`.
      shrinkage const for each new tree.
    pruning_mode: An `int` that is `>= 0`.
      0-No pruning, 1-Pre-pruning, 2-Post-pruning.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BoostedTreesUpdateEnsemble", name, _ctx._post_execution_callbacks,
        tree_ensemble_handle, feature_ids, node_ids, gains, thresholds,
        left_node_contribs, right_node_contribs, max_depth, learning_rate,
        "pruning_mode", pruning_mode)
      return _result
    except _core._FallbackException:
      try:
        return boosted_trees_update_ensemble_eager_fallback(
            tree_ensemble_handle, feature_ids, node_ids, gains, thresholds,
            left_node_contribs, right_node_contribs, max_depth, learning_rate,
            pruning_mode=pruning_mode, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(node_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'node_ids' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % node_ids)
  _attr_num_features = len(node_ids)
  if not isinstance(gains, (list, tuple)):
    raise TypeError(
        "Expected list for 'gains' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % gains)
  if len(gains) != _attr_num_features:
    raise ValueError(
        "List argument 'gains' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(gains), _attr_num_features))
  if not isinstance(thresholds, (list, tuple)):
    raise TypeError(
        "Expected list for 'thresholds' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % thresholds)
  if len(thresholds) != _attr_num_features:
    raise ValueError(
        "List argument 'thresholds' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(thresholds), _attr_num_features))
  if not isinstance(left_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'left_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % left_node_contribs)
  if len(left_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'left_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(left_node_contribs), _attr_num_features))
  if not isinstance(right_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'right_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % right_node_contribs)
  if len(right_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'right_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(right_node_contribs), _attr_num_features))
  pruning_mode = _execute.make_int(pruning_mode, "pruning_mode")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BoostedTreesUpdateEnsemble", tree_ensemble_handle=tree_ensemble_handle,
                                      feature_ids=feature_ids,
                                      node_ids=node_ids, gains=gains,
                                      thresholds=thresholds,
                                      left_node_contribs=left_node_contribs,
                                      right_node_contribs=right_node_contribs,
                                      max_depth=max_depth,
                                      learning_rate=learning_rate,
                                      pruning_mode=pruning_mode, name=name)
  return _op
  _result = None
  return _result



def boosted_trees_update_ensemble_eager_fallback(tree_ensemble_handle, feature_ids, node_ids, gains, thresholds, left_node_contribs, right_node_contribs, max_depth, learning_rate, pruning_mode, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function boosted_trees_update_ensemble
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(node_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'node_ids' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % node_ids)
  _attr_num_features = len(node_ids)
  if not isinstance(gains, (list, tuple)):
    raise TypeError(
        "Expected list for 'gains' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % gains)
  if len(gains) != _attr_num_features:
    raise ValueError(
        "List argument 'gains' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(gains), _attr_num_features))
  if not isinstance(thresholds, (list, tuple)):
    raise TypeError(
        "Expected list for 'thresholds' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % thresholds)
  if len(thresholds) != _attr_num_features:
    raise ValueError(
        "List argument 'thresholds' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(thresholds), _attr_num_features))
  if not isinstance(left_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'left_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % left_node_contribs)
  if len(left_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'left_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(left_node_contribs), _attr_num_features))
  if not isinstance(right_node_contribs, (list, tuple)):
    raise TypeError(
        "Expected list for 'right_node_contribs' argument to "
        "'boosted_trees_update_ensemble' Op, not %r." % right_node_contribs)
  if len(right_node_contribs) != _attr_num_features:
    raise ValueError(
        "List argument 'right_node_contribs' to 'boosted_trees_update_ensemble' Op with length %d "
        "must match length %d of argument 'node_ids'." %
        (len(right_node_contribs), _attr_num_features))
  pruning_mode = _execute.make_int(pruning_mode, "pruning_mode")
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  feature_ids = _ops.convert_to_tensor(feature_ids, _dtypes.int32)
  node_ids = _ops.convert_n_to_tensor(node_ids, _dtypes.int32)
  gains = _ops.convert_n_to_tensor(gains, _dtypes.float32)
  thresholds = _ops.convert_n_to_tensor(thresholds, _dtypes.int32)
  left_node_contribs = _ops.convert_n_to_tensor(left_node_contribs, _dtypes.float32)
  right_node_contribs = _ops.convert_n_to_tensor(right_node_contribs, _dtypes.float32)
  max_depth = _ops.convert_to_tensor(max_depth, _dtypes.int32)
  learning_rate = _ops.convert_to_tensor(learning_rate, _dtypes.float32)
  _inputs_flat = [tree_ensemble_handle, feature_ids] + list(node_ids) + list(gains) + list(thresholds) + list(left_node_contribs) + list(right_node_contribs) + [max_depth, learning_rate]
  _attrs = ("pruning_mode", pruning_mode, "num_features", _attr_num_features)
  _result = _execute.execute(b"BoostedTreesUpdateEnsemble", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def is_boosted_trees_ensemble_initialized(tree_ensemble_handle, name=None):
  r"""Checks whether a tree ensemble has been initialized.

  Args:
    tree_ensemble_handle: A `Tensor` of type `resource`.
      Handle to the tree ensemble resouce.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IsBoostedTreesEnsembleInitialized", name,
        _ctx._post_execution_callbacks, tree_ensemble_handle)
      return _result
    except _core._FallbackException:
      try:
        return is_boosted_trees_ensemble_initialized_eager_fallback(
            tree_ensemble_handle, name=name, ctx=_ctx)
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
        "IsBoostedTreesEnsembleInitialized", tree_ensemble_handle=tree_ensemble_handle,
                                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IsBoostedTreesEnsembleInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def is_boosted_trees_ensemble_initialized_eager_fallback(tree_ensemble_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_boosted_trees_ensemble_initialized
  """
  _ctx = ctx if ctx else _context.context()
  tree_ensemble_handle = _ops.convert_to_tensor(tree_ensemble_handle, _dtypes.resource)
  _inputs_flat = [tree_ensemble_handle]
  _attrs = None
  _result = _execute.execute(b"IsBoostedTreesEnsembleInitialized", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IsBoostedTreesEnsembleInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def is_boosted_trees_quantile_stream_resource_initialized(quantile_stream_resource_handle, name=None):
  r"""Checks whether a quantile stream has been initialized.

  An Op that checks if quantile stream resource is initialized.

  Args:
    quantile_stream_resource_handle: A `Tensor` of type `resource`.
      resource; The reference to quantile stream resource handle.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IsBoostedTreesQuantileStreamResourceInitialized", name,
        _ctx._post_execution_callbacks, quantile_stream_resource_handle)
      return _result
    except _core._FallbackException:
      try:
        return is_boosted_trees_quantile_stream_resource_initialized_eager_fallback(
            quantile_stream_resource_handle, name=name, ctx=_ctx)
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
        "IsBoostedTreesQuantileStreamResourceInitialized", quantile_stream_resource_handle=quantile_stream_resource_handle,
                                                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IsBoostedTreesQuantileStreamResourceInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def is_boosted_trees_quantile_stream_resource_initialized_eager_fallback(quantile_stream_resource_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function is_boosted_trees_quantile_stream_resource_initialized
  """
  _ctx = ctx if ctx else _context.context()
  quantile_stream_resource_handle = _ops.convert_to_tensor(quantile_stream_resource_handle, _dtypes.resource)
  _inputs_flat = [quantile_stream_resource_handle]
  _attrs = None
  _result = _execute.execute(b"IsBoostedTreesQuantileStreamResourceInitialized",
                             1, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IsBoostedTreesQuantileStreamResourceInitialized", _inputs_flat, _attrs, _result, name)
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
#   name: "BoostedTreesBucketize"
#   input_arg {
#     name: "float_values"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "bucket_boundaries"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "buckets"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
# }
# op {
#   name: "BoostedTreesCalculateBestGainsPerFeature"
#   input_arg {
#     name: "node_id_range"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "stats_summary_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "l1"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "tree_complexity"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_node_weight"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "node_ids_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "gains_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "thresholds_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "left_node_contribs_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "right_node_contribs_list"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   attr {
#     name: "max_splits"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "BoostedTreesCenterBias"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mean_gradients"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mean_hessians"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l1"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "l2"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "continue_centering"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesCreateEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesCreateQuantileStreamResource"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "epsilon"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "num_streams"
#     type: DT_INT64
#   }
#   attr {
#     name: "max_elements"
#     type: "int"
#     default_value {
#       i: 1099511627776
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesDeserializeEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesEnsembleResourceHandleOp"
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
#   name: "BoostedTreesExampleDebugOutputs"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "examples_debug_outputs_serialized"
#     type: DT_STRING
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesGetEnsembleStates"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "num_trees"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "num_finalized_trees"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "num_attempted_layers"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "last_layer_nodes_range"
#     type: DT_INT32
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesMakeQuantileSummaries"
#   input_arg {
#     name: "float_values"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "example_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "epsilon"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "summaries"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
# }
# op {
#   name: "BoostedTreesMakeStatsSummary"
#   input_arg {
#     name: "node_ids"
#     type: DT_INT32
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
#     name: "bucketized_features_list"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   output_arg {
#     name: "stats_summary"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "max_splits"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "BoostedTreesPredict"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "logits"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesQuantileStreamResourceAddSummaries"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "summaries"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesQuantileStreamResourceDeserialize"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "bucket_boundaries"
#     type: DT_FLOAT
#     number_attr: "num_streams"
#   }
#   attr {
#     name: "num_streams"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesQuantileStreamResourceFlush"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "num_buckets"
#     type: DT_INT64
#   }
#   attr {
#     name: "generate_quantiles"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesQuantileStreamResourceGetBucketBoundaries"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "bucket_boundaries"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesQuantileStreamResourceHandleOp"
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
#   name: "BoostedTreesSerializeEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "tree_ensemble_serialized"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesTrainingPredict"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "cached_tree_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "cached_node_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bucketized_features"
#     type: DT_INT32
#     number_attr: "num_bucketized_features"
#   }
#   output_arg {
#     name: "partial_logits"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "tree_ids"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "node_ids"
#     type: DT_INT32
#   }
#   attr {
#     name: "num_bucketized_features"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "logits_dimension"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "BoostedTreesUpdateEnsemble"
#   input_arg {
#     name: "tree_ensemble_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "feature_ids"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "node_ids"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "gains"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "thresholds"
#     type: DT_INT32
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "left_node_contribs"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "right_node_contribs"
#     type: DT_FLOAT
#     number_attr: "num_features"
#   }
#   input_arg {
#     name: "max_depth"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "learning_rate"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "pruning_mode"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "IsBoostedTreesEnsembleInitialized"
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
#   name: "IsBoostedTreesQuantileStreamResourceInitialized"
#   input_arg {
#     name: "quantile_stream_resource_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\216\001\n\025BoostedTreesBucketize\022\036\n\014float_values\030\001*\014num_features\022#\n\021bucket_boundaries\030\001*\014num_features\032\031\n\007buckets\030\003*\014num_features\"\025\n\014num_features\022\003int(\001\n\206\003\n(BoostedTreesCalculateBestGainsPerFeature\022\021\n\rnode_id_range\030\003\022$\n\022stats_summary_list\030\001*\014num_features\022\006\n\002l1\030\001\022\006\n\002l2\030\001\022\023\n\017tree_complexity\030\001\022\023\n\017min_node_weight\030\001\032\037\n\rnode_ids_list\030\003*\014num_features\032\034\n\ngains_list\030\001*\014num_features\032!\n\017thresholds_list\030\003*\014num_features\032)\n\027left_node_contribs_list\030\001*\014num_features\032*\n\030right_node_contribs_list\030\001*\014num_features\"\025\n\nmax_splits\022\003int(\0010\001\"\027\n\014num_features\022\003int(\0010\001\n\204\001\n\026BoostedTreesCenterBias\022\030\n\024tree_ensemble_handle\030\024\022\022\n\016mean_gradients\030\001\022\021\n\rmean_hessians\030\001\022\006\n\002l1\030\001\022\006\n\002l2\030\001\032\026\n\022continue_centering\030\n\210\001\001\nh\n\032BoostedTreesCreateEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\034\n\030tree_ensemble_serialized\030\007\210\001\001\n\216\001\n(BoostedTreesCreateQuantileStreamResource\022#\n\037quantile_stream_resource_handle\030\024\022\013\n\007epsilon\030\001\022\017\n\013num_streams\030\t\"\034\n\014max_elements\022\003int\032\007\030\200\200\200\200\200 \210\001\001\nm\n\037BoostedTreesDeserializeEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013stamp_token\030\t\022\034\n\030tree_ensemble_serialized\030\007\210\001\001\nk\n$BoostedTreesEnsembleResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\324\001\n\037BoostedTreesExampleDebugOutputs\022\030\n\024tree_ensemble_handle\030\024\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032%\n!examples_debug_outputs_serialized\030\007\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\n\253\001\n\035BoostedTreesGetEnsembleStates\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\032\r\n\tnum_trees\030\003\032\027\n\023num_finalized_trees\030\003\032\030\n\024num_attempted_layers\030\003\032\032\n\026last_layer_nodes_range\030\003\210\001\001\n\231\001\n!BoostedTreesMakeQuantileSummaries\022\036\n\014float_values\030\001*\014num_features\022\023\n\017example_weights\030\001\022\013\n\007epsilon\030\001\032\033\n\tsummaries\030\001*\014num_features\"\025\n\014num_features\022\003int(\001\n\320\001\n\034BoostedTreesMakeStatsSummary\022\014\n\010node_ids\030\003\022\r\n\tgradients\030\001\022\014\n\010hessians\030\001\022*\n\030bucketized_features_list\030\003*\014num_features\032\021\n\rstats_summary\030\001\"\025\n\nmax_splits\022\003int(\0010\001\"\026\n\013num_buckets\022\003int(\0010\001\"\027\n\014num_features\022\003int(\0010\001\n\255\001\n\023BoostedTreesPredict\022\030\n\024tree_ensemble_handle\030\024\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032\n\n\006logits\030\001\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\n\214\001\n.BoostedTreesQuantileStreamResourceAddSummaries\022#\n\037quantile_stream_resource_handle\030\024\022\033\n\tsummaries\030\001*\014num_features\"\025\n\014num_features\022\003int(\001\210\001\001\n\223\001\n-BoostedTreesQuantileStreamResourceDeserialize\022#\n\037quantile_stream_resource_handle\030\024\022\"\n\021bucket_boundaries\030\001*\013num_streams\"\026\n\013num_streams\022\003int(\0010\001\210\001\001\n\202\001\n\'BoostedTreesQuantileStreamResourceFlush\022#\n\037quantile_stream_resource_handle\030\024\022\017\n\013num_buckets\030\t\"\036\n\022generate_quantiles\022\004bool\032\002(\000\210\001\001\n\233\001\n5BoostedTreesQuantileStreamResourceGetBucketBoundaries\022#\n\037quantile_stream_resource_handle\030\024\032#\n\021bucket_boundaries\030\001*\014num_features\"\025\n\014num_features\022\003int(\001\210\001\001\nq\n*BoostedTreesQuantileStreamResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nk\n\035BoostedTreesSerializeEnsemble\022\030\n\024tree_ensemble_handle\030\024\032\017\n\013stamp_token\030\t\032\034\n\030tree_ensemble_serialized\030\007\210\001\001\n\203\002\n\033BoostedTreesTrainingPredict\022\030\n\024tree_ensemble_handle\030\024\022\023\n\017cached_tree_ids\030\003\022\023\n\017cached_node_ids\030\003\0220\n\023bucketized_features\030\003*\027num_bucketized_features\032\022\n\016partial_logits\030\001\032\014\n\010tree_ids\030\003\032\014\n\010node_ids\030\003\"\"\n\027num_bucketized_features\022\003int(\0010\001\"\027\n\020logits_dimension\022\003int\210\001\001\n\272\002\n\032BoostedTreesUpdateEnsemble\022\030\n\024tree_ensemble_handle\030\024\022\017\n\013feature_ids\030\003\022\032\n\010node_ids\030\003*\014num_features\022\027\n\005gains\030\001*\014num_features\022\034\n\nthresholds\030\003*\014num_features\022$\n\022left_node_contribs\030\001*\014num_features\022%\n\023right_node_contribs\030\001*\014num_features\022\r\n\tmax_depth\030\003\022\021\n\rlearning_rate\030\001\"\025\n\014pruning_mode\022\003int(\001\"\025\n\014num_features\022\003int(\001\210\001\001\nT\n!IsBoostedTreesEnsembleInitialized\022\030\n\024tree_ensemble_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\nm\n/IsBoostedTreesQuantileStreamResourceInitialized\022#\n\037quantile_stream_resource_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001")
