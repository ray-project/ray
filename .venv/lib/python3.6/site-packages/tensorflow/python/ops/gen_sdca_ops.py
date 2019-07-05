"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: sdca_ops.cc
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
@tf_export('train.sdca_fprint')
def sdca_fprint(input, name=None):
  r"""Computes fingerprints of the input strings.

  Args:
    input: A `Tensor` of type `string`.
      vector of strings to compute fingerprints on.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SdcaFprint",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return sdca_fprint_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sdca_fprint, input=input, name=name)
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
        "SdcaFprint", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sdca_fprint, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "SdcaFprint", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sdca_fprint_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sdca_fprint
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"SdcaFprint", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SdcaFprint", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sdca_optimizer_outputs = ["out_example_state_data",
                          "out_delta_sparse_weights",
                          "out_delta_dense_weights"]
_SdcaOptimizerOutput = _collections.namedtuple(
    "SdcaOptimizer", _sdca_optimizer_outputs)


@_dispatch.add_dispatch_list
@tf_export('train.sdca_optimizer')
def sdca_optimizer(sparse_example_indices, sparse_feature_indices, sparse_feature_values, dense_features, example_weights, example_labels, sparse_indices, sparse_weights, dense_weights, example_state_data, loss_type, l1, l2, num_loss_partitions, num_inner_iterations, adaptative=True, name=None):
  r"""Distributed version of Stochastic Dual Coordinate Ascent (SDCA) optimizer for

  linear models with L1 + L2 regularization. As global optimization objective is
  strongly-convex, the optimizer optimizes the dual objective at each step. The
  optimizer applies each update one example at a time. Examples are sampled
  uniformly, and the optimizer is learning rate free and enjoys linear convergence
  rate.

  [Proximal Stochastic Dual Coordinate Ascent](http://arxiv.org/pdf/1211.2717v1.pdf).<br>
  Shai Shalev-Shwartz, Tong Zhang. 2012

  $$Loss Objective = \sum f_{i} (wx_{i}) + (l2 / 2) * |w|^2 + l1 * |w|$$

  [Adding vs. Averaging in Distributed Primal-Dual Optimization](http://arxiv.org/abs/1502.03508).<br>
  Chenxin Ma, Virginia Smith, Martin Jaggi, Michael I. Jordan,
  Peter Richtarik, Martin Takac. 2015

  [Stochastic Dual Coordinate Ascent with Adaptive Probabilities](https://arxiv.org/abs/1502.08053).<br>
  Dominik Csiba, Zheng Qu, Peter Richtarik. 2015

  Args:
    sparse_example_indices: A list of `Tensor` objects with type `int64`.
      a list of vectors which contain example indices.
    sparse_feature_indices: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `int64`.
      a list of vectors which contain feature indices.
    sparse_feature_values: A list of `Tensor` objects with type `float32`.
      a list of vectors which contains feature value
      associated with each feature group.
    dense_features: A list of `Tensor` objects with type `float32`.
      a list of matrices which contains the dense feature values.
    example_weights: A `Tensor` of type `float32`.
      a vector which contains the weight associated with each
      example.
    example_labels: A `Tensor` of type `float32`.
      a vector which contains the label/target associated with each
      example.
    sparse_indices: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `int64`.
      a list of vectors where each value is the indices which has
      corresponding weights in sparse_weights. This field maybe omitted for the
      dense approach.
    sparse_weights: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `float32`.
      a list of vectors where each value is the weight associated with
      a sparse feature group.
    dense_weights: A list with the same length as `dense_features` of `Tensor` objects with type `float32`.
      a list of vectors where the values are the weights associated
      with a dense feature group.
    example_state_data: A `Tensor` of type `float32`.
      a list of vectors containing the example state data.
    loss_type: A `string` from: `"logistic_loss", "squared_loss", "hinge_loss", "smooth_hinge_loss", "poisson_loss"`.
      Type of the primal loss. Currently SdcaSolver supports logistic,
      squared and hinge losses.
    l1: A `float`. Symmetric l1 regularization strength.
    l2: A `float`. Symmetric l2 regularization strength.
    num_loss_partitions: An `int` that is `>= 1`.
      Number of partitions of the global loss function.
    num_inner_iterations: An `int` that is `>= 1`.
      Number of iterations per mini-batch.
    adaptative: An optional `bool`. Defaults to `True`.
      Whether to use Adaptive SDCA for the inner loop.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (out_example_state_data, out_delta_sparse_weights, out_delta_dense_weights).

    out_example_state_data: A `Tensor` of type `float32`.
    out_delta_sparse_weights: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `float32`.
    out_delta_dense_weights: A list with the same length as `dense_features` of `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SdcaOptimizer", name, _ctx._post_execution_callbacks,
        sparse_example_indices, sparse_feature_indices, sparse_feature_values,
        dense_features, example_weights, example_labels, sparse_indices,
        sparse_weights, dense_weights, example_state_data, "loss_type",
        loss_type, "adaptative", adaptative, "l1", l1, "l2", l2,
        "num_loss_partitions", num_loss_partitions, "num_inner_iterations",
        num_inner_iterations)
      _result = _SdcaOptimizerOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sdca_optimizer_eager_fallback(
            sparse_example_indices, sparse_feature_indices,
            sparse_feature_values, dense_features, example_weights,
            example_labels, sparse_indices, sparse_weights, dense_weights,
            example_state_data, loss_type=loss_type, adaptative=adaptative,
            l1=l1, l2=l2, num_loss_partitions=num_loss_partitions,
            num_inner_iterations=num_inner_iterations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              sdca_optimizer, sparse_example_indices=sparse_example_indices,
                              sparse_feature_indices=sparse_feature_indices,
                              sparse_feature_values=sparse_feature_values,
                              dense_features=dense_features,
                              example_weights=example_weights,
                              example_labels=example_labels,
                              sparse_indices=sparse_indices,
                              sparse_weights=sparse_weights,
                              dense_weights=dense_weights,
                              example_state_data=example_state_data,
                              loss_type=loss_type, l1=l1, l2=l2,
                              num_loss_partitions=num_loss_partitions,
                              num_inner_iterations=num_inner_iterations,
                              adaptative=adaptative, name=name)
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
  if not isinstance(sparse_example_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_example_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_example_indices)
  _attr_num_sparse_features = len(sparse_example_indices)
  if not isinstance(sparse_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_feature_indices)
  if len(sparse_feature_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_feature_indices' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_feature_indices), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_indices), _attr_num_sparse_features))
  if not isinstance(sparse_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_weights' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_weights)
  if len(sparse_weights) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_weights' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_weights), _attr_num_sparse_features))
  if not isinstance(sparse_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_values' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_feature_values)
  _attr_num_sparse_features_with_values = len(sparse_feature_values)
  if not isinstance(dense_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_features' argument to "
        "'sdca_optimizer' Op, not %r." % dense_features)
  _attr_num_dense_features = len(dense_features)
  if not isinstance(dense_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_weights' argument to "
        "'sdca_optimizer' Op, not %r." % dense_weights)
  if len(dense_weights) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_weights' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'dense_features'." %
        (len(dense_weights), _attr_num_dense_features))
  loss_type = _execute.make_str(loss_type, "loss_type")
  l1 = _execute.make_float(l1, "l1")
  l2 = _execute.make_float(l2, "l2")
  num_loss_partitions = _execute.make_int(num_loss_partitions, "num_loss_partitions")
  num_inner_iterations = _execute.make_int(num_inner_iterations, "num_inner_iterations")
  if adaptative is None:
    adaptative = True
  adaptative = _execute.make_bool(adaptative, "adaptative")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SdcaOptimizer", sparse_example_indices=sparse_example_indices,
                         sparse_feature_indices=sparse_feature_indices,
                         sparse_feature_values=sparse_feature_values,
                         dense_features=dense_features,
                         example_weights=example_weights,
                         example_labels=example_labels,
                         sparse_indices=sparse_indices,
                         sparse_weights=sparse_weights,
                         dense_weights=dense_weights,
                         example_state_data=example_state_data,
                         loss_type=loss_type, l1=l1, l2=l2,
                         num_loss_partitions=num_loss_partitions,
                         num_inner_iterations=num_inner_iterations,
                         adaptative=adaptative, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sdca_optimizer, sparse_example_indices=sparse_example_indices,
                          sparse_feature_indices=sparse_feature_indices,
                          sparse_feature_values=sparse_feature_values,
                          dense_features=dense_features,
                          example_weights=example_weights,
                          example_labels=example_labels,
                          sparse_indices=sparse_indices,
                          sparse_weights=sparse_weights,
                          dense_weights=dense_weights,
                          example_state_data=example_state_data,
                          loss_type=loss_type, l1=l1, l2=l2,
                          num_loss_partitions=num_loss_partitions,
                          num_inner_iterations=num_inner_iterations,
                          adaptative=adaptative, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("loss_type", _op.get_attr("loss_type"), "adaptative",
            _op.get_attr("adaptative"), "num_sparse_features",
            _op.get_attr("num_sparse_features"),
            "num_sparse_features_with_values",
            _op.get_attr("num_sparse_features_with_values"),
            "num_dense_features", _op.get_attr("num_dense_features"), "l1",
            _op.get_attr("l1"), "l2", _op.get_attr("l2"),
            "num_loss_partitions", _op.get_attr("num_loss_partitions"),
            "num_inner_iterations", _op.get_attr("num_inner_iterations"))
  _execute.record_gradient(
      "SdcaOptimizer", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:1 + _attr_num_sparse_features]] + _result[1 + _attr_num_sparse_features:]
  _result = _result[:2] + [_result[2:]]
  _result = _SdcaOptimizerOutput._make(_result)
  return _result



def sdca_optimizer_eager_fallback(sparse_example_indices, sparse_feature_indices, sparse_feature_values, dense_features, example_weights, example_labels, sparse_indices, sparse_weights, dense_weights, example_state_data, loss_type, l1, l2, num_loss_partitions, num_inner_iterations, adaptative=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sdca_optimizer
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sparse_example_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_example_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_example_indices)
  _attr_num_sparse_features = len(sparse_example_indices)
  if not isinstance(sparse_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_feature_indices)
  if len(sparse_feature_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_feature_indices' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_feature_indices), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_indices), _attr_num_sparse_features))
  if not isinstance(sparse_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_weights' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_weights)
  if len(sparse_weights) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_weights' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_weights), _attr_num_sparse_features))
  if not isinstance(sparse_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_values' argument to "
        "'sdca_optimizer' Op, not %r." % sparse_feature_values)
  _attr_num_sparse_features_with_values = len(sparse_feature_values)
  if not isinstance(dense_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_features' argument to "
        "'sdca_optimizer' Op, not %r." % dense_features)
  _attr_num_dense_features = len(dense_features)
  if not isinstance(dense_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_weights' argument to "
        "'sdca_optimizer' Op, not %r." % dense_weights)
  if len(dense_weights) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_weights' to 'sdca_optimizer' Op with length %d "
        "must match length %d of argument 'dense_features'." %
        (len(dense_weights), _attr_num_dense_features))
  loss_type = _execute.make_str(loss_type, "loss_type")
  l1 = _execute.make_float(l1, "l1")
  l2 = _execute.make_float(l2, "l2")
  num_loss_partitions = _execute.make_int(num_loss_partitions, "num_loss_partitions")
  num_inner_iterations = _execute.make_int(num_inner_iterations, "num_inner_iterations")
  if adaptative is None:
    adaptative = True
  adaptative = _execute.make_bool(adaptative, "adaptative")
  sparse_example_indices = _ops.convert_n_to_tensor(sparse_example_indices, _dtypes.int64)
  sparse_feature_indices = _ops.convert_n_to_tensor(sparse_feature_indices, _dtypes.int64)
  sparse_feature_values = _ops.convert_n_to_tensor(sparse_feature_values, _dtypes.float32)
  dense_features = _ops.convert_n_to_tensor(dense_features, _dtypes.float32)
  example_weights = _ops.convert_to_tensor(example_weights, _dtypes.float32)
  example_labels = _ops.convert_to_tensor(example_labels, _dtypes.float32)
  sparse_indices = _ops.convert_n_to_tensor(sparse_indices, _dtypes.int64)
  sparse_weights = _ops.convert_n_to_tensor(sparse_weights, _dtypes.float32)
  dense_weights = _ops.convert_n_to_tensor(dense_weights, _dtypes.float32)
  example_state_data = _ops.convert_to_tensor(example_state_data, _dtypes.float32)
  _inputs_flat = list(sparse_example_indices) + list(sparse_feature_indices) + list(sparse_feature_values) + list(dense_features) + [example_weights, example_labels] + list(sparse_indices) + list(sparse_weights) + list(dense_weights) + [example_state_data]
  _attrs = ("loss_type", loss_type, "adaptative", adaptative,
  "num_sparse_features", _attr_num_sparse_features,
  "num_sparse_features_with_values", _attr_num_sparse_features_with_values,
  "num_dense_features", _attr_num_dense_features, "l1", l1, "l2", l2,
  "num_loss_partitions", num_loss_partitions, "num_inner_iterations",
  num_inner_iterations)
  _result = _execute.execute(b"SdcaOptimizer", _attr_num_sparse_features +
                             _attr_num_dense_features + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SdcaOptimizer", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:1 + _attr_num_sparse_features]] + _result[1 + _attr_num_sparse_features:]
  _result = _result[:2] + [_result[2:]]
  _result = _SdcaOptimizerOutput._make(_result)
  return _result


_sdca_optimizer_v2_outputs = ["out_example_state_data",
                             "out_delta_sparse_weights",
                             "out_delta_dense_weights"]
_SdcaOptimizerV2Output = _collections.namedtuple(
    "SdcaOptimizerV2", _sdca_optimizer_v2_outputs)


def sdca_optimizer_v2(sparse_example_indices, sparse_feature_indices, sparse_feature_values, dense_features, example_weights, example_labels, sparse_indices, sparse_weights, dense_weights, example_state_data, loss_type, l1, l2, num_loss_partitions, num_inner_iterations, adaptive=True, name=None):
  r"""Distributed version of Stochastic Dual Coordinate Ascent (SDCA) optimizer for

  linear models with L1 + L2 regularization. As global optimization objective is
  strongly-convex, the optimizer optimizes the dual objective at each step. The
  optimizer applies each update one example at a time. Examples are sampled
  uniformly, and the optimizer is learning rate free and enjoys linear convergence
  rate.

  [Proximal Stochastic Dual Coordinate Ascent](http://arxiv.org/pdf/1211.2717v1.pdf).<br>
  Shai Shalev-Shwartz, Tong Zhang. 2012

  $$Loss Objective = \sum f_{i} (wx_{i}) + (l2 / 2) * |w|^2 + l1 * |w|$$

  [Adding vs. Averaging in Distributed Primal-Dual Optimization](http://arxiv.org/abs/1502.03508).<br>
  Chenxin Ma, Virginia Smith, Martin Jaggi, Michael I. Jordan,
  Peter Richtarik, Martin Takac. 2015

  [Stochastic Dual Coordinate Ascent with Adaptive Probabilities](https://arxiv.org/abs/1502.08053).<br>
  Dominik Csiba, Zheng Qu, Peter Richtarik. 2015

  Args:
    sparse_example_indices: A list of `Tensor` objects with type `int64`.
      a list of vectors which contain example indices.
    sparse_feature_indices: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `int64`.
      a list of vectors which contain feature indices.
    sparse_feature_values: A list of `Tensor` objects with type `float32`.
      a list of vectors which contains feature value
      associated with each feature group.
    dense_features: A list of `Tensor` objects with type `float32`.
      a list of matrices which contains the dense feature values.
    example_weights: A `Tensor` of type `float32`.
      a vector which contains the weight associated with each
      example.
    example_labels: A `Tensor` of type `float32`.
      a vector which contains the label/target associated with each
      example.
    sparse_indices: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `int64`.
      a list of vectors where each value is the indices which has
      corresponding weights in sparse_weights. This field maybe omitted for the
      dense approach.
    sparse_weights: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `float32`.
      a list of vectors where each value is the weight associated with
      a sparse feature group.
    dense_weights: A list with the same length as `dense_features` of `Tensor` objects with type `float32`.
      a list of vectors where the values are the weights associated
      with a dense feature group.
    example_state_data: A `Tensor` of type `float32`.
      a list of vectors containing the example state data.
    loss_type: A `string` from: `"logistic_loss", "squared_loss", "hinge_loss", "smooth_hinge_loss", "poisson_loss"`.
      Type of the primal loss. Currently SdcaSolver supports logistic,
      squared and hinge losses.
    l1: A `float`. Symmetric l1 regularization strength.
    l2: A `float`. Symmetric l2 regularization strength.
    num_loss_partitions: An `int` that is `>= 1`.
      Number of partitions of the global loss function.
    num_inner_iterations: An `int` that is `>= 1`.
      Number of iterations per mini-batch.
    adaptive: An optional `bool`. Defaults to `True`.
      Whether to use Adaptive SDCA for the inner loop.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (out_example_state_data, out_delta_sparse_weights, out_delta_dense_weights).

    out_example_state_data: A `Tensor` of type `float32`.
    out_delta_sparse_weights: A list with the same length as `sparse_example_indices` of `Tensor` objects with type `float32`.
    out_delta_dense_weights: A list with the same length as `dense_features` of `Tensor` objects with type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SdcaOptimizerV2", name, _ctx._post_execution_callbacks,
        sparse_example_indices, sparse_feature_indices, sparse_feature_values,
        dense_features, example_weights, example_labels, sparse_indices,
        sparse_weights, dense_weights, example_state_data, "loss_type",
        loss_type, "adaptive", adaptive, "l1", l1, "l2", l2,
        "num_loss_partitions", num_loss_partitions, "num_inner_iterations",
        num_inner_iterations)
      _result = _SdcaOptimizerV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sdca_optimizer_v2_eager_fallback(
            sparse_example_indices, sparse_feature_indices,
            sparse_feature_values, dense_features, example_weights,
            example_labels, sparse_indices, sparse_weights, dense_weights,
            example_state_data, loss_type=loss_type, adaptive=adaptive, l1=l1,
            l2=l2, num_loss_partitions=num_loss_partitions,
            num_inner_iterations=num_inner_iterations, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(sparse_example_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_example_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_example_indices)
  _attr_num_sparse_features = len(sparse_example_indices)
  if not isinstance(sparse_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_feature_indices)
  if len(sparse_feature_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_feature_indices' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_feature_indices), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_indices), _attr_num_sparse_features))
  if not isinstance(sparse_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_weights' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_weights)
  if len(sparse_weights) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_weights' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_weights), _attr_num_sparse_features))
  if not isinstance(sparse_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_values' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_feature_values)
  _attr_num_sparse_features_with_values = len(sparse_feature_values)
  if not isinstance(dense_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_features' argument to "
        "'sdca_optimizer_v2' Op, not %r." % dense_features)
  _attr_num_dense_features = len(dense_features)
  if not isinstance(dense_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_weights' argument to "
        "'sdca_optimizer_v2' Op, not %r." % dense_weights)
  if len(dense_weights) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_weights' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'dense_features'." %
        (len(dense_weights), _attr_num_dense_features))
  loss_type = _execute.make_str(loss_type, "loss_type")
  l1 = _execute.make_float(l1, "l1")
  l2 = _execute.make_float(l2, "l2")
  num_loss_partitions = _execute.make_int(num_loss_partitions, "num_loss_partitions")
  num_inner_iterations = _execute.make_int(num_inner_iterations, "num_inner_iterations")
  if adaptive is None:
    adaptive = True
  adaptive = _execute.make_bool(adaptive, "adaptive")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SdcaOptimizerV2", sparse_example_indices=sparse_example_indices,
                           sparse_feature_indices=sparse_feature_indices,
                           sparse_feature_values=sparse_feature_values,
                           dense_features=dense_features,
                           example_weights=example_weights,
                           example_labels=example_labels,
                           sparse_indices=sparse_indices,
                           sparse_weights=sparse_weights,
                           dense_weights=dense_weights,
                           example_state_data=example_state_data,
                           loss_type=loss_type, l1=l1, l2=l2,
                           num_loss_partitions=num_loss_partitions,
                           num_inner_iterations=num_inner_iterations,
                           adaptive=adaptive, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("loss_type", _op.get_attr("loss_type"), "adaptive",
            _op.get_attr("adaptive"), "num_sparse_features",
            _op.get_attr("num_sparse_features"),
            "num_sparse_features_with_values",
            _op.get_attr("num_sparse_features_with_values"),
            "num_dense_features", _op.get_attr("num_dense_features"), "l1",
            _op.get_attr("l1"), "l2", _op.get_attr("l2"),
            "num_loss_partitions", _op.get_attr("num_loss_partitions"),
            "num_inner_iterations", _op.get_attr("num_inner_iterations"))
  _execute.record_gradient(
      "SdcaOptimizerV2", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:1 + _attr_num_sparse_features]] + _result[1 + _attr_num_sparse_features:]
  _result = _result[:2] + [_result[2:]]
  _result = _SdcaOptimizerV2Output._make(_result)
  return _result



def sdca_optimizer_v2_eager_fallback(sparse_example_indices, sparse_feature_indices, sparse_feature_values, dense_features, example_weights, example_labels, sparse_indices, sparse_weights, dense_weights, example_state_data, loss_type, l1, l2, num_loss_partitions, num_inner_iterations, adaptive=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sdca_optimizer_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sparse_example_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_example_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_example_indices)
  _attr_num_sparse_features = len(sparse_example_indices)
  if not isinstance(sparse_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_feature_indices)
  if len(sparse_feature_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_feature_indices' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_feature_indices), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_indices), _attr_num_sparse_features))
  if not isinstance(sparse_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_weights' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_weights)
  if len(sparse_weights) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_weights' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'sparse_example_indices'." %
        (len(sparse_weights), _attr_num_sparse_features))
  if not isinstance(sparse_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_feature_values' argument to "
        "'sdca_optimizer_v2' Op, not %r." % sparse_feature_values)
  _attr_num_sparse_features_with_values = len(sparse_feature_values)
  if not isinstance(dense_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_features' argument to "
        "'sdca_optimizer_v2' Op, not %r." % dense_features)
  _attr_num_dense_features = len(dense_features)
  if not isinstance(dense_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_weights' argument to "
        "'sdca_optimizer_v2' Op, not %r." % dense_weights)
  if len(dense_weights) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_weights' to 'sdca_optimizer_v2' Op with length %d "
        "must match length %d of argument 'dense_features'." %
        (len(dense_weights), _attr_num_dense_features))
  loss_type = _execute.make_str(loss_type, "loss_type")
  l1 = _execute.make_float(l1, "l1")
  l2 = _execute.make_float(l2, "l2")
  num_loss_partitions = _execute.make_int(num_loss_partitions, "num_loss_partitions")
  num_inner_iterations = _execute.make_int(num_inner_iterations, "num_inner_iterations")
  if adaptive is None:
    adaptive = True
  adaptive = _execute.make_bool(adaptive, "adaptive")
  sparse_example_indices = _ops.convert_n_to_tensor(sparse_example_indices, _dtypes.int64)
  sparse_feature_indices = _ops.convert_n_to_tensor(sparse_feature_indices, _dtypes.int64)
  sparse_feature_values = _ops.convert_n_to_tensor(sparse_feature_values, _dtypes.float32)
  dense_features = _ops.convert_n_to_tensor(dense_features, _dtypes.float32)
  example_weights = _ops.convert_to_tensor(example_weights, _dtypes.float32)
  example_labels = _ops.convert_to_tensor(example_labels, _dtypes.float32)
  sparse_indices = _ops.convert_n_to_tensor(sparse_indices, _dtypes.int64)
  sparse_weights = _ops.convert_n_to_tensor(sparse_weights, _dtypes.float32)
  dense_weights = _ops.convert_n_to_tensor(dense_weights, _dtypes.float32)
  example_state_data = _ops.convert_to_tensor(example_state_data, _dtypes.float32)
  _inputs_flat = list(sparse_example_indices) + list(sparse_feature_indices) + list(sparse_feature_values) + list(dense_features) + [example_weights, example_labels] + list(sparse_indices) + list(sparse_weights) + list(dense_weights) + [example_state_data]
  _attrs = ("loss_type", loss_type, "adaptive", adaptive,
  "num_sparse_features", _attr_num_sparse_features,
  "num_sparse_features_with_values", _attr_num_sparse_features_with_values,
  "num_dense_features", _attr_num_dense_features, "l1", l1, "l2", l2,
  "num_loss_partitions", num_loss_partitions, "num_inner_iterations",
  num_inner_iterations)
  _result = _execute.execute(b"SdcaOptimizerV2", _attr_num_sparse_features +
                             _attr_num_dense_features + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SdcaOptimizerV2", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:1 + _attr_num_sparse_features]] + _result[1 + _attr_num_sparse_features:]
  _result = _result[:2] + [_result[2:]]
  _result = _SdcaOptimizerV2Output._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('train.sdca_shrink_l1')
def sdca_shrink_l1(weights, l1, l2, name=None):
  r"""Applies L1 regularization shrink step on the parameters.

  Args:
    weights: A list of `Tensor` objects with type mutable `float32`.
      a list of vectors where each value is the weight associated with a
      feature group.
    l1: A `float`. Symmetric l1 regularization strength.
    l2: A `float`.
      Symmetric l2 regularization strength. Should be a positive float.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sdca_shrink_l1 op does not support eager execution. Arg 'weights' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'weights' argument to "
        "'sdca_shrink_l1' Op, not %r." % weights)
  _attr_num_features = len(weights)
  l1 = _execute.make_float(l1, "l1")
  l2 = _execute.make_float(l2, "l2")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SdcaShrinkL1", weights=weights, l1=l1, l2=l2, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          sdca_shrink_l1, weights=weights, l1=l1, l2=l2, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def sdca_shrink_l1_eager_fallback(weights, l1, l2, name=None, ctx=None):
  raise RuntimeError("sdca_shrink_l1 op does not support eager execution. Arg 'weights' is a ref.")
def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "SdcaFprint"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT64
#   }
# }
# op {
#   name: "SdcaOptimizer"
#   input_arg {
#     name: "sparse_example_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features_with_values"
#   }
#   input_arg {
#     name: "dense_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "example_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "example_labels"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_weights"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "dense_weights"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "example_state_data"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out_example_state_data"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out_delta_sparse_weights"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   output_arg {
#     name: "out_delta_dense_weights"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   attr {
#     name: "loss_type"
#     type: "string"
#     allowed_values {
#       list {
#         s: "logistic_loss"
#         s: "squared_loss"
#         s: "hinge_loss"
#         s: "smooth_hinge_loss"
#         s: "poisson_loss"
#       }
#     }
#   }
#   attr {
#     name: "adaptative"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "num_sparse_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_features_with_values"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_dense_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "l1"
#     type: "float"
#   }
#   attr {
#     name: "l2"
#     type: "float"
#   }
#   attr {
#     name: "num_loss_partitions"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_inner_iterations"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "SdcaOptimizerV2"
#   input_arg {
#     name: "sparse_example_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features_with_values"
#   }
#   input_arg {
#     name: "dense_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "example_weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "example_labels"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_weights"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "dense_weights"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "example_state_data"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out_example_state_data"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out_delta_sparse_weights"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   output_arg {
#     name: "out_delta_dense_weights"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   attr {
#     name: "loss_type"
#     type: "string"
#     allowed_values {
#       list {
#         s: "logistic_loss"
#         s: "squared_loss"
#         s: "hinge_loss"
#         s: "smooth_hinge_loss"
#         s: "poisson_loss"
#       }
#     }
#   }
#   attr {
#     name: "adaptive"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "num_sparse_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_features_with_values"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_dense_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "l1"
#     type: "float"
#   }
#   attr {
#     name: "l2"
#     type: "float"
#   }
#   attr {
#     name: "num_loss_partitions"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_inner_iterations"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "SdcaShrinkL1"
#   input_arg {
#     name: "weights"
#     type: DT_FLOAT
#     number_attr: "num_features"
#     is_ref: true
#   }
#   attr {
#     name: "num_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "l1"
#     type: "float"
#   }
#   attr {
#     name: "l2"
#     type: "float"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n#\n\nSdcaFprint\022\t\n\005input\030\007\032\n\n\006output\030\t\n\312\006\n\rSdcaOptimizer\022/\n\026sparse_example_indices\030\t*\023num_sparse_features\022/\n\026sparse_feature_indices\030\t*\023num_sparse_features\022:\n\025sparse_feature_values\030\001*\037num_sparse_features_with_values\022&\n\016dense_features\030\001*\022num_dense_features\022\023\n\017example_weights\030\001\022\022\n\016example_labels\030\001\022\'\n\016sparse_indices\030\t*\023num_sparse_features\022\'\n\016sparse_weights\030\001*\023num_sparse_features\022%\n\rdense_weights\030\001*\022num_dense_features\022\026\n\022example_state_data\030\001\032\032\n\026out_example_state_data\030\001\0321\n\030out_delta_sparse_weights\030\001*\023num_sparse_features\032/\n\027out_delta_dense_weights\030\001*\022num_dense_features\"a\n\tloss_type\022\006string:L\nJ\022\rlogistic_loss\022\014squared_loss\022\nhinge_loss\022\021smooth_hinge_loss\022\014poisson_loss\"\026\n\nadaptative\022\004bool\032\002(\000\"\034\n\023num_sparse_features\022\003int(\001\"(\n\037num_sparse_features_with_values\022\003int(\001\"\033\n\022num_dense_features\022\003int(\001\"\013\n\002l1\022\005float\"\013\n\002l2\022\005float\"\036\n\023num_loss_partitions\022\003int(\0010\001\"\037\n\024num_inner_iterations\022\003int(\0010\001\n\312\006\n\017SdcaOptimizerV2\022/\n\026sparse_example_indices\030\t*\023num_sparse_features\022/\n\026sparse_feature_indices\030\t*\023num_sparse_features\022:\n\025sparse_feature_values\030\001*\037num_sparse_features_with_values\022&\n\016dense_features\030\001*\022num_dense_features\022\023\n\017example_weights\030\001\022\022\n\016example_labels\030\001\022\'\n\016sparse_indices\030\t*\023num_sparse_features\022\'\n\016sparse_weights\030\001*\023num_sparse_features\022%\n\rdense_weights\030\001*\022num_dense_features\022\026\n\022example_state_data\030\001\032\032\n\026out_example_state_data\030\001\0321\n\030out_delta_sparse_weights\030\001*\023num_sparse_features\032/\n\027out_delta_dense_weights\030\001*\022num_dense_features\"a\n\tloss_type\022\006string:L\nJ\022\rlogistic_loss\022\014squared_loss\022\nhinge_loss\022\021smooth_hinge_loss\022\014poisson_loss\"\024\n\010adaptive\022\004bool\032\002(\000\"\034\n\023num_sparse_features\022\003int(\001\"(\n\037num_sparse_features_with_values\022\003int(\001\"\033\n\022num_dense_features\022\003int(\001\"\013\n\002l1\022\005float\"\013\n\002l2\022\005float\"\036\n\023num_loss_partitions\022\003int(\0010\001\"\037\n\024num_inner_iterations\022\003int(\0010\001\n]\n\014SdcaShrinkL1\022\034\n\007weights\030\001*\014num_features\200\001\001\"\025\n\014num_features\022\003int(\001\"\013\n\002l1\022\005float\"\013\n\002l2\022\005float")
