"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_quantile_ops_py_wrap.cc
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
@tf_export('bucketize_with_input_boundaries')
def bucketize_with_input_boundaries(input, boundaries, name=None):
  r"""Bucketizes 'input' based on 'boundaries'. This function is similar to Bucketize

  op in core math_ops, except that boundaries are specified using an input tensor,
  as compared with a fixed attribute in Bucketize().

  For example, if the inputs are
      boundaries = [0, 10, 100]
      input = [[-5, 10000]
               [150,   10]
               [5,    100]]

  then the output will be
      output = [[0, 3]
                [3, 2]
                [1, 3]]

  Args:
    input: A `Tensor`. Must be one of the following types: `int32`, `int64`, `float32`, `float64`.
      Any shape of Tensor contains with numeric type.
    boundaries: A `Tensor` of type `float32`.
      A vector Tensor of sorted floats specifies the boundaries
      of the buckets.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
    Same shape as 'input', where each value of input is replaced with its corresponding bucket index.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BucketizeWithInputBoundaries", name, _ctx._post_execution_callbacks,
        input, boundaries)
      return _result
    except _core._FallbackException:
      try:
        return bucketize_with_input_boundaries_eager_fallback(
            input, boundaries, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bucketize_with_input_boundaries, input=input,
                                               boundaries=boundaries,
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
        "BucketizeWithInputBoundaries", input=input, boundaries=boundaries,
                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bucketize_with_input_boundaries, input=input, boundaries=boundaries,
                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "BucketizeWithInputBoundaries", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def bucketize_with_input_boundaries_eager_fallback(input, boundaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bucketize_with_input_boundaries
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  boundaries = _ops.convert_to_tensor(boundaries, _dtypes.float32)
  _inputs_flat = [input, boundaries]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"BucketizeWithInputBoundaries", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "BucketizeWithInputBoundaries", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("BucketizeWithInputBoundaries")(None)


@_dispatch.add_dispatch_list
@tf_export('create_quantile_accumulator')
def create_quantile_accumulator(quantile_accumulator_handle, stamp_token, epsilon, num_quantiles, container="", shared_name="", max_elements=1099511627776, generate_quantiles=False, name=None):
  r"""Creates a stateful accumulator for quantile summaries.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
      The handle to the accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Token to use as the initial value of the resource stamp.
    epsilon: A `float`. Error bound on the quantile summary.
    num_quantiles: An `int`. Number of buckets that we create from the data.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    max_elements: An optional `int`. Defaults to `1099511627776`.
    generate_quantiles: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CreateQuantileAccumulator", name, _ctx._post_execution_callbacks,
        quantile_accumulator_handle, stamp_token, "container", container,
        "shared_name", shared_name, "max_elements", max_elements, "epsilon",
        epsilon, "num_quantiles", num_quantiles, "generate_quantiles",
        generate_quantiles)
      return _result
    except _core._FallbackException:
      try:
        return create_quantile_accumulator_eager_fallback(
            quantile_accumulator_handle, stamp_token, container=container,
            shared_name=shared_name, max_elements=max_elements,
            epsilon=epsilon, num_quantiles=num_quantiles,
            generate_quantiles=generate_quantiles, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              create_quantile_accumulator, quantile_accumulator_handle=quantile_accumulator_handle,
                                           stamp_token=stamp_token,
                                           epsilon=epsilon,
                                           num_quantiles=num_quantiles,
                                           container=container,
                                           shared_name=shared_name,
                                           max_elements=max_elements,
                                           generate_quantiles=generate_quantiles,
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
  epsilon = _execute.make_float(epsilon, "epsilon")
  num_quantiles = _execute.make_int(num_quantiles, "num_quantiles")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if max_elements is None:
    max_elements = 1099511627776
  max_elements = _execute.make_int(max_elements, "max_elements")
  if generate_quantiles is None:
    generate_quantiles = False
  generate_quantiles = _execute.make_bool(generate_quantiles, "generate_quantiles")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "CreateQuantileAccumulator", quantile_accumulator_handle=quantile_accumulator_handle,
                                     stamp_token=stamp_token, epsilon=epsilon,
                                     num_quantiles=num_quantiles,
                                     container=container,
                                     shared_name=shared_name,
                                     max_elements=max_elements,
                                     generate_quantiles=generate_quantiles,
                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          create_quantile_accumulator, quantile_accumulator_handle=quantile_accumulator_handle,
                                       stamp_token=stamp_token,
                                       epsilon=epsilon,
                                       num_quantiles=num_quantiles,
                                       container=container,
                                       shared_name=shared_name,
                                       max_elements=max_elements,
                                       generate_quantiles=generate_quantiles,
                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def create_quantile_accumulator_eager_fallback(quantile_accumulator_handle, stamp_token, epsilon, num_quantiles, container="", shared_name="", max_elements=1099511627776, generate_quantiles=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function create_quantile_accumulator
  """
  _ctx = ctx if ctx else _context.context()
  epsilon = _execute.make_float(epsilon, "epsilon")
  num_quantiles = _execute.make_int(num_quantiles, "num_quantiles")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if max_elements is None:
    max_elements = 1099511627776
  max_elements = _execute.make_int(max_elements, "max_elements")
  if generate_quantiles is None:
    generate_quantiles = False
  generate_quantiles = _execute.make_bool(generate_quantiles, "generate_quantiles")
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  _inputs_flat = [quantile_accumulator_handle, stamp_token]
  _attrs = ("container", container, "shared_name", shared_name,
  "max_elements", max_elements, "epsilon", epsilon, "num_quantiles",
  num_quantiles, "generate_quantiles", generate_quantiles)
  _result = _execute.execute(b"CreateQuantileAccumulator", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("CreateQuantileAccumulator")(None)


_make_quantile_summaries_outputs = ["dense_summaries", "sparse_summaries"]
_MakeQuantileSummariesOutput = _collections.namedtuple(
    "MakeQuantileSummaries", _make_quantile_summaries_outputs)


@_dispatch.add_dispatch_list
@tf_export('make_quantile_summaries')
def make_quantile_summaries(dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, example_weights, epsilon, name=None):
  r"""Creates a summary for the given features.

  Args:
    dense_float_features: A list of `Tensor` objects with type `float32`.
      A list of vectors which contains dense values.
    sparse_float_feature_indices: A list of `Tensor` objects with type `int64`.
      List of rank 2 tensors containing the sparse float
      feature indices.
    sparse_float_feature_values: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`.
      List of rank 1 tensors containing the sparse float
      feature values.
    sparse_float_feature_shapes: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `int64`.
      List of rank 1 tensors containing the shape of the
      float feature.
    example_weights: A `Tensor` of type `float32`.
      Rank 2 (N, 1) tensor of per-example weights. Should match
      dense and sparse features shape.
    epsilon: A `float`. Error bound on the computed summary.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (dense_summaries, sparse_summaries).

    dense_summaries: A list with the same length as `dense_float_features` of `Tensor` objects with type `string`. A list of serialized QuantileSummaryState for dense columns.
    sparse_summaries: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `string`. A list of serialized QuantileSummaryState for sparse columns.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MakeQuantileSummaries", name, _ctx._post_execution_callbacks,
        dense_float_features, sparse_float_feature_indices,
        sparse_float_feature_values, sparse_float_feature_shapes,
        example_weights, "epsilon", epsilon)
      _result = _MakeQuantileSummariesOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return make_quantile_summaries_eager_fallback(
            dense_float_features, sparse_float_feature_indices,
            sparse_float_feature_values, sparse_float_feature_shapes,
            example_weights, epsilon=epsilon, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              make_quantile_summaries, dense_float_features=dense_float_features,
                                       sparse_float_feature_indices=sparse_float_feature_indices,
                                       sparse_float_feature_values=sparse_float_feature_values,
                                       sparse_float_feature_shapes=sparse_float_feature_shapes,
                                       example_weights=example_weights,
                                       epsilon=epsilon, name=name)
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
        "'make_quantile_summaries' Op, not %r." % dense_float_features)
  _attr_num_dense_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'make_quantile_summaries' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'make_quantile_summaries' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_features))
  epsilon = _execute.make_float(epsilon, "epsilon")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "MakeQuantileSummaries", dense_float_features=dense_float_features,
                                 sparse_float_feature_indices=sparse_float_feature_indices,
                                 sparse_float_feature_values=sparse_float_feature_values,
                                 sparse_float_feature_shapes=sparse_float_feature_shapes,
                                 example_weights=example_weights,
                                 epsilon=epsilon, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          make_quantile_summaries, dense_float_features=dense_float_features,
                                   sparse_float_feature_indices=sparse_float_feature_indices,
                                   sparse_float_feature_values=sparse_float_feature_values,
                                   sparse_float_feature_shapes=sparse_float_feature_shapes,
                                   example_weights=example_weights,
                                   epsilon=epsilon, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_dense_features", _op.get_attr("num_dense_features"),
            "num_sparse_features", _op.get_attr("num_sparse_features"),
            "epsilon", _op.get_attr("epsilon"))
  _execute.record_gradient(
      "MakeQuantileSummaries", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _MakeQuantileSummariesOutput._make(_result)
  return _result



def make_quantile_summaries_eager_fallback(dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, example_weights, epsilon, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function make_quantile_summaries
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'make_quantile_summaries' Op, not %r." % dense_float_features)
  _attr_num_dense_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'make_quantile_summaries' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'make_quantile_summaries' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'make_quantile_summaries' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_features))
  epsilon = _execute.make_float(epsilon, "epsilon")
  dense_float_features = _ops.convert_n_to_tensor(dense_float_features, _dtypes.float32)
  sparse_float_feature_indices = _ops.convert_n_to_tensor(sparse_float_feature_indices, _dtypes.int64)
  sparse_float_feature_values = _ops.convert_n_to_tensor(sparse_float_feature_values, _dtypes.float32)
  sparse_float_feature_shapes = _ops.convert_n_to_tensor(sparse_float_feature_shapes, _dtypes.int64)
  example_weights = _ops.convert_to_tensor(example_weights, _dtypes.float32)
  _inputs_flat = list(dense_float_features) + list(sparse_float_feature_indices) + list(sparse_float_feature_values) + list(sparse_float_feature_shapes) + [example_weights]
  _attrs = ("num_dense_features", _attr_num_dense_features,
  "num_sparse_features", _attr_num_sparse_features, "epsilon", epsilon)
  _result = _execute.execute(b"MakeQuantileSummaries",
                             _attr_num_dense_features +
                             _attr_num_sparse_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MakeQuantileSummaries", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _MakeQuantileSummariesOutput._make(_result)
  return _result

_ops.RegisterShape("MakeQuantileSummaries")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_add_summaries')
def quantile_accumulator_add_summaries(quantile_accumulator_handles, stamp_token, summaries, name=None):
  r"""Adds each quantile summary to its stream.

  Args:
    quantile_accumulator_handles: A list of at least 1 `Tensor` objects with type `resource`.
      The handles to the quantile stream resources.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token to validate the Read/Write operation.
    summaries: A list with the same length as `quantile_accumulator_handles` of `Tensor` objects with type `string`.
      A list of serialized QuantileSummaryState.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorAddSummaries", name,
        _ctx._post_execution_callbacks, quantile_accumulator_handles,
        stamp_token, summaries)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_add_summaries_eager_fallback(
            quantile_accumulator_handles, stamp_token, summaries, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_add_summaries, quantile_accumulator_handles=quantile_accumulator_handles,
                                                  stamp_token=stamp_token,
                                                  summaries=summaries,
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
  if not isinstance(quantile_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'quantile_accumulator_handles' argument to "
        "'quantile_accumulator_add_summaries' Op, not %r." % quantile_accumulator_handles)
  _attr_num_resource_handles = len(quantile_accumulator_handles)
  if not isinstance(summaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'summaries' argument to "
        "'quantile_accumulator_add_summaries' Op, not %r." % summaries)
  if len(summaries) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'summaries' to 'quantile_accumulator_add_summaries' Op with length %d "
        "must match length %d of argument 'quantile_accumulator_handles'." %
        (len(summaries), _attr_num_resource_handles))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "QuantileAccumulatorAddSummaries", quantile_accumulator_handles=quantile_accumulator_handles,
                                           stamp_token=stamp_token,
                                           summaries=summaries, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_add_summaries, quantile_accumulator_handles=quantile_accumulator_handles,
                                              stamp_token=stamp_token,
                                              summaries=summaries, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def quantile_accumulator_add_summaries_eager_fallback(quantile_accumulator_handles, stamp_token, summaries, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_add_summaries
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(quantile_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'quantile_accumulator_handles' argument to "
        "'quantile_accumulator_add_summaries' Op, not %r." % quantile_accumulator_handles)
  _attr_num_resource_handles = len(quantile_accumulator_handles)
  if not isinstance(summaries, (list, tuple)):
    raise TypeError(
        "Expected list for 'summaries' argument to "
        "'quantile_accumulator_add_summaries' Op, not %r." % summaries)
  if len(summaries) != _attr_num_resource_handles:
    raise ValueError(
        "List argument 'summaries' to 'quantile_accumulator_add_summaries' Op with length %d "
        "must match length %d of argument 'quantile_accumulator_handles'." %
        (len(summaries), _attr_num_resource_handles))
  quantile_accumulator_handles = _ops.convert_n_to_tensor(quantile_accumulator_handles, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  summaries = _ops.convert_n_to_tensor(summaries, _dtypes.string)
  _inputs_flat = list(quantile_accumulator_handles) + [stamp_token] + list(summaries)
  _attrs = ("num_resource_handles", _attr_num_resource_handles)
  _result = _execute.execute(b"QuantileAccumulatorAddSummaries", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("QuantileAccumulatorAddSummaries")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_deserialize')
def quantile_accumulator_deserialize(quantile_accumulator_handle, stamp_token, stream_state, are_buckets_ready, buckets, name=None):
  r"""Serializes the state of the given resource.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
      The handle to the accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    stream_state: A `Tensor` of type `string`.
      A serialized QuantileStreamState.
    are_buckets_ready: A `Tensor` of type `bool`.
      Whether the buckets are ready or not.
    buckets: A `Tensor` of type `float32`.
      Output quantile summary representing boundaries with "num_quantile"
      elements.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorDeserialize", name,
        _ctx._post_execution_callbacks, quantile_accumulator_handle,
        stamp_token, stream_state, are_buckets_ready, buckets)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_deserialize_eager_fallback(
            quantile_accumulator_handle, stamp_token, stream_state,
            are_buckets_ready, buckets, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_deserialize, quantile_accumulator_handle=quantile_accumulator_handle,
                                                stamp_token=stamp_token,
                                                stream_state=stream_state,
                                                are_buckets_ready=are_buckets_ready,
                                                buckets=buckets, name=name)
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
        "QuantileAccumulatorDeserialize", quantile_accumulator_handle=quantile_accumulator_handle,
                                          stamp_token=stamp_token,
                                          stream_state=stream_state,
                                          are_buckets_ready=are_buckets_ready,
                                          buckets=buckets, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_deserialize, quantile_accumulator_handle=quantile_accumulator_handle,
                                            stamp_token=stamp_token,
                                            stream_state=stream_state,
                                            are_buckets_ready=are_buckets_ready,
                                            buckets=buckets, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def quantile_accumulator_deserialize_eager_fallback(quantile_accumulator_handle, stamp_token, stream_state, are_buckets_ready, buckets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_deserialize
  """
  _ctx = ctx if ctx else _context.context()
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  stream_state = _ops.convert_to_tensor(stream_state, _dtypes.string)
  are_buckets_ready = _ops.convert_to_tensor(are_buckets_ready, _dtypes.bool)
  buckets = _ops.convert_to_tensor(buckets, _dtypes.float32)
  _inputs_flat = [quantile_accumulator_handle, stamp_token, stream_state, are_buckets_ready, buckets]
  _attrs = None
  _result = _execute.execute(b"QuantileAccumulatorDeserialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("QuantileAccumulatorDeserialize")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_flush')
def quantile_accumulator_flush(quantile_accumulator_handle, stamp_token, next_stamp_token, name=None):
  r"""Resets quantile summary streams for each column with a new token.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
      The handle to the accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token to be used for the next iteration.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorFlush", name, _ctx._post_execution_callbacks,
        quantile_accumulator_handle, stamp_token, next_stamp_token)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_flush_eager_fallback(
            quantile_accumulator_handle, stamp_token, next_stamp_token,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_flush, quantile_accumulator_handle=quantile_accumulator_handle,
                                          stamp_token=stamp_token,
                                          next_stamp_token=next_stamp_token,
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
        "QuantileAccumulatorFlush", quantile_accumulator_handle=quantile_accumulator_handle,
                                    stamp_token=stamp_token,
                                    next_stamp_token=next_stamp_token,
                                    name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_flush, quantile_accumulator_handle=quantile_accumulator_handle,
                                      stamp_token=stamp_token,
                                      next_stamp_token=next_stamp_token,
                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def quantile_accumulator_flush_eager_fallback(quantile_accumulator_handle, stamp_token, next_stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_flush
  """
  _ctx = ctx if ctx else _context.context()
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  _inputs_flat = [quantile_accumulator_handle, stamp_token, next_stamp_token]
  _attrs = None
  _result = _execute.execute(b"QuantileAccumulatorFlush", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("QuantileAccumulatorFlush")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_flush_summary')
def quantile_accumulator_flush_summary(quantile_accumulator_handle, stamp_token, next_stamp_token, name=None):
  r"""Resets quantile summary stream and returns the summary.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
      The handle to the accumulator.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    next_stamp_token: A `Tensor` of type `int64`.
      Stamp token to be used for the next iteration.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    A scalar string that is the a summary of the accumulator.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorFlushSummary", name,
        _ctx._post_execution_callbacks, quantile_accumulator_handle,
        stamp_token, next_stamp_token)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_flush_summary_eager_fallback(
            quantile_accumulator_handle, stamp_token, next_stamp_token,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_flush_summary, quantile_accumulator_handle=quantile_accumulator_handle,
                                                  stamp_token=stamp_token,
                                                  next_stamp_token=next_stamp_token,
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
        "QuantileAccumulatorFlushSummary", quantile_accumulator_handle=quantile_accumulator_handle,
                                           stamp_token=stamp_token,
                                           next_stamp_token=next_stamp_token,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_flush_summary, quantile_accumulator_handle=quantile_accumulator_handle,
                                              stamp_token=stamp_token,
                                              next_stamp_token=next_stamp_token,
                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QuantileAccumulatorFlushSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantile_accumulator_flush_summary_eager_fallback(quantile_accumulator_handle, stamp_token, next_stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_flush_summary
  """
  _ctx = ctx if ctx else _context.context()
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  next_stamp_token = _ops.convert_to_tensor(next_stamp_token, _dtypes.int64)
  _inputs_flat = [quantile_accumulator_handle, stamp_token, next_stamp_token]
  _attrs = None
  _result = _execute.execute(b"QuantileAccumulatorFlushSummary", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantileAccumulatorFlushSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("QuantileAccumulatorFlushSummary")(None)


_quantile_accumulator_get_buckets_outputs = ["are_buckets_ready", "buckets"]
_QuantileAccumulatorGetBucketsOutput = _collections.namedtuple(
    "QuantileAccumulatorGetBuckets",
    _quantile_accumulator_get_buckets_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_get_buckets')
def quantile_accumulator_get_buckets(quantile_accumulator_handles, stamp_token, name=None):
  r"""Returns quantile buckets created during previous flush of the accumulator.

  Args:
    quantile_accumulator_handles: A list of at least 1 `Tensor` objects with type `resource`.
      The handles to the quantile stream resources.
    stamp_token: A `Tensor` of type `int64`.
      Stamp token to validate the Read/Write operation.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (are_buckets_ready, buckets).

    are_buckets_ready: A list with the same length as `quantile_accumulator_handles` of `Tensor` objects with type `bool`. Whether the buckets are ready or not.
    buckets: A list with the same length as `quantile_accumulator_handles` of `Tensor` objects with type `float32`. Output quantile summary representing boundaries with "num_quantile"
      elements.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorGetBuckets", name, _ctx._post_execution_callbacks,
        quantile_accumulator_handles, stamp_token)
      _result = _QuantileAccumulatorGetBucketsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_get_buckets_eager_fallback(
            quantile_accumulator_handles, stamp_token, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_get_buckets, quantile_accumulator_handles=quantile_accumulator_handles,
                                                stamp_token=stamp_token,
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
  if not isinstance(quantile_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'quantile_accumulator_handles' argument to "
        "'quantile_accumulator_get_buckets' Op, not %r." % quantile_accumulator_handles)
  _attr_num_resource_handles = len(quantile_accumulator_handles)
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "QuantileAccumulatorGetBuckets", quantile_accumulator_handles=quantile_accumulator_handles,
                                         stamp_token=stamp_token, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_get_buckets, quantile_accumulator_handles=quantile_accumulator_handles,
                                            stamp_token=stamp_token,
                                            name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_resource_handles", _op.get_attr("num_resource_handles"))
  _execute.record_gradient(
      "QuantileAccumulatorGetBuckets", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_resource_handles]] + _result[_attr_num_resource_handles:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantileAccumulatorGetBucketsOutput._make(_result)
  return _result



def quantile_accumulator_get_buckets_eager_fallback(quantile_accumulator_handles, stamp_token, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_get_buckets
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(quantile_accumulator_handles, (list, tuple)):
    raise TypeError(
        "Expected list for 'quantile_accumulator_handles' argument to "
        "'quantile_accumulator_get_buckets' Op, not %r." % quantile_accumulator_handles)
  _attr_num_resource_handles = len(quantile_accumulator_handles)
  quantile_accumulator_handles = _ops.convert_n_to_tensor(quantile_accumulator_handles, _dtypes.resource)
  stamp_token = _ops.convert_to_tensor(stamp_token, _dtypes.int64)
  _inputs_flat = list(quantile_accumulator_handles) + [stamp_token]
  _attrs = ("num_resource_handles", _attr_num_resource_handles)
  _result = _execute.execute(b"QuantileAccumulatorGetBuckets",
                             _attr_num_resource_handles +
                             _attr_num_resource_handles, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantileAccumulatorGetBuckets", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_resource_handles]] + _result[_attr_num_resource_handles:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantileAccumulatorGetBucketsOutput._make(_result)
  return _result

_ops.RegisterShape("QuantileAccumulatorGetBuckets")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_is_initialized')
def quantile_accumulator_is_initialized(quantile_accumulator_handle, name=None):
  r"""Checks whether a quantile accumulator has been initialized.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorIsInitialized", name,
        _ctx._post_execution_callbacks, quantile_accumulator_handle)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_is_initialized_eager_fallback(
            quantile_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_is_initialized, quantile_accumulator_handle=quantile_accumulator_handle,
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
        "QuantileAccumulatorIsInitialized", quantile_accumulator_handle=quantile_accumulator_handle,
                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_is_initialized, quantile_accumulator_handle=quantile_accumulator_handle,
                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QuantileAccumulatorIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantile_accumulator_is_initialized_eager_fallback(quantile_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_is_initialized
  """
  _ctx = ctx if ctx else _context.context()
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  _inputs_flat = [quantile_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"QuantileAccumulatorIsInitialized", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantileAccumulatorIsInitialized", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("QuantileAccumulatorIsInitialized")(None)


_quantile_accumulator_serialize_outputs = ["stamp_token", "stream_state",
                                          "are_buckets_ready", "buckets"]
_QuantileAccumulatorSerializeOutput = _collections.namedtuple(
    "QuantileAccumulatorSerialize", _quantile_accumulator_serialize_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantile_accumulator_serialize')
def quantile_accumulator_serialize(quantile_accumulator_handle, name=None):
  r"""Serializes the state of the given resource.

  Args:
    quantile_accumulator_handle: A `Tensor` of type `resource`.
      The handle to the accumulator.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (stamp_token, stream_state, are_buckets_ready, buckets).

    stamp_token: A `Tensor` of type `int64`. Stamp token for Read/Write operations.
      Any operation with a mismatching token will be dropped.
    stream_state: A `Tensor` of type `string`. A serialized QuantileStreamState.
    are_buckets_ready: A `Tensor` of type `bool`. Whether the buckets are ready or not.
    buckets: A `Tensor` of type `float32`. Output quantile buckets representing boundaries with "num_quantile"
      elements.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileAccumulatorSerialize", name, _ctx._post_execution_callbacks,
        quantile_accumulator_handle)
      _result = _QuantileAccumulatorSerializeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantile_accumulator_serialize_eager_fallback(
            quantile_accumulator_handle, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_accumulator_serialize, quantile_accumulator_handle=quantile_accumulator_handle,
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
        "QuantileAccumulatorSerialize", quantile_accumulator_handle=quantile_accumulator_handle,
                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_accumulator_serialize, quantile_accumulator_handle=quantile_accumulator_handle,
                                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QuantileAccumulatorSerialize", _inputs_flat, _attrs, _result, name)
  _result = _QuantileAccumulatorSerializeOutput._make(_result)
  return _result



def quantile_accumulator_serialize_eager_fallback(quantile_accumulator_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_accumulator_serialize
  """
  _ctx = ctx if ctx else _context.context()
  quantile_accumulator_handle = _ops.convert_to_tensor(quantile_accumulator_handle, _dtypes.resource)
  _inputs_flat = [quantile_accumulator_handle]
  _attrs = None
  _result = _execute.execute(b"QuantileAccumulatorSerialize", 4,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantileAccumulatorSerialize", _inputs_flat, _attrs, _result, name)
  _result = _QuantileAccumulatorSerializeOutput._make(_result)
  return _result

_ops.RegisterShape("QuantileAccumulatorSerialize")(None)


_quantile_buckets_outputs = ["dense_buckets", "sparse_buckets"]
_QuantileBucketsOutput = _collections.namedtuple(
    "QuantileBuckets", _quantile_buckets_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantile_buckets')
def quantile_buckets(dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, example_weights, dense_config, sparse_config, name=None):
  r"""Computes quantile buckets for a given list of dense and sparse features with

  given example weights.

  Args:
    dense_float_features: A list of `Tensor` objects with type `float32`.
      A list of vectors which contains dense values.
    sparse_float_feature_indices: A list of `Tensor` objects with type `int64`.
      List of rank 2 tensors containing the sparse float
      feature indices.
    sparse_float_feature_values: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`.
      List of rank 1 tensors containing the sparse float
      feature values.
    sparse_float_feature_shapes: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `int64`.
      List of rank 1 tensors containing the shape of the
      float feature.
    example_weights: A `Tensor` of type `float32`.
      Rank 1 tensor containing the example weight tensor.
    dense_config: A list of `strings`.
      Config for computing buckets for dense values.
      Each entry is QuantileConfig proto.
    sparse_config: A list of `strings`.
      Config for computing buckets for sparse feature values.
      Each entry is QuantileConfig proto.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (dense_buckets, sparse_buckets).

    dense_buckets: A list with the same length as `dense_float_features` of `Tensor` objects with type `float32`. Output quantile summary for each dense float tensor
      representing boundaries each with "num_quantile" elements.
    sparse_buckets: A list with the same length as `sparse_float_feature_indices` of `Tensor` objects with type `float32`. Output quantile summary for each sparse float value tensor
      representing boundaries each with "num_quantile" elements.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantileBuckets", name, _ctx._post_execution_callbacks,
        dense_float_features, sparse_float_feature_indices,
        sparse_float_feature_values, sparse_float_feature_shapes,
        example_weights, "dense_config", dense_config, "sparse_config",
        sparse_config)
      _result = _QuantileBucketsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantile_buckets_eager_fallback(
            dense_float_features, sparse_float_feature_indices,
            sparse_float_feature_values, sparse_float_feature_shapes,
            example_weights, dense_config=dense_config,
            sparse_config=sparse_config, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_buckets, dense_float_features=dense_float_features,
                                sparse_float_feature_indices=sparse_float_feature_indices,
                                sparse_float_feature_values=sparse_float_feature_values,
                                sparse_float_feature_shapes=sparse_float_feature_shapes,
                                example_weights=example_weights,
                                dense_config=dense_config,
                                sparse_config=sparse_config, name=name)
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
        "'quantile_buckets' Op, not %r." % dense_float_features)
  _attr_num_dense_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'quantile_buckets' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'quantile_buckets' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_features))
  if not isinstance(dense_config, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_config' argument to "
        "'quantile_buckets' Op, not %r." % dense_config)
  dense_config = [_execute.make_str(_s, "dense_config") for _s in dense_config]
  if not isinstance(sparse_config, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_config' argument to "
        "'quantile_buckets' Op, not %r." % sparse_config)
  sparse_config = [_execute.make_str(_s, "sparse_config") for _s in sparse_config]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "QuantileBuckets", dense_float_features=dense_float_features,
                           sparse_float_feature_indices=sparse_float_feature_indices,
                           sparse_float_feature_values=sparse_float_feature_values,
                           sparse_float_feature_shapes=sparse_float_feature_shapes,
                           example_weights=example_weights,
                           dense_config=dense_config,
                           sparse_config=sparse_config, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_buckets, dense_float_features=dense_float_features,
                            sparse_float_feature_indices=sparse_float_feature_indices,
                            sparse_float_feature_values=sparse_float_feature_values,
                            sparse_float_feature_shapes=sparse_float_feature_shapes,
                            example_weights=example_weights,
                            dense_config=dense_config,
                            sparse_config=sparse_config, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_dense_features", _op.get_attr("num_dense_features"),
            "num_sparse_features", _op.get_attr("num_sparse_features"),
            "dense_config", _op.get_attr("dense_config"), "sparse_config",
            _op.get_attr("sparse_config"))
  _execute.record_gradient(
      "QuantileBuckets", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantileBucketsOutput._make(_result)
  return _result



def quantile_buckets_eager_fallback(dense_float_features, sparse_float_feature_indices, sparse_float_feature_values, sparse_float_feature_shapes, example_weights, dense_config, sparse_config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_buckets
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_float_features, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_float_features' argument to "
        "'quantile_buckets' Op, not %r." % dense_float_features)
  _attr_num_dense_features = len(dense_float_features)
  if not isinstance(sparse_float_feature_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_indices' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_indices)
  _attr_num_sparse_features = len(sparse_float_feature_indices)
  if not isinstance(sparse_float_feature_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_values' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_values)
  if len(sparse_float_feature_values) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_values' to 'quantile_buckets' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_values), _attr_num_sparse_features))
  if not isinstance(sparse_float_feature_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_float_feature_shapes' argument to "
        "'quantile_buckets' Op, not %r." % sparse_float_feature_shapes)
  if len(sparse_float_feature_shapes) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_float_feature_shapes' to 'quantile_buckets' Op with length %d "
        "must match length %d of argument 'sparse_float_feature_indices'." %
        (len(sparse_float_feature_shapes), _attr_num_sparse_features))
  if not isinstance(dense_config, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_config' argument to "
        "'quantile_buckets' Op, not %r." % dense_config)
  dense_config = [_execute.make_str(_s, "dense_config") for _s in dense_config]
  if not isinstance(sparse_config, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_config' argument to "
        "'quantile_buckets' Op, not %r." % sparse_config)
  sparse_config = [_execute.make_str(_s, "sparse_config") for _s in sparse_config]
  dense_float_features = _ops.convert_n_to_tensor(dense_float_features, _dtypes.float32)
  sparse_float_feature_indices = _ops.convert_n_to_tensor(sparse_float_feature_indices, _dtypes.int64)
  sparse_float_feature_values = _ops.convert_n_to_tensor(sparse_float_feature_values, _dtypes.float32)
  sparse_float_feature_shapes = _ops.convert_n_to_tensor(sparse_float_feature_shapes, _dtypes.int64)
  example_weights = _ops.convert_to_tensor(example_weights, _dtypes.float32)
  _inputs_flat = list(dense_float_features) + list(sparse_float_feature_indices) + list(sparse_float_feature_values) + list(sparse_float_feature_shapes) + [example_weights]
  _attrs = ("num_dense_features", _attr_num_dense_features,
  "num_sparse_features", _attr_num_sparse_features, "dense_config",
  dense_config, "sparse_config", sparse_config)
  _result = _execute.execute(b"QuantileBuckets", _attr_num_dense_features +
                             _attr_num_sparse_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QuantileBuckets", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantileBucketsOutput._make(_result)
  return _result

_ops.RegisterShape("QuantileBuckets")(None)


@_dispatch.add_dispatch_list
@tf_export('quantile_stream_resource_handle_op')
def quantile_stream_resource_handle_op(container="", shared_name="", name=None):
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
        "QuantileStreamResourceHandleOp", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return quantile_stream_resource_handle_op_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantile_stream_resource_handle_op, container=container,
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
        "QuantileStreamResourceHandleOp", container=container,
                                          shared_name=shared_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantile_stream_resource_handle_op, container=container,
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
      "QuantileStreamResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def quantile_stream_resource_handle_op_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantile_stream_resource_handle_op
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
  _result = _execute.execute(b"QuantileStreamResourceHandleOp", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantileStreamResourceHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("QuantileStreamResourceHandleOp")(None)


_quantiles_outputs = ["dense_quantiles", "sparse_quantiles"]
_QuantilesOutput = _collections.namedtuple(
    "Quantiles", _quantiles_outputs)


@_dispatch.add_dispatch_list
@tf_export('quantiles')
def quantiles(dense_values, sparse_values, dense_buckets, sparse_buckets, sparse_indices, name=None):
  r"""Computes quantile for each a given list of dense and sparse feature values using

  the given buckets.

  Args:
    dense_values: A list of `Tensor` objects with type `float32`.
      List of rank 1 tensors containing the dense values.
    sparse_values: A list of `Tensor` objects with type `float32`.
      List of rank 1 tensors containing the sparse feature values.
    dense_buckets: A list with the same length as `dense_values` of `Tensor` objects with type `float32`.
      Quantile summary for each of the dense float tensor.
    sparse_buckets: A list with the same length as `sparse_values` of `Tensor` objects with type `float32`.
      Quantile summary for each of the sparse feature float tensor.
    sparse_indices: A list with the same length as `sparse_values` of `Tensor` objects with type `int64`.
      List of rank 2 tensors with indices for sparse float
      tensors.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (dense_quantiles, sparse_quantiles).

    dense_quantiles: A list with the same length as `dense_values` of `Tensor` objects with type `int32`. Rank 2 tensors representing associated quantiles for each of
      dense float tensors and the dimension.
    sparse_quantiles: A list with the same length as `sparse_values` of `Tensor` objects with type `int32`. Rank 2 tensors representing associated quantiles for each of
      the sparse feature tensors for each of sparse feature dimensions:
      [quantile id, dimension id].
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Quantiles",
        name, _ctx._post_execution_callbacks, dense_values, sparse_values,
        dense_buckets, sparse_buckets, sparse_indices)
      _result = _QuantilesOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantiles_eager_fallback(
            dense_values, sparse_values, dense_buckets, sparse_buckets,
            sparse_indices, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              quantiles, dense_values=dense_values,
                         sparse_values=sparse_values,
                         dense_buckets=dense_buckets,
                         sparse_buckets=sparse_buckets,
                         sparse_indices=sparse_indices, name=name)
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
  if not isinstance(dense_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_values' argument to "
        "'quantiles' Op, not %r." % dense_values)
  _attr_num_dense_features = len(dense_values)
  if not isinstance(dense_buckets, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_buckets' argument to "
        "'quantiles' Op, not %r." % dense_buckets)
  if len(dense_buckets) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_buckets' to 'quantiles' Op with length %d "
        "must match length %d of argument 'dense_values'." %
        (len(dense_buckets), _attr_num_dense_features))
  if not isinstance(sparse_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_values' argument to "
        "'quantiles' Op, not %r." % sparse_values)
  _attr_num_sparse_features = len(sparse_values)
  if not isinstance(sparse_buckets, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_buckets' argument to "
        "'quantiles' Op, not %r." % sparse_buckets)
  if len(sparse_buckets) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_buckets' to 'quantiles' Op with length %d "
        "must match length %d of argument 'sparse_values'." %
        (len(sparse_buckets), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'quantiles' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'quantiles' Op with length %d "
        "must match length %d of argument 'sparse_values'." %
        (len(sparse_indices), _attr_num_sparse_features))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Quantiles", dense_values=dense_values, sparse_values=sparse_values,
                     dense_buckets=dense_buckets,
                     sparse_buckets=sparse_buckets,
                     sparse_indices=sparse_indices, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          quantiles, dense_values=dense_values, sparse_values=sparse_values,
                     dense_buckets=dense_buckets,
                     sparse_buckets=sparse_buckets,
                     sparse_indices=sparse_indices, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_dense_features", _op.get_attr("num_dense_features"),
            "num_sparse_features", _op.get_attr("num_sparse_features"))
  _execute.record_gradient(
      "Quantiles", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantilesOutput._make(_result)
  return _result



def quantiles_eager_fallback(dense_values, sparse_values, dense_buckets, sparse_buckets, sparse_indices, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantiles
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dense_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_values' argument to "
        "'quantiles' Op, not %r." % dense_values)
  _attr_num_dense_features = len(dense_values)
  if not isinstance(dense_buckets, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_buckets' argument to "
        "'quantiles' Op, not %r." % dense_buckets)
  if len(dense_buckets) != _attr_num_dense_features:
    raise ValueError(
        "List argument 'dense_buckets' to 'quantiles' Op with length %d "
        "must match length %d of argument 'dense_values'." %
        (len(dense_buckets), _attr_num_dense_features))
  if not isinstance(sparse_values, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_values' argument to "
        "'quantiles' Op, not %r." % sparse_values)
  _attr_num_sparse_features = len(sparse_values)
  if not isinstance(sparse_buckets, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_buckets' argument to "
        "'quantiles' Op, not %r." % sparse_buckets)
  if len(sparse_buckets) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_buckets' to 'quantiles' Op with length %d "
        "must match length %d of argument 'sparse_values'." %
        (len(sparse_buckets), _attr_num_sparse_features))
  if not isinstance(sparse_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_indices' argument to "
        "'quantiles' Op, not %r." % sparse_indices)
  if len(sparse_indices) != _attr_num_sparse_features:
    raise ValueError(
        "List argument 'sparse_indices' to 'quantiles' Op with length %d "
        "must match length %d of argument 'sparse_values'." %
        (len(sparse_indices), _attr_num_sparse_features))
  dense_values = _ops.convert_n_to_tensor(dense_values, _dtypes.float32)
  sparse_values = _ops.convert_n_to_tensor(sparse_values, _dtypes.float32)
  dense_buckets = _ops.convert_n_to_tensor(dense_buckets, _dtypes.float32)
  sparse_buckets = _ops.convert_n_to_tensor(sparse_buckets, _dtypes.float32)
  sparse_indices = _ops.convert_n_to_tensor(sparse_indices, _dtypes.int64)
  _inputs_flat = list(dense_values) + list(sparse_values) + list(dense_buckets) + list(sparse_buckets) + list(sparse_indices)
  _attrs = ("num_dense_features", _attr_num_dense_features,
  "num_sparse_features", _attr_num_sparse_features)
  _result = _execute.execute(b"Quantiles", _attr_num_dense_features +
                             _attr_num_sparse_features, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Quantiles", _inputs_flat, _attrs, _result, name)
  _result = [_result[:_attr_num_dense_features]] + _result[_attr_num_dense_features:]
  _result = _result[:1] + [_result[1:]]
  _result = _QuantilesOutput._make(_result)
  return _result

_ops.RegisterShape("Quantiles")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BucketizeWithInputBoundaries"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "boundaries"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "CreateQuantileAccumulator"
#   input_arg {
#     name: "quantile_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
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
#   attr {
#     name: "max_elements"
#     type: "int"
#     default_value {
#       i: 1099511627776
#     }
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#   }
#   attr {
#     name: "num_quantiles"
#     type: "int"
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
#   name: "MakeQuantileSummaries"
#   input_arg {
#     name: "dense_float_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "example_weights"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "dense_summaries"
#     type: DT_STRING
#     number_attr: "num_dense_features"
#   }
#   output_arg {
#     name: "sparse_summaries"
#     type: DT_STRING
#     number_attr: "num_sparse_features"
#   }
#   attr {
#     name: "num_dense_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "epsilon"
#     type: "float"
#   }
# }
# op {
#   name: "QuantileAccumulatorAddSummaries"
#   input_arg {
#     name: "quantile_accumulator_handles"
#     type: DT_RESOURCE
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "summaries"
#     type: DT_STRING
#     number_attr: "num_resource_handles"
#   }
#   attr {
#     name: "num_resource_handles"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorDeserialize"
#   input_arg {
#     name: "quantile_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "stream_state"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "are_buckets_ready"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "buckets"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorFlush"
#   input_arg {
#     name: "quantile_accumulator_handle"
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
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorFlushSummary"
#   input_arg {
#     name: "quantile_accumulator_handle"
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
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorGetBuckets"
#   input_arg {
#     name: "quantile_accumulator_handles"
#     type: DT_RESOURCE
#     number_attr: "num_resource_handles"
#   }
#   input_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "are_buckets_ready"
#     type: DT_BOOL
#     number_attr: "num_resource_handles"
#   }
#   output_arg {
#     name: "buckets"
#     type: DT_FLOAT
#     number_attr: "num_resource_handles"
#   }
#   attr {
#     name: "num_resource_handles"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorIsInitialized"
#   input_arg {
#     name: "quantile_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileAccumulatorSerialize"
#   input_arg {
#     name: "quantile_accumulator_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "stamp_token"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "stream_state"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "are_buckets_ready"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "buckets"
#     type: DT_FLOAT
#   }
#   is_stateful: true
# }
# op {
#   name: "QuantileBuckets"
#   input_arg {
#     name: "dense_float_features"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_float_feature_shapes"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "example_weights"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "dense_buckets"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   output_arg {
#     name: "sparse_buckets"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   attr {
#     name: "num_dense_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "dense_config"
#     type: "list(string)"
#   }
#   attr {
#     name: "sparse_config"
#     type: "list(string)"
#   }
# }
# op {
#   name: "QuantileStreamResourceHandleOp"
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
#   name: "Quantiles"
#   input_arg {
#     name: "dense_values"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "sparse_values"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "dense_buckets"
#     type: DT_FLOAT
#     number_attr: "num_dense_features"
#   }
#   input_arg {
#     name: "sparse_buckets"
#     type: DT_FLOAT
#     number_attr: "num_sparse_features"
#   }
#   input_arg {
#     name: "sparse_indices"
#     type: DT_INT64
#     number_attr: "num_sparse_features"
#   }
#   output_arg {
#     name: "dense_quantiles"
#     type: DT_INT32
#     number_attr: "num_dense_features"
#   }
#   output_arg {
#     name: "sparse_quantiles"
#     type: DT_INT32
#     number_attr: "num_sparse_features"
#   }
#   attr {
#     name: "num_dense_features"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_sparse_features"
#     type: "int"
#     has_minimum: true
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n[\n\034BucketizeWithInputBoundaries\022\n\n\005input\"\001T\022\016\n\nboundaries\030\001\032\n\n\006output\030\003\"\023\n\001T\022\004type:\010\n\0062\004\003\t\001\002\n\352\001\n\031CreateQuantileAccumulator\022\037\n\033quantile_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\034\n\014max_elements\022\003int\032\007\030\200\200\200\200\200 \"\020\n\007epsilon\022\005float\"\024\n\rnum_quantiles\022\003int\"\036\n\022generate_quantiles\022\004bool\032\002(\000\210\001\001\n\236\003\n\025MakeQuantileSummaries\022,\n\024dense_float_features\030\001*\022num_dense_features\0225\n\034sparse_float_feature_indices\030\t*\023num_sparse_features\0224\n\033sparse_float_feature_values\030\001*\023num_sparse_features\0224\n\033sparse_float_feature_shapes\030\t*\023num_sparse_features\022\023\n\017example_weights\030\001\032\'\n\017dense_summaries\030\007*\022num_dense_features\032)\n\020sparse_summaries\030\007*\023num_sparse_features\"\033\n\022num_dense_features\022\003int(\001\"\034\n\023num_sparse_features\022\003int(\001\"\020\n\007epsilon\022\005float\n\263\001\n\037QuantileAccumulatorAddSummaries\0226\n\034quantile_accumulator_handles\030\024*\024num_resource_handles\022\017\n\013stamp_token\030\t\022#\n\tsummaries\030\007*\024num_resource_handles\"\037\n\024num_resource_handles\022\003int(\0010\001\210\001\001\n\213\001\n\036QuantileAccumulatorDeserialize\022\037\n\033quantile_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\020\n\014stream_state\030\007\022\025\n\021are_buckets_ready\030\n\022\013\n\007buckets\030\001\210\001\001\ne\n\030QuantileAccumulatorFlush\022\037\n\033quantile_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\210\001\001\nx\n\037QuantileAccumulatorFlushSummary\022\037\n\033quantile_accumulator_handle\030\024\022\017\n\013stamp_token\030\t\022\024\n\020next_stamp_token\030\t\032\n\n\006output\030\007\210\001\001\n\334\001\n\035QuantileAccumulatorGetBuckets\0226\n\034quantile_accumulator_handles\030\024*\024num_resource_handles\022\017\n\013stamp_token\030\t\032+\n\021are_buckets_ready\030\n*\024num_resource_handles\032!\n\007buckets\030\001*\024num_resource_handles\"\037\n\024num_resource_handles\022\003int(\0010\001\210\001\001\nZ\n QuantileAccumulatorIsInitialized\022\037\n\033quantile_accumulator_handle\030\024\032\022\n\016is_initialized\030\n\210\001\001\n\211\001\n\034QuantileAccumulatorSerialize\022\037\n\033quantile_accumulator_handle\030\024\032\017\n\013stamp_token\030\t\032\020\n\014stream_state\030\007\032\025\n\021are_buckets_ready\030\n\032\013\n\007buckets\030\001\210\001\001\n\277\003\n\017QuantileBuckets\022,\n\024dense_float_features\030\001*\022num_dense_features\0225\n\034sparse_float_feature_indices\030\t*\023num_sparse_features\0224\n\033sparse_float_feature_values\030\001*\023num_sparse_features\0224\n\033sparse_float_feature_shapes\030\t*\023num_sparse_features\022\023\n\017example_weights\030\001\032%\n\rdense_buckets\030\001*\022num_dense_features\032\'\n\016sparse_buckets\030\001*\023num_sparse_features\"\033\n\022num_dense_features\022\003int(\001\"\034\n\023num_sparse_features\022\003int(\001\"\034\n\014dense_config\022\014list(string)\"\035\n\rsparse_config\022\014list(string)\ne\n\036QuantileStreamResourceHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\341\002\n\tQuantiles\022$\n\014dense_values\030\001*\022num_dense_features\022&\n\rsparse_values\030\001*\023num_sparse_features\022%\n\rdense_buckets\030\001*\022num_dense_features\022\'\n\016sparse_buckets\030\001*\023num_sparse_features\022\'\n\016sparse_indices\030\t*\023num_sparse_features\032\'\n\017dense_quantiles\030\003*\022num_dense_features\032)\n\020sparse_quantiles\030\003*\023num_sparse_features\"\033\n\022num_dense_features\022\003int(\001\"\034\n\023num_sparse_features\022\003int(\001")
