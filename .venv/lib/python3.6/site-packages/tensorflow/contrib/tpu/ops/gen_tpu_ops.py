"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: tpu_ops.cc
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
@tf_export('all_to_all')
def all_to_all(input, group_assignment, concat_dimension, split_dimension, split_count, name=None):
  r"""An Op to exchange data across TPU replicas. On each replica, the input is

  split into `split_count` blocks along `split_dimension` and send to the other
  replicas given group_assignment. After receiving `split_count` - 1 blocks from
  other replicas, we concatenate the blocks along `concat_dimension` as the
  output.

  For example, suppose there are 2 TPU replicas:
  replica 0 receives input: `[[A, B]]`
  replica 1 receives input: `[[C, D]]`

  group_assignment=`[[0, 1]]`
  concat_dimension=0
  split_dimension=1
  split_count=2

  replica 0's output: `[[A], [C]]`
  replica 1's output: `[[B], [D]]`

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`.
      The local input to the sum.
    group_assignment: A `Tensor` of type `int32`. An int32 tensor with shape
      [num_groups, num_replicas_per_group]. `group_assignment[i]` represents the
      replica ids in the ith subgroup.
    concat_dimension: An `int`. The dimension number to concatenate.
    split_dimension: An `int`. The dimension number to split.
    split_count: An `int`.
      The number of splits, this number must equal to the sub-group
      size(group_assignment.get_shape()[1])
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`. The exchanged result.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AllToAll",
        name, _ctx._post_execution_callbacks, input, group_assignment,
        "concat_dimension", concat_dimension, "split_dimension",
        split_dimension, "split_count", split_count)
      return _result
    except _core._FallbackException:
      try:
        return all_to_all_eager_fallback(
            input, group_assignment, concat_dimension=concat_dimension,
            split_dimension=split_dimension, split_count=split_count,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              all_to_all, input=input, group_assignment=group_assignment,
                          concat_dimension=concat_dimension,
                          split_dimension=split_dimension,
                          split_count=split_count, name=name)
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
  concat_dimension = _execute.make_int(concat_dimension, "concat_dimension")
  split_dimension = _execute.make_int(split_dimension, "split_dimension")
  split_count = _execute.make_int(split_count, "split_count")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "AllToAll", input=input, group_assignment=group_assignment,
                    concat_dimension=concat_dimension,
                    split_dimension=split_dimension, split_count=split_count,
                    name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          all_to_all, input=input, group_assignment=group_assignment,
                      concat_dimension=concat_dimension,
                      split_dimension=split_dimension,
                      split_count=split_count, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "concat_dimension",
            _op.get_attr("concat_dimension"), "split_dimension",
            _op.get_attr("split_dimension"), "split_count",
            _op.get_attr("split_count"))
  _execute.record_gradient(
      "AllToAll", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def all_to_all_eager_fallback(input, group_assignment, concat_dimension, split_dimension, split_count, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function all_to_all
  """
  _ctx = ctx if ctx else _context.context()
  concat_dimension = _execute.make_int(concat_dimension, "concat_dimension")
  split_dimension = _execute.make_int(split_dimension, "split_dimension")
  split_count = _execute.make_int(split_count, "split_count")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  group_assignment = _ops.convert_to_tensor(group_assignment, _dtypes.int32)
  _inputs_flat = [input, group_assignment]
  _attrs = ("T", _attr_T, "concat_dimension", concat_dimension,
  "split_dimension", split_dimension, "split_count", split_count)
  _result = _execute.execute(b"AllToAll", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AllToAll", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("AllToAll")(None)


@_dispatch.add_dispatch_list
@tf_export('collective_permute')
def collective_permute(input, source_target_pairs, name=None):
  r"""An Op to permute tensors across replicated TPU instances. Each instance

  supplies its own input.

  For example, suppose there are 4 TPU instances: `[A, B, C, D]`. Passing
  source_target_pairs=`[[0,1],[1,2],[2,3],[3,0]]` gets the outputs:
  `[D, A, B, C]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The local input to be permuted. Currently only supports float and
      bfloat16.
    source_target_pairs: A `Tensor` of type `int32`.
      A tensor with shape [num_pairs, 2].
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`. The permuted input.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CollectivePermute", name, _ctx._post_execution_callbacks, input,
        source_target_pairs)
      return _result
    except _core._FallbackException:
      try:
        return collective_permute_eager_fallback(
            input, source_target_pairs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              collective_permute, input=input,
                                  source_target_pairs=source_target_pairs,
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
        "CollectivePermute", input=input,
                             source_target_pairs=source_target_pairs,
                             name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          collective_permute, input=input,
                              source_target_pairs=source_target_pairs,
                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "CollectivePermute", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def collective_permute_eager_fallback(input, source_target_pairs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function collective_permute
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  source_target_pairs = _ops.convert_to_tensor(source_target_pairs, _dtypes.int32)
  _inputs_flat = [input, source_target_pairs]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"CollectivePermute", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CollectivePermute", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("CollectivePermute")(None)


@_dispatch.add_dispatch_list
@tf_export('configure_distributed_tpu')
def configure_distributed_tpu(embedding_config="", tpu_embedding_config="", is_global_init=False, name=None):
  r"""An op that sets up the centralized structures for a distributed TPU

  system.

  Args:
    embedding_config: An optional `string`. Defaults to `""`.
      Reserved. Do not use.
    tpu_embedding_config: An optional `string`. Defaults to `""`.
      Serialized tensorflow.tpu.TPUEmbeddingConfiguration that
      describes the embedding lookups of the program.
    is_global_init: An optional `bool`. Defaults to `False`.
      Reserved. Do not use.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    A serialized tensorflow.tpu.TopologyProto that describes the TPU
    topology.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ConfigureDistributedTPU", name, _ctx._post_execution_callbacks,
        "embedding_config", embedding_config, "tpu_embedding_config",
        tpu_embedding_config, "is_global_init", is_global_init)
      return _result
    except _core._FallbackException:
      try:
        return configure_distributed_tpu_eager_fallback(
            embedding_config=embedding_config,
            tpu_embedding_config=tpu_embedding_config,
            is_global_init=is_global_init, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              configure_distributed_tpu, embedding_config=embedding_config,
                                         tpu_embedding_config=tpu_embedding_config,
                                         is_global_init=is_global_init,
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
  if embedding_config is None:
    embedding_config = ""
  embedding_config = _execute.make_str(embedding_config, "embedding_config")
  if tpu_embedding_config is None:
    tpu_embedding_config = ""
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  if is_global_init is None:
    is_global_init = False
  is_global_init = _execute.make_bool(is_global_init, "is_global_init")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ConfigureDistributedTPU", embedding_config=embedding_config,
                                   tpu_embedding_config=tpu_embedding_config,
                                   is_global_init=is_global_init, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          configure_distributed_tpu, embedding_config=embedding_config,
                                     tpu_embedding_config=tpu_embedding_config,
                                     is_global_init=is_global_init, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("embedding_config", _op.get_attr("embedding_config"),
            "tpu_embedding_config", _op.get_attr("tpu_embedding_config"),
            "is_global_init", _op.get_attr("is_global_init"))
  _execute.record_gradient(
      "ConfigureDistributedTPU", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def configure_distributed_tpu_eager_fallback(embedding_config="", tpu_embedding_config="", is_global_init=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function configure_distributed_tpu
  """
  _ctx = ctx if ctx else _context.context()
  if embedding_config is None:
    embedding_config = ""
  embedding_config = _execute.make_str(embedding_config, "embedding_config")
  if tpu_embedding_config is None:
    tpu_embedding_config = ""
  tpu_embedding_config = _execute.make_str(tpu_embedding_config, "tpu_embedding_config")
  if is_global_init is None:
    is_global_init = False
  is_global_init = _execute.make_bool(is_global_init, "is_global_init")
  _inputs_flat = []
  _attrs = ("embedding_config", embedding_config, "tpu_embedding_config",
  tpu_embedding_config, "is_global_init", is_global_init)
  _result = _execute.execute(b"ConfigureDistributedTPU", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ConfigureDistributedTPU", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ConfigureDistributedTPU")(None)


@_dispatch.add_dispatch_list
@tf_export('cross_replica_sum')
def cross_replica_sum(input, group_assignment, name=None):
  r"""An Op to sum inputs across replicated TPU instances. Each instance supplies its

  own input.

  For example, suppose there are 8 TPU instances: `[A, B, C, D, E, F, G, H]`.
  Passing group_assignment=`[[0,2,4,6],[1,3,5,7]]` sets `A, C, E, G` as group 0,
  and `B, D, F, H` as group 1. Thus we get the outputs:
  `[A+C+E+G, B+D+F+H, A+C+E+G, B+D+F+H, A+C+E+G, B+D+F+H, A+C+E+G, B+D+F+H]`.

  Args:
    input: A `Tensor`. Must be one of the following types: `bfloat16`, `float32`.
      The local input to the sum.
    group_assignment: A `Tensor` of type `int32`. An int32 tensor with shape
      [num_groups, num_replicas_per_group]. `group_assignment[i]` represents the
      replica ids in the ith subgroup.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
    The sum of all the distributed inputs.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CrossReplicaSum", name, _ctx._post_execution_callbacks, input,
        group_assignment)
      return _result
    except _core._FallbackException:
      try:
        return cross_replica_sum_eager_fallback(
            input, group_assignment, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              cross_replica_sum, input=input,
                                 group_assignment=group_assignment, name=name)
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
        "CrossReplicaSum", input=input, group_assignment=group_assignment,
                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          cross_replica_sum, input=input, group_assignment=group_assignment,
                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "CrossReplicaSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cross_replica_sum_eager_fallback(input, group_assignment, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cross_replica_sum
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  group_assignment = _ops.convert_to_tensor(group_assignment, _dtypes.int32)
  _inputs_flat = [input, group_assignment]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"CrossReplicaSum", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CrossReplicaSum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("CrossReplicaSum")(None)


@_dispatch.add_dispatch_list
@tf_export('enqueue_tpu_embedding_integer_batch')
def _enqueue_tpu_embedding_integer_batch(batch, mode_override, device_ordinal=-1, name=None):
  r"""An op that enqueues a list of input batch tensors to TPUEmbedding.

  Args:
    batch: A list of at least 1 `Tensor` objects with type `int32`.
      A list of 1D tensors, one for each embedding table, containing the
      indices into the tables.
    mode_override: A `Tensor` of type `string`.
      A string input that overrides the mode specified in the
      TPUEmbeddingConfiguration. Supported values are {'unspecified', 'inference',
      'training', 'backward_pass_only'}. When set to 'unspecified', the mode set
      in TPUEmbeddingConfiguration is used, otherwise mode_override is used.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. Should be >= 0 and less than the number
      of TPU cores in the task on which the node is placed.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "EnqueueTPUEmbeddingIntegerBatch", name,
        _ctx._post_execution_callbacks, batch, mode_override,
        "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      try:
        return _enqueue_tpu_embedding_integer_batch_eager_fallback(
            batch, mode_override, device_ordinal=device_ordinal, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              _enqueue_tpu_embedding_integer_batch, batch=batch,
                                                    mode_override=mode_override,
                                                    device_ordinal=device_ordinal,
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
  if not isinstance(batch, (list, tuple)):
    raise TypeError(
        "Expected list for 'batch' argument to "
        "'enqueue_tpu_embedding_integer_batch' Op, not %r." % batch)
  _attr_N = len(batch)
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EnqueueTPUEmbeddingIntegerBatch", batch=batch,
                                           mode_override=mode_override,
                                           device_ordinal=device_ordinal,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          _enqueue_tpu_embedding_integer_batch, batch=batch,
                                                mode_override=mode_override,
                                                device_ordinal=device_ordinal,
                                                name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def _enqueue_tpu_embedding_integer_batch_eager_fallback(batch, mode_override, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _enqueue_tpu_embedding_integer_batch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(batch, (list, tuple)):
    raise TypeError(
        "Expected list for 'batch' argument to "
        "'enqueue_tpu_embedding_integer_batch' Op, not %r." % batch)
  _attr_N = len(batch)
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  batch = _ops.convert_n_to_tensor(batch, _dtypes.int32)
  mode_override = _ops.convert_to_tensor(mode_override, _dtypes.string)
  _inputs_flat = list(batch) + [mode_override]
  _attrs = ("N", _attr_N, "device_ordinal", device_ordinal)
  _result = _execute.execute(b"EnqueueTPUEmbeddingIntegerBatch", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("EnqueueTPUEmbeddingIntegerBatch")(None)


@_dispatch.add_dispatch_list
@tf_export('enqueue_tpu_embedding_sparse_batch')
def _enqueue_tpu_embedding_sparse_batch(sample_indices, embedding_indices, aggregation_weights, mode_override, device_ordinal=-1, combiners=[], name=None):
  r"""An op that enqueues TPUEmbedding input indices from a SparseTensor.

  This Op eases the porting of code that uses embedding_lookup_sparse(),
  although some Python preprocessing of the SparseTensor arguments to
  embedding_lookup_sparse() is required to produce the arguments to this Op,
  since only a single EnqueueTPUEmbeddingSparseBatch Op is allowed per training
  step.

  The tensors at corresponding positions in the three input lists
  must have the same shape, i.e. rank 1 with dim_size() equal to the total
  number of lookups into the table described by the corresponding table_id.

  Args:
    sample_indices: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors specifying the training example and
      feature to which the corresponding embedding_indices and aggregation_weights
      values belong. sample_indices[i] must equal b * nf + f, where nf is the
      number of features from the corresponding table, f is in [0, nf), and
      b is in [0, batch size).
    embedding_indices: A list with the same length as `sample_indices` of `Tensor` objects with type `int32`.
      A list of rank 1 Tensors, indices into the embedding tables.
    aggregation_weights: A list with the same length as `sample_indices` of `Tensor` objects with type `float32`.
      A list of rank 1 Tensors containing per sample -- i.e. per
      (training example, feature) -- aggregation weights.
    mode_override: A `Tensor` of type `string`.
      A string input that overrides the mode specified in the
      TPUEmbeddingConfiguration. Supported values are {'unspecified', 'inference',
      'training', 'backward_pass_only'}. When set to 'unspecified', the mode set
      in TPUEmbeddingConfiguration is used, otherwise mode_override is used.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. Should be >= 0 and less than the number
      of TPU cores in the task on which the node is placed.
    combiners: An optional list of `strings`. Defaults to `[]`.
      A list of string scalars, one for each embedding table that specify
      how to normalize the embedding activations after weighted summation.
      Supported combiners are 'mean', 'sum', or 'sqrtn'. It is invalid to have
      the sum of the weights be 0 for 'mean' or the sum of the squared weights be
      0 for 'sqrtn'. If combiners isn't passed, the default is to use 'sum' for
      all tables.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "EnqueueTPUEmbeddingSparseBatch", name,
        _ctx._post_execution_callbacks, sample_indices, embedding_indices,
        aggregation_weights, mode_override, "device_ordinal", device_ordinal,
        "combiners", combiners)
      return _result
    except _core._FallbackException:
      try:
        return _enqueue_tpu_embedding_sparse_batch_eager_fallback(
            sample_indices, embedding_indices, aggregation_weights,
            mode_override, device_ordinal=device_ordinal, combiners=combiners,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              _enqueue_tpu_embedding_sparse_batch, sample_indices=sample_indices,
                                                   embedding_indices=embedding_indices,
                                                   aggregation_weights=aggregation_weights,
                                                   mode_override=mode_override,
                                                   device_ordinal=device_ordinal,
                                                   combiners=combiners,
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
  if not isinstance(sample_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sample_indices' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % sample_indices)
  _attr_N = len(sample_indices)
  if not isinstance(embedding_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'embedding_indices' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % embedding_indices)
  if len(embedding_indices) != _attr_N:
    raise ValueError(
        "List argument 'embedding_indices' to 'enqueue_tpu_embedding_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(embedding_indices), _attr_N))
  if not isinstance(aggregation_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'aggregation_weights' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % aggregation_weights)
  if len(aggregation_weights) != _attr_N:
    raise ValueError(
        "List argument 'aggregation_weights' to 'enqueue_tpu_embedding_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(aggregation_weights), _attr_N))
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  if combiners is None:
    combiners = []
  if not isinstance(combiners, (list, tuple)):
    raise TypeError(
        "Expected list for 'combiners' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % combiners)
  combiners = [_execute.make_str(_s, "combiners") for _s in combiners]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EnqueueTPUEmbeddingSparseBatch", sample_indices=sample_indices,
                                          embedding_indices=embedding_indices,
                                          aggregation_weights=aggregation_weights,
                                          mode_override=mode_override,
                                          device_ordinal=device_ordinal,
                                          combiners=combiners, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          _enqueue_tpu_embedding_sparse_batch, sample_indices=sample_indices,
                                               embedding_indices=embedding_indices,
                                               aggregation_weights=aggregation_weights,
                                               mode_override=mode_override,
                                               device_ordinal=device_ordinal,
                                               combiners=combiners, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def _enqueue_tpu_embedding_sparse_batch_eager_fallback(sample_indices, embedding_indices, aggregation_weights, mode_override, device_ordinal=-1, combiners=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _enqueue_tpu_embedding_sparse_batch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sample_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sample_indices' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % sample_indices)
  _attr_N = len(sample_indices)
  if not isinstance(embedding_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'embedding_indices' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % embedding_indices)
  if len(embedding_indices) != _attr_N:
    raise ValueError(
        "List argument 'embedding_indices' to 'enqueue_tpu_embedding_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(embedding_indices), _attr_N))
  if not isinstance(aggregation_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'aggregation_weights' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % aggregation_weights)
  if len(aggregation_weights) != _attr_N:
    raise ValueError(
        "List argument 'aggregation_weights' to 'enqueue_tpu_embedding_sparse_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(aggregation_weights), _attr_N))
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  if combiners is None:
    combiners = []
  if not isinstance(combiners, (list, tuple)):
    raise TypeError(
        "Expected list for 'combiners' argument to "
        "'enqueue_tpu_embedding_sparse_batch' Op, not %r." % combiners)
  combiners = [_execute.make_str(_s, "combiners") for _s in combiners]
  sample_indices = _ops.convert_n_to_tensor(sample_indices, _dtypes.int32)
  embedding_indices = _ops.convert_n_to_tensor(embedding_indices, _dtypes.int32)
  aggregation_weights = _ops.convert_n_to_tensor(aggregation_weights, _dtypes.float32)
  mode_override = _ops.convert_to_tensor(mode_override, _dtypes.string)
  _inputs_flat = list(sample_indices) + list(embedding_indices) + list(aggregation_weights) + [mode_override]
  _attrs = ("N", _attr_N, "device_ordinal", device_ordinal, "combiners",
  combiners)
  _result = _execute.execute(b"EnqueueTPUEmbeddingSparseBatch", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("EnqueueTPUEmbeddingSparseBatch")(None)


@_dispatch.add_dispatch_list
@tf_export('enqueue_tpu_embedding_sparse_tensor_batch')
def _enqueue_tpu_embedding_sparse_tensor_batch(sample_indices, embedding_indices, aggregation_weights, mode_override, table_ids, device_ordinal=-1, combiners=[], name=None):
  r"""This Op eases the porting of code that uses tf.nn.embedding_lookup_sparse().

  sample_indices[i], embedding_indices[i] and aggregation_weights[i] correspond
  to the ith feature. table_ids[i] indicates which embedding table to look up ith
  feature.

  The tensors at corresponding positions in the three input lists (sample_indices,
  embedding_indices and aggregation_weights) must have the same shape, i.e. rank 1
  with dim_size() equal to the total number of lookups into the table described by
  the corresponding feature.

  Args:
    sample_indices: A list of at least 1 `Tensor` objects with type `int32`.
      A list of rank 1 Tensors specifying the training example to
      which the corresponding embedding_indices and aggregation_weights values
      belong. It corresponds to sp_ids.indices[:,0] in  embedding_lookup_sparse().
    embedding_indices: A list with the same length as `sample_indices` of `Tensor` objects with type `int32`.
      A list of rank 1 Tensors, indices into the embedding tables.
      It corresponds to sp_ids.values in embedding_lookup_sparse().
    aggregation_weights: A list with the same length as `sample_indices` of `Tensor` objects with type `float32`.
      A list of rank 1 Tensors containing per training example
      aggregation weights. It corresponds to sp_weights.values in
      embedding_lookup_sparse().
    mode_override: A `Tensor` of type `string`.
      A string input that overrides the mode specified in the
      TPUEmbeddingConfiguration. Supported values are {'unspecified', 'inference',
      'training', 'backward_pass_only'}. When set to 'unspecified', the mode set
      in TPUEmbeddingConfiguration is used, otherwise mode_override is used.
    table_ids: A list of `ints`.
      A list of integers specifying the identifier of the embedding table
      (offset of TableDescriptor in the TPUEmbeddingConfiguration) to lookup the
      corresponding input. The ith input is looked up using table_ids[i]. The size
      of the table_ids list must be equal to that of sample_indices,
      embedding_indices and aggregation_weights.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. Should be >= 0 and less than the number
      of TPU cores in the task on which the node is placed.
    combiners: An optional list of `strings`. Defaults to `[]`.
      A list of string scalars, one for each embedding table that specify
      how to normalize the embedding activations after weighted summation.
      Supported combiners are 'mean', 'sum', or 'sqrtn'. It is invalid to have
      the sum of the weights be 0 for 'mean' or the sum of the squared weights be
      0 for 'sqrtn'. If combiners isn't passed, the default is to use 'sum' for
      all tables.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "EnqueueTPUEmbeddingSparseTensorBatch", name,
        _ctx._post_execution_callbacks, sample_indices, embedding_indices,
        aggregation_weights, mode_override, "device_ordinal", device_ordinal,
        "combiners", combiners, "table_ids", table_ids)
      return _result
    except _core._FallbackException:
      try:
        return _enqueue_tpu_embedding_sparse_tensor_batch_eager_fallback(
            sample_indices, embedding_indices, aggregation_weights,
            mode_override, device_ordinal=device_ordinal, combiners=combiners,
            table_ids=table_ids, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              _enqueue_tpu_embedding_sparse_tensor_batch, sample_indices=sample_indices,
                                                          embedding_indices=embedding_indices,
                                                          aggregation_weights=aggregation_weights,
                                                          mode_override=mode_override,
                                                          table_ids=table_ids,
                                                          device_ordinal=device_ordinal,
                                                          combiners=combiners,
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
  if not isinstance(sample_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sample_indices' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % sample_indices)
  _attr_N = len(sample_indices)
  if not isinstance(embedding_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'embedding_indices' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % embedding_indices)
  if len(embedding_indices) != _attr_N:
    raise ValueError(
        "List argument 'embedding_indices' to 'enqueue_tpu_embedding_sparse_tensor_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(embedding_indices), _attr_N))
  if not isinstance(aggregation_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'aggregation_weights' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % aggregation_weights)
  if len(aggregation_weights) != _attr_N:
    raise ValueError(
        "List argument 'aggregation_weights' to 'enqueue_tpu_embedding_sparse_tensor_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(aggregation_weights), _attr_N))
  if not isinstance(table_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'table_ids' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % table_ids)
  table_ids = [_execute.make_int(_i, "table_ids") for _i in table_ids]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  if combiners is None:
    combiners = []
  if not isinstance(combiners, (list, tuple)):
    raise TypeError(
        "Expected list for 'combiners' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % combiners)
  combiners = [_execute.make_str(_s, "combiners") for _s in combiners]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EnqueueTPUEmbeddingSparseTensorBatch", sample_indices=sample_indices,
                                                embedding_indices=embedding_indices,
                                                aggregation_weights=aggregation_weights,
                                                mode_override=mode_override,
                                                table_ids=table_ids,
                                                device_ordinal=device_ordinal,
                                                combiners=combiners,
                                                name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          _enqueue_tpu_embedding_sparse_tensor_batch, sample_indices=sample_indices,
                                                      embedding_indices=embedding_indices,
                                                      aggregation_weights=aggregation_weights,
                                                      mode_override=mode_override,
                                                      table_ids=table_ids,
                                                      device_ordinal=device_ordinal,
                                                      combiners=combiners,
                                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def _enqueue_tpu_embedding_sparse_tensor_batch_eager_fallback(sample_indices, embedding_indices, aggregation_weights, mode_override, table_ids, device_ordinal=-1, combiners=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _enqueue_tpu_embedding_sparse_tensor_batch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sample_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'sample_indices' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % sample_indices)
  _attr_N = len(sample_indices)
  if not isinstance(embedding_indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'embedding_indices' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % embedding_indices)
  if len(embedding_indices) != _attr_N:
    raise ValueError(
        "List argument 'embedding_indices' to 'enqueue_tpu_embedding_sparse_tensor_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(embedding_indices), _attr_N))
  if not isinstance(aggregation_weights, (list, tuple)):
    raise TypeError(
        "Expected list for 'aggregation_weights' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % aggregation_weights)
  if len(aggregation_weights) != _attr_N:
    raise ValueError(
        "List argument 'aggregation_weights' to 'enqueue_tpu_embedding_sparse_tensor_batch' Op with length %d "
        "must match length %d of argument 'sample_indices'." %
        (len(aggregation_weights), _attr_N))
  if not isinstance(table_ids, (list, tuple)):
    raise TypeError(
        "Expected list for 'table_ids' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % table_ids)
  table_ids = [_execute.make_int(_i, "table_ids") for _i in table_ids]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  if combiners is None:
    combiners = []
  if not isinstance(combiners, (list, tuple)):
    raise TypeError(
        "Expected list for 'combiners' argument to "
        "'enqueue_tpu_embedding_sparse_tensor_batch' Op, not %r." % combiners)
  combiners = [_execute.make_str(_s, "combiners") for _s in combiners]
  sample_indices = _ops.convert_n_to_tensor(sample_indices, _dtypes.int32)
  embedding_indices = _ops.convert_n_to_tensor(embedding_indices, _dtypes.int32)
  aggregation_weights = _ops.convert_n_to_tensor(aggregation_weights, _dtypes.float32)
  mode_override = _ops.convert_to_tensor(mode_override, _dtypes.string)
  _inputs_flat = list(sample_indices) + list(embedding_indices) + list(aggregation_weights) + [mode_override]
  _attrs = ("N", _attr_N, "device_ordinal", device_ordinal, "combiners",
  combiners, "table_ids", table_ids)
  _result = _execute.execute(b"EnqueueTPUEmbeddingSparseTensorBatch", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("EnqueueTPUEmbeddingSparseTensorBatch")(None)


@_dispatch.add_dispatch_list
@tf_export('infeed_dequeue')
def infeed_dequeue(dtype, shape, name=None):
  r"""A placeholder op for a value that will be fed into the computation.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: A `tf.TensorShape` or list of `ints`. The shape of the tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
    A tensor that will be provided using the infeed mechanism.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedDequeue", name, _ctx._post_execution_callbacks, "dtype", dtype,
        "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return infeed_dequeue_eager_fallback(
            dtype=dtype, shape=shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              infeed_dequeue, dtype=dtype, shape=shape, name=name)
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
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedDequeue", dtype=dtype, shape=shape, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          infeed_dequeue, dtype=dtype, shape=shape, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "InfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def infeed_dequeue_eager_fallback(dtype, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_dequeue
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"InfeedDequeue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("InfeedDequeue")(None)


@_dispatch.add_dispatch_list
@tf_export('infeed_dequeue_tuple')
def infeed_dequeue_tuple(dtypes, shapes, name=None):
  r"""A placeholder op for multiple values that will be fed into the computation

  simultaneously as an XLA tuple.

  Args:
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
      The element types of each element in `outputs`.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `outputs`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
    A list of tensors that will be provided using the infeed mechanism.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedDequeueTuple", name, _ctx._post_execution_callbacks, "dtypes",
        dtypes, "shapes", shapes)
      return _result
    except _core._FallbackException:
      try:
        return infeed_dequeue_tuple_eager_fallback(
            dtypes=dtypes, shapes=shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              infeed_dequeue_tuple, dtypes=dtypes, shapes=shapes, name=name)
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedDequeueTuple", dtypes=dtypes, shapes=shapes, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          infeed_dequeue_tuple, dtypes=dtypes, shapes=shapes, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("dtypes", _op.get_attr("dtypes"), "shapes",
            _op.get_attr("shapes"))
  _execute.record_gradient(
      "InfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result



def infeed_dequeue_tuple_eager_fallback(dtypes, shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_dequeue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  _inputs_flat = []
  _attrs = ("dtypes", dtypes, "shapes", shapes)
  _result = _execute.execute(b"InfeedDequeueTuple", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "InfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("InfeedDequeueTuple")(None)


@_dispatch.add_dispatch_list
@tf_export('infeed_enqueue')
def infeed_enqueue(input, shape=[], device_ordinal=-1, name=None):
  r"""An op which feeds a single Tensor value into the computation.

  Args:
    input: A `Tensor`.
      A tensor that will be provided using the infeed mechanism.
    shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
      The shape of the tensor.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedEnqueue", name, _ctx._post_execution_callbacks, input, "shape",
        shape, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      try:
        return infeed_enqueue_eager_fallback(
            input, shape=shape, device_ordinal=device_ordinal, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              infeed_enqueue, input=input, shape=shape,
                              device_ordinal=device_ordinal, name=name)
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
  if shape is None:
    shape = []
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedEnqueue", input=input, shape=shape,
                         device_ordinal=device_ordinal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          infeed_enqueue, input=input, shape=shape,
                          device_ordinal=device_ordinal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def infeed_enqueue_eager_fallback(input, shape=[], device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_enqueue
  """
  _ctx = ctx if ctx else _context.context()
  if shape is None:
    shape = []
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _attr_dtype, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("dtype", _attr_dtype, "shape", shape, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"InfeedEnqueue", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("InfeedEnqueue")(None)


@_dispatch.add_dispatch_list
@tf_export('infeed_enqueue_tuple')
def infeed_enqueue_tuple(inputs, shapes, device_ordinal=-1, name=None):
  r"""An op which feeds multiple Tensor values into the computation as an XLA tuple.

  Args:
    inputs: A list of `Tensor` objects.
      A list of tensors that will be provided using the infeed mechanism.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `inputs`.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InfeedEnqueueTuple", name, _ctx._post_execution_callbacks, inputs,
        "shapes", shapes, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      try:
        return infeed_enqueue_tuple_eager_fallback(
            inputs, shapes=shapes, device_ordinal=device_ordinal, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              infeed_enqueue_tuple, inputs=inputs, shapes=shapes,
                                    device_ordinal=device_ordinal, name=name)
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
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_enqueue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "InfeedEnqueueTuple", inputs=inputs, shapes=shapes,
                              device_ordinal=device_ordinal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          infeed_enqueue_tuple, inputs=inputs, shapes=shapes,
                                device_ordinal=device_ordinal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def infeed_enqueue_tuple_eager_fallback(inputs, shapes, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function infeed_enqueue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'infeed_enqueue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _attr_dtypes, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("dtypes", _attr_dtypes, "shapes", shapes, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"InfeedEnqueueTuple", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("InfeedEnqueueTuple")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adam_parameters')
def load_tpu_embedding_adam_parameters(parameters, momenta, velocities, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the ADAM optimization algorithm.
  momenta: A tensor containing the initial embedding table momenta to use in embedding
  lookups using the ADAM optimization algorithm.
  velocities: A tensor containing the initial embedding table velocities to use in embedding
  lookups using the ADAM optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the ADAM optimization algorithm.
    momenta: A `Tensor` of type `float32`.
      Value of momenta used in the ADAM optimization algorithm.
    velocities: A `Tensor` of type `float32`.
      Value of velocities used in the ADAM optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingADAMParameters", name,
        _ctx._post_execution_callbacks, parameters, momenta, velocities,
        "table_id", table_id, "table_name", table_name, "num_shards",
        num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adam_parameters_eager_fallback(
            parameters, momenta, velocities, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adam_parameters, parameters=parameters,
                                                  momenta=momenta,
                                                  velocities=velocities,
                                                  num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingADAMParameters", parameters=parameters,
                                          momenta=momenta,
                                          velocities=velocities,
                                          num_shards=num_shards,
                                          shard_id=shard_id,
                                          table_id=table_id,
                                          table_name=table_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adam_parameters, parameters=parameters,
                                              momenta=momenta,
                                              velocities=velocities,
                                              num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adam_parameters_eager_fallback(parameters, momenta, velocities, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adam_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  momenta = _ops.convert_to_tensor(momenta, _dtypes.float32)
  velocities = _ops.convert_to_tensor(velocities, _dtypes.float32)
  _inputs_flat = [parameters, momenta, velocities]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingADAMParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingADAMParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adam_parameters_grad_accum_debug')
def load_tpu_embedding_adam_parameters_grad_accum_debug(parameters, momenta, velocities, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the ADAM optimization algorithm.
  momenta: A tensor containing the initial embedding table momenta to use in embedding
  lookups using the ADAM optimization algorithm.
  velocities: A tensor containing the initial embedding table velocities to use in embedding
  lookups using the ADAM optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the ADAM optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the ADAM optimization algorithm.
    momenta: A `Tensor` of type `float32`.
      Value of momenta used in the ADAM optimization algorithm.
    velocities: A `Tensor` of type `float32`.
      Value of velocities used in the ADAM optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the ADAM optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingADAMParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, momenta, velocities,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adam_parameters_grad_accum_debug_eager_fallback(
            parameters, momenta, velocities, gradient_accumulators,
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adam_parameters_grad_accum_debug, parameters=parameters,
                                                                   momenta=momenta,
                                                                   velocities=velocities,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingADAMParametersGradAccumDebug", parameters=parameters,
                                                        momenta=momenta,
                                                        velocities=velocities,
                                                        gradient_accumulators=gradient_accumulators,
                                                        num_shards=num_shards,
                                                        shard_id=shard_id,
                                                        table_id=table_id,
                                                        table_name=table_name,
                                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adam_parameters_grad_accum_debug, parameters=parameters,
                                                               momenta=momenta,
                                                               velocities=velocities,
                                                               gradient_accumulators=gradient_accumulators,
                                                               num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adam_parameters_grad_accum_debug_eager_fallback(parameters, momenta, velocities, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adam_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  momenta = _ops.convert_to_tensor(momenta, _dtypes.float32)
  velocities = _ops.convert_to_tensor(velocities, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, momenta, velocities, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingADAMParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingADAMParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adadelta_parameters')
def load_tpu_embedding_adadelta_parameters(parameters, accumulators, updates, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Adadelta optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the Adadelta optimization algorithm.
  updates: A tensor containing the initial embedding table updates to use in embedding
  lookups using the Adadelta optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Adadelta optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the Adadelta optimization algorithm.
    updates: A `Tensor` of type `float32`.
      Value of updates used in the Adadelta optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingAdadeltaParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators, updates,
        "table_id", table_id, "table_name", table_name, "num_shards",
        num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adadelta_parameters_eager_fallback(
            parameters, accumulators, updates, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adadelta_parameters, parameters=parameters,
                                                      accumulators=accumulators,
                                                      updates=updates,
                                                      num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingAdadeltaParameters", parameters=parameters,
                                              accumulators=accumulators,
                                              updates=updates,
                                              num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adadelta_parameters, parameters=parameters,
                                                  accumulators=accumulators,
                                                  updates=updates,
                                                  num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adadelta_parameters_eager_fallback(parameters, accumulators, updates, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adadelta_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  updates = _ops.convert_to_tensor(updates, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, updates]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingAdadeltaParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingAdadeltaParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adadelta_parameters_grad_accum_debug')
def load_tpu_embedding_adadelta_parameters_grad_accum_debug(parameters, accumulators, updates, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Adadelta optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the Adadelta optimization algorithm.
  updates: A tensor containing the initial embedding table updates to use in embedding
  lookups using the Adadelta optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the Adadelta optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Adadelta optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the Adadelta optimization algorithm.
    updates: A `Tensor` of type `float32`.
      Value of updates used in the Adadelta optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the Adadelta optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingAdadeltaParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, accumulators, updates,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adadelta_parameters_grad_accum_debug_eager_fallback(
            parameters, accumulators, updates, gradient_accumulators,
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adadelta_parameters_grad_accum_debug, parameters=parameters,
                                                                       accumulators=accumulators,
                                                                       updates=updates,
                                                                       gradient_accumulators=gradient_accumulators,
                                                                       num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingAdadeltaParametersGradAccumDebug", parameters=parameters,
                                                            accumulators=accumulators,
                                                            updates=updates,
                                                            gradient_accumulators=gradient_accumulators,
                                                            num_shards=num_shards,
                                                            shard_id=shard_id,
                                                            table_id=table_id,
                                                            table_name=table_name,
                                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adadelta_parameters_grad_accum_debug, parameters=parameters,
                                                                   accumulators=accumulators,
                                                                   updates=updates,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adadelta_parameters_grad_accum_debug_eager_fallback(parameters, accumulators, updates, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adadelta_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  updates = _ops.convert_to_tensor(updates, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, updates, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingAdadeltaParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingAdadeltaParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adagrad_parameters')
def load_tpu_embedding_adagrad_parameters(parameters, accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Adagrad optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the Adagrad optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingAdagradParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators, "table_id",
        table_id, "table_name", table_name, "num_shards", num_shards,
        "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adagrad_parameters_eager_fallback(
            parameters, accumulators, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adagrad_parameters, parameters=parameters,
                                                     accumulators=accumulators,
                                                     num_shards=num_shards,
                                                     shard_id=shard_id,
                                                     table_id=table_id,
                                                     table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingAdagradParameters", parameters=parameters,
                                             accumulators=accumulators,
                                             num_shards=num_shards,
                                             shard_id=shard_id,
                                             table_id=table_id,
                                             table_name=table_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adagrad_parameters, parameters=parameters,
                                                 accumulators=accumulators,
                                                 num_shards=num_shards,
                                                 shard_id=shard_id,
                                                 table_id=table_id,
                                                 table_name=table_name,
                                                 name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adagrad_parameters_eager_fallback(parameters, accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingAdagradParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingAdagradParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_adagrad_parameters_grad_accum_debug')
def load_tpu_embedding_adagrad_parameters_grad_accum_debug(parameters, accumulators, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Adagrad optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the Adagrad optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the Adagrad optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the Adagrad optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingAdagradParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, accumulators,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_adagrad_parameters_grad_accum_debug_eager_fallback(
            parameters, accumulators, gradient_accumulators,
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_adagrad_parameters_grad_accum_debug, parameters=parameters,
                                                                      accumulators=accumulators,
                                                                      gradient_accumulators=gradient_accumulators,
                                                                      num_shards=num_shards,
                                                                      shard_id=shard_id,
                                                                      table_id=table_id,
                                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingAdagradParametersGradAccumDebug", parameters=parameters,
                                                           accumulators=accumulators,
                                                           gradient_accumulators=gradient_accumulators,
                                                           num_shards=num_shards,
                                                           shard_id=shard_id,
                                                           table_id=table_id,
                                                           table_name=table_name,
                                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_adagrad_parameters_grad_accum_debug, parameters=parameters,
                                                                  accumulators=accumulators,
                                                                  gradient_accumulators=gradient_accumulators,
                                                                  num_shards=num_shards,
                                                                  shard_id=shard_id,
                                                                  table_id=table_id,
                                                                  table_name=table_name,
                                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_adagrad_parameters_grad_accum_debug_eager_fallback(parameters, accumulators, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_adagrad_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingAdagradParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingAdagradParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_centered_rms_prop_parameters')
def load_tpu_embedding_centered_rms_prop_parameters(parameters, ms, mom, mg, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the centered RMSProp optimization algorithm.
  ms: A tensor containing the initial embedding table ms to use in embedding
  lookups using the centered RMSProp optimization algorithm.
  mom: A tensor containing the initial embedding table mom to use in embedding
  lookups using the centered RMSProp optimization algorithm.
  mg: A tensor containing the initial embedding table mg to use in embedding
  lookups using the centered RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the centered RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`.
      Value of ms used in the centered RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`.
      Value of mom used in the centered RMSProp optimization algorithm.
    mg: A `Tensor` of type `float32`.
      Value of mg used in the centered RMSProp optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingCenteredRMSPropParameters", name,
        _ctx._post_execution_callbacks, parameters, ms, mom, mg, "table_id",
        table_id, "table_name", table_name, "num_shards", num_shards,
        "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_centered_rms_prop_parameters_eager_fallback(
            parameters, ms, mom, mg, table_id=table_id, table_name=table_name,
            num_shards=num_shards, shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_centered_rms_prop_parameters, parameters=parameters,
                                                               ms=ms, mom=mom,
                                                               mg=mg,
                                                               num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingCenteredRMSPropParameters", parameters=parameters,
                                                     ms=ms, mom=mom, mg=mg,
                                                     num_shards=num_shards,
                                                     shard_id=shard_id,
                                                     table_id=table_id,
                                                     table_name=table_name,
                                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_centered_rms_prop_parameters, parameters=parameters,
                                                           ms=ms, mom=mom,
                                                           mg=mg,
                                                           num_shards=num_shards,
                                                           shard_id=shard_id,
                                                           table_id=table_id,
                                                           table_name=table_name,
                                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_centered_rms_prop_parameters_eager_fallback(parameters, ms, mom, mg, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_centered_rms_prop_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  ms = _ops.convert_to_tensor(ms, _dtypes.float32)
  mom = _ops.convert_to_tensor(mom, _dtypes.float32)
  mg = _ops.convert_to_tensor(mg, _dtypes.float32)
  _inputs_flat = [parameters, ms, mom, mg]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingCenteredRMSPropParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingCenteredRMSPropParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_ftrl_parameters')
def load_tpu_embedding_ftrl_parameters(parameters, accumulators, linears, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the FTRL optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the FTRL optimization algorithm.
  linears: A tensor containing the initial embedding table linears to use in embedding
  lookups using the FTRL optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the FTRL optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the FTRL optimization algorithm.
    linears: A `Tensor` of type `float32`.
      Value of linears used in the FTRL optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingFTRLParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators, linears,
        "table_id", table_id, "table_name", table_name, "num_shards",
        num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_ftrl_parameters_eager_fallback(
            parameters, accumulators, linears, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_ftrl_parameters, parameters=parameters,
                                                  accumulators=accumulators,
                                                  linears=linears,
                                                  num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingFTRLParameters", parameters=parameters,
                                          accumulators=accumulators,
                                          linears=linears,
                                          num_shards=num_shards,
                                          shard_id=shard_id,
                                          table_id=table_id,
                                          table_name=table_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_ftrl_parameters, parameters=parameters,
                                              accumulators=accumulators,
                                              linears=linears,
                                              num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_ftrl_parameters_eager_fallback(parameters, accumulators, linears, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_ftrl_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  linears = _ops.convert_to_tensor(linears, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, linears]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingFTRLParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingFTRLParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_ftrl_parameters_grad_accum_debug')
def load_tpu_embedding_ftrl_parameters_grad_accum_debug(parameters, accumulators, linears, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the FTRL optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the FTRL optimization algorithm.
  linears: A tensor containing the initial embedding table linears to use in embedding
  lookups using the FTRL optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the FTRL optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the FTRL optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the FTRL optimization algorithm.
    linears: A `Tensor` of type `float32`.
      Value of linears used in the FTRL optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the FTRL optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingFTRLParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, accumulators, linears,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_ftrl_parameters_grad_accum_debug_eager_fallback(
            parameters, accumulators, linears, gradient_accumulators,
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_ftrl_parameters_grad_accum_debug, parameters=parameters,
                                                                   accumulators=accumulators,
                                                                   linears=linears,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingFTRLParametersGradAccumDebug", parameters=parameters,
                                                        accumulators=accumulators,
                                                        linears=linears,
                                                        gradient_accumulators=gradient_accumulators,
                                                        num_shards=num_shards,
                                                        shard_id=shard_id,
                                                        table_id=table_id,
                                                        table_name=table_name,
                                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_ftrl_parameters_grad_accum_debug, parameters=parameters,
                                                               accumulators=accumulators,
                                                               linears=linears,
                                                               gradient_accumulators=gradient_accumulators,
                                                               num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_ftrl_parameters_grad_accum_debug_eager_fallback(parameters, accumulators, linears, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_ftrl_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  linears = _ops.convert_to_tensor(linears, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, linears, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingFTRLParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingFTRLParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_mdl_adagrad_light_parameters')
def load_tpu_embedding_mdl_adagrad_light_parameters(parameters, accumulators, weights, benefits, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the MDL Adagrad Light optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the MDL Adagrad Light optimization algorithm.
  weights: A tensor containing the initial embedding table weights to use in embedding
  lookups using the MDL Adagrad Light optimization algorithm.
  benefits: A tensor containing the initial embedding table benefits to use in embedding
  lookups using the MDL Adagrad Light optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the MDL Adagrad Light optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the MDL Adagrad Light optimization algorithm.
    weights: A `Tensor` of type `float32`.
      Value of weights used in the MDL Adagrad Light optimization algorithm.
    benefits: A `Tensor` of type `float32`.
      Value of benefits used in the MDL Adagrad Light optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingMDLAdagradLightParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators, weights,
        benefits, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_mdl_adagrad_light_parameters_eager_fallback(
            parameters, accumulators, weights, benefits, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_mdl_adagrad_light_parameters, parameters=parameters,
                                                               accumulators=accumulators,
                                                               weights=weights,
                                                               benefits=benefits,
                                                               num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingMDLAdagradLightParameters", parameters=parameters,
                                                     accumulators=accumulators,
                                                     weights=weights,
                                                     benefits=benefits,
                                                     num_shards=num_shards,
                                                     shard_id=shard_id,
                                                     table_id=table_id,
                                                     table_name=table_name,
                                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_mdl_adagrad_light_parameters, parameters=parameters,
                                                           accumulators=accumulators,
                                                           weights=weights,
                                                           benefits=benefits,
                                                           num_shards=num_shards,
                                                           shard_id=shard_id,
                                                           table_id=table_id,
                                                           table_name=table_name,
                                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_mdl_adagrad_light_parameters_eager_fallback(parameters, accumulators, weights, benefits, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_mdl_adagrad_light_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  weights = _ops.convert_to_tensor(weights, _dtypes.float32)
  benefits = _ops.convert_to_tensor(benefits, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, weights, benefits]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingMDLAdagradLightParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingMDLAdagradLightParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_momentum_parameters')
def load_tpu_embedding_momentum_parameters(parameters, momenta, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Momentum optimization algorithm.
  momenta: A tensor containing the initial embedding table momenta to use in embedding
  lookups using the Momentum optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Momentum optimization algorithm.
    momenta: A `Tensor` of type `float32`.
      Value of momenta used in the Momentum optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingMomentumParameters", name,
        _ctx._post_execution_callbacks, parameters, momenta, "table_id",
        table_id, "table_name", table_name, "num_shards", num_shards,
        "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_momentum_parameters_eager_fallback(
            parameters, momenta, table_id=table_id, table_name=table_name,
            num_shards=num_shards, shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_momentum_parameters, parameters=parameters,
                                                      momenta=momenta,
                                                      num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingMomentumParameters", parameters=parameters,
                                              momenta=momenta,
                                              num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_momentum_parameters, parameters=parameters,
                                                  momenta=momenta,
                                                  num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_momentum_parameters_eager_fallback(parameters, momenta, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_momentum_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  momenta = _ops.convert_to_tensor(momenta, _dtypes.float32)
  _inputs_flat = [parameters, momenta]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingMomentumParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingMomentumParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_momentum_parameters_grad_accum_debug')
def load_tpu_embedding_momentum_parameters_grad_accum_debug(parameters, momenta, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the Momentum optimization algorithm.
  momenta: A tensor containing the initial embedding table momenta to use in embedding
  lookups using the Momentum optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the Momentum optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the Momentum optimization algorithm.
    momenta: A `Tensor` of type `float32`.
      Value of momenta used in the Momentum optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the Momentum optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingMomentumParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, momenta,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_momentum_parameters_grad_accum_debug_eager_fallback(
            parameters, momenta, gradient_accumulators, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_momentum_parameters_grad_accum_debug, parameters=parameters,
                                                                       momenta=momenta,
                                                                       gradient_accumulators=gradient_accumulators,
                                                                       num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingMomentumParametersGradAccumDebug", parameters=parameters,
                                                            momenta=momenta,
                                                            gradient_accumulators=gradient_accumulators,
                                                            num_shards=num_shards,
                                                            shard_id=shard_id,
                                                            table_id=table_id,
                                                            table_name=table_name,
                                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_momentum_parameters_grad_accum_debug, parameters=parameters,
                                                                   momenta=momenta,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_momentum_parameters_grad_accum_debug_eager_fallback(parameters, momenta, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_momentum_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  momenta = _ops.convert_to_tensor(momenta, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, momenta, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingMomentumParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingMomentumParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_proximal_adagrad_parameters')
def load_tpu_embedding_proximal_adagrad_parameters(parameters, accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the proximal Adagrad optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the proximal Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the proximal Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the proximal Adagrad optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingProximalAdagradParameters", name,
        _ctx._post_execution_callbacks, parameters, accumulators, "table_id",
        table_id, "table_name", table_name, "num_shards", num_shards,
        "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_proximal_adagrad_parameters_eager_fallback(
            parameters, accumulators, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_proximal_adagrad_parameters, parameters=parameters,
                                                              accumulators=accumulators,
                                                              num_shards=num_shards,
                                                              shard_id=shard_id,
                                                              table_id=table_id,
                                                              table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingProximalAdagradParameters", parameters=parameters,
                                                     accumulators=accumulators,
                                                     num_shards=num_shards,
                                                     shard_id=shard_id,
                                                     table_id=table_id,
                                                     table_name=table_name,
                                                     name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_proximal_adagrad_parameters, parameters=parameters,
                                                          accumulators=accumulators,
                                                          num_shards=num_shards,
                                                          shard_id=shard_id,
                                                          table_id=table_id,
                                                          table_name=table_name,
                                                          name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_proximal_adagrad_parameters_eager_fallback(parameters, accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_proximal_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingProximalAdagradParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingProximalAdagradParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug')
def load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug(parameters, accumulators, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the proximal Adagrad optimization algorithm.
  accumulators: A tensor containing the initial embedding table accumulators to use in embedding
  lookups using the proximal Adagrad optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the proximal Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the proximal Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`.
      Value of accumulators used in the proximal Adagrad optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the proximal Adagrad optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, accumulators,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_eager_fallback(
            parameters, accumulators, gradient_accumulators,
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug, parameters=parameters, accumulators=accumulators, gradient_accumulators=gradient_accumulators, num_shards=num_shards, shard_id=shard_id, table_id=table_id, table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug", parameters=parameters,
                                                                   accumulators=accumulators,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug, parameters=parameters,
                                                                           accumulators=accumulators,
                                                                           gradient_accumulators=gradient_accumulators,
                                                                           num_shards=num_shards,
                                                                           shard_id=shard_id,
                                                                           table_id=table_id,
                                                                           table_name=table_name,
                                                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_eager_fallback(parameters, accumulators, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  accumulators = _ops.convert_to_tensor(accumulators, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, accumulators, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_rms_prop_parameters')
def load_tpu_embedding_rms_prop_parameters(parameters, ms, mom, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the RMSProp optimization algorithm.
  ms: A tensor containing the initial embedding table ms to use in embedding
  lookups using the RMSProp optimization algorithm.
  mom: A tensor containing the initial embedding table mom to use in embedding
  lookups using the RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`.
      Value of ms used in the RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`.
      Value of mom used in the RMSProp optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingRMSPropParameters", name,
        _ctx._post_execution_callbacks, parameters, ms, mom, "table_id",
        table_id, "table_name", table_name, "num_shards", num_shards,
        "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_rms_prop_parameters_eager_fallback(
            parameters, ms, mom, table_id=table_id, table_name=table_name,
            num_shards=num_shards, shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_rms_prop_parameters, parameters=parameters,
                                                      ms=ms, mom=mom,
                                                      num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingRMSPropParameters", parameters=parameters, ms=ms,
                                             mom=mom, num_shards=num_shards,
                                             shard_id=shard_id,
                                             table_id=table_id,
                                             table_name=table_name, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_rms_prop_parameters, parameters=parameters,
                                                  ms=ms, mom=mom,
                                                  num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_rms_prop_parameters_eager_fallback(parameters, ms, mom, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_rms_prop_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  ms = _ops.convert_to_tensor(ms, _dtypes.float32)
  mom = _ops.convert_to_tensor(mom, _dtypes.float32)
  _inputs_flat = [parameters, ms, mom]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingRMSPropParameters", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingRMSPropParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_rms_prop_parameters_grad_accum_debug')
def load_tpu_embedding_rms_prop_parameters_grad_accum_debug(parameters, ms, mom, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the RMSProp optimization algorithm.
  ms: A tensor containing the initial embedding table ms to use in embedding
  lookups using the RMSProp optimization algorithm.
  mom: A tensor containing the initial embedding table mom to use in embedding
  lookups using the RMSProp optimization algorithm.
  gradient_accumulators: A tensor containing the initial embedding table gradient_accumulators to use in embedding
  lookups using the RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`.
      Value of ms used in the RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`.
      Value of mom used in the RMSProp optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`.
      Value of gradient_accumulators used in the RMSProp optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingRMSPropParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, parameters, ms, mom,
        gradient_accumulators, "table_id", table_id, "table_name", table_name,
        "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_rms_prop_parameters_grad_accum_debug_eager_fallback(
            parameters, ms, mom, gradient_accumulators, table_id=table_id,
            table_name=table_name, num_shards=num_shards, shard_id=shard_id,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_rms_prop_parameters_grad_accum_debug, parameters=parameters,
                                                                       ms=ms,
                                                                       mom=mom,
                                                                       gradient_accumulators=gradient_accumulators,
                                                                       num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingRMSPropParametersGradAccumDebug", parameters=parameters,
                                                           ms=ms, mom=mom,
                                                           gradient_accumulators=gradient_accumulators,
                                                           num_shards=num_shards,
                                                           shard_id=shard_id,
                                                           table_id=table_id,
                                                           table_name=table_name,
                                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_rms_prop_parameters_grad_accum_debug, parameters=parameters,
                                                                   ms=ms,
                                                                   mom=mom,
                                                                   gradient_accumulators=gradient_accumulators,
                                                                   num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_rms_prop_parameters_grad_accum_debug_eager_fallback(parameters, ms, mom, gradient_accumulators, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_rms_prop_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  ms = _ops.convert_to_tensor(ms, _dtypes.float32)
  mom = _ops.convert_to_tensor(mom, _dtypes.float32)
  gradient_accumulators = _ops.convert_to_tensor(gradient_accumulators, _dtypes.float32)
  _inputs_flat = [parameters, ms, mom, gradient_accumulators]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingRMSPropParametersGradAccumDebug",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingRMSPropParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('load_tpu_embedding_stochastic_gradient_descent_parameters')
def load_tpu_embedding_stochastic_gradient_descent_parameters(parameters, num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Load embedding parameters for a single table.

  
  An op that loads optimization parameters into HBM for embedding. Must be
  preceded by a ConfigureTPUEmbeddingHost op that sets up the correct
  embedding table configuration. For example, this op is used to install
  parameters that are loaded from a checkpoint before a training loop is
  executed.

  parameters: A tensor containing the initial embedding table parameters to use in embedding
  lookups using the stochastic gradient descent optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    parameters: A `Tensor` of type `float32`.
      Value of parameters used in the stochastic gradient descent optimization algorithm.
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LoadTPUEmbeddingStochasticGradientDescentParameters", name,
        _ctx._post_execution_callbacks, parameters, "table_id", table_id,
        "table_name", table_name, "num_shards", num_shards, "shard_id",
        shard_id)
      return _result
    except _core._FallbackException:
      try:
        return load_tpu_embedding_stochastic_gradient_descent_parameters_eager_fallback(
            parameters, table_id=table_id, table_name=table_name,
            num_shards=num_shards, shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              load_tpu_embedding_stochastic_gradient_descent_parameters, parameters=parameters,
                                                                         num_shards=num_shards,
                                                                         shard_id=shard_id,
                                                                         table_id=table_id,
                                                                         table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LoadTPUEmbeddingStochasticGradientDescentParameters", parameters=parameters,
                                                               num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          load_tpu_embedding_stochastic_gradient_descent_parameters, parameters=parameters,
                                                                     num_shards=num_shards,
                                                                     shard_id=shard_id,
                                                                     table_id=table_id,
                                                                     table_name=table_name,
                                                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def load_tpu_embedding_stochastic_gradient_descent_parameters_eager_fallback(parameters, num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function load_tpu_embedding_stochastic_gradient_descent_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  parameters = _ops.convert_to_tensor(parameters, _dtypes.float32)
  _inputs_flat = [parameters]
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"LoadTPUEmbeddingStochasticGradientDescentParameters",
                             0, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("LoadTPUEmbeddingStochasticGradientDescentParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('outfeed_dequeue')
def outfeed_dequeue(dtype, shape, device_ordinal=-1, name=None):
  r"""Retrieves a single tensor from the computation outfeed.  This operation will

  block indefinitely until data is available.

  Args:
    dtype: A `tf.DType`. The type of elements in the tensor.
    shape: A `tf.TensorShape` or list of `ints`. The shape of the tensor.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
    A tensor that will be read from the device outfeed.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedDequeue", name, _ctx._post_execution_callbacks, "dtype",
        dtype, "shape", shape, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      try:
        return outfeed_dequeue_eager_fallback(
            dtype=dtype, shape=shape, device_ordinal=device_ordinal,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              outfeed_dequeue, dtype=dtype, shape=shape,
                               device_ordinal=device_ordinal, name=name)
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
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedDequeue", dtype=dtype, shape=shape,
                          device_ordinal=device_ordinal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          outfeed_dequeue, dtype=dtype, shape=shape,
                           device_ordinal=device_ordinal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"),
            "device_ordinal", _op.get_attr("device_ordinal"))
  _execute.record_gradient(
      "OutfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def outfeed_dequeue_eager_fallback(dtype, shape, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_dequeue
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape, "device_ordinal", device_ordinal)
  _result = _execute.execute(b"OutfeedDequeue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OutfeedDequeue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("OutfeedDequeue")(None)


@_dispatch.add_dispatch_list
@tf_export('outfeed_dequeue_tuple')
def outfeed_dequeue_tuple(dtypes, shapes, device_ordinal=-1, name=None):
  r"""Retrieve multiple values that will be emitted by the computation as an XLA

  tuple.  This operations will block indefinitely until data is available.
  Output `i` corresponds to XLA tuple element `i`.

  Args:
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
      The element types of each element in `outputs`.
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shapes of each tensor in `outputs`.
    device_ordinal: An optional `int`. Defaults to `-1`.
      The TPU device to use. This should be -1 when the Op
      is running on a TPU device, and >= 0 when the Op is running on the CPU
      device.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
    A list of tensors that will be read from the outfeed.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedDequeueTuple", name, _ctx._post_execution_callbacks, "dtypes",
        dtypes, "shapes", shapes, "device_ordinal", device_ordinal)
      return _result
    except _core._FallbackException:
      try:
        return outfeed_dequeue_tuple_eager_fallback(
            dtypes=dtypes, shapes=shapes, device_ordinal=device_ordinal,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              outfeed_dequeue_tuple, dtypes=dtypes, shapes=shapes,
                                     device_ordinal=device_ordinal, name=name)
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "OutfeedDequeueTuple", dtypes=dtypes, shapes=shapes,
                               device_ordinal=device_ordinal, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          outfeed_dequeue_tuple, dtypes=dtypes, shapes=shapes,
                                 device_ordinal=device_ordinal, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("dtypes", _op.get_attr("dtypes"), "shapes",
            _op.get_attr("shapes"), "device_ordinal",
            _op.get_attr("device_ordinal"))
  _execute.record_gradient(
      "OutfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result



def outfeed_dequeue_tuple_eager_fallback(dtypes, shapes, device_ordinal=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_dequeue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'outfeed_dequeue_tuple' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if device_ordinal is None:
    device_ordinal = -1
  device_ordinal = _execute.make_int(device_ordinal, "device_ordinal")
  _inputs_flat = []
  _attrs = ("dtypes", dtypes, "shapes", shapes, "device_ordinal",
  device_ordinal)
  _result = _execute.execute(b"OutfeedDequeueTuple", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OutfeedDequeueTuple", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("OutfeedDequeueTuple")(None)


@_dispatch.add_dispatch_list
@tf_export('outfeed_enqueue')
def outfeed_enqueue(input, name=None):
  r"""An op which emits a single Tensor value from an XLA computation.

  Args:
    input: A `Tensor`. A tensor that will be inserted into the outfeed queue.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedEnqueue", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return outfeed_enqueue_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              outfeed_enqueue, input=input, name=name)
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
        "OutfeedEnqueue", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          outfeed_enqueue, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def outfeed_enqueue_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_enqueue
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"OutfeedEnqueue", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("OutfeedEnqueue")(None)


@_dispatch.add_dispatch_list
@tf_export('outfeed_enqueue_tuple')
def outfeed_enqueue_tuple(inputs, name=None):
  r"""An op which emits multiple Tensor values from an XLA computation.

  Args:
    inputs: A list of `Tensor` objects.
      A list of tensors that will be inserted into the outfeed queue as an
      XLA tuple.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OutfeedEnqueueTuple", name, _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      try:
        return outfeed_enqueue_tuple_eager_fallback(
            inputs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              outfeed_enqueue_tuple, inputs=inputs, name=name)
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
        "OutfeedEnqueueTuple", inputs=inputs, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          outfeed_enqueue_tuple, inputs=inputs, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def outfeed_enqueue_tuple_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function outfeed_enqueue_tuple
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtypes, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("dtypes", _attr_dtypes)
  _result = _execute.execute(b"OutfeedEnqueueTuple", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("OutfeedEnqueueTuple")(None)


@_dispatch.add_dispatch_list
@tf_export('recv_tpu_embedding_activations')
def recv_tpu_embedding_activations(num_outputs, config, name=None):
  r"""An op that receives embedding activations on the TPU.

  The TPU system performs the embedding lookups and aggregations specified by
  the arguments to TPUEmbeddingEnqueue(Integer/Sparse/SparseTensor)Batch. The
  results of these aggregations are visible to the Tensorflow Graph as the
  outputs of a RecvTPUEmbeddingActivations op. This op returns a list containing
  one Tensor of activations per table specified in the model. There can be at
  most one RecvTPUEmbeddingActivations op in the TPU graph.

  Args:
    num_outputs: An `int` that is `>= 1`.
      The number of output activation tensors, equal to the number of
      embedding tables in the model.
    config: A `string`. Serialized TPUEmbeddingConfiguration proto.
    name: A name for the operation (optional).

  Returns:
    A list of `num_outputs` `Tensor` objects with type `float32`.
    A TensorList of embedding activations containing one Tensor per
    embedding table in the model.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RecvTPUEmbeddingActivations", name, _ctx._post_execution_callbacks,
        "num_outputs", num_outputs, "config", config)
      return _result
    except _core._FallbackException:
      try:
        return recv_tpu_embedding_activations_eager_fallback(
            num_outputs=num_outputs, config=config, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              recv_tpu_embedding_activations, num_outputs=num_outputs,
                                              config=config, name=name)
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
  num_outputs = _execute.make_int(num_outputs, "num_outputs")
  config = _execute.make_str(config, "config")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RecvTPUEmbeddingActivations", num_outputs=num_outputs, config=config,
                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          recv_tpu_embedding_activations, num_outputs=num_outputs,
                                          config=config, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("num_outputs", _op.get_attr("num_outputs"), "config",
            _op.get_attr("config"))
  _execute.record_gradient(
      "RecvTPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
  return _result



def recv_tpu_embedding_activations_eager_fallback(num_outputs, config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function recv_tpu_embedding_activations
  """
  _ctx = ctx if ctx else _context.context()
  num_outputs = _execute.make_int(num_outputs, "num_outputs")
  config = _execute.make_str(config, "config")
  _inputs_flat = []
  _attrs = ("num_outputs", num_outputs, "config", config)
  _result = _execute.execute(b"RecvTPUEmbeddingActivations", num_outputs,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RecvTPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("RecvTPUEmbeddingActivations")(None)


_retrieve_tpu_embedding_adam_parameters_outputs = ["parameters", "momenta",
                                                  "velocities"]
_RetrieveTPUEmbeddingADAMParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingADAMParameters",
    _retrieve_tpu_embedding_adam_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adam_parameters')
def retrieve_tpu_embedding_adam_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  momenta: A tensor containing the embedding table momenta to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  velocities: A tensor containing the embedding table velocities to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, momenta, velocities).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the ADAM optimization algorithm.
    momenta: A `Tensor` of type `float32`. Parameter momenta updated by the ADAM optimization algorithm.
    velocities: A `Tensor` of type `float32`. Parameter velocities updated by the ADAM optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingADAMParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingADAMParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adam_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adam_parameters, num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingADAMParameters", num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adam_parameters, num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingADAMParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingADAMParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adam_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adam_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingADAMParameters", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingADAMParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingADAMParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingADAMParameters")(None)


_retrieve_tpu_embedding_adam_parameters_grad_accum_debug_outputs = ["parameters",
                                                                   "momenta",
                                                                   "velocities",
                                                                   "gradient_accumulators"]
_RetrieveTPUEmbeddingADAMParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingADAMParametersGradAccumDebug",
    _retrieve_tpu_embedding_adam_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adam_parameters_grad_accum_debug')
def retrieve_tpu_embedding_adam_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  momenta: A tensor containing the embedding table momenta to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  velocities: A tensor containing the embedding table velocities to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the ADAM optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, momenta, velocities, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the ADAM optimization algorithm.
    momenta: A `Tensor` of type `float32`. Parameter momenta updated by the ADAM optimization algorithm.
    velocities: A `Tensor` of type `float32`. Parameter velocities updated by the ADAM optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the ADAM optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingADAMParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingADAMParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adam_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adam_parameters_grad_accum_debug, num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingADAMParametersGradAccumDebug", num_shards=num_shards,
                                                            shard_id=shard_id,
                                                            table_id=table_id,
                                                            table_name=table_name,
                                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adam_parameters_grad_accum_debug, num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingADAMParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingADAMParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adam_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adam_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingADAMParametersGradAccumDebug",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingADAMParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingADAMParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingADAMParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_adadelta_parameters_outputs = ["parameters",
                                                      "accumulators",
                                                      "updates"]
_RetrieveTPUEmbeddingAdadeltaParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingAdadeltaParameters",
    _retrieve_tpu_embedding_adadelta_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adadelta_parameters')
def retrieve_tpu_embedding_adadelta_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  updates: A tensor containing the embedding table updates to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, updates).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Adadelta optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the Adadelta optimization algorithm.
    updates: A `Tensor` of type `float32`. Parameter updates updated by the Adadelta optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingAdadeltaParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingAdadeltaParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adadelta_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adadelta_parameters, num_shards=num_shards,
                                                          shard_id=shard_id,
                                                          table_id=table_id,
                                                          table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingAdadeltaParameters", num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adadelta_parameters, num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
                                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdadeltaParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdadeltaParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adadelta_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adadelta_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingAdadeltaParameters", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdadeltaParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdadeltaParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingAdadeltaParameters")(None)


_retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug_outputs = ["parameters",
                                                                       "accumulators",
                                                                       "updates",
                                                                       "gradient_accumulators"]
_RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug",
    _retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug')
def retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  updates: A tensor containing the embedding table updates to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the Adadelta optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, updates, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Adadelta optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the Adadelta optimization algorithm.
    updates: A `Tensor` of type `float32`. Parameter updates updated by the Adadelta optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the Adadelta optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug, num_shards=num_shards,
                                                                           shard_id=shard_id,
                                                                           table_id=table_id,
                                                                           table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug", num_shards=num_shards,
                                                                shard_id=shard_id,
                                                                table_id=table_id,
                                                                table_name=table_name,
                                                                name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug, num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
                                                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adadelta_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_adagrad_parameters_outputs = ["parameters",
                                                     "accumulators"]
_RetrieveTPUEmbeddingAdagradParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingAdagradParameters",
    _retrieve_tpu_embedding_adagrad_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adagrad_parameters')
def retrieve_tpu_embedding_adagrad_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Adagrad optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the Adagrad optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingAdagradParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingAdagradParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adagrad_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adagrad_parameters, num_shards=num_shards,
                                                         shard_id=shard_id,
                                                         table_id=table_id,
                                                         table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingAdagradParameters", num_shards=num_shards,
                                                 shard_id=shard_id,
                                                 table_id=table_id,
                                                 table_name=table_name,
                                                 name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adagrad_parameters, num_shards=num_shards,
                                                     shard_id=shard_id,
                                                     table_id=table_id,
                                                     table_name=table_name,
                                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdagradParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdagradParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adagrad_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingAdagradParameters", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdagradParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdagradParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingAdagradParameters")(None)


_retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug_outputs = ["parameters",
                                                                      "accumulators",
                                                                      "gradient_accumulators"]
_RetrieveTPUEmbeddingAdagradParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug",
    _retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug')
def retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Adagrad optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the Adagrad optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the Adagrad optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the Adagrad optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingAdagradParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug, num_shards=num_shards,
                                                                          shard_id=shard_id,
                                                                          table_id=table_id,
                                                                          table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug", num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug, num_shards=num_shards,
                                                                      shard_id=shard_id,
                                                                      table_id=table_id,
                                                                      table_name=table_name,
                                                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdagradParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_adagrad_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingAdagradParametersGradAccumDebug",
                             3, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingAdagradParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingAdagradParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_centered_rms_prop_parameters_outputs = ["parameters",
                                                               "ms", "mom",
                                                               "mg"]
_RetrieveTPUEmbeddingCenteredRMSPropParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingCenteredRMSPropParameters",
    _retrieve_tpu_embedding_centered_rms_prop_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_centered_rms_prop_parameters')
def retrieve_tpu_embedding_centered_rms_prop_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the centered RMSProp optimization algorithm.
  ms: A tensor containing the embedding table ms to store with the
  parameters from embedding updates using the centered RMSProp optimization algorithm.
  mom: A tensor containing the embedding table mom to store with the
  parameters from embedding updates using the centered RMSProp optimization algorithm.
  mg: A tensor containing the embedding table mg to store with the
  parameters from embedding updates using the centered RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, ms, mom, mg).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the centered RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`. Parameter ms updated by the centered RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`. Parameter mom updated by the centered RMSProp optimization algorithm.
    mg: A `Tensor` of type `float32`. Parameter mg updated by the centered RMSProp optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingCenteredRMSPropParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingCenteredRMSPropParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_centered_rms_prop_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_centered_rms_prop_parameters, num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingCenteredRMSPropParameters", num_shards=num_shards,
                                                         shard_id=shard_id,
                                                         table_id=table_id,
                                                         table_name=table_name,
                                                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_centered_rms_prop_parameters, num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingCenteredRMSPropParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingCenteredRMSPropParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_centered_rms_prop_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_centered_rms_prop_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingCenteredRMSPropParameters",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingCenteredRMSPropParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingCenteredRMSPropParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingCenteredRMSPropParameters")(None)


_retrieve_tpu_embedding_ftrl_parameters_outputs = ["parameters",
                                                  "accumulators", "linears"]
_RetrieveTPUEmbeddingFTRLParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingFTRLParameters",
    _retrieve_tpu_embedding_ftrl_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_ftrl_parameters')
def retrieve_tpu_embedding_ftrl_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  linears: A tensor containing the embedding table linears to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, linears).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the FTRL optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the FTRL optimization algorithm.
    linears: A `Tensor` of type `float32`. Parameter linears updated by the FTRL optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingFTRLParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingFTRLParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_ftrl_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_ftrl_parameters, num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingFTRLParameters", num_shards=num_shards,
                                              shard_id=shard_id,
                                              table_id=table_id,
                                              table_name=table_name,
                                              name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_ftrl_parameters, num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingFTRLParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingFTRLParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_ftrl_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_ftrl_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingFTRLParameters", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingFTRLParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingFTRLParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingFTRLParameters")(None)


_retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug_outputs = ["parameters",
                                                                   "accumulators",
                                                                   "linears",
                                                                   "gradient_accumulators"]
_RetrieveTPUEmbeddingFTRLParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug",
    _retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug')
def retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  linears: A tensor containing the embedding table linears to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the FTRL optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, linears, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the FTRL optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the FTRL optimization algorithm.
    linears: A `Tensor` of type `float32`. Parameter linears updated by the FTRL optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the FTRL optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingFTRLParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug, num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug", num_shards=num_shards,
                                                            shard_id=shard_id,
                                                            table_id=table_id,
                                                            table_name=table_name,
                                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug, num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingFTRLParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_ftrl_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingFTRLParametersGradAccumDebug",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingFTRLParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingFTRLParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_mdl_adagrad_light_parameters_outputs = ["parameters",
                                                               "accumulators",
                                                               "weights",
                                                               "benefits"]
_RetrieveTPUEmbeddingMDLAdagradLightParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingMDLAdagradLightParameters",
    _retrieve_tpu_embedding_mdl_adagrad_light_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_mdl_adagrad_light_parameters')
def retrieve_tpu_embedding_mdl_adagrad_light_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the MDL Adagrad Light optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the MDL Adagrad Light optimization algorithm.
  weights: A tensor containing the embedding table weights to store with the
  parameters from embedding updates using the MDL Adagrad Light optimization algorithm.
  benefits: A tensor containing the embedding table benefits to store with the
  parameters from embedding updates using the MDL Adagrad Light optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, weights, benefits).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the MDL Adagrad Light optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the MDL Adagrad Light optimization algorithm.
    weights: A `Tensor` of type `float32`. Parameter weights updated by the MDL Adagrad Light optimization algorithm.
    benefits: A `Tensor` of type `float32`. Parameter benefits updated by the MDL Adagrad Light optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingMDLAdagradLightParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingMDLAdagradLightParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_mdl_adagrad_light_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_mdl_adagrad_light_parameters, num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingMDLAdagradLightParameters", num_shards=num_shards,
                                                         shard_id=shard_id,
                                                         table_id=table_id,
                                                         table_name=table_name,
                                                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_mdl_adagrad_light_parameters, num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMDLAdagradLightParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMDLAdagradLightParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_mdl_adagrad_light_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_mdl_adagrad_light_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingMDLAdagradLightParameters",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMDLAdagradLightParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMDLAdagradLightParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingMDLAdagradLightParameters")(None)


_retrieve_tpu_embedding_momentum_parameters_outputs = ["parameters",
                                                      "momenta"]
_RetrieveTPUEmbeddingMomentumParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingMomentumParameters",
    _retrieve_tpu_embedding_momentum_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_momentum_parameters')
def retrieve_tpu_embedding_momentum_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Momentum optimization algorithm.
  momenta: A tensor containing the embedding table momenta to store with the
  parameters from embedding updates using the Momentum optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, momenta).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Momentum optimization algorithm.
    momenta: A `Tensor` of type `float32`. Parameter momenta updated by the Momentum optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingMomentumParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingMomentumParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_momentum_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_momentum_parameters, num_shards=num_shards,
                                                          shard_id=shard_id,
                                                          table_id=table_id,
                                                          table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingMomentumParameters", num_shards=num_shards,
                                                  shard_id=shard_id,
                                                  table_id=table_id,
                                                  table_name=table_name,
                                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_momentum_parameters, num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
                                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMomentumParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMomentumParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_momentum_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_momentum_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingMomentumParameters", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMomentumParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMomentumParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingMomentumParameters")(None)


_retrieve_tpu_embedding_momentum_parameters_grad_accum_debug_outputs = ["parameters",
                                                                       "momenta",
                                                                       "gradient_accumulators"]
_RetrieveTPUEmbeddingMomentumParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug",
    _retrieve_tpu_embedding_momentum_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_momentum_parameters_grad_accum_debug')
def retrieve_tpu_embedding_momentum_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the Momentum optimization algorithm.
  momenta: A tensor containing the embedding table momenta to store with the
  parameters from embedding updates using the Momentum optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the Momentum optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, momenta, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the Momentum optimization algorithm.
    momenta: A `Tensor` of type `float32`. Parameter momenta updated by the Momentum optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the Momentum optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingMomentumParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_momentum_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_momentum_parameters_grad_accum_debug, num_shards=num_shards,
                                                                           shard_id=shard_id,
                                                                           table_id=table_id,
                                                                           table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug", num_shards=num_shards,
                                                                shard_id=shard_id,
                                                                table_id=table_id,
                                                                table_name=table_name,
                                                                name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_momentum_parameters_grad_accum_debug, num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
                                                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMomentumParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_momentum_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_momentum_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingMomentumParametersGradAccumDebug",
                             3, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingMomentumParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingMomentumParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_proximal_adagrad_parameters_outputs = ["parameters",
                                                              "accumulators"]
_RetrieveTPUEmbeddingProximalAdagradParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingProximalAdagradParameters",
    _retrieve_tpu_embedding_proximal_adagrad_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_proximal_adagrad_parameters')
def retrieve_tpu_embedding_proximal_adagrad_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the proximal Adagrad optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the proximal Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the proximal Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the proximal Adagrad optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingProximalAdagradParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingProximalAdagradParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_proximal_adagrad_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_proximal_adagrad_parameters, num_shards=num_shards,
                                                                  shard_id=shard_id,
                                                                  table_id=table_id,
                                                                  table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingProximalAdagradParameters", num_shards=num_shards,
                                                         shard_id=shard_id,
                                                         table_id=table_id,
                                                         table_name=table_name,
                                                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_proximal_adagrad_parameters, num_shards=num_shards,
                                                              shard_id=shard_id,
                                                              table_id=table_id,
                                                              table_name=table_name,
                                                              name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingProximalAdagradParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingProximalAdagradParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_proximal_adagrad_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_proximal_adagrad_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingProximalAdagradParameters",
                             2, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingProximalAdagradParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingProximalAdagradParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingProximalAdagradParameters")(None)


_retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_outputs = ["parameters", "accumulators",
                                                                               "gradient_accumulators"]
_RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug",
    _retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug')
def retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the proximal Adagrad optimization algorithm.
  accumulators: A tensor containing the embedding table accumulators to store with the
  parameters from embedding updates using the proximal Adagrad optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the proximal Adagrad optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, accumulators, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the proximal Adagrad optimization algorithm.
    accumulators: A `Tensor` of type `float32`. Parameter accumulators updated by the proximal Adagrad optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the proximal Adagrad optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug, num_shards=num_shards, shard_id=shard_id, table_id=table_id, table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug", num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
                                                                       name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug, num_shards=num_shards, shard_id=shard_id, table_id=table_id, table_name=table_name,
                                                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_proximal_adagrad_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug",
                             3, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug")(None)


_retrieve_tpu_embedding_rms_prop_parameters_outputs = ["parameters", "ms",
                                                      "mom"]
_RetrieveTPUEmbeddingRMSPropParametersOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingRMSPropParameters",
    _retrieve_tpu_embedding_rms_prop_parameters_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_rms_prop_parameters')
def retrieve_tpu_embedding_rms_prop_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  ms: A tensor containing the embedding table ms to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  mom: A tensor containing the embedding table mom to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, ms, mom).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`. Parameter ms updated by the RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`. Parameter mom updated by the RMSProp optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingRMSPropParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingRMSPropParametersOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_rms_prop_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_rms_prop_parameters, num_shards=num_shards,
                                                          shard_id=shard_id,
                                                          table_id=table_id,
                                                          table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingRMSPropParameters", num_shards=num_shards,
                                                 shard_id=shard_id,
                                                 table_id=table_id,
                                                 table_name=table_name,
                                                 name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_rms_prop_parameters, num_shards=num_shards,
                                                      shard_id=shard_id,
                                                      table_id=table_id,
                                                      table_name=table_name,
                                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingRMSPropParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingRMSPropParametersOutput._make(_result)
  return _result



def retrieve_tpu_embedding_rms_prop_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_rms_prop_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingRMSPropParameters", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingRMSPropParameters", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingRMSPropParametersOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingRMSPropParameters")(None)


_retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug_outputs = ["parameters",
                                                                       "ms",
                                                                       "mom",
                                                                       "gradient_accumulators"]
_RetrieveTPUEmbeddingRMSPropParametersGradAccumDebugOutput = _collections.namedtuple(
    "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug",
    _retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug_outputs)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug')
def retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  ms: A tensor containing the embedding table ms to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  mom: A tensor containing the embedding table mom to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  gradient_accumulators: A tensor containing the embedding table gradient_accumulators to store with the
  parameters from embedding updates using the RMSProp optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (parameters, ms, mom, gradient_accumulators).

    parameters: A `Tensor` of type `float32`. Parameter parameters updated by the RMSProp optimization algorithm.
    ms: A `Tensor` of type `float32`. Parameter ms updated by the RMSProp optimization algorithm.
    mom: A `Tensor` of type `float32`. Parameter mom updated by the RMSProp optimization algorithm.
    gradient_accumulators: A `Tensor` of type `float32`. Parameter gradient_accumulators updated by the RMSProp optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      _result = _RetrieveTPUEmbeddingRMSPropParametersGradAccumDebugOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug, num_shards=num_shards,
                                                                           shard_id=shard_id,
                                                                           table_id=table_id,
                                                                           table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug", num_shards=num_shards,
                                                               shard_id=shard_id,
                                                               table_id=table_id,
                                                               table_name=table_name,
                                                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug, num_shards=num_shards,
                                                                       shard_id=shard_id,
                                                                       table_id=table_id,
                                                                       table_name=table_name,
                                                                       name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingRMSPropParametersGradAccumDebugOutput._make(_result)
  return _result



def retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_rms_prop_parameters_grad_accum_debug
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug",
                             4, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug", _inputs_flat, _attrs, _result, name)
  _result = _RetrieveTPUEmbeddingRMSPropParametersGradAccumDebugOutput._make(_result)
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug")(None)


@_dispatch.add_dispatch_list
@tf_export('retrieve_tpu_embedding_stochastic_gradient_descent_parameters')
def retrieve_tpu_embedding_stochastic_gradient_descent_parameters(num_shards, shard_id, table_id=-1, table_name="", name=None):
  r"""Retrieve embedding parameters for a single table.

  
  An op that retrieves optimization parameters from embedding to host
  memory. Must be preceded by a ConfigureTPUEmbeddingHost op that sets up
  the correct embedding table configuration. For example, this op is
  used to retrieve updated parameters before saving a checkpoint.

  parameters: A tensor containing the embedding table parameters to store with the
  parameters from embedding updates using the stochastic gradient descent optimization algorithm.
  table_name: Name of this table; must match a name in the
    TPUEmbeddingConfiguration proto (overrides table_id).
  num_shards: Number of shards into which the embedding tables are divided.
  shard_id: Identifier of shard for this operation.
  table_id: Index of this table in the EmbeddingLayerConfiguration proto
    (deprecated).

  Args:
    num_shards: An `int`.
    shard_id: An `int`.
    table_id: An optional `int` that is `>= -1`. Defaults to `-1`.
    table_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
    Parameter parameters updated by the stochastic gradient descent optimization algorithm.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RetrieveTPUEmbeddingStochasticGradientDescentParameters", name,
        _ctx._post_execution_callbacks, "table_id", table_id, "table_name",
        table_name, "num_shards", num_shards, "shard_id", shard_id)
      return _result
    except _core._FallbackException:
      try:
        return retrieve_tpu_embedding_stochastic_gradient_descent_parameters_eager_fallback(
            table_id=table_id, table_name=table_name, num_shards=num_shards,
            shard_id=shard_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              retrieve_tpu_embedding_stochastic_gradient_descent_parameters, num_shards=num_shards,
                                                                             shard_id=shard_id,
                                                                             table_id=table_id,
                                                                             table_name=table_name,
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
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "RetrieveTPUEmbeddingStochasticGradientDescentParameters", num_shards=num_shards,
                                                                   shard_id=shard_id,
                                                                   table_id=table_id,
                                                                   table_name=table_name,
                                                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          retrieve_tpu_embedding_stochastic_gradient_descent_parameters, num_shards=num_shards,
                                                                         shard_id=shard_id,
                                                                         table_id=table_id,
                                                                         table_name=table_name,
                                                                         name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "table_name",
            _op.get_attr("table_name"), "num_shards",
            _op.get_attr("num_shards"), "shard_id", _op.get_attr("shard_id"))
  _execute.record_gradient(
      "RetrieveTPUEmbeddingStochasticGradientDescentParameters", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def retrieve_tpu_embedding_stochastic_gradient_descent_parameters_eager_fallback(num_shards, shard_id, table_id=-1, table_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function retrieve_tpu_embedding_stochastic_gradient_descent_parameters
  """
  _ctx = ctx if ctx else _context.context()
  num_shards = _execute.make_int(num_shards, "num_shards")
  shard_id = _execute.make_int(shard_id, "shard_id")
  if table_id is None:
    table_id = -1
  table_id = _execute.make_int(table_id, "table_id")
  if table_name is None:
    table_name = ""
  table_name = _execute.make_str(table_name, "table_name")
  _inputs_flat = []
  _attrs = ("table_id", table_id, "table_name", table_name, "num_shards",
  num_shards, "shard_id", shard_id)
  _result = _execute.execute(b"RetrieveTPUEmbeddingStochasticGradientDescentParameters",
                             1, inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "RetrieveTPUEmbeddingStochasticGradientDescentParameters", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("RetrieveTPUEmbeddingStochasticGradientDescentParameters")(None)


@_dispatch.add_dispatch_list
@tf_export('send_tpu_embedding_gradients')
def _send_tpu_embedding_gradients(inputs, learning_rates, config, name=None):
  r"""An op that performs gradient updates of embedding tables.

  The TensorList argument has the same length and shapes as the return value of
  TPUEmbeddingReceiveActivations, but contains gradients of the model's loss
  with respect to the embedding activations. The embedding tables are updated
  from these gradients via the optimizer specified in the configuration given
  to tpu.initialize_system.

  Args:
    inputs: A list of at least 1 `Tensor` objects with type `float32`.
      A TensorList of gradients with which to update embedding tables.
      It contains one tensor per embedding table in the model.
    learning_rates: A list of `Tensor` objects with type `float32`.
      A list of float32 scalars, one for each embedding table,
      containing the learning rates for each table when dynamic learning rate is
      enabled through the OptimizationParameters in TPUEmbeddingConfiguration.
      When the learning rate is constant, the list should be empty.
    config: A `string`. Serialized TPUEmbeddingConfiguration proto.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SendTPUEmbeddingGradients", name, _ctx._post_execution_callbacks,
        inputs, learning_rates, "config", config)
      return _result
    except _core._FallbackException:
      try:
        return _send_tpu_embedding_gradients_eager_fallback(
            inputs, learning_rates, config=config, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              _send_tpu_embedding_gradients, inputs=inputs,
                                             learning_rates=learning_rates,
                                             config=config, name=name)
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
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'send_tpu_embedding_gradients' Op, not %r." % inputs)
  _attr_N = len(inputs)
  if not isinstance(learning_rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'learning_rates' argument to "
        "'send_tpu_embedding_gradients' Op, not %r." % learning_rates)
  _attr_NN = len(learning_rates)
  config = _execute.make_str(config, "config")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SendTPUEmbeddingGradients", inputs=inputs,
                                     learning_rates=learning_rates,
                                     config=config, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          _send_tpu_embedding_gradients, inputs=inputs,
                                         learning_rates=learning_rates,
                                         config=config, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def _send_tpu_embedding_gradients_eager_fallback(inputs, learning_rates, config, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _send_tpu_embedding_gradients
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'send_tpu_embedding_gradients' Op, not %r." % inputs)
  _attr_N = len(inputs)
  if not isinstance(learning_rates, (list, tuple)):
    raise TypeError(
        "Expected list for 'learning_rates' argument to "
        "'send_tpu_embedding_gradients' Op, not %r." % learning_rates)
  _attr_NN = len(learning_rates)
  config = _execute.make_str(config, "config")
  inputs = _ops.convert_n_to_tensor(inputs, _dtypes.float32)
  learning_rates = _ops.convert_n_to_tensor(learning_rates, _dtypes.float32)
  _inputs_flat = list(inputs) + list(learning_rates)
  _attrs = ("N", _attr_N, "NN", _attr_NN, "config", config)
  _result = _execute.execute(b"SendTPUEmbeddingGradients", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("SendTPUEmbeddingGradients")(None)


@_dispatch.add_dispatch_list
@tf_export('shutdown_distributed_tpu')
def shutdown_distributed_tpu(name=None):
  r"""An op that shuts down a running distributed TPU system. The Op returns

  an error if no system is running.

  Args:
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ShutdownDistributedTPU", name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return shutdown_distributed_tpu_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              shutdown_distributed_tpu, name=name)
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
        "ShutdownDistributedTPU", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          shutdown_distributed_tpu, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def shutdown_distributed_tpu_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shutdown_distributed_tpu
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"ShutdownDistributedTPU", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result

_ops.RegisterShape("ShutdownDistributedTPU")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_compilation_result')
def tpu_compilation_result(name=None):
  r"""TODO: add doc.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUCompilationResult", name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return tpu_compilation_result_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_compilation_result, name=name)
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
        "TPUCompilationResult", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_compilation_result, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TPUCompilationResult", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tpu_compilation_result_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_compilation_result
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"TPUCompilationResult", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TPUCompilationResult", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUCompilationResult")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_embedding_activations')
def tpu_embedding_activations(embedding_variable, sliced_activations, table_id, lookup_id, name=None):
  r"""An op enabling differentiation of TPU Embeddings.

  This op simply returns its first input, which is assumed to have been sliced
  from the Tensors returned by TPUEmbeddingDequeueActivations. The presence of this
  op, and its first argument being a trainable Variable, enables automatic
  differentiation of graphs containing embeddings via the TPU Embedding Python
  libraries.

  Args:
    embedding_variable: A `Tensor` of type `float32`.
      A trainable variable, enabling optimizers to find this op.
    sliced_activations: A `Tensor` of type `float32`.
      The embedding activations Tensor to return.
    table_id: An `int` that is `>= 0`.
      The id of the table in the embedding layer configuration from which
      these activations were computed.
    lookup_id: An `int` that is `>= 0`.
      Identifier of the set of embedding indices which produced these
      activations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUEmbeddingActivations", name, _ctx._post_execution_callbacks,
        embedding_variable, sliced_activations, "table_id", table_id,
        "lookup_id", lookup_id)
      return _result
    except _core._FallbackException:
      try:
        return tpu_embedding_activations_eager_fallback(
            embedding_variable, sliced_activations, table_id=table_id,
            lookup_id=lookup_id, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_embedding_activations, embedding_variable=embedding_variable,
                                         sliced_activations=sliced_activations,
                                         table_id=table_id,
                                         lookup_id=lookup_id, name=name)
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
  table_id = _execute.make_int(table_id, "table_id")
  lookup_id = _execute.make_int(lookup_id, "lookup_id")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUEmbeddingActivations", embedding_variable=embedding_variable,
                                   sliced_activations=sliced_activations,
                                   table_id=table_id, lookup_id=lookup_id,
                                   name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_embedding_activations, embedding_variable=embedding_variable,
                                     sliced_activations=sliced_activations,
                                     table_id=table_id, lookup_id=lookup_id,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("table_id", _op.get_attr("table_id"), "lookup_id",
            _op.get_attr("lookup_id"))
  _execute.record_gradient(
      "TPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tpu_embedding_activations_eager_fallback(embedding_variable, sliced_activations, table_id, lookup_id, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_embedding_activations
  """
  _ctx = ctx if ctx else _context.context()
  table_id = _execute.make_int(table_id, "table_id")
  lookup_id = _execute.make_int(lookup_id, "lookup_id")
  embedding_variable = _ops.convert_to_tensor(embedding_variable, _dtypes.float32)
  sliced_activations = _ops.convert_to_tensor(sliced_activations, _dtypes.float32)
  _inputs_flat = [embedding_variable, sliced_activations]
  _attrs = ("table_id", table_id, "lookup_id", lookup_id)
  _result = _execute.execute(b"TPUEmbeddingActivations", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUEmbeddingActivations", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUEmbeddingActivations")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_replicate')
def tpu_replicate(inputs, broadcast_inputs, variables, guaranteed_constants, computation, num_replicas, output_types, num_cores_per_replica=1, topology="", use_tpu=True, device_assignment=[], host_compute_core=[], name=None):
  r"""Runs replicated computations on a distributed TPU system.

  Args:
    inputs: A list of `Tensor` objects.
      the inputs to 'computation', flattened, in replica-major order.
    broadcast_inputs: A list of `Tensor` objects.
      additional arguments to broadcast to all replicas. The
      broadcast inputs are appended to the per-replica inputs when calling
      computation.
    variables: A list of `Tensor` objects with type `resource`.
    guaranteed_constants: A list of `Tensor` objects.
      arguments which have been guaranteed to not
      change their values during the session lifetime. These contain tensors marked as
      constant using the GuaranteeConstOp.
    computation: A function decorated with @Defun.
      a function containing the computation to run.
    num_replicas: An `int` that is `>= 1`.
      the number of replicas of the computation to run.
    output_types: A list of `tf.DTypes`.
      the types of the outputs of 'computation'.
    num_cores_per_replica: An optional `int`. Defaults to `1`.
      the number of logical cores in each replica.
    topology: An optional `string`. Defaults to `""`.
      A serialized tensorflow.tpu.TopologyProto that describes the TPU
      topology.
    use_tpu: An optional `bool`. Defaults to `True`.
      a bool indicating if this computation will run on TPU or CPU/GPU.
      Currently, only supports a default placement (computation is placed on GPU
      if one is available, and on CPU if not).
    device_assignment: An optional list of `ints`. Defaults to `[]`.
      a flattened array with shape
      [replica, num_cores_per_replica, mesh_dimension] that maps the coordinates
      of logical cores in each replica of a computation to physical coordinates in
      the TPU topology.
    host_compute_core: An optional list of `strings`. Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
    the outputs of 'computation'.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TPUReplicate",
        name, _ctx._post_execution_callbacks, inputs, broadcast_inputs,
        variables, guaranteed_constants, "computation", computation,
        "num_replicas", num_replicas, "num_cores_per_replica",
        num_cores_per_replica, "topology", topology, "use_tpu", use_tpu,
        "device_assignment", device_assignment, "host_compute_core",
        host_compute_core, "output_types", output_types)
      return _result
    except _core._FallbackException:
      try:
        return tpu_replicate_eager_fallback(
            inputs, broadcast_inputs, variables, guaranteed_constants,
            computation=computation, num_replicas=num_replicas,
            num_cores_per_replica=num_cores_per_replica, topology=topology,
            use_tpu=use_tpu, device_assignment=device_assignment,
            host_compute_core=host_compute_core, output_types=output_types,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_replicate, inputs=inputs, broadcast_inputs=broadcast_inputs,
                             variables=variables,
                             guaranteed_constants=guaranteed_constants,
                             computation=computation,
                             num_replicas=num_replicas,
                             output_types=output_types,
                             num_cores_per_replica=num_cores_per_replica,
                             topology=topology, use_tpu=use_tpu,
                             device_assignment=device_assignment,
                             host_compute_core=host_compute_core, name=name)
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
  if not isinstance(variables, (list, tuple)):
    raise TypeError(
        "Expected list for 'variables' argument to "
        "'tpu_replicate' Op, not %r." % variables)
  _attr_NumVariables = len(variables)
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'tpu_replicate' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if num_cores_per_replica is None:
    num_cores_per_replica = 1
  num_cores_per_replica = _execute.make_int(num_cores_per_replica, "num_cores_per_replica")
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicate", inputs=inputs, broadcast_inputs=broadcast_inputs,
                        variables=variables,
                        guaranteed_constants=guaranteed_constants,
                        computation=computation, num_replicas=num_replicas,
                        output_types=output_types,
                        num_cores_per_replica=num_cores_per_replica,
                        topology=topology, use_tpu=use_tpu,
                        device_assignment=device_assignment,
                        host_compute_core=host_compute_core, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_replicate, inputs=inputs, broadcast_inputs=broadcast_inputs,
                         variables=variables,
                         guaranteed_constants=guaranteed_constants,
                         computation=computation, num_replicas=num_replicas,
                         output_types=output_types,
                         num_cores_per_replica=num_cores_per_replica,
                         topology=topology, use_tpu=use_tpu,
                         device_assignment=device_assignment,
                         host_compute_core=host_compute_core, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("computation", _op.get_attr("computation"), "num_replicas",
            _op.get_attr("num_replicas"), "num_cores_per_replica",
            _op.get_attr("num_cores_per_replica"), "topology",
            _op.get_attr("topology"), "use_tpu", _op.get_attr("use_tpu"),
            "device_assignment", _op.get_attr("device_assignment"),
            "host_compute_core", _op.get_attr("host_compute_core"), "Tinputs",
            _op.get_attr("Tinputs"), "Tbroadcast_inputs",
            _op.get_attr("Tbroadcast_inputs"), "NumVariables",
            _op.get_attr("NumVariables"), "Tguaranteed_constants",
            _op.get_attr("Tguaranteed_constants"), "output_types",
            _op.get_attr("output_types"))
  _execute.record_gradient(
      "TPUReplicate", _inputs_flat, _attrs, _result, name)
  return _result



def tpu_replicate_eager_fallback(inputs, broadcast_inputs, variables, guaranteed_constants, computation, num_replicas, output_types, num_cores_per_replica=1, topology="", use_tpu=True, device_assignment=[], host_compute_core=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicate
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(variables, (list, tuple)):
    raise TypeError(
        "Expected list for 'variables' argument to "
        "'tpu_replicate' Op, not %r." % variables)
  _attr_NumVariables = len(variables)
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'tpu_replicate' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if num_cores_per_replica is None:
    num_cores_per_replica = 1
  num_cores_per_replica = _execute.make_int(num_cores_per_replica, "num_cores_per_replica")
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  _attr_Tinputs, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _attr_Tbroadcast_inputs, broadcast_inputs = _execute.convert_to_mixed_eager_tensors(broadcast_inputs, _ctx)
  _attr_Tguaranteed_constants, guaranteed_constants = _execute.convert_to_mixed_eager_tensors(guaranteed_constants, _ctx)
  variables = _ops.convert_n_to_tensor(variables, _dtypes.resource)
  _inputs_flat = list(inputs) + list(broadcast_inputs) + list(variables) + list(guaranteed_constants)
  _attrs = ("computation", computation, "num_replicas", num_replicas,
  "num_cores_per_replica", num_cores_per_replica, "topology", topology,
  "use_tpu", use_tpu, "device_assignment", device_assignment,
  "host_compute_core", host_compute_core, "Tinputs", _attr_Tinputs,
  "Tbroadcast_inputs", _attr_Tbroadcast_inputs, "NumVariables",
  _attr_NumVariables, "Tguaranteed_constants", _attr_Tguaranteed_constants,
  "output_types", output_types)
  _result = _execute.execute(b"TPUReplicate", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUReplicate", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TPUReplicate")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_replicate_metadata')
def tpu_replicate_metadata(num_replicas, num_cores_per_replica=1, topology="", use_tpu=True, device_assignment=[], computation_shape=[], host_compute_core=[], name=None):
  r"""TODO: add doc.

  Args:
    num_replicas: An `int` that is `>= 0`.
    num_cores_per_replica: An optional `int`. Defaults to `1`.
    topology: An optional `string`. Defaults to `""`.
    use_tpu: An optional `bool`. Defaults to `True`.
    device_assignment: An optional list of `ints`. Defaults to `[]`.
    computation_shape: An optional list of `ints`. Defaults to `[]`.
    host_compute_core: An optional list of `strings`. Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicateMetadata", name, _ctx._post_execution_callbacks,
        "num_replicas", num_replicas, "num_cores_per_replica",
        num_cores_per_replica, "topology", topology, "use_tpu", use_tpu,
        "device_assignment", device_assignment, "computation_shape",
        computation_shape, "host_compute_core", host_compute_core)
      return _result
    except _core._FallbackException:
      try:
        return tpu_replicate_metadata_eager_fallback(
            num_replicas=num_replicas,
            num_cores_per_replica=num_cores_per_replica, topology=topology,
            use_tpu=use_tpu, device_assignment=device_assignment,
            computation_shape=computation_shape,
            host_compute_core=host_compute_core, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_replicate_metadata, num_replicas=num_replicas,
                                      num_cores_per_replica=num_cores_per_replica,
                                      topology=topology, use_tpu=use_tpu,
                                      device_assignment=device_assignment,
                                      computation_shape=computation_shape,
                                      host_compute_core=host_compute_core,
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
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if num_cores_per_replica is None:
    num_cores_per_replica = 1
  num_cores_per_replica = _execute.make_int(num_cores_per_replica, "num_cores_per_replica")
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate_metadata' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if computation_shape is None:
    computation_shape = []
  if not isinstance(computation_shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'computation_shape' argument to "
        "'tpu_replicate_metadata' Op, not %r." % computation_shape)
  computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate_metadata' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicateMetadata", num_replicas=num_replicas,
                                num_cores_per_replica=num_cores_per_replica,
                                topology=topology, use_tpu=use_tpu,
                                device_assignment=device_assignment,
                                computation_shape=computation_shape,
                                host_compute_core=host_compute_core,
                                name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_replicate_metadata, num_replicas=num_replicas,
                                  num_cores_per_replica=num_cores_per_replica,
                                  topology=topology, use_tpu=use_tpu,
                                  device_assignment=device_assignment,
                                  computation_shape=computation_shape,
                                  host_compute_core=host_compute_core,
                                  name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def tpu_replicate_metadata_eager_fallback(num_replicas, num_cores_per_replica=1, topology="", use_tpu=True, device_assignment=[], computation_shape=[], host_compute_core=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicate_metadata
  """
  _ctx = ctx if ctx else _context.context()
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  if num_cores_per_replica is None:
    num_cores_per_replica = 1
  num_cores_per_replica = _execute.make_int(num_cores_per_replica, "num_cores_per_replica")
  if topology is None:
    topology = ""
  topology = _execute.make_str(topology, "topology")
  if use_tpu is None:
    use_tpu = True
  use_tpu = _execute.make_bool(use_tpu, "use_tpu")
  if device_assignment is None:
    device_assignment = []
  if not isinstance(device_assignment, (list, tuple)):
    raise TypeError(
        "Expected list for 'device_assignment' argument to "
        "'tpu_replicate_metadata' Op, not %r." % device_assignment)
  device_assignment = [_execute.make_int(_i, "device_assignment") for _i in device_assignment]
  if computation_shape is None:
    computation_shape = []
  if not isinstance(computation_shape, (list, tuple)):
    raise TypeError(
        "Expected list for 'computation_shape' argument to "
        "'tpu_replicate_metadata' Op, not %r." % computation_shape)
  computation_shape = [_execute.make_int(_i, "computation_shape") for _i in computation_shape]
  if host_compute_core is None:
    host_compute_core = []
  if not isinstance(host_compute_core, (list, tuple)):
    raise TypeError(
        "Expected list for 'host_compute_core' argument to "
        "'tpu_replicate_metadata' Op, not %r." % host_compute_core)
  host_compute_core = [_execute.make_str(_s, "host_compute_core") for _s in host_compute_core]
  _inputs_flat = []
  _attrs = ("num_replicas", num_replicas, "num_cores_per_replica",
  num_cores_per_replica, "topology", topology, "use_tpu", use_tpu,
  "device_assignment", device_assignment, "computation_shape",
  computation_shape, "host_compute_core", host_compute_core)
  _result = _execute.execute(b"TPUReplicateMetadata", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

_ops.RegisterShape("TPUReplicateMetadata")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_replicated_input')
def tpu_replicated_input(inputs, name=None):
  r"""Operator that connects N unreplicated inputs to an N-way replicated TPU computation.

  Args:
    inputs: A list of at least 1 `Tensor` objects with the same type.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `inputs`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicatedInput", name, _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      try:
        return tpu_replicated_input_eager_fallback(
            inputs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_replicated_input, inputs=inputs, name=name)
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
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'tpu_replicated_input' Op, not %r." % inputs)
  _attr_N = len(inputs)
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicatedInput", inputs=inputs, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_replicated_input, inputs=inputs, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "TPUReplicatedInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tpu_replicated_input_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicated_input
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'tpu_replicated_input' Op, not %r." % inputs)
  _attr_N = len(inputs)
  _attr_T, inputs = _execute.args_to_matching_eager(list(inputs), _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"TPUReplicatedInput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TPUReplicatedInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("TPUReplicatedInput")(None)


@_dispatch.add_dispatch_list
@tf_export('tpu_replicated_output')
def tpu_replicated_output(input, num_replicas, name=None):
  r"""Operator that connects the output of an N-way replicated TPU computation to N separate outputs.

  Args:
    input: A `Tensor`.
    num_replicas: An `int` that is `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A list of `num_replicas` `Tensor` objects with the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TPUReplicatedOutput", name, _ctx._post_execution_callbacks, input,
        "num_replicas", num_replicas)
      return _result
    except _core._FallbackException:
      try:
        return tpu_replicated_output_eager_fallback(
            input, num_replicas=num_replicas, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              tpu_replicated_output, input=input, num_replicas=num_replicas,
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
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TPUReplicatedOutput", input=input, num_replicas=num_replicas,
                               name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          tpu_replicated_output, input=input, num_replicas=num_replicas,
                                 name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_replicas", _op.get_attr("num_replicas"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "TPUReplicatedOutput", _inputs_flat, _attrs, _result, name)
  return _result



def tpu_replicated_output_eager_fallback(input, num_replicas, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tpu_replicated_output
  """
  _ctx = ctx if ctx else _context.context()
  num_replicas = _execute.make_int(num_replicas, "num_replicas")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("num_replicas", num_replicas, "T", _attr_T)
  _result = _execute.execute(b"TPUReplicatedOutput", num_replicas,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TPUReplicatedOutput", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TPUReplicatedOutput")(None)


@_dispatch.add_dispatch_list
@tf_export('worker_heartbeat')
def worker_heartbeat(request, name=None):
  r"""Worker heartbeat op.

  Heartbeats may be sent periodically to indicate the coordinator is still active,
  to retrieve the current worker status and to expedite shutdown when necessary.

  Args:
    request: A `Tensor` of type `string`.
      A string tensor containing a serialized WorkerHeartbeatRequest
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    A string tensor containing a serialized WorkerHeartbeatResponse
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "WorkerHeartbeat", name, _ctx._post_execution_callbacks, request)
      return _result
    except _core._FallbackException:
      try:
        return worker_heartbeat_eager_fallback(
            request, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              worker_heartbeat, request=request, name=name)
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
        "WorkerHeartbeat", request=request, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          worker_heartbeat, request=request, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "WorkerHeartbeat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def worker_heartbeat_eager_fallback(request, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function worker_heartbeat
  """
  _ctx = ctx if ctx else _context.context()
  request = _ops.convert_to_tensor(request, _dtypes.string)
  _inputs_flat = [request]
  _attrs = None
  _result = _execute.execute(b"WorkerHeartbeat", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "WorkerHeartbeat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("WorkerHeartbeat")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AllToAll"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "group_assignment"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "concat_dimension"
#     type: "int"
#   }
#   attr {
#     name: "split_dimension"
#     type: "int"
#   }
#   attr {
#     name: "split_count"
#     type: "int"
#   }
# }
# op {
#   name: "CollectivePermute"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "source_target_pairs"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_INT8
#         type: DT_COMPLEX64
#         type: DT_INT64
#         type: DT_QINT8
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_COMPLEX128
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
# }
# op {
#   name: "ConfigureDistributedTPU"
#   output_arg {
#     name: "topology"
#     type: DT_STRING
#   }
#   attr {
#     name: "embedding_config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "tpu_embedding_config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "is_global_init"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "CrossReplicaSum"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "group_assignment"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "EnqueueTPUEmbeddingIntegerBatch"
#   input_arg {
#     name: "batch"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "mode_override"
#     type: DT_STRING
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "EnqueueTPUEmbeddingSparseBatch"
#   input_arg {
#     name: "sample_indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "embedding_indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "aggregation_weights"
#     type: DT_FLOAT
#     number_attr: "N"
#   }
#   input_arg {
#     name: "mode_override"
#     type: DT_STRING
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "combiners"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "EnqueueTPUEmbeddingSparseTensorBatch"
#   input_arg {
#     name: "sample_indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "embedding_indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "aggregation_weights"
#     type: DT_FLOAT
#     number_attr: "N"
#   }
#   input_arg {
#     name: "mode_override"
#     type: DT_STRING
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "combiners"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "table_ids"
#     type: "list(int)"
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedDequeue"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedDequeueTuple"
#   output_arg {
#     name: "outputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedEnqueue"
#   input_arg {
#     name: "input"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "InfeedEnqueueTuple"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingADAMParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "velocities"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingADAMParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "velocities"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingAdadeltaParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "updates"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingAdadeltaParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "updates"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingAdagradParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingAdagradParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingCenteredRMSPropParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mg"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingFTRLParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "linears"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingFTRLParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "linears"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingMDLAdagradLightParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "weights"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "benefits"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingMomentumParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingMomentumParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingProximalAdagradParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingRMSPropParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingRMSPropParametersGradAccumDebug"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "LoadTPUEmbeddingStochasticGradientDescentParameters"
#   input_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedDequeue"
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "shape"
#     type: "shape"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedDequeueTuple"
#   output_arg {
#     name: "outputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "device_ordinal"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedEnqueue"
#   input_arg {
#     name: "input"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "OutfeedEnqueueTuple"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "RecvTPUEmbeddingActivations"
#   output_arg {
#     name: "outputs"
#     type: DT_FLOAT
#     number_attr: "num_outputs"
#   }
#   attr {
#     name: "num_outputs"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "config"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingADAMParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "velocities"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingADAMParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "velocities"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingAdadeltaParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "updates"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "updates"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingAdagradParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingAdagradParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingCenteredRMSPropParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "mg"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingFTRLParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "linears"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingFTRLParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "linears"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingMDLAdagradLightParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "weights"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "benefits"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingMomentumParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingMomentumParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "momenta"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingProximalAdagradParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "accumulators"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingRMSPropParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "ms"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "mom"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "gradient_accumulators"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "RetrieveTPUEmbeddingStochasticGradientDescentParameters"
#   output_arg {
#     name: "parameters"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "table_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "num_shards"
#     type: "int"
#   }
#   attr {
#     name: "shard_id"
#     type: "int"
#   }
#   is_stateful: true
# }
# op {
#   name: "SendTPUEmbeddingGradients"
#   input_arg {
#     name: "inputs"
#     type: DT_FLOAT
#     number_attr: "N"
#   }
#   input_arg {
#     name: "learning_rates"
#     type: DT_FLOAT
#     number_attr: "NN"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "NN"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "config"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "ShutdownDistributedTPU"
#   is_stateful: true
# }
# op {
#   name: "TPUCompilationResult"
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
# }
# op {
#   name: "TPUEmbeddingActivations"
#   input_arg {
#     name: "embedding_variable"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sliced_activations"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "table_id"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "lookup_id"
#     type: "int"
#     has_minimum: true
#   }
# }
# op {
#   name: "TPUReplicate"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "Tinputs"
#   }
#   input_arg {
#     name: "broadcast_inputs"
#     type_list_attr: "Tbroadcast_inputs"
#   }
#   input_arg {
#     name: "variables"
#     type: DT_RESOURCE
#     number_attr: "NumVariables"
#   }
#   input_arg {
#     name: "guaranteed_constants"
#     type_list_attr: "Tguaranteed_constants"
#   }
#   output_arg {
#     name: "outputs"
#     type_list_attr: "output_types"
#   }
#   attr {
#     name: "computation"
#     type: "func"
#   }
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "num_cores_per_replica"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "topology"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "use_tpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "device_assignment"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "host_compute_core"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "Tinputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tbroadcast_inputs"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "NumVariables"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "Tguaranteed_constants"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "TPUReplicateMetadata"
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#   }
#   attr {
#     name: "num_cores_per_replica"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "topology"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "use_tpu"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "device_assignment"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "computation_shape"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "host_compute_core"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
# }
# op {
#   name: "TPUReplicatedInput"
#   input_arg {
#     name: "inputs"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "TPUReplicatedOutput"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "outputs"
#     type_attr: "T"
#     number_attr: "num_replicas"
#   }
#   attr {
#     name: "num_replicas"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "WorkerHeartbeat"
#   input_arg {
#     name: "request"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "response"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\221\001\n\010AllToAll\022\n\n\005input\"\001T\022\024\n\020group_assignment\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\016\001\"\027\n\020concat_dimension\022\003int\"\026\n\017split_dimension\022\003int\"\022\n\013split_count\022\003int\ng\n\021CollectivePermute\022\n\n\005input\"\001T\022\027\n\023source_target_pairs\030\003\032\013\n\006output\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\212\001\n\027ConfigureDistributedTPU\032\014\n\010topology\030\007\"\036\n\020embedding_config\022\006string\032\002\022\000\"\"\n\024tpu_embedding_config\022\006string\032\002\022\000\"\032\n\016is_global_init\022\004bool\032\002(\000\210\001\001\nS\n\017CrossReplicaSum\022\n\n\005input\"\001T\022\024\n\020group_assignment\030\003\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\016\001\nw\n\037EnqueueTPUEmbeddingIntegerBatch\022\014\n\005batch\030\003*\001N\022\021\n\rmode_override\030\007\"\014\n\001N\022\003int(\0010\001\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\324\001\n\036EnqueueTPUEmbeddingSparseBatch\022\025\n\016sample_indices\030\003*\001N\022\030\n\021embedding_indices\030\003*\001N\022\032\n\023aggregation_weights\030\001*\001N\022\021\n\rmode_override\030\007\"\014\n\001N\022\003int(\0010\001\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\035\n\tcombiners\022\014list(string)\032\002\n\000\210\001\001\n\362\001\n$EnqueueTPUEmbeddingSparseTensorBatch\022\025\n\016sample_indices\030\003*\001N\022\030\n\021embedding_indices\030\003*\001N\022\032\n\023aggregation_weights\030\001*\001N\022\021\n\rmode_override\030\007\"\014\n\001N\022\003int(\0010\001\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\035\n\tcombiners\022\014list(string)\032\002\n\000\"\026\n\ttable_ids\022\tlist(int)\210\001\001\nB\n\rInfeedDequeue\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\210\001\001\n[\n\022InfeedDequeueTuple\032\021\n\007outputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\210\001\001\ni\n\rInfeedEnqueue\022\016\n\005input\"\005dtype\"\r\n\005dtype\022\004type\"\022\n\005shape\022\005shape\032\002:\000\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n~\n\022InfeedEnqueueTuple\022\020\n\006inputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\271\001\n\036LoadTPUEmbeddingADAMParameters\022\016\n\nparameters\030\001\022\013\n\007momenta\030\001\022\016\n\nvelocities\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\342\001\n,LoadTPUEmbeddingADAMParametersGradAccumDebug\022\016\n\nparameters\030\001\022\013\n\007momenta\030\001\022\016\n\nvelocities\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\277\001\n\"LoadTPUEmbeddingAdadeltaParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\013\n\007updates\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\350\001\n0LoadTPUEmbeddingAdadeltaParametersGradAccumDebug\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\013\n\007updates\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\261\001\n!LoadTPUEmbeddingAdagradParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\332\001\n/LoadTPUEmbeddingAdagradParametersGradAccumDebug\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\300\001\n)LoadTPUEmbeddingCenteredRMSPropParameters\022\016\n\nparameters\030\001\022\006\n\002ms\030\001\022\007\n\003mom\030\001\022\006\n\002mg\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\273\001\n\036LoadTPUEmbeddingFTRLParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\013\n\007linears\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\344\001\n,LoadTPUEmbeddingFTRLParametersGradAccumDebug\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\013\n\007linears\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\324\001\n)LoadTPUEmbeddingMDLAdagradLightParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\013\n\007weights\030\001\022\014\n\010benefits\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\255\001\n\"LoadTPUEmbeddingMomentumParameters\022\016\n\nparameters\030\001\022\013\n\007momenta\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\326\001\n0LoadTPUEmbeddingMomentumParametersGradAccumDebug\022\016\n\nparameters\030\001\022\013\n\007momenta\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\271\001\n)LoadTPUEmbeddingProximalAdagradParameters\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\342\001\n7LoadTPUEmbeddingProximalAdagradParametersGradAccumDebug\022\016\n\nparameters\030\001\022\020\n\014accumulators\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\260\001\n!LoadTPUEmbeddingRMSPropParameters\022\016\n\nparameters\030\001\022\006\n\002ms\030\001\022\007\n\003mom\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\331\001\n/LoadTPUEmbeddingRMSPropParametersGradAccumDebug\022\016\n\nparameters\030\001\022\006\n\002ms\030\001\022\007\n\003mom\030\001\022\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\261\001\n3LoadTPUEmbeddingStochasticGradientDescentParameters\022\016\n\nparameters\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\ng\n\016OutfeedDequeue\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\200\001\n\023OutfeedDequeueTuple\032\021\n\007outputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\025\n\006shapes\022\013list(shape)\"\"\n\016device_ordinal\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n2\n\016OutfeedEnqueue\022\016\n\005input\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\nD\n\023OutfeedEnqueueTuple\022\020\n\006inputs2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\210\001\001\nd\n\033RecvTPUEmbeddingActivations\032\030\n\007outputs\030\001*\013num_outputs\"\026\n\013num_outputs\022\003int(\0010\001\"\020\n\006config\022\006string\210\001\001\n\275\001\n\"RetrieveTPUEmbeddingADAMParameters\032\016\n\nparameters\030\001\032\013\n\007momenta\030\001\032\016\n\nvelocities\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\346\001\n0RetrieveTPUEmbeddingADAMParametersGradAccumDebug\032\016\n\nparameters\030\001\032\013\n\007momenta\030\001\032\016\n\nvelocities\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\303\001\n&RetrieveTPUEmbeddingAdadeltaParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\013\n\007updates\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\354\001\n4RetrieveTPUEmbeddingAdadeltaParametersGradAccumDebug\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\013\n\007updates\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\265\001\n%RetrieveTPUEmbeddingAdagradParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\336\001\n3RetrieveTPUEmbeddingAdagradParametersGradAccumDebug\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\304\001\n-RetrieveTPUEmbeddingCenteredRMSPropParameters\032\016\n\nparameters\030\001\032\006\n\002ms\030\001\032\007\n\003mom\030\001\032\006\n\002mg\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\277\001\n\"RetrieveTPUEmbeddingFTRLParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\013\n\007linears\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\350\001\n0RetrieveTPUEmbeddingFTRLParametersGradAccumDebug\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\013\n\007linears\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\330\001\n-RetrieveTPUEmbeddingMDLAdagradLightParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\013\n\007weights\030\001\032\014\n\010benefits\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\261\001\n&RetrieveTPUEmbeddingMomentumParameters\032\016\n\nparameters\030\001\032\013\n\007momenta\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\332\001\n4RetrieveTPUEmbeddingMomentumParametersGradAccumDebug\032\016\n\nparameters\030\001\032\013\n\007momenta\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\275\001\n-RetrieveTPUEmbeddingProximalAdagradParameters\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\346\001\n;RetrieveTPUEmbeddingProximalAdagradParametersGradAccumDebug\032\016\n\nparameters\030\001\032\020\n\014accumulators\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\264\001\n%RetrieveTPUEmbeddingRMSPropParameters\032\016\n\nparameters\030\001\032\006\n\002ms\030\001\032\007\n\003mom\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\335\001\n3RetrieveTPUEmbeddingRMSPropParametersGradAccumDebug\032\016\n\nparameters\030\001\032\006\n\002ms\030\001\032\007\n\003mom\030\001\032\031\n\025gradient_accumulators\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\n\265\001\n7RetrieveTPUEmbeddingStochasticGradientDescentParameters\032\016\n\nparameters\030\001\")\n\010table_id\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\ntable_name\022\006string\032\002\022\000\"\021\n\nnum_shards\022\003int\"\017\n\010shard_id\022\003int\210\001\001\nv\n\031SendTPUEmbeddingGradients\022\r\n\006inputs\030\001*\001N\022\026\n\016learning_rates\030\001*\002NN\"\014\n\001N\022\003int(\0010\001\"\017\n\002NN\022\003int\032\002\030\000(\001\"\020\n\006config\022\006string\210\001\001\n\033\n\026ShutdownDistributedTPU\210\001\001\n\"\n\024TPUCompilationResult\032\n\n\006output\030\007\n|\n\027TPUEmbeddingActivations\022\026\n\022embedding_variable\030\001\022\026\n\022sliced_activations\030\001\032\n\n\006output\030\001\"\021\n\010table_id\022\003int(\001\"\022\n\tlookup_id\022\003int(\001\n\220\004\n\014TPUReplicate\022\021\n\006inputs2\007Tinputs\022%\n\020broadcast_inputs2\021Tbroadcast_inputs\022\033\n\tvariables\030\024*\014NumVariables\022-\n\024guaranteed_constants2\025Tguaranteed_constants\032\027\n\007outputs2\014output_types\"\023\n\013computation\022\004func\"\027\n\014num_replicas\022\003int(\0010\001\" \n\025num_cores_per_replica\022\003int\032\002\030\001\"\026\n\010topology\022\006string\032\002\022\000\"\023\n\007use_tpu\022\004bool\032\002(\001\"\"\n\021device_assignment\022\tlist(int)\032\002\n\000\"%\n\021host_compute_core\022\014list(string)\032\002\n\000\"\027\n\007Tinputs\022\nlist(type)(\001\"!\n\021Tbroadcast_inputs\022\nlist(type)(\001\"\025\n\014NumVariables\022\003int(\001\"%\n\025Tguaranteed_constants\022\nlist(type)(\001\"\034\n\014output_types\022\nlist(type)(\001\210\001\001\n\353\001\n\024TPUReplicateMetadata\"\025\n\014num_replicas\022\003int(\001\" \n\025num_cores_per_replica\022\003int\032\002\030\001\"\026\n\010topology\022\006string\032\002\022\000\"\023\n\007use_tpu\022\004bool\032\002(\001\"\"\n\021device_assignment\022\tlist(int)\032\002\n\000\"\"\n\021computation_shape\022\tlist(int)\032\002\n\000\"%\n\021host_compute_core\022\014list(string)\032\002\n\000\nJ\n\022TPUReplicatedInput\022\016\n\006inputs\"\001T*\001N\032\013\n\006output\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\na\n\023TPUReplicatedOutput\022\n\n\005input\"\001T\032\032\n\007outputs\"\001T*\014num_replicas\"\027\n\014num_replicas\022\003int(\0010\001\"\t\n\001T\022\004type\n/\n\017WorkerHeartbeat\022\013\n\007request\030\007\032\014\n\010response\030\007\210\001\001")
