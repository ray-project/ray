"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: data_flow_ops.cc
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


def accumulator_apply_gradient(handle, local_step, gradient, name=None):
  r"""Applies a gradient to a given accumulator.

  Does not add if local_step is lesser than the accumulator's global_step.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a accumulator.
    local_step: A `Tensor` of type `int64`.
      The local_step value at which the gradient was computed.
    gradient: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of the gradient to be accumulated.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("accumulator_apply_gradient op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "AccumulatorApplyGradient", handle=handle, local_step=local_step,
                                    gradient=gradient, name=name)
  return _op
  _result = None
  return _result



def accumulator_apply_gradient_eager_fallback(handle, local_step, gradient, name=None, ctx=None):
  raise RuntimeError("accumulator_apply_gradient op does not support eager execution. Arg 'handle' is a ref.")

def accumulator_num_accumulated(handle, name=None):
  r"""Returns the number of gradients aggregated in the given accumulators.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to an accumulator.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("accumulator_num_accumulated op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "AccumulatorNumAccumulated", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "AccumulatorNumAccumulated", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def accumulator_num_accumulated_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("accumulator_num_accumulated op does not support eager execution. Arg 'handle' is a ref.")

def accumulator_set_global_step(handle, new_global_step, name=None):
  r"""Updates the accumulator with a new value for global_step.

  Logs warning if the accumulator's value is already higher than
  new_global_step.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to an accumulator.
    new_global_step: A `Tensor` of type `int64`.
      The new global_step value to set.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("accumulator_set_global_step op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "AccumulatorSetGlobalStep", handle=handle,
                                    new_global_step=new_global_step,
                                    name=name)
  return _op
  _result = None
  return _result



def accumulator_set_global_step_eager_fallback(handle, new_global_step, name=None, ctx=None):
  raise RuntimeError("accumulator_set_global_step op does not support eager execution. Arg 'handle' is a ref.")

def accumulator_take_gradient(handle, num_required, dtype, name=None):
  r"""Extracts the average gradient in the given ConditionalAccumulator.

  The op blocks until sufficient (i.e., more than num_required)
  gradients have been accumulated.  If the accumulator has already
  aggregated more than num_required gradients, it returns the average of
  the accumulated gradients.  Also automatically increments the recorded
  global_step in the accumulator by 1, and resets the aggregate to 0.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to an accumulator.
    num_required: A `Tensor` of type `int32`.
      Number of gradients required before we return an aggregate.
    dtype: A `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.uint8, tf.int16, tf.int8, tf.complex64, tf.int64, tf.qint8, tf.quint8, tf.qint32, tf.bfloat16, tf.uint16, tf.complex128, tf.half, tf.uint32, tf.uint64`.
      The data type of accumulated gradients. Needs to correspond to the type
      of the accumulator.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("accumulator_take_gradient op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AccumulatorTakeGradient", handle=handle, num_required=num_required,
                                   dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "AccumulatorTakeGradient", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def accumulator_take_gradient_eager_fallback(handle, num_required, dtype, name=None, ctx=None):
  raise RuntimeError("accumulator_take_gradient op does not support eager execution. Arg 'handle' is a ref.")

def barrier(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None):
  r"""Defines a barrier that persists across different graph executions.

  A barrier represents a key-value map, where each key is a string, and
  each value is a tuple of tensors.

  At runtime, the barrier contains 'complete' and 'incomplete'
  elements. A complete element has defined tensors for all components of
  its value tuple, and may be accessed using BarrierTakeMany. An
  incomplete element has some undefined components in its value tuple,
  and may be updated using BarrierInsertMany.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. Each shape must be 1 in the
      first dimension. The length of this attr must be the same as the length of
      component_types.
    capacity: An optional `int`. Defaults to `-1`.
      The capacity of the barrier.  The default capacity is MAX_INT32,
      which is the largest capacity of the underlying queue.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this barrier is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this barrier will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'barrier' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'barrier' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Barrier", component_types=component_types, shapes=shapes,
                   capacity=capacity, container=container,
                   shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "Barrier", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def barrier_eager_fallback(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("barrier op does not support eager execution. Arg 'handle' is a ref.")

def barrier_close(handle, cancel_pending_enqueues=False, name=None):
  r"""Closes the given barrier.

  This operation signals that no more new elements will be inserted in the
  given barrier. Subsequent InsertMany that try to introduce a new key will fail.
  Subsequent InsertMany operations that just add missing components to already
  existing elements will continue to succeed. Subsequent TakeMany operations will
  continue to succeed if sufficient completed elements remain in the barrier.
  Subsequent TakeMany operations that would block will fail immediately.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a barrier.
    cancel_pending_enqueues: An optional `bool`. Defaults to `False`.
      If true, all pending enqueue requests that are
      blocked on the barrier's queue will be canceled. InsertMany will fail, even
      if no new key is introduced.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier_close op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if cancel_pending_enqueues is None:
    cancel_pending_enqueues = False
  cancel_pending_enqueues = _execute.make_bool(cancel_pending_enqueues, "cancel_pending_enqueues")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BarrierClose", handle=handle,
                        cancel_pending_enqueues=cancel_pending_enqueues,
                        name=name)
  return _op
  _result = None
  return _result



def barrier_close_eager_fallback(handle, cancel_pending_enqueues=False, name=None, ctx=None):
  raise RuntimeError("barrier_close op does not support eager execution. Arg 'handle' is a ref.")

def barrier_incomplete_size(handle, name=None):
  r"""Computes the number of incomplete elements in the given barrier.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a barrier.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier_incomplete_size op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BarrierIncompleteSize", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BarrierIncompleteSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def barrier_incomplete_size_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("barrier_incomplete_size op does not support eager execution. Arg 'handle' is a ref.")

def barrier_insert_many(handle, keys, values, component_index, name=None):
  r"""For each key, assigns the respective value to the specified component.

  If a key is not found in the barrier, this operation will create a new
  incomplete element. If a key is found in the barrier, and the element
  already has a value at component_index, this operation will fail with
  INVALID_ARGUMENT, and leave the barrier in an undefined state.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a barrier.
    keys: A `Tensor` of type `string`.
      A one-dimensional tensor of keys, with length n.
    values: A `Tensor`.
      An any-dimensional tensor of values, which are associated with the
      respective keys. The 0th dimension must have length n.
    component_index: An `int`.
      The component of the barrier elements that is being assigned.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier_insert_many op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  component_index = _execute.make_int(component_index, "component_index")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BarrierInsertMany", handle=handle, keys=keys, values=values,
                             component_index=component_index, name=name)
  return _op
  _result = None
  return _result



def barrier_insert_many_eager_fallback(handle, keys, values, component_index, name=None, ctx=None):
  raise RuntimeError("barrier_insert_many op does not support eager execution. Arg 'handle' is a ref.")

def barrier_ready_size(handle, name=None):
  r"""Computes the number of complete elements in the given barrier.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a barrier.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier_ready_size op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "BarrierReadySize", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "BarrierReadySize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def barrier_ready_size_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("barrier_ready_size op does not support eager execution. Arg 'handle' is a ref.")

_barrier_take_many_outputs = ["indices", "keys", "values"]
_BarrierTakeManyOutput = _collections.namedtuple(
    "BarrierTakeMany", _barrier_take_many_outputs)


def barrier_take_many(handle, num_elements, component_types, allow_small_batch=False, wait_for_incomplete=False, timeout_ms=-1, name=None):
  r"""Takes the given number of completed elements from a barrier.

  This operation concatenates completed-element component tensors along
  the 0th dimension to make a single component tensor.

  Elements come out of the barrier when they are complete, and in the order
  in which they were placed into the barrier.  The indices output provides
  information about the batch in which each element was originally inserted
  into the barrier.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a barrier.
    num_elements: A `Tensor` of type `int32`.
      A single-element tensor containing the number of elements to
      take.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    allow_small_batch: An optional `bool`. Defaults to `False`.
      Allow to return less than num_elements items if barrier is
      already closed.
    wait_for_incomplete: An optional `bool`. Defaults to `False`.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is empty, this operation will block for up to
      timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (indices, keys, values).

    indices: A `Tensor` of type `int64`.
    keys: A `Tensor` of type `string`.
    values: A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("barrier_take_many op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'barrier_take_many' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if allow_small_batch is None:
    allow_small_batch = False
  allow_small_batch = _execute.make_bool(allow_small_batch, "allow_small_batch")
  if wait_for_incomplete is None:
    wait_for_incomplete = False
  wait_for_incomplete = _execute.make_bool(wait_for_incomplete, "wait_for_incomplete")
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "BarrierTakeMany", handle=handle, num_elements=num_elements,
                           component_types=component_types,
                           allow_small_batch=allow_small_batch,
                           wait_for_incomplete=wait_for_incomplete,
                           timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"),
            "allow_small_batch", _op.get_attr("allow_small_batch"),
            "wait_for_incomplete", _op.get_attr("wait_for_incomplete"),
            "timeout_ms", _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "BarrierTakeMany", _inputs_flat, _attrs, _result, name)
  _result = _result[:2] + [_result[2:]]
  _result = _BarrierTakeManyOutput._make(_result)
  return _result



def barrier_take_many_eager_fallback(handle, num_elements, component_types, allow_small_batch=False, wait_for_incomplete=False, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("barrier_take_many op does not support eager execution. Arg 'handle' is a ref.")

def conditional_accumulator(dtype, shape, container="", shared_name="", reduction_type="MEAN", name=None):
  r"""A conditional accumulator for aggregating gradients.

  The accumulator accepts gradients marked with local_step greater or
  equal to the most recent global_step known to the accumulator. The
  average can be extracted from the accumulator, provided sufficient
  gradients have been accumulated. Extracting the average automatically
  resets the aggregate to 0, and increments the global_step recorded by
  the accumulator.

  Args:
    dtype: A `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.uint8, tf.int16, tf.int8, tf.complex64, tf.int64, tf.qint8, tf.quint8, tf.qint32, tf.bfloat16, tf.uint16, tf.complex128, tf.half, tf.uint32, tf.uint64`.
      The type of the value being accumulated.
    shape: A `tf.TensorShape` or list of `ints`.
      The shape of the values, can be [], in which case shape is unknown.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this accumulator is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this accumulator will be shared under the
      given name across multiple sessions.
    reduction_type: An optional `string` from: `"MEAN", "SUM"`. Defaults to `"MEAN"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("conditional_accumulator op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if reduction_type is None:
    reduction_type = "MEAN"
  reduction_type = _execute.make_str(reduction_type, "reduction_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ConditionalAccumulator", dtype=dtype, shape=shape,
                                  container=container,
                                  shared_name=shared_name,
                                  reduction_type=reduction_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "reduction_type",
            _op.get_attr("reduction_type"))
  _execute.record_gradient(
      "ConditionalAccumulator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def conditional_accumulator_eager_fallback(dtype, shape, container="", shared_name="", reduction_type="MEAN", name=None, ctx=None):
  raise RuntimeError("conditional_accumulator op does not support eager execution. Arg 'handle' is a ref.")

def delete_session_tensor(handle, name=None):
  r"""Delete the tensor specified by its handle in the session.

  Args:
    handle: A `Tensor` of type `string`.
      The handle for a tensor stored in the session state.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DeleteSessionTensor", name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return delete_session_tensor_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "DeleteSessionTensor", handle=handle, name=name)
  return _op
  _result = None
  return _result



def delete_session_tensor_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function delete_session_tensor
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"DeleteSessionTensor", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


@_dispatch.add_dispatch_list
@tf_export('dynamic_partition')
def dynamic_partition(data, partitions, num_partitions, name=None):
  r"""Partitions `data` into `num_partitions` tensors using indices from `partitions`.

  For each index tuple `js` of size `partitions.ndim`, the slice `data[js, ...]`
  becomes part of `outputs[partitions[js]]`.  The slices with `partitions[js] = i`
  are placed in `outputs[i]` in lexicographic order of `js`, and the first
  dimension of `outputs[i]` is the number of entries in `partitions` equal to `i`.
  In detail,

  ```python
      outputs[i].shape = [sum(partitions == i)] + data.shape[partitions.ndim:]

      outputs[i] = pack([data[js, ...] for js if partitions[js] == i])
  ```

  `data.shape` must start with `partitions.shape`.

  For example:

  ```python
      # Scalar partitions.
      partitions = 1
      num_partitions = 2
      data = [10, 20]
      outputs[0] = []  # Empty with shape [0, 2]
      outputs[1] = [[10, 20]]

      # Vector partitions.
      partitions = [0, 0, 1, 1, 0]
      num_partitions = 2
      data = [10, 20, 30, 40, 50]
      outputs[0] = [10, 20, 50]
      outputs[1] = [30, 40]
  ```

  See `dynamic_stitch` for an example on how to merge partitions back.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/DynamicPartition.png" alt>
  </div>

  Args:
    data: A `Tensor`.
    partitions: A `Tensor` of type `int32`.
      Any shape.  Indices in the range `[0, num_partitions)`.
    num_partitions: An `int` that is `>= 1`.
      The number of partitions to output.
    name: A name for the operation (optional).

  Returns:
    A list of `num_partitions` `Tensor` objects with the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DynamicPartition", name, _ctx._post_execution_callbacks, data,
        partitions, "num_partitions", num_partitions)
      return _result
    except _core._FallbackException:
      try:
        return dynamic_partition_eager_fallback(
            data, partitions, num_partitions=num_partitions, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              dynamic_partition, data=data, partitions=partitions,
                                 num_partitions=num_partitions, name=name)
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
  num_partitions = _execute.make_int(num_partitions, "num_partitions")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DynamicPartition", data=data, partitions=partitions,
                            num_partitions=num_partitions, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          dynamic_partition, data=data, partitions=partitions,
                             num_partitions=num_partitions, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_partitions", _op.get_attr("num_partitions"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "DynamicPartition", _inputs_flat, _attrs, _result, name)
  return _result



def dynamic_partition_eager_fallback(data, partitions, num_partitions, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dynamic_partition
  """
  _ctx = ctx if ctx else _context.context()
  num_partitions = _execute.make_int(num_partitions, "num_partitions")
  _attr_T, (data,) = _execute.args_to_matching_eager([data], _ctx)
  partitions = _ops.convert_to_tensor(partitions, _dtypes.int32)
  _inputs_flat = [data, partitions]
  _attrs = ("num_partitions", num_partitions, "T", _attr_T)
  _result = _execute.execute(b"DynamicPartition", num_partitions,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DynamicPartition", _inputs_flat, _attrs, _result, name)
  return _result


@_dispatch.add_dispatch_list
@tf_export('dynamic_stitch')
def dynamic_stitch(indices, data, name=None):
  r"""Interleave the values from the `data` tensors into a single tensor.

  Builds a merged tensor such that

  ```python
      merged[indices[m][i, ..., j], ...] = data[m][i, ..., j, ...]
  ```

  For example, if each `indices[m]` is scalar or vector, we have

  ```python
      # Scalar indices:
      merged[indices[m], ...] = data[m][...]

      # Vector indices:
      merged[indices[m][i], ...] = data[m][i, ...]
  ```

  Each `data[i].shape` must start with the corresponding `indices[i].shape`,
  and the rest of `data[i].shape` must be constant w.r.t. `i`.  That is, we
  must have `data[i].shape = indices[i].shape + constant`.  In terms of this
  `constant`, the output shape is

      merged.shape = [max(indices)] + constant

  Values are merged in order, so if an index appears in both `indices[m][i]` and
  `indices[n][j]` for `(m,i) < (n,j)` the slice `data[n][j]` will appear in the
  merged result. If you do not need this guarantee, ParallelDynamicStitch might
  perform better on some devices.

  For example:

  ```python
      indices[0] = 6
      indices[1] = [4, 1]
      indices[2] = [[5, 2], [0, 3]]
      data[0] = [61, 62]
      data[1] = [[41, 42], [11, 12]]
      data[2] = [[[51, 52], [21, 22]], [[1, 2], [31, 32]]]
      merged = [[1, 2], [11, 12], [21, 22], [31, 32], [41, 42],
                [51, 52], [61, 62]]
  ```

  This method can be used to merge partitions created by `dynamic_partition`
  as illustrated on the following example:

  ```python
      # Apply function (increments x_i) on elements for which a certain condition
      # apply (x_i != -1 in this example).
      x=tf.constant([0.1, -1., 5.2, 4.3, -1., 7.4])
      condition_mask=tf.not_equal(x,tf.constant(-1.))
      partitioned_data = tf.dynamic_partition(
          x, tf.cast(condition_mask, tf.int32) , 2)
      partitioned_data[1] = partitioned_data[1] + 1.0
      condition_indices = tf.dynamic_partition(
          tf.range(tf.shape(x)[0]), tf.cast(condition_mask, tf.int32) , 2)
      x = tf.dynamic_stitch(condition_indices, partitioned_data)
      # Here x=[1.1, -1., 6.2, 5.3, -1, 8.4], the -1. values remain
      # unchanged.
  ```

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/DynamicStitch.png" alt>
  </div>

  Args:
    indices: A list of at least 1 `Tensor` objects with type `int32`.
    data: A list with the same length as `indices` of `Tensor` objects with the same type.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DynamicStitch", name, _ctx._post_execution_callbacks, indices, data)
      return _result
    except _core._FallbackException:
      try:
        return dynamic_stitch_eager_fallback(
            indices, data, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              dynamic_stitch, indices=indices, data=data, name=name)
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
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'dynamic_stitch' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(data, (list, tuple)):
    raise TypeError(
        "Expected list for 'data' argument to "
        "'dynamic_stitch' Op, not %r." % data)
  if len(data) != _attr_N:
    raise ValueError(
        "List argument 'data' to 'dynamic_stitch' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(data), _attr_N))
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DynamicStitch", indices=indices, data=data, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          dynamic_stitch, indices=indices, data=data, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "DynamicStitch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dynamic_stitch_eager_fallback(indices, data, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dynamic_stitch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'dynamic_stitch' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(data, (list, tuple)):
    raise TypeError(
        "Expected list for 'data' argument to "
        "'dynamic_stitch' Op, not %r." % data)
  if len(data) != _attr_N:
    raise ValueError(
        "List argument 'data' to 'dynamic_stitch' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(data), _attr_N))
  _attr_T, data = _execute.args_to_matching_eager(list(data), _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int32)
  _inputs_flat = list(indices) + list(data)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"DynamicStitch", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DynamicStitch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def fifo_queue(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements in first-in first-out order.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("fifo_queue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'fifo_queue' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'fifo_queue' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FIFOQueue", component_types=component_types, shapes=shapes,
                     capacity=capacity, container=container,
                     shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "FIFOQueue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fifo_queue_eager_fallback(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("fifo_queue op does not support eager execution. Arg 'handle' is a ref.")

def fifo_queue_v2(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements in first-in first-out order.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FIFOQueueV2",
        name, _ctx._post_execution_callbacks, "component_types",
        component_types, "shapes", shapes, "capacity", capacity, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return fifo_queue_v2_eager_fallback(
            component_types=component_types, shapes=shapes, capacity=capacity,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'fifo_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'fifo_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FIFOQueueV2", component_types=component_types, shapes=shapes,
                       capacity=capacity, container=container,
                       shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "FIFOQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fifo_queue_v2_eager_fallback(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fifo_queue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'fifo_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'fifo_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("component_types", component_types, "shapes", shapes, "capacity",
  capacity, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"FIFOQueueV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FIFOQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def fake_queue(resource, name=None):
  r"""Deprecated. Do not use.

  Args:
    resource: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("fake_queue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "FakeQueue", resource=resource, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "FakeQueue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_queue_eager_fallback(resource, name=None, ctx=None):
  raise RuntimeError("fake_queue op does not support eager execution. Arg 'handle' is a ref.")

def get_session_handle(value, name=None):
  r"""Store the input tensor in the state of the current session.

  Args:
    value: A `Tensor`. The tensor to be stored.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GetSessionHandle", name, _ctx._post_execution_callbacks, value)
      return _result
    except _core._FallbackException:
      try:
        return get_session_handle_eager_fallback(
            value, name=name, ctx=_ctx)
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
        "GetSessionHandle", value=value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GetSessionHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def get_session_handle_eager_fallback(value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function get_session_handle
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GetSessionHandle", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GetSessionHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def get_session_handle_v2(value, name=None):
  r"""Store the input tensor in the state of the current session.

  Args:
    value: A `Tensor`. The tensor to be stored.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GetSessionHandleV2", name, _ctx._post_execution_callbacks, value)
      return _result
    except _core._FallbackException:
      try:
        return get_session_handle_v2_eager_fallback(
            value, name=name, ctx=_ctx)
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
        "GetSessionHandleV2", value=value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GetSessionHandleV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def get_session_handle_v2_eager_fallback(value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function get_session_handle_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GetSessionHandleV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GetSessionHandleV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def get_session_tensor(handle, dtype, name=None):
  r"""Get the value of the tensor specified by its handle.

  Args:
    handle: A `Tensor` of type `string`.
      The handle for a tensor stored in the session state.
    dtype: A `tf.DType`. The type of the output value.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GetSessionTensor", name, _ctx._post_execution_callbacks, handle,
        "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return get_session_tensor_eager_fallback(
            handle, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "GetSessionTensor", handle=handle, dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "GetSessionTensor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def get_session_tensor_eager_fallback(handle, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function get_session_tensor
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  _inputs_flat = [handle]
  _attrs = ("dtype", dtype)
  _result = _execute.execute(b"GetSessionTensor", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GetSessionTensor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def map_clear(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes all elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapClear",
        name, _ctx._post_execution_callbacks, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_clear_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapClear", dtypes=dtypes, capacity=capacity,
                    memory_limit=memory_limit, container=container,
                    shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def map_clear_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_clear
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapClear", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def map_incomplete_size(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op returns the number of incomplete elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MapIncompleteSize", name, _ctx._post_execution_callbacks, "capacity",
        capacity, "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_incomplete_size_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_incomplete_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapIncompleteSize", dtypes=dtypes, capacity=capacity,
                             memory_limit=memory_limit, container=container,
                             shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MapIncompleteSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def map_incomplete_size_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_incomplete_size
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_incomplete_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapIncompleteSize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MapIncompleteSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def map_peek(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op peeks at the values at the specified key.  If the

  underlying container does not contain this key
  this op will block until it does.

  Args:
    key: A `Tensor` of type `int64`.
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapPeek",
        name, _ctx._post_execution_callbacks, key, indices, "capacity",
        capacity, "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_peek_eager_fallback(
            key, indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapPeek", key=key, indices=indices, dtypes=dtypes, capacity=capacity,
                   memory_limit=memory_limit, container=container,
                   shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MapPeek", _inputs_flat, _attrs, _result, name)
  return _result



def map_peek_eager_fallback(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_peek
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapPeek", len(dtypes), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MapPeek", _inputs_flat, _attrs, _result, name)
  return _result


def map_size(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op returns the number of elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapSize",
        name, _ctx._post_execution_callbacks, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_size_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapSize", dtypes=dtypes, capacity=capacity,
                   memory_limit=memory_limit, container=container,
                   shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MapSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def map_size_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_size
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapSize", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "MapSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def map_stage(key, indices, values, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Stage (key, values) in the underlying container which behaves like a hashtable.

  Args:
    key: A `Tensor` of type `int64`. int64
    indices: A `Tensor` of type `int32`.
    values: A list of `Tensor` objects. a list of tensors
      dtypes A list of data types that inserted values should adhere to.
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
      Maximum number of elements in the Staging Area. If > 0, inserts
      on the container will block when the capacity is reached.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container. Otherwise,
      a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      It is necessary to match this name to the matching Unstage Op.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapStage",
        name, _ctx._post_execution_callbacks, key, indices, values,
        "capacity", capacity, "memory_limit", memory_limit, "dtypes", dtypes,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_stage_eager_fallback(
            key, indices, values, capacity=capacity,
            memory_limit=memory_limit, dtypes=dtypes, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
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
        "'map_stage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapStage", key=key, indices=indices, values=values, dtypes=dtypes,
                    capacity=capacity, memory_limit=memory_limit,
                    container=container, shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def map_stage_eager_fallback(key, indices, values, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_stage
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_stage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_fake_dtypes, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices] + list(values)
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "fake_dtypes", _attr_fake_dtypes, "container", container,
  "shared_name", shared_name)
  _result = _execute.execute(b"MapStage", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def map_unstage(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes and returns the values associated with the key

  from the underlying container.   If the underlying container
  does not contain this key, the op will block until it does.

  Args:
    key: A `Tensor` of type `int64`.
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapUnstage",
        name, _ctx._post_execution_callbacks, key, indices, "capacity",
        capacity, "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return map_unstage_eager_fallback(
            key, indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapUnstage", key=key, indices=indices, dtypes=dtypes,
                      capacity=capacity, memory_limit=memory_limit,
                      container=container, shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MapUnstage", _inputs_flat, _attrs, _result, name)
  return _result



def map_unstage_eager_fallback(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_unstage
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapUnstage", len(dtypes), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MapUnstage", _inputs_flat, _attrs, _result, name)
  return _result


_map_unstage_no_key_outputs = ["key", "values"]
_MapUnstageNoKeyOutput = _collections.namedtuple(
    "MapUnstageNoKey", _map_unstage_no_key_outputs)


def map_unstage_no_key(indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes and returns a random (key, value)

  from the underlying container.   If the underlying container
  does not contain elements, the op will block until it does.

  Args:
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (key, values).

    key: A `Tensor` of type `int64`.
    values: A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MapUnstageNoKey", name, _ctx._post_execution_callbacks, indices,
        "capacity", capacity, "memory_limit", memory_limit, "dtypes", dtypes,
        "container", container, "shared_name", shared_name)
      _result = _MapUnstageNoKeyOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return map_unstage_no_key_eager_fallback(
            indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_unstage_no_key' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapUnstageNoKey", indices=indices, dtypes=dtypes, capacity=capacity,
                           memory_limit=memory_limit, container=container,
                           shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MapUnstageNoKey", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _MapUnstageNoKeyOutput._make(_result)
  return _result



def map_unstage_no_key_eager_fallback(indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_unstage_no_key
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'map_unstage_no_key' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"MapUnstageNoKey", len(dtypes) + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MapUnstageNoKey", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _MapUnstageNoKeyOutput._make(_result)
  return _result


def ordered_map_clear(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes all elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapClear", name, _ctx._post_execution_callbacks, "capacity",
        capacity, "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_clear_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapClear", dtypes=dtypes, capacity=capacity,
                           memory_limit=memory_limit, container=container,
                           shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def ordered_map_clear_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_clear
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapClear", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def ordered_map_incomplete_size(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op returns the number of incomplete elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapIncompleteSize", name, _ctx._post_execution_callbacks,
        "capacity", capacity, "memory_limit", memory_limit, "dtypes", dtypes,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_incomplete_size_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_incomplete_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapIncompleteSize", dtypes=dtypes, capacity=capacity,
                                    memory_limit=memory_limit,
                                    container=container,
                                    shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OrderedMapIncompleteSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ordered_map_incomplete_size_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_incomplete_size
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_incomplete_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapIncompleteSize", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OrderedMapIncompleteSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def ordered_map_peek(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op peeks at the values at the specified key.  If the

  underlying container does not contain this key
  this op will block until it does.   This Op is optimized for
  performance.

  Args:
    key: A `Tensor` of type `int64`.
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapPeek", name, _ctx._post_execution_callbacks, key, indices,
        "capacity", capacity, "memory_limit", memory_limit, "dtypes", dtypes,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_peek_eager_fallback(
            key, indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapPeek", key=key, indices=indices, dtypes=dtypes,
                          capacity=capacity, memory_limit=memory_limit,
                          container=container, shared_name=shared_name,
                          name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OrderedMapPeek", _inputs_flat, _attrs, _result, name)
  return _result



def ordered_map_peek_eager_fallback(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_peek
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapPeek", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OrderedMapPeek", _inputs_flat, _attrs, _result, name)
  return _result


def ordered_map_size(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op returns the number of elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapSize", name, _ctx._post_execution_callbacks, "capacity",
        capacity, "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_size_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapSize", dtypes=dtypes, capacity=capacity,
                          memory_limit=memory_limit, container=container,
                          shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OrderedMapSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def ordered_map_size_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_size
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapSize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OrderedMapSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def ordered_map_stage(key, indices, values, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Stage (key, values) in the underlying container which behaves like a ordered

  associative container.   Elements are ordered by key.

  Args:
    key: A `Tensor` of type `int64`. int64
    indices: A `Tensor` of type `int32`.
    values: A list of `Tensor` objects. a list of tensors
      dtypes A list of data types that inserted values should adhere to.
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
      Maximum number of elements in the Staging Area. If > 0, inserts
      on the container will block when the capacity is reached.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container. Otherwise,
      a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      It is necessary to match this name to the matching Unstage Op.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapStage", name, _ctx._post_execution_callbacks, key, indices,
        values, "capacity", capacity, "memory_limit", memory_limit, "dtypes",
        dtypes, "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_stage_eager_fallback(
            key, indices, values, capacity=capacity,
            memory_limit=memory_limit, dtypes=dtypes, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
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
        "'ordered_map_stage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapStage", key=key, indices=indices, values=values,
                           dtypes=dtypes, capacity=capacity,
                           memory_limit=memory_limit, container=container,
                           shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def ordered_map_stage_eager_fallback(key, indices, values, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_stage
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_stage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_fake_dtypes, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices] + list(values)
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "fake_dtypes", _attr_fake_dtypes, "container", container,
  "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapStage", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def ordered_map_unstage(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes and returns the values associated with the key

  from the underlying container.   If the underlying container
  does not contain this key, the op will block until it does.

  Args:
    key: A `Tensor` of type `int64`.
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapUnstage", name, _ctx._post_execution_callbacks, key,
        indices, "capacity", capacity, "memory_limit", memory_limit, "dtypes",
        dtypes, "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_unstage_eager_fallback(
            key, indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapUnstage", key=key, indices=indices, dtypes=dtypes,
                             capacity=capacity, memory_limit=memory_limit,
                             container=container, shared_name=shared_name,
                             name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OrderedMapUnstage", _inputs_flat, _attrs, _result, name)
  return _result



def ordered_map_unstage_eager_fallback(key, indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_unstage
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  key = _ops.convert_to_tensor(key, _dtypes.int64)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [key, indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapUnstage", len(dtypes),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OrderedMapUnstage", _inputs_flat, _attrs, _result, name)
  return _result


_ordered_map_unstage_no_key_outputs = ["key", "values"]
_OrderedMapUnstageNoKeyOutput = _collections.namedtuple(
    "OrderedMapUnstageNoKey", _ordered_map_unstage_no_key_outputs)


def ordered_map_unstage_no_key(indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes and returns the (key, value) element with the smallest

  key from the underlying container.   If the underlying container
  does not contain elements, the op will block until it does.

  Args:
    indices: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (key, values).

    key: A `Tensor` of type `int64`.
    values: A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OrderedMapUnstageNoKey", name, _ctx._post_execution_callbacks,
        indices, "capacity", capacity, "memory_limit", memory_limit, "dtypes",
        dtypes, "container", container, "shared_name", shared_name)
      _result = _OrderedMapUnstageNoKeyOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return ordered_map_unstage_no_key_eager_fallback(
            indices, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_unstage_no_key' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OrderedMapUnstageNoKey", indices=indices, dtypes=dtypes,
                                  capacity=capacity,
                                  memory_limit=memory_limit,
                                  container=container,
                                  shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OrderedMapUnstageNoKey", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _OrderedMapUnstageNoKeyOutput._make(_result)
  return _result



def ordered_map_unstage_no_key_eager_fallback(indices, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function ordered_map_unstage_no_key
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'ordered_map_unstage_no_key' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  _inputs_flat = [indices]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"OrderedMapUnstageNoKey", len(dtypes) + 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OrderedMapUnstageNoKey", _inputs_flat, _attrs, _result, name)
  _result = _result[:1] + [_result[1:]]
  _result = _OrderedMapUnstageNoKeyOutput._make(_result)
  return _result


def padding_fifo_queue(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements in first-in first-out order.

  Variable-size shapes are allowed by setting the corresponding shape dimensions
  to 0 in the shape attr.  In this case DequeueMany will pad up to the maximum
  size of any given element in the minibatch.  See below for details.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types.
      Shapes of fixed rank but variable size are allowed by setting
      any shape dimension to -1.  In this case, the inputs' shape may vary along
      the given dimension, and DequeueMany will pad the given dimension with
      zeros up to the maximum shape of all elements in the given batch.
      If the length of this attr is 0, different queue elements may have
      different ranks and shapes, but only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("padding_fifo_queue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'padding_fifo_queue' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'padding_fifo_queue' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PaddingFIFOQueue", component_types=component_types, shapes=shapes,
                            capacity=capacity, container=container,
                            shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "PaddingFIFOQueue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def padding_fifo_queue_eager_fallback(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("padding_fifo_queue op does not support eager execution. Arg 'handle' is a ref.")

def padding_fifo_queue_v2(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements in first-in first-out order.

  Variable-size shapes are allowed by setting the corresponding shape dimensions
  to 0 in the shape attr.  In this case DequeueMany will pad up to the maximum
  size of any given element in the minibatch.  See below for details.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types.
      Shapes of fixed rank but variable size are allowed by setting
      any shape dimension to -1.  In this case, the inputs' shape may vary along
      the given dimension, and DequeueMany will pad the given dimension with
      zeros up to the maximum shape of all elements in the given batch.
      If the length of this attr is 0, different queue elements may have
      different ranks and shapes, but only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PaddingFIFOQueueV2", name, _ctx._post_execution_callbacks,
        "component_types", component_types, "shapes", shapes, "capacity",
        capacity, "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return padding_fifo_queue_v2_eager_fallback(
            component_types=component_types, shapes=shapes, capacity=capacity,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'padding_fifo_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'padding_fifo_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PaddingFIFOQueueV2", component_types=component_types, shapes=shapes,
                              capacity=capacity, container=container,
                              shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "PaddingFIFOQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def padding_fifo_queue_v2_eager_fallback(component_types, shapes=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function padding_fifo_queue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'padding_fifo_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'padding_fifo_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("component_types", component_types, "shapes", shapes, "capacity",
  capacity, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"PaddingFIFOQueueV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PaddingFIFOQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def parallel_dynamic_stitch(indices, data, name=None):
  r"""Interleave the values from the `data` tensors into a single tensor.

  Builds a merged tensor such that

  ```python
      merged[indices[m][i, ..., j], ...] = data[m][i, ..., j, ...]
  ```

  For example, if each `indices[m]` is scalar or vector, we have

  ```python
      # Scalar indices:
      merged[indices[m], ...] = data[m][...]

      # Vector indices:
      merged[indices[m][i], ...] = data[m][i, ...]
  ```

  Each `data[i].shape` must start with the corresponding `indices[i].shape`,
  and the rest of `data[i].shape` must be constant w.r.t. `i`.  That is, we
  must have `data[i].shape = indices[i].shape + constant`.  In terms of this
  `constant`, the output shape is

      merged.shape = [max(indices)] + constant

  Values may be merged in parallel, so if an index appears in both `indices[m][i]`
  and `indices[n][j]`, the result may be invalid. This differs from the normal
  DynamicStitch operator that defines the behavior in that case.

  For example:

  ```python
      indices[0] = 6
      indices[1] = [4, 1]
      indices[2] = [[5, 2], [0, 3]]
      data[0] = [61, 62]
      data[1] = [[41, 42], [11, 12]]
      data[2] = [[[51, 52], [21, 22]], [[1, 2], [31, 32]]]
      merged = [[1, 2], [11, 12], [21, 22], [31, 32], [41, 42],
                [51, 52], [61, 62]]
  ```

  This method can be used to merge partitions created by `dynamic_partition`
  as illustrated on the following example:

  ```python
      # Apply function (increments x_i) on elements for which a certain condition
      # apply (x_i != -1 in this example).
      x=tf.constant([0.1, -1., 5.2, 4.3, -1., 7.4])
      condition_mask=tf.not_equal(x,tf.constant(-1.))
      partitioned_data = tf.dynamic_partition(
          x, tf.cast(condition_mask, tf.int32) , 2)
      partitioned_data[1] = partitioned_data[1] + 1.0
      condition_indices = tf.dynamic_partition(
          tf.range(tf.shape(x)[0]), tf.cast(condition_mask, tf.int32) , 2)
      x = tf.dynamic_stitch(condition_indices, partitioned_data)
      # Here x=[1.1, -1., 6.2, 5.3, -1, 8.4], the -1. values remain
      # unchanged.
  ```

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src="https://www.tensorflow.org/images/DynamicStitch.png" alt>
  </div>

  Args:
    indices: A list of at least 1 `Tensor` objects with type `int32`.
    data: A list with the same length as `indices` of `Tensor` objects with the same type.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `data`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ParallelDynamicStitch", name, _ctx._post_execution_callbacks,
        indices, data)
      return _result
    except _core._FallbackException:
      try:
        return parallel_dynamic_stitch_eager_fallback(
            indices, data, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'parallel_dynamic_stitch' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(data, (list, tuple)):
    raise TypeError(
        "Expected list for 'data' argument to "
        "'parallel_dynamic_stitch' Op, not %r." % data)
  if len(data) != _attr_N:
    raise ValueError(
        "List argument 'data' to 'parallel_dynamic_stitch' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(data), _attr_N))
  _, _, _op = _op_def_lib._apply_op_helper(
        "ParallelDynamicStitch", indices=indices, data=data, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "ParallelDynamicStitch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def parallel_dynamic_stitch_eager_fallback(indices, data, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function parallel_dynamic_stitch
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(indices, (list, tuple)):
    raise TypeError(
        "Expected list for 'indices' argument to "
        "'parallel_dynamic_stitch' Op, not %r." % indices)
  _attr_N = len(indices)
  if not isinstance(data, (list, tuple)):
    raise TypeError(
        "Expected list for 'data' argument to "
        "'parallel_dynamic_stitch' Op, not %r." % data)
  if len(data) != _attr_N:
    raise ValueError(
        "List argument 'data' to 'parallel_dynamic_stitch' Op with length %d "
        "must match length %d of argument 'indices'." %
        (len(data), _attr_N))
  _attr_T, data = _execute.args_to_matching_eager(list(data), _ctx)
  indices = _ops.convert_n_to_tensor(indices, _dtypes.int32)
  _inputs_flat = list(indices) + list(data)
  _attrs = ("N", _attr_N, "T", _attr_T)
  _result = _execute.execute(b"ParallelDynamicStitch", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ParallelDynamicStitch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def priority_queue(shapes, component_types=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements sorted by the first component value.

  Note that the PriorityQueue requires the first component of any element
  to be a scalar int64, in addition to the other elements declared by
  component_types.  Therefore calls to Enqueue and EnqueueMany (resp. Dequeue
  and DequeueMany) on a PriorityQueue will all require (resp. output) one extra
  entry in their input (resp. output) lists.

  Args:
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    component_types: An optional list of `tf.DTypes`. Defaults to `[]`.
      The type of each component in a value.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("priority_queue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'priority_queue' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if component_types is None:
    component_types = []
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'priority_queue' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PriorityQueue", shapes=shapes, component_types=component_types,
                         capacity=capacity, container=container,
                         shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "PriorityQueue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def priority_queue_eager_fallback(shapes, component_types=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("priority_queue op does not support eager execution. Arg 'handle' is a ref.")

def priority_queue_v2(shapes, component_types=[], capacity=-1, container="", shared_name="", name=None):
  r"""A queue that produces elements sorted by the first component value.

  Note that the PriorityQueue requires the first component of any element
  to be a scalar int64, in addition to the other elements declared by
  component_types.  Therefore calls to Enqueue and EnqueueMany (resp. Dequeue
  and DequeueMany) on a PriorityQueue will all require (resp. output) one extra
  entry in their input (resp. output) lists.

  Args:
    shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    component_types: An optional list of `tf.DTypes`. Defaults to `[]`.
      The type of each component in a value.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PriorityQueueV2", name, _ctx._post_execution_callbacks,
        "component_types", component_types, "shapes", shapes, "capacity",
        capacity, "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return priority_queue_v2_eager_fallback(
            component_types=component_types, shapes=shapes, capacity=capacity,
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
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'priority_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if component_types is None:
    component_types = []
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'priority_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PriorityQueueV2", shapes=shapes, component_types=component_types,
                           capacity=capacity, container=container,
                           shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "PriorityQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def priority_queue_v2_eager_fallback(shapes, component_types=[], capacity=-1, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function priority_queue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'priority_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if component_types is None:
    component_types = []
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'priority_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("component_types", component_types, "shapes", shapes, "capacity",
  capacity, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"PriorityQueueV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PriorityQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def queue_close(handle, cancel_pending_enqueues=False, name=None):
  r"""Closes the given queue.

  This operation signals that no more elements will be enqueued in the
  given queue. Subsequent Enqueue(Many) operations will fail.
  Subsequent Dequeue(Many) operations will continue to succeed if
  sufficient elements remain in the queue. Subsequent Dequeue(Many)
  operations that would block will fail immediately.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    cancel_pending_enqueues: An optional `bool`. Defaults to `False`.
      If true, all pending enqueue requests that are
      blocked on the given queue will be canceled.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_close op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if cancel_pending_enqueues is None:
    cancel_pending_enqueues = False
  cancel_pending_enqueues = _execute.make_bool(cancel_pending_enqueues, "cancel_pending_enqueues")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueClose", handle=handle,
                      cancel_pending_enqueues=cancel_pending_enqueues,
                      name=name)
  return _op
  _result = None
  return _result



def queue_close_eager_fallback(handle, cancel_pending_enqueues=False, name=None, ctx=None):
  raise RuntimeError("queue_close op does not support eager execution. Arg 'handle' is a ref.")

def queue_close_v2(handle, cancel_pending_enqueues=False, name=None):
  r"""Closes the given queue.

  This operation signals that no more elements will be enqueued in the
  given queue. Subsequent Enqueue(Many) operations will fail.
  Subsequent Dequeue(Many) operations will continue to succeed if
  sufficient elements remain in the queue. Subsequent Dequeue(Many)
  operations that would block will fail immediately.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    cancel_pending_enqueues: An optional `bool`. Defaults to `False`.
      If true, all pending enqueue requests that are
      blocked on the given queue will be canceled.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "QueueCloseV2",
        name, _ctx._post_execution_callbacks, handle,
        "cancel_pending_enqueues", cancel_pending_enqueues)
      return _result
    except _core._FallbackException:
      try:
        return queue_close_v2_eager_fallback(
            handle, cancel_pending_enqueues=cancel_pending_enqueues,
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
  if cancel_pending_enqueues is None:
    cancel_pending_enqueues = False
  cancel_pending_enqueues = _execute.make_bool(cancel_pending_enqueues, "cancel_pending_enqueues")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueCloseV2", handle=handle,
                        cancel_pending_enqueues=cancel_pending_enqueues,
                        name=name)
  return _op
  _result = None
  return _result



def queue_close_v2_eager_fallback(handle, cancel_pending_enqueues=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_close_v2
  """
  _ctx = ctx if ctx else _context.context()
  if cancel_pending_enqueues is None:
    cancel_pending_enqueues = False
  cancel_pending_enqueues = _execute.make_bool(cancel_pending_enqueues, "cancel_pending_enqueues")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = ("cancel_pending_enqueues", cancel_pending_enqueues)
  _result = _execute.execute(b"QueueCloseV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def queue_dequeue(handle, component_types, timeout_ms=-1, name=None):
  r"""Dequeues a tuple of one or more tensors from the given queue.

  This operation has k outputs, where k is the number of components
  in the tuples stored in the given queue, and output i is the ith
  component of the dequeued tuple.

  N.B. If the queue is empty, this operation will block until an element
  has been dequeued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is empty, this operation will block for up to
      timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_dequeue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeue", handle=handle, component_types=component_types,
                        timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeue", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_eager_fallback(handle, component_types, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("queue_dequeue op does not support eager execution. Arg 'handle' is a ref.")

def queue_dequeue_many(handle, n, component_types, timeout_ms=-1, name=None):
  r"""Dequeues `n` tuples of one or more tensors from the given queue.

  If the queue is closed and there are fewer than `n` elements, then an
  OutOfRange error is returned.

  This operation concatenates queue-element component tensors along the
  0th dimension to make a single component tensor.  All of the components
  in the dequeued tuple will have size `n` in the 0th dimension.

  This operation has `k` outputs, where `k` is the number of components in
  the tuples stored in the given queue, and output `i` is the ith
  component of the dequeued tuple.

  N.B. If the queue is empty, this operation will block until `n` elements
  have been dequeued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    n: A `Tensor` of type `int32`. The number of tuples to dequeue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue has fewer than n elements, this operation
      will block for up to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_dequeue_many op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_many' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeueMany", handle=handle, n=n,
                            component_types=component_types,
                            timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeueMany", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_many_eager_fallback(handle, n, component_types, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("queue_dequeue_many op does not support eager execution. Arg 'handle' is a ref.")

def queue_dequeue_many_v2(handle, n, component_types, timeout_ms=-1, name=None):
  r"""Dequeues `n` tuples of one or more tensors from the given queue.

  If the queue is closed and there are fewer than `n` elements, then an
  OutOfRange error is returned.

  This operation concatenates queue-element component tensors along the
  0th dimension to make a single component tensor.  All of the components
  in the dequeued tuple will have size `n` in the 0th dimension.

  This operation has `k` outputs, where `k` is the number of components in
  the tuples stored in the given queue, and output `i` is the ith
  component of the dequeued tuple.

  N.B. If the queue is empty, this operation will block until `n` elements
  have been dequeued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    n: A `Tensor` of type `int32`. The number of tuples to dequeue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue has fewer than n elements, this operation
      will block for up to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueDequeueManyV2", name, _ctx._post_execution_callbacks, handle, n,
        "component_types", component_types, "timeout_ms", timeout_ms)
      return _result
    except _core._FallbackException:
      try:
        return queue_dequeue_many_v2_eager_fallback(
            handle, n, component_types=component_types, timeout_ms=timeout_ms,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_many_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeueManyV2", handle=handle, n=n,
                              component_types=component_types,
                              timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeueManyV2", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_many_v2_eager_fallback(handle, n, component_types, timeout_ms=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_dequeue_many_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_many_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  n = _ops.convert_to_tensor(n, _dtypes.int32)
  _inputs_flat = [handle, n]
  _attrs = ("component_types", component_types, "timeout_ms", timeout_ms)
  _result = _execute.execute(b"QueueDequeueManyV2", len(component_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QueueDequeueManyV2", _inputs_flat, _attrs, _result, name)
  return _result


def queue_dequeue_up_to(handle, n, component_types, timeout_ms=-1, name=None):
  r"""Dequeues `n` tuples of one or more tensors from the given queue.

  This operation is not supported by all queues.  If a queue does not support
  DequeueUpTo, then an Unimplemented error is returned.

  If the queue is closed and there are more than 0 but less than `n`
  elements remaining, then instead of returning an OutOfRange error like
  QueueDequeueMany, less than `n` elements are returned immediately.  If
  the queue is closed and there are 0 elements left in the queue, then
  an OutOfRange error is returned just like in QueueDequeueMany.
  Otherwise the behavior is identical to QueueDequeueMany:

  This operation concatenates queue-element component tensors along the
  0th dimension to make a single component tensor.  All of the components
  in the dequeued tuple will have size `n` in the 0th dimension.

  This operation has k outputs, where `k` is the number of components in
  the tuples stored in the given queue, and output `i` is the ith
  component of the dequeued tuple.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    n: A `Tensor` of type `int32`. The number of tuples to dequeue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue has fewer than n elements, this operation
      will block for up to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_dequeue_up_to op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_up_to' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeueUpTo", handle=handle, n=n,
                            component_types=component_types,
                            timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeueUpTo", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_up_to_eager_fallback(handle, n, component_types, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("queue_dequeue_up_to op does not support eager execution. Arg 'handle' is a ref.")

def queue_dequeue_up_to_v2(handle, n, component_types, timeout_ms=-1, name=None):
  r"""Dequeues `n` tuples of one or more tensors from the given queue.

  This operation is not supported by all queues.  If a queue does not support
  DequeueUpTo, then an Unimplemented error is returned.

  If the queue is closed and there are more than 0 but less than `n`
  elements remaining, then instead of returning an OutOfRange error like
  QueueDequeueMany, less than `n` elements are returned immediately.  If
  the queue is closed and there are 0 elements left in the queue, then
  an OutOfRange error is returned just like in QueueDequeueMany.
  Otherwise the behavior is identical to QueueDequeueMany:

  This operation concatenates queue-element component tensors along the
  0th dimension to make a single component tensor.  All of the components
  in the dequeued tuple will have size n in the 0th dimension.

  This operation has `k` outputs, where `k` is the number of components in
  the tuples stored in the given queue, and output `i` is the ith
  component of the dequeued tuple.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    n: A `Tensor` of type `int32`. The number of tuples to dequeue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue has fewer than n elements, this operation
      will block for up to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueDequeueUpToV2", name, _ctx._post_execution_callbacks, handle, n,
        "component_types", component_types, "timeout_ms", timeout_ms)
      return _result
    except _core._FallbackException:
      try:
        return queue_dequeue_up_to_v2_eager_fallback(
            handle, n, component_types=component_types, timeout_ms=timeout_ms,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_up_to_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeueUpToV2", handle=handle, n=n,
                              component_types=component_types,
                              timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeueUpToV2", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_up_to_v2_eager_fallback(handle, n, component_types, timeout_ms=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_dequeue_up_to_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_up_to_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  n = _ops.convert_to_tensor(n, _dtypes.int32)
  _inputs_flat = [handle, n]
  _attrs = ("component_types", component_types, "timeout_ms", timeout_ms)
  _result = _execute.execute(b"QueueDequeueUpToV2", len(component_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QueueDequeueUpToV2", _inputs_flat, _attrs, _result, name)
  return _result


def queue_dequeue_v2(handle, component_types, timeout_ms=-1, name=None):
  r"""Dequeues a tuple of one or more tensors from the given queue.

  This operation has k outputs, where k is the number of components
  in the tuples stored in the given queue, and output i is the ith
  component of the dequeued tuple.

  N.B. If the queue is empty, this operation will block until an element
  has been dequeued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a tuple.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is empty, this operation will block for up to
      timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `component_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueDequeueV2", name, _ctx._post_execution_callbacks, handle,
        "component_types", component_types, "timeout_ms", timeout_ms)
      return _result
    except _core._FallbackException:
      try:
        return queue_dequeue_v2_eager_fallback(
            handle, component_types=component_types, timeout_ms=timeout_ms,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueDequeueV2", handle=handle, component_types=component_types,
                          timeout_ms=timeout_ms, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "timeout_ms",
            _op.get_attr("timeout_ms"))
  _execute.record_gradient(
      "QueueDequeueV2", _inputs_flat, _attrs, _result, name)
  return _result



def queue_dequeue_v2_eager_fallback(handle, component_types, timeout_ms=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_dequeue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'queue_dequeue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = ("component_types", component_types, "timeout_ms", timeout_ms)
  _result = _execute.execute(b"QueueDequeueV2", len(component_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QueueDequeueV2", _inputs_flat, _attrs, _result, name)
  return _result


def queue_enqueue(handle, components, timeout_ms=-1, name=None):
  r"""Enqueues a tuple of one or more tensors in the given queue.

  The components input has k elements, which correspond to the components of
  tuples stored in the given queue.

  N.B. If the queue is full, this operation will block until the given
  element has been enqueued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    components: A list of `Tensor` objects.
      One or more tensors from which the enqueued tensors should be taken.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is full, this operation will block for up to
      timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_enqueue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueEnqueue", handle=handle, components=components,
                        timeout_ms=timeout_ms, name=name)
  return _op
  _result = None
  return _result



def queue_enqueue_eager_fallback(handle, components, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("queue_enqueue op does not support eager execution. Arg 'handle' is a ref.")

def queue_enqueue_many(handle, components, timeout_ms=-1, name=None):
  r"""Enqueues zero or more tuples of one or more tensors in the given queue.

  This operation slices each component tensor along the 0th dimension to
  make multiple queue elements. All of the tuple components must have the
  same size in the 0th dimension.

  The components input has k elements, which correspond to the components of
  tuples stored in the given queue.

  N.B. If the queue is full, this operation will block until the given
  elements have been enqueued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    components: A list of `Tensor` objects.
      One or more tensors from which the enqueued tensors should
      be taken.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is too full, this operation will block for up
      to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_enqueue_many op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueEnqueueMany", handle=handle, components=components,
                            timeout_ms=timeout_ms, name=name)
  return _op
  _result = None
  return _result



def queue_enqueue_many_eager_fallback(handle, components, timeout_ms=-1, name=None, ctx=None):
  raise RuntimeError("queue_enqueue_many op does not support eager execution. Arg 'handle' is a ref.")

def queue_enqueue_many_v2(handle, components, timeout_ms=-1, name=None):
  r"""Enqueues zero or more tuples of one or more tensors in the given queue.

  This operation slices each component tensor along the 0th dimension to
  make multiple queue elements. All of the tuple components must have the
  same size in the 0th dimension.

  The components input has k elements, which correspond to the components of
  tuples stored in the given queue.

  N.B. If the queue is full, this operation will block until the given
  elements have been enqueued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    components: A list of `Tensor` objects.
      One or more tensors from which the enqueued tensors should
      be taken.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is too full, this operation will block for up
      to timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueEnqueueManyV2", name, _ctx._post_execution_callbacks, handle,
        components, "timeout_ms", timeout_ms)
      return _result
    except _core._FallbackException:
      try:
        return queue_enqueue_many_v2_eager_fallback(
            handle, components, timeout_ms=timeout_ms, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueEnqueueManyV2", handle=handle, components=components,
                              timeout_ms=timeout_ms, name=name)
  return _op
  _result = None
  return _result



def queue_enqueue_many_v2_eager_fallback(handle, components, timeout_ms=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_enqueue_many_v2
  """
  _ctx = ctx if ctx else _context.context()
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _attr_Tcomponents, components = _execute.convert_to_mixed_eager_tensors(components, _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle] + list(components)
  _attrs = ("Tcomponents", _attr_Tcomponents, "timeout_ms", timeout_ms)
  _result = _execute.execute(b"QueueEnqueueManyV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def queue_enqueue_v2(handle, components, timeout_ms=-1, name=None):
  r"""Enqueues a tuple of one or more tensors in the given queue.

  The components input has k elements, which correspond to the components of
  tuples stored in the given queue.

  N.B. If the queue is full, this operation will block until the given
  element has been enqueued (or 'timeout_ms' elapses, if specified).

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    components: A list of `Tensor` objects.
      One or more tensors from which the enqueued tensors should be taken.
    timeout_ms: An optional `int`. Defaults to `-1`.
      If the queue is full, this operation will block for up to
      timeout_ms milliseconds.
      Note: This option is not supported yet.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueEnqueueV2", name, _ctx._post_execution_callbacks, handle,
        components, "timeout_ms", timeout_ms)
      return _result
    except _core._FallbackException:
      try:
        return queue_enqueue_v2_eager_fallback(
            handle, components, timeout_ms=timeout_ms, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueEnqueueV2", handle=handle, components=components,
                          timeout_ms=timeout_ms, name=name)
  return _op
  _result = None
  return _result



def queue_enqueue_v2_eager_fallback(handle, components, timeout_ms=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_enqueue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if timeout_ms is None:
    timeout_ms = -1
  timeout_ms = _execute.make_int(timeout_ms, "timeout_ms")
  _attr_Tcomponents, components = _execute.convert_to_mixed_eager_tensors(components, _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle] + list(components)
  _attrs = ("Tcomponents", _attr_Tcomponents, "timeout_ms", timeout_ms)
  _result = _execute.execute(b"QueueEnqueueV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def queue_is_closed(handle, name=None):
  r"""Returns true if queue is closed.

  This operation returns true if the queue is closed and false if the queue
  is open.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_is_closed op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueIsClosed", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QueueIsClosed", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def queue_is_closed_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("queue_is_closed op does not support eager execution. Arg 'handle' is a ref.")

def queue_is_closed_v2(handle, name=None):
  r"""Returns true if queue is closed.

  This operation returns true if the queue is closed and false if the queue
  is open.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QueueIsClosedV2", name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return queue_is_closed_v2_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "QueueIsClosedV2", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QueueIsClosedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def queue_is_closed_v2_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_is_closed_v2
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"QueueIsClosedV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QueueIsClosedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def queue_size(handle, name=None):
  r"""Computes the number of elements in the given queue.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a queue.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("queue_size op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "QueueSize", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QueueSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def queue_size_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("queue_size op does not support eager execution. Arg 'handle' is a ref.")

def queue_size_v2(handle, name=None):
  r"""Computes the number of elements in the given queue.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a queue.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "QueueSizeV2",
        name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return queue_size_v2_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "QueueSizeV2", handle=handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "QueueSizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def queue_size_v2_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function queue_size_v2
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"QueueSizeV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "QueueSizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_shuffle_queue(component_types, shapes=[], capacity=-1, min_after_dequeue=0, seed=0, seed2=0, container="", shared_name="", name=None):
  r"""A queue that randomizes the order of elements.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    min_after_dequeue: An optional `int`. Defaults to `0`.
      Dequeue will block unless there would be this
      many elements after the dequeue or the queue is closed. This
      ensures a minimum level of mixing of elements.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 is set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, a random seed is used.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("random_shuffle_queue op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'random_shuffle_queue' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'random_shuffle_queue' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if min_after_dequeue is None:
    min_after_dequeue = 0
  min_after_dequeue = _execute.make_int(min_after_dequeue, "min_after_dequeue")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomShuffleQueue", component_types=component_types, shapes=shapes,
                              capacity=capacity,
                              min_after_dequeue=min_after_dequeue, seed=seed,
                              seed2=seed2, container=container,
                              shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "min_after_dequeue", _op.get_attr("min_after_dequeue"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "RandomShuffleQueue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_shuffle_queue_eager_fallback(component_types, shapes=[], capacity=-1, min_after_dequeue=0, seed=0, seed2=0, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("random_shuffle_queue op does not support eager execution. Arg 'handle' is a ref.")

def random_shuffle_queue_v2(component_types, shapes=[], capacity=-1, min_after_dequeue=0, seed=0, seed2=0, container="", shared_name="", name=None):
  r"""A queue that randomizes the order of elements.

  Args:
    component_types: A list of `tf.DTypes` that has length `>= 1`.
      The type of each component in a value.
    shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The shape of each component in a value. The length of this attr must
      be either 0 or the same as the length of component_types. If the length of
      this attr is 0, the shapes of queue elements are not constrained, and
      only one element may be dequeued at a time.
    capacity: An optional `int`. Defaults to `-1`.
      The upper bound on the number of elements in this queue.
      Negative numbers mean no limit.
    min_after_dequeue: An optional `int`. Defaults to `0`.
      Dequeue will block unless there would be this
      many elements after the dequeue or the queue is closed. This
      ensures a minimum level of mixing of elements.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 is set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, a random seed is used.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this queue will be shared under the given name
      across multiple sessions.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomShuffleQueueV2", name, _ctx._post_execution_callbacks,
        "component_types", component_types, "shapes", shapes, "capacity",
        capacity, "min_after_dequeue", min_after_dequeue, "seed", seed,
        "seed2", seed2, "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return random_shuffle_queue_v2_eager_fallback(
            component_types=component_types, shapes=shapes, capacity=capacity,
            min_after_dequeue=min_after_dequeue, seed=seed, seed2=seed2,
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
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'random_shuffle_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'random_shuffle_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if min_after_dequeue is None:
    min_after_dequeue = 0
  min_after_dequeue = _execute.make_int(min_after_dequeue, "min_after_dequeue")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomShuffleQueueV2", component_types=component_types,
                                shapes=shapes, capacity=capacity,
                                min_after_dequeue=min_after_dequeue,
                                seed=seed, seed2=seed2, container=container,
                                shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("component_types", _op.get_attr("component_types"), "shapes",
            _op.get_attr("shapes"), "capacity", _op.get_attr("capacity"),
            "min_after_dequeue", _op.get_attr("min_after_dequeue"), "seed",
            _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "RandomShuffleQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_shuffle_queue_v2_eager_fallback(component_types, shapes=[], capacity=-1, min_after_dequeue=0, seed=0, seed2=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_shuffle_queue_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(component_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'component_types' argument to "
        "'random_shuffle_queue_v2' Op, not %r." % component_types)
  component_types = [_execute.make_type(_t, "component_types") for _t in component_types]
  if shapes is None:
    shapes = []
  if not isinstance(shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'shapes' argument to "
        "'random_shuffle_queue_v2' Op, not %r." % shapes)
  shapes = [_execute.make_shape(_s, "shapes") for _s in shapes]
  if capacity is None:
    capacity = -1
  capacity = _execute.make_int(capacity, "capacity")
  if min_after_dequeue is None:
    min_after_dequeue = 0
  min_after_dequeue = _execute.make_int(min_after_dequeue, "min_after_dequeue")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("component_types", component_types, "shapes", shapes, "capacity",
  capacity, "min_after_dequeue", min_after_dequeue, "seed", seed, "seed2",
  seed2, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"RandomShuffleQueueV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomShuffleQueueV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def record_input(file_pattern, file_random_seed=301, file_shuffle_shift_ratio=0, file_buffer_size=10000, file_parallelism=16, batch_size=32, compression_type="", name=None):
  r"""Emits randomized records.

  Args:
    file_pattern: A `string`. Glob pattern for the data files.
    file_random_seed: An optional `int`. Defaults to `301`.
      Random seeds used to produce randomized records.
    file_shuffle_shift_ratio: An optional `float`. Defaults to `0`.
      Shifts the list of files after the list is randomly
      shuffled.
    file_buffer_size: An optional `int`. Defaults to `10000`.
      The randomization shuffling buffer.
    file_parallelism: An optional `int`. Defaults to `16`.
      How many sstables are opened and concurrently iterated over.
    batch_size: An optional `int`. Defaults to `32`. The batch size.
    compression_type: An optional `string`. Defaults to `""`.
      The type of compression for the file. Currently ZLIB and
      GZIP are supported. Defaults to none.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RecordInput",
        name, _ctx._post_execution_callbacks, "file_pattern", file_pattern,
        "file_random_seed", file_random_seed, "file_shuffle_shift_ratio",
        file_shuffle_shift_ratio, "file_buffer_size", file_buffer_size,
        "file_parallelism", file_parallelism, "batch_size", batch_size,
        "compression_type", compression_type)
      return _result
    except _core._FallbackException:
      try:
        return record_input_eager_fallback(
            file_pattern=file_pattern, file_random_seed=file_random_seed,
            file_shuffle_shift_ratio=file_shuffle_shift_ratio,
            file_buffer_size=file_buffer_size,
            file_parallelism=file_parallelism, batch_size=batch_size,
            compression_type=compression_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  file_pattern = _execute.make_str(file_pattern, "file_pattern")
  if file_random_seed is None:
    file_random_seed = 301
  file_random_seed = _execute.make_int(file_random_seed, "file_random_seed")
  if file_shuffle_shift_ratio is None:
    file_shuffle_shift_ratio = 0
  file_shuffle_shift_ratio = _execute.make_float(file_shuffle_shift_ratio, "file_shuffle_shift_ratio")
  if file_buffer_size is None:
    file_buffer_size = 10000
  file_buffer_size = _execute.make_int(file_buffer_size, "file_buffer_size")
  if file_parallelism is None:
    file_parallelism = 16
  file_parallelism = _execute.make_int(file_parallelism, "file_parallelism")
  if batch_size is None:
    batch_size = 32
  batch_size = _execute.make_int(batch_size, "batch_size")
  if compression_type is None:
    compression_type = ""
  compression_type = _execute.make_str(compression_type, "compression_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RecordInput", file_pattern=file_pattern,
                       file_random_seed=file_random_seed,
                       file_shuffle_shift_ratio=file_shuffle_shift_ratio,
                       file_buffer_size=file_buffer_size,
                       file_parallelism=file_parallelism,
                       batch_size=batch_size,
                       compression_type=compression_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("file_pattern", _op.get_attr("file_pattern"), "file_random_seed",
            _op.get_attr("file_random_seed"), "file_shuffle_shift_ratio",
            _op.get_attr("file_shuffle_shift_ratio"), "file_buffer_size",
            _op.get_attr("file_buffer_size"), "file_parallelism",
            _op.get_attr("file_parallelism"), "batch_size",
            _op.get_attr("batch_size"), "compression_type",
            _op.get_attr("compression_type"))
  _execute.record_gradient(
      "RecordInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def record_input_eager_fallback(file_pattern, file_random_seed=301, file_shuffle_shift_ratio=0, file_buffer_size=10000, file_parallelism=16, batch_size=32, compression_type="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function record_input
  """
  _ctx = ctx if ctx else _context.context()
  file_pattern = _execute.make_str(file_pattern, "file_pattern")
  if file_random_seed is None:
    file_random_seed = 301
  file_random_seed = _execute.make_int(file_random_seed, "file_random_seed")
  if file_shuffle_shift_ratio is None:
    file_shuffle_shift_ratio = 0
  file_shuffle_shift_ratio = _execute.make_float(file_shuffle_shift_ratio, "file_shuffle_shift_ratio")
  if file_buffer_size is None:
    file_buffer_size = 10000
  file_buffer_size = _execute.make_int(file_buffer_size, "file_buffer_size")
  if file_parallelism is None:
    file_parallelism = 16
  file_parallelism = _execute.make_int(file_parallelism, "file_parallelism")
  if batch_size is None:
    batch_size = 32
  batch_size = _execute.make_int(batch_size, "batch_size")
  if compression_type is None:
    compression_type = ""
  compression_type = _execute.make_str(compression_type, "compression_type")
  _inputs_flat = []
  _attrs = ("file_pattern", file_pattern, "file_random_seed",
  file_random_seed, "file_shuffle_shift_ratio", file_shuffle_shift_ratio,
  "file_buffer_size", file_buffer_size, "file_parallelism", file_parallelism,
  "batch_size", batch_size, "compression_type", compression_type)
  _result = _execute.execute(b"RecordInput", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RecordInput", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_accumulator_apply_gradient(handle, local_step, gradient_indices, gradient_values, gradient_shape, has_known_shape, name=None):
  r"""Applies a sparse gradient to a given accumulator.

  Does not add if local_step is smaller than the accumulator's
  global_step.

  Args:
    handle: A `Tensor` of type mutable `string`. The handle to a accumulator.
    local_step: A `Tensor` of type `int64`.
      The local_step value at which the sparse gradient was computed.
    gradient_indices: A `Tensor` of type `int64`.
      Indices of the sparse gradient to be accumulated. Must be a
      vector.
    gradient_values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Values are the non-zero slices of the gradient, and must have
      the same first dimension as indices, i.e., the nnz represented by indices and
      values must be consistent.
    gradient_shape: A `Tensor` of type `int64`.
      Shape of the sparse gradient to be accumulated.
    has_known_shape: A `bool`.
      Boolean indicating whether gradient_shape is unknown, in which
      case the input is ignored during validation.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_accumulator_apply_gradient op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  has_known_shape = _execute.make_bool(has_known_shape, "has_known_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseAccumulatorApplyGradient", handle=handle,
                                          local_step=local_step,
                                          gradient_indices=gradient_indices,
                                          gradient_values=gradient_values,
                                          gradient_shape=gradient_shape,
                                          has_known_shape=has_known_shape,
                                          name=name)
  return _op
  _result = None
  return _result



def sparse_accumulator_apply_gradient_eager_fallback(handle, local_step, gradient_indices, gradient_values, gradient_shape, has_known_shape, name=None, ctx=None):
  raise RuntimeError("sparse_accumulator_apply_gradient op does not support eager execution. Arg 'handle' is a ref.")

_sparse_accumulator_take_gradient_outputs = ["indices", "values", "shape"]
_SparseAccumulatorTakeGradientOutput = _collections.namedtuple(
    "SparseAccumulatorTakeGradient",
    _sparse_accumulator_take_gradient_outputs)


def sparse_accumulator_take_gradient(handle, num_required, dtype, name=None):
  r"""Extracts the average sparse gradient in a SparseConditionalAccumulator.

  The op will blocks until sufficient (i.e., more than num_required)
  gradients have been accumulated. If the accumulator has already
  aggregated more than num_required gradients, it will return its
  average of the accumulated gradients.  Also automatically increments
  the recorded global_step in the accumulator by 1, and resets the
  aggregate to 0.

  Args:
    handle: A `Tensor` of type mutable `string`.
      The handle to a SparseConditionalAccumulator.
    num_required: A `Tensor` of type `int32`.
      Number of gradients required before we return an aggregate.
    dtype: A `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.uint8, tf.int16, tf.int8, tf.complex64, tf.int64, tf.qint8, tf.quint8, tf.qint32, tf.bfloat16, tf.uint16, tf.complex128, tf.half, tf.uint32, tf.uint64`.
      The data type of accumulated gradients. Needs to correspond to the type
      of the accumulator.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (indices, values, shape).

    indices: A `Tensor` of type `int64`.
    values: A `Tensor` of type `dtype`.
    shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_accumulator_take_gradient op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseAccumulatorTakeGradient", handle=handle,
                                         num_required=num_required,
                                         dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "SparseAccumulatorTakeGradient", _inputs_flat, _attrs, _result, name)
  _result = _SparseAccumulatorTakeGradientOutput._make(_result)
  return _result



def sparse_accumulator_take_gradient_eager_fallback(handle, num_required, dtype, name=None, ctx=None):
  raise RuntimeError("sparse_accumulator_take_gradient op does not support eager execution. Arg 'handle' is a ref.")

def sparse_conditional_accumulator(dtype, shape, container="", shared_name="", reduction_type="MEAN", name=None):
  r"""A conditional accumulator for aggregating sparse gradients.

  The accumulator accepts gradients marked with local_step greater or
  equal to the most recent global_step known to the accumulator. The
  average can be extracted from the accumulator, provided sufficient
  gradients have been accumulated. Extracting the average automatically
  resets the aggregate to 0, and increments the global_step recorded by
  the accumulator.

  Args:
    dtype: A `tf.DType` from: `tf.float32, tf.float64, tf.int32, tf.uint8, tf.int16, tf.int8, tf.complex64, tf.int64, tf.qint8, tf.quint8, tf.qint32, tf.bfloat16, tf.uint16, tf.complex128, tf.half, tf.uint32, tf.uint64`.
      The type of the value being accumulated.
    shape: A `tf.TensorShape` or list of `ints`. The shape of the values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this accumulator is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this accumulator will be shared under the given name
      across multiple sessions.
    reduction_type: An optional `string` from: `"MEAN", "SUM"`. Defaults to `"MEAN"`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_conditional_accumulator op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if reduction_type is None:
    reduction_type = "MEAN"
  reduction_type = _execute.make_str(reduction_type, "reduction_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseConditionalAccumulator", dtype=dtype, shape=shape,
                                        container=container,
                                        shared_name=shared_name,
                                        reduction_type=reduction_type,
                                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "reduction_type",
            _op.get_attr("reduction_type"))
  _execute.record_gradient(
      "SparseConditionalAccumulator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_conditional_accumulator_eager_fallback(dtype, shape, container="", shared_name="", reduction_type="MEAN", name=None, ctx=None):
  raise RuntimeError("sparse_conditional_accumulator op does not support eager execution. Arg 'handle' is a ref.")

def _stack(elem_type, stack_name="", name=None):
  r"""Deprecated, use StackV2.

  Args:
    elem_type: A `tf.DType`.
    stack_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("stack op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  elem_type = _execute.make_type(elem_type, "elem_type")
  if stack_name is None:
    stack_name = ""
  stack_name = _execute.make_str(stack_name, "stack_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Stack", elem_type=elem_type, stack_name=stack_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("elem_type", _op.get_attr("elem_type"), "stack_name",
            _op.get_attr("stack_name"))
  _execute.record_gradient(
      "Stack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _stack_eager_fallback(elem_type, stack_name="", name=None, ctx=None):
  raise RuntimeError("stack op does not support eager execution. Arg 'handle' is a ref.")

def stack_close(handle, name=None):
  r"""Deprecated, use StackCloseV2.

  Args:
    handle: A `Tensor` of type mutable `string`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("stack_close op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackClose", handle=handle, name=name)
  return _op
  _result = None
  return _result



def stack_close_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("stack_close op does not support eager execution. Arg 'handle' is a ref.")

def stack_close_v2(handle, name=None):
  r"""Delete the stack from its resource container.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a stack.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StackCloseV2",
        name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return stack_close_v2_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "StackCloseV2", handle=handle, name=name)
  return _op
  _result = None
  return _result



def stack_close_v2_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stack_close_v2
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"StackCloseV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def stack_pop(handle, elem_type, name=None):
  r"""Deprecated, use StackPopV2.

  Args:
    handle: A `Tensor` of type mutable `string`.
    elem_type: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `elem_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("stack_pop op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  elem_type = _execute.make_type(elem_type, "elem_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackPop", handle=handle, elem_type=elem_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("elem_type", _op.get_attr("elem_type"))
  _execute.record_gradient(
      "StackPop", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stack_pop_eager_fallback(handle, elem_type, name=None, ctx=None):
  raise RuntimeError("stack_pop op does not support eager execution. Arg 'handle' is a ref.")

def stack_pop_v2(handle, elem_type, name=None):
  r"""Pop the element at the top of the stack.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a stack.
    elem_type: A `tf.DType`. The type of the elem that is popped.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `elem_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StackPopV2",
        name, _ctx._post_execution_callbacks, handle, "elem_type", elem_type)
      return _result
    except _core._FallbackException:
      try:
        return stack_pop_v2_eager_fallback(
            handle, elem_type=elem_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  elem_type = _execute.make_type(elem_type, "elem_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackPopV2", handle=handle, elem_type=elem_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("elem_type", _op.get_attr("elem_type"))
  _execute.record_gradient(
      "StackPopV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stack_pop_v2_eager_fallback(handle, elem_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stack_pop_v2
  """
  _ctx = ctx if ctx else _context.context()
  elem_type = _execute.make_type(elem_type, "elem_type")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = ("elem_type", elem_type)
  _result = _execute.execute(b"StackPopV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StackPopV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stack_push(handle, elem, swap_memory=False, name=None):
  r"""Deprecated, use StackPushV2.

  Args:
    handle: A `Tensor` of type mutable `string`.
    elem: A `Tensor`.
    swap_memory: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `elem`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("stack_push op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if swap_memory is None:
    swap_memory = False
  swap_memory = _execute.make_bool(swap_memory, "swap_memory")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackPush", handle=handle, elem=elem, swap_memory=swap_memory,
                     name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "swap_memory",
            _op.get_attr("swap_memory"))
  _execute.record_gradient(
      "StackPush", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stack_push_eager_fallback(handle, elem, swap_memory=False, name=None, ctx=None):
  raise RuntimeError("stack_push op does not support eager execution. Arg 'handle' is a ref.")

def stack_push_v2(handle, elem, swap_memory=False, name=None):
  r"""Push an element onto the stack.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a stack.
    elem: A `Tensor`. The tensor to be pushed onto the stack.
    swap_memory: An optional `bool`. Defaults to `False`.
      Swap `elem` to CPU. Default to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `elem`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StackPushV2",
        name, _ctx._post_execution_callbacks, handle, elem, "swap_memory",
        swap_memory)
      return _result
    except _core._FallbackException:
      try:
        return stack_push_v2_eager_fallback(
            handle, elem, swap_memory=swap_memory, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if swap_memory is None:
    swap_memory = False
  swap_memory = _execute.make_bool(swap_memory, "swap_memory")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackPushV2", handle=handle, elem=elem, swap_memory=swap_memory,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "swap_memory",
            _op.get_attr("swap_memory"))
  _execute.record_gradient(
      "StackPushV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stack_push_v2_eager_fallback(handle, elem, swap_memory=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stack_push_v2
  """
  _ctx = ctx if ctx else _context.context()
  if swap_memory is None:
    swap_memory = False
  swap_memory = _execute.make_bool(swap_memory, "swap_memory")
  _attr_T, (elem,) = _execute.args_to_matching_eager([elem], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle, elem]
  _attrs = ("T", _attr_T, "swap_memory", swap_memory)
  _result = _execute.execute(b"StackPushV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StackPushV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stack_v2(max_size, elem_type, stack_name="", name=None):
  r"""A stack that produces elements in first-in last-out order.

  Args:
    max_size: A `Tensor` of type `int32`.
      The maximum size of the stack if non-negative. If negative, the stack
      size is unlimited.
    elem_type: A `tf.DType`. The type of the elements on the stack.
    stack_name: An optional `string`. Defaults to `""`.
      Overrides the name used for the temporary stack resource. Default
      value is the name of the 'Stack' op (which is guaranteed unique).
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StackV2",
        name, _ctx._post_execution_callbacks, max_size, "elem_type",
        elem_type, "stack_name", stack_name)
      return _result
    except _core._FallbackException:
      try:
        return stack_v2_eager_fallback(
            max_size, elem_type=elem_type, stack_name=stack_name, name=name,
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
  elem_type = _execute.make_type(elem_type, "elem_type")
  if stack_name is None:
    stack_name = ""
  stack_name = _execute.make_str(stack_name, "stack_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StackV2", max_size=max_size, elem_type=elem_type,
                   stack_name=stack_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("elem_type", _op.get_attr("elem_type"), "stack_name",
            _op.get_attr("stack_name"))
  _execute.record_gradient(
      "StackV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stack_v2_eager_fallback(max_size, elem_type, stack_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stack_v2
  """
  _ctx = ctx if ctx else _context.context()
  elem_type = _execute.make_type(elem_type, "elem_type")
  if stack_name is None:
    stack_name = ""
  stack_name = _execute.make_str(stack_name, "stack_name")
  max_size = _ops.convert_to_tensor(max_size, _dtypes.int32)
  _inputs_flat = [max_size]
  _attrs = ("elem_type", elem_type, "stack_name", stack_name)
  _result = _execute.execute(b"StackV2", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "StackV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def stage(values, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Stage values similar to a lightweight Enqueue.

  The basic functionality of this Op is similar to a queue with many
  fewer capabilities and options.  This Op is optimized for performance.

  Args:
    values: A list of `Tensor` objects. a list of tensors
      dtypes A list of data types that inserted values should adhere to.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
      Maximum number of elements in the Staging Area. If > 0, inserts
      on the container will block when the capacity is reached.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
      The maximum number of bytes allowed for Tensors in the Staging Area.
      If > 0, inserts will block until sufficient space is available.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this queue is placed in the given container. Otherwise,
      a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      It is necessary to match this name to the matching Unstage Op.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Stage", name,
        _ctx._post_execution_callbacks, values, "capacity", capacity,
        "memory_limit", memory_limit, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stage_eager_fallback(
            values, capacity=capacity, memory_limit=memory_limit,
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
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Stage", values=values, capacity=capacity, memory_limit=memory_limit,
                 container=container, shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def stage_eager_fallback(values, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stage
  """
  _ctx = ctx if ctx else _context.context()
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _attr_dtypes, values = _execute.convert_to_mixed_eager_tensors(values, _ctx)
  _inputs_flat = list(values)
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  _attr_dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"Stage", 0, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _result = None
  return _result


def stage_clear(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op removes all elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StageClear",
        name, _ctx._post_execution_callbacks, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stage_clear_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StageClear", dtypes=dtypes, capacity=capacity,
                      memory_limit=memory_limit, container=container,
                      shared_name=shared_name, name=name)
  return _op
  _result = None
  return _result



def stage_clear_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stage_clear
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_clear' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"StageClear", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def stage_peek(index, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op peeks at the values at the specified index.  If the

  underlying container does not contain sufficient elements
  this op will block until it does.   This Op is optimized for
  performance.

  Args:
    index: A `Tensor` of type `int32`.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StagePeek",
        name, _ctx._post_execution_callbacks, index, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stage_peek_eager_fallback(
            index, capacity=capacity, memory_limit=memory_limit,
            dtypes=dtypes, container=container, shared_name=shared_name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StagePeek", index=index, dtypes=dtypes, capacity=capacity,
                     memory_limit=memory_limit, container=container,
                     shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "StagePeek", _inputs_flat, _attrs, _result, name)
  return _result



def stage_peek_eager_fallback(index, dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stage_peek
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_peek' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  _inputs_flat = [index]
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"StagePeek", len(dtypes), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StagePeek", _inputs_flat, _attrs, _result, name)
  return _result


def stage_size(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op returns the number of elements in the underlying container.

  Args:
    dtypes: A list of `tf.DTypes`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StageSize",
        name, _ctx._post_execution_callbacks, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return stage_size_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StageSize", dtypes=dtypes, capacity=capacity,
                     memory_limit=memory_limit, container=container,
                     shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "StageSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def stage_size_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stage_size
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'stage_size' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"StageSize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StageSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array(size, dtype, dynamic_size=False, clear_after_read=True, tensor_array_name="", element_shape=None, name=None):
  r"""TODO: add doc.

  Args:
    size: A `Tensor` of type `int32`.
    dtype: A `tf.DType`.
    dynamic_size: An optional `bool`. Defaults to `False`.
    clear_after_read: An optional `bool`. Defaults to `True`.
    tensor_array_name: An optional `string`. Defaults to `""`.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if dynamic_size is None:
    dynamic_size = False
  dynamic_size = _execute.make_bool(dynamic_size, "dynamic_size")
  if clear_after_read is None:
    clear_after_read = True
  clear_after_read = _execute.make_bool(clear_after_read, "clear_after_read")
  if tensor_array_name is None:
    tensor_array_name = ""
  tensor_array_name = _execute.make_str(tensor_array_name, "tensor_array_name")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArray", size=size, dtype=dtype, dynamic_size=dynamic_size,
                       clear_after_read=clear_after_read,
                       tensor_array_name=tensor_array_name,
                       element_shape=element_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "dynamic_size",
            _op.get_attr("dynamic_size"), "clear_after_read",
            _op.get_attr("clear_after_read"), "tensor_array_name",
            _op.get_attr("tensor_array_name"), "element_shape",
            _op.get_attr("element_shape"))
  _execute.record_gradient(
      "TensorArray", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_eager_fallback(size, dtype, dynamic_size=False, clear_after_read=True, tensor_array_name="", element_shape=None, name=None, ctx=None):
  raise RuntimeError("tensor_array op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_close(handle, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_close op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayClose", handle=handle, name=name)
  return _op
  _result = None
  return _result



def tensor_array_close_eager_fallback(handle, name=None, ctx=None):
  raise RuntimeError("tensor_array_close op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_close_v2(handle, name=None):
  r"""Deprecated. Use TensorArrayCloseV3

  Args:
    handle: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayCloseV2", name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_close_v2_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "TensorArrayCloseV2", handle=handle, name=name)
  return _op
  _result = None
  return _result



def tensor_array_close_v2_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_close_v2
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"TensorArrayCloseV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def tensor_array_close_v3(handle, name=None):
  r"""Delete the TensorArray from its resource container.

  This enables the user to close and release the resource in the middle
  of a step/run.

  Args:
    handle: A `Tensor` of type `resource`.
      The handle to a TensorArray (output of TensorArray or TensorArrayGrad).
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayCloseV3", name, _ctx._post_execution_callbacks, handle)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_close_v3_eager_fallback(
            handle, name=name, ctx=_ctx)
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
        "TensorArrayCloseV3", handle=handle, name=name)
  return _op
  _result = None
  return _result



def tensor_array_close_v3_eager_fallback(handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_close_v3
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  _inputs_flat = [handle]
  _attrs = None
  _result = _execute.execute(b"TensorArrayCloseV3", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


_tensor_array_concat_outputs = ["value", "lengths"]
_TensorArrayConcatOutput = _collections.namedtuple(
    "TensorArrayConcat", _tensor_array_concat_outputs)


def tensor_array_concat(handle, flow_in, dtype, element_shape_except0=None, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    element_shape_except0: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (value, lengths).

    value: A `Tensor` of type `dtype`.
    lengths: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_concat op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape_except0 is None:
    element_shape_except0 = None
  element_shape_except0 = _execute.make_shape(element_shape_except0, "element_shape_except0")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayConcat", handle=handle, flow_in=flow_in, dtype=dtype,
                             element_shape_except0=element_shape_except0,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape_except0",
            _op.get_attr("element_shape_except0"))
  _execute.record_gradient(
      "TensorArrayConcat", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayConcatOutput._make(_result)
  return _result



def tensor_array_concat_eager_fallback(handle, flow_in, dtype, element_shape_except0=None, name=None, ctx=None):
  raise RuntimeError("tensor_array_concat op does not support eager execution. Arg 'handle' is a ref.")

_tensor_array_concat_v2_outputs = ["value", "lengths"]
_TensorArrayConcatV2Output = _collections.namedtuple(
    "TensorArrayConcatV2", _tensor_array_concat_v2_outputs)


def tensor_array_concat_v2(handle, flow_in, dtype, element_shape_except0=None, name=None):
  r"""Deprecated. Use TensorArrayConcatV3

  Args:
    handle: A `Tensor` of type `string`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    element_shape_except0: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (value, lengths).

    value: A `Tensor` of type `dtype`.
    lengths: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayConcatV2", name, _ctx._post_execution_callbacks, handle,
        flow_in, "dtype", dtype, "element_shape_except0",
        element_shape_except0)
      _result = _TensorArrayConcatV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_concat_v2_eager_fallback(
            handle, flow_in, dtype=dtype,
            element_shape_except0=element_shape_except0, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape_except0 is None:
    element_shape_except0 = None
  element_shape_except0 = _execute.make_shape(element_shape_except0, "element_shape_except0")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayConcatV2", handle=handle, flow_in=flow_in, dtype=dtype,
                               element_shape_except0=element_shape_except0,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape_except0",
            _op.get_attr("element_shape_except0"))
  _execute.record_gradient(
      "TensorArrayConcatV2", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayConcatV2Output._make(_result)
  return _result



def tensor_array_concat_v2_eager_fallback(handle, flow_in, dtype, element_shape_except0=None, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_concat_v2
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape_except0 is None:
    element_shape_except0 = None
  element_shape_except0 = _execute.make_shape(element_shape_except0, "element_shape_except0")
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = ("dtype", dtype, "element_shape_except0", element_shape_except0)
  _result = _execute.execute(b"TensorArrayConcatV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayConcatV2", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayConcatV2Output._make(_result)
  return _result


_tensor_array_concat_v3_outputs = ["value", "lengths"]
_TensorArrayConcatV3Output = _collections.namedtuple(
    "TensorArrayConcatV3", _tensor_array_concat_v3_outputs)


def tensor_array_concat_v3(handle, flow_in, dtype, element_shape_except0=None, name=None):
  r"""Concat the elements from the TensorArray into value `value`.

  Takes `T` elements of shapes

    ```
    (n0 x d0 x d1 x ...), (n1 x d0 x d1 x ...), ..., (n(T-1) x d0 x d1 x ...)
    ```

  and concatenates them into a Tensor of shape:

    ```(n0 + n1 + ... + n(T-1) x d0 x d1 x ...)```

  All elements must have the same shape (excepting the first dimension).

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    dtype: A `tf.DType`. The type of the elem that is returned.
    element_shape_except0: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
      The expected shape of an element, if known,
      excluding the first dimension. Used to validate the shapes of
      TensorArray elements. If this shape is not fully specified, concatenating
      zero-size TensorArrays is an error.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (value, lengths).

    value: A `Tensor` of type `dtype`.
    lengths: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayConcatV3", name, _ctx._post_execution_callbacks, handle,
        flow_in, "dtype", dtype, "element_shape_except0",
        element_shape_except0)
      _result = _TensorArrayConcatV3Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_concat_v3_eager_fallback(
            handle, flow_in, dtype=dtype,
            element_shape_except0=element_shape_except0, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape_except0 is None:
    element_shape_except0 = None
  element_shape_except0 = _execute.make_shape(element_shape_except0, "element_shape_except0")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayConcatV3", handle=handle, flow_in=flow_in, dtype=dtype,
                               element_shape_except0=element_shape_except0,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape_except0",
            _op.get_attr("element_shape_except0"))
  _execute.record_gradient(
      "TensorArrayConcatV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayConcatV3Output._make(_result)
  return _result



def tensor_array_concat_v3_eager_fallback(handle, flow_in, dtype, element_shape_except0=None, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_concat_v3
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape_except0 is None:
    element_shape_except0 = None
  element_shape_except0 = _execute.make_shape(element_shape_except0, "element_shape_except0")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = ("dtype", dtype, "element_shape_except0", element_shape_except0)
  _result = _execute.execute(b"TensorArrayConcatV3", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayConcatV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayConcatV3Output._make(_result)
  return _result


def tensor_array_gather(handle, indices, flow_in, dtype, element_shape=None, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    indices: A `Tensor` of type `int32`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_gather op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGather", handle=handle, indices=indices, flow_in=flow_in,
                             dtype=dtype, element_shape=element_shape,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"))
  _execute.record_gradient(
      "TensorArrayGather", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_gather_eager_fallback(handle, indices, flow_in, dtype, element_shape=None, name=None, ctx=None):
  raise RuntimeError("tensor_array_gather op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_gather_v2(handle, indices, flow_in, dtype, element_shape=None, name=None):
  r"""Deprecated. Use TensorArrayGatherV3

  Args:
    handle: A `Tensor` of type `string`.
    indices: A `Tensor` of type `int32`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayGatherV2", name, _ctx._post_execution_callbacks, handle,
        indices, flow_in, "dtype", dtype, "element_shape", element_shape)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_gather_v2_eager_fallback(
            handle, indices, flow_in, dtype=dtype,
            element_shape=element_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGatherV2", handle=handle, indices=indices,
                               flow_in=flow_in, dtype=dtype,
                               element_shape=element_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"))
  _execute.record_gradient(
      "TensorArrayGatherV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_gather_v2_eager_fallback(handle, indices, flow_in, dtype, element_shape=None, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_gather_v2
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, indices, flow_in]
  _attrs = ("dtype", dtype, "element_shape", element_shape)
  _result = _execute.execute(b"TensorArrayGatherV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayGatherV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_gather_v3(handle, indices, flow_in, dtype, element_shape=None, name=None):
  r"""Gather specific elements from the TensorArray into output `value`.

  All elements selected by `indices` must have the same shape.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    indices: A `Tensor` of type `int32`.
      The locations in the TensorArray from which to read tensor elements.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    dtype: A `tf.DType`. The type of the elem that is returned.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
      The expected shape of an element, if known. Used to
      validate the shapes of TensorArray elements. If this shape is not
      fully specified, gathering zero-size TensorArrays is an error.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayGatherV3", name, _ctx._post_execution_callbacks, handle,
        indices, flow_in, "dtype", dtype, "element_shape", element_shape)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_gather_v3_eager_fallback(
            handle, indices, flow_in, dtype=dtype,
            element_shape=element_shape, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGatherV3", handle=handle, indices=indices,
                               flow_in=flow_in, dtype=dtype,
                               element_shape=element_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"))
  _execute.record_gradient(
      "TensorArrayGatherV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_gather_v3_eager_fallback(handle, indices, flow_in, dtype, element_shape=None, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_gather_v3
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, indices, flow_in]
  _attrs = ("dtype", dtype, "element_shape", element_shape)
  _result = _execute.execute(b"TensorArrayGatherV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayGatherV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_grad(handle, flow_in, source, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type `string`.
    flow_in: A `Tensor` of type `float32`.
    source: A `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_grad op does not support eager execution. Arg 'grad_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  source = _execute.make_str(source, "source")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGrad", handle=handle, flow_in=flow_in, source=source,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("source", _op.get_attr("source"))
  _execute.record_gradient(
      "TensorArrayGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_grad_eager_fallback(handle, flow_in, source, name=None, ctx=None):
  raise RuntimeError("tensor_array_grad op does not support eager execution. Arg 'grad_handle' is a ref.")

def tensor_array_grad_v2(handle, flow_in, source, name=None):
  r"""Deprecated. Use TensorArrayGradV3

  Args:
    handle: A `Tensor` of type `string`.
    flow_in: A `Tensor` of type `float32`.
    source: A `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayGradV2", name, _ctx._post_execution_callbacks, handle,
        flow_in, "source", source)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_grad_v2_eager_fallback(
            handle, flow_in, source=source, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  source = _execute.make_str(source, "source")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGradV2", handle=handle, flow_in=flow_in, source=source,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("source", _op.get_attr("source"))
  _execute.record_gradient(
      "TensorArrayGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_grad_v2_eager_fallback(handle, flow_in, source, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_grad_v2
  """
  _ctx = ctx if ctx else _context.context()
  source = _execute.make_str(source, "source")
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = ("source", source)
  _result = _execute.execute(b"TensorArrayGradV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayGradV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_tensor_array_grad_v3_outputs = ["grad_handle", "flow_out"]
_TensorArrayGradV3Output = _collections.namedtuple(
    "TensorArrayGradV3", _tensor_array_grad_v3_outputs)


def tensor_array_grad_v3(handle, flow_in, source, name=None):
  r"""Creates a TensorArray for storing the gradients of values in the given handle.

  If the given TensorArray gradient already exists, returns a reference to it.

  Locks the size of the original TensorArray by disabling its dynamic size flag.

  **A note about the input flow_in:**

  The handle flow_in forces the execution of the gradient lookup to occur
  only after certain other operations have occurred.  For example, when
  the forward TensorArray is dynamically sized, writes to this TensorArray
  may resize the object.  The gradient TensorArray is statically sized based
  on the size of the forward TensorArray when this operation executes.
  Furthermore, the size of the forward TensorArray is frozen by this call.
  As a result, the flow is used to ensure that the call to generate the gradient
  TensorArray only happens after all writes are executed.

  In the case of dynamically sized TensorArrays, gradient computation should
  only be performed on read operations that have themselves been chained via
  flow to occur only after all writes have executed. That way the final size
  of the forward TensorArray is known when this operation is called.

  **A note about the source attribute:**

  TensorArray gradient calls use an accumulator TensorArray object.  If
  multiple gradients are calculated and run in the same session, the multiple
  gradient nodes may accidentally flow through the same accumulator TensorArray.
  This double counts and generally breaks the TensorArray gradient flow.

  The solution is to identify which gradient call this particular
  TensorArray gradient is being called in.  This is performed by identifying
  a unique string (e.g. "gradients", "gradients_1", ...) from the input
  gradient Tensor's name.  This string is used as a suffix when creating
  the TensorArray gradient object here (the attribute `source`).

  The attribute `source` is added as a suffix to the forward TensorArray's
  name when performing the creation / lookup, so that each separate gradient
  calculation gets its own TensorArray accumulator.

  Args:
    handle: A `Tensor` of type `resource`.
      The handle to the forward TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    source: A `string`.
      The gradient source string, used to decide which gradient TensorArray
      to return.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (grad_handle, flow_out).

    grad_handle: A `Tensor` of type `resource`.
    flow_out: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayGradV3", name, _ctx._post_execution_callbacks, handle,
        flow_in, "source", source)
      _result = _TensorArrayGradV3Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_grad_v3_eager_fallback(
            handle, flow_in, source=source, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  source = _execute.make_str(source, "source")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGradV3", handle=handle, flow_in=flow_in, source=source,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("source", _op.get_attr("source"))
  _execute.record_gradient(
      "TensorArrayGradV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayGradV3Output._make(_result)
  return _result



def tensor_array_grad_v3_eager_fallback(handle, flow_in, source, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_grad_v3
  """
  _ctx = ctx if ctx else _context.context()
  source = _execute.make_str(source, "source")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = ("source", source)
  _result = _execute.execute(b"TensorArrayGradV3", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayGradV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayGradV3Output._make(_result)
  return _result


_tensor_array_grad_with_shape_outputs = ["grad_handle", "flow_out"]
_TensorArrayGradWithShapeOutput = _collections.namedtuple(
    "TensorArrayGradWithShape", _tensor_array_grad_with_shape_outputs)


def tensor_array_grad_with_shape(handle, flow_in, shape_to_prepend, source, name=None):
  r"""Creates a TensorArray for storing multiple gradients of values in the given handle.

  Similar to TensorArrayGradV3. However it creates an accumulator with an
  expanded shape compared to the input TensorArray whose gradient is being
  computed. This enables multiple gradients for the same TensorArray to be
  calculated using the same accumulator.

  Args:
    handle: A `Tensor` of type `resource`.
      The handle to the forward TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    shape_to_prepend: A `Tensor` of type `int32`.
      An int32 vector representing a shape. Elements in the gradient accumulator will
      have shape which is this shape_to_prepend value concatenated with shape of the
      elements in the TensorArray corresponding to the input handle.
    source: A `string`.
      The gradient source string, used to decide which gradient TensorArray
      to return.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (grad_handle, flow_out).

    grad_handle: A `Tensor` of type `resource`.
    flow_out: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayGradWithShape", name, _ctx._post_execution_callbacks,
        handle, flow_in, shape_to_prepend, "source", source)
      _result = _TensorArrayGradWithShapeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_grad_with_shape_eager_fallback(
            handle, flow_in, shape_to_prepend, source=source, name=name,
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
  source = _execute.make_str(source, "source")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayGradWithShape", handle=handle, flow_in=flow_in,
                                    shape_to_prepend=shape_to_prepend,
                                    source=source, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("source", _op.get_attr("source"))
  _execute.record_gradient(
      "TensorArrayGradWithShape", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayGradWithShapeOutput._make(_result)
  return _result



def tensor_array_grad_with_shape_eager_fallback(handle, flow_in, shape_to_prepend, source, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_grad_with_shape
  """
  _ctx = ctx if ctx else _context.context()
  source = _execute.make_str(source, "source")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  shape_to_prepend = _ops.convert_to_tensor(shape_to_prepend, _dtypes.int32)
  _inputs_flat = [handle, flow_in, shape_to_prepend]
  _attrs = ("source", source)
  _result = _execute.execute(b"TensorArrayGradWithShape", 2,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "TensorArrayGradWithShape", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayGradWithShapeOutput._make(_result)
  return _result


def tensor_array_pack(handle, flow_in, dtype, element_shape=None, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_pack op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayPack", handle=handle, flow_in=flow_in, dtype=dtype,
                           element_shape=element_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"))
  _execute.record_gradient(
      "TensorArrayPack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_pack_eager_fallback(handle, flow_in, dtype, element_shape=None, name=None, ctx=None):
  raise RuntimeError("tensor_array_pack op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_read(handle, index, flow_in, dtype, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    index: A `Tensor` of type `int32`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_read op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayRead", handle=handle, index=index, flow_in=flow_in,
                           dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "TensorArrayRead", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_read_eager_fallback(handle, index, flow_in, dtype, name=None, ctx=None):
  raise RuntimeError("tensor_array_read op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_read_v2(handle, index, flow_in, dtype, name=None):
  r"""Deprecated. Use TensorArrayReadV3

  Args:
    handle: A `Tensor` of type `string`.
    index: A `Tensor` of type `int32`.
    flow_in: A `Tensor` of type `float32`.
    dtype: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayReadV2", name, _ctx._post_execution_callbacks, handle,
        index, flow_in, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_read_v2_eager_fallback(
            handle, index, flow_in, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayReadV2", handle=handle, index=index, flow_in=flow_in,
                             dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "TensorArrayReadV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_read_v2_eager_fallback(handle, index, flow_in, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_read_v2
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, index, flow_in]
  _attrs = ("dtype", dtype)
  _result = _execute.execute(b"TensorArrayReadV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayReadV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_read_v3(handle, index, flow_in, dtype, name=None):
  r"""Read an element from the TensorArray into output `value`.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    index: A `Tensor` of type `int32`.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    dtype: A `tf.DType`. The type of the elem that is returned.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayReadV3", name, _ctx._post_execution_callbacks, handle,
        index, flow_in, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_read_v3_eager_fallback(
            handle, index, flow_in, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayReadV3", handle=handle, index=index, flow_in=flow_in,
                             dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "TensorArrayReadV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_read_v3_eager_fallback(handle, index, flow_in, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_read_v3
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, index, flow_in]
  _attrs = ("dtype", dtype)
  _result = _execute.execute(b"TensorArrayReadV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayReadV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_scatter(handle, indices, value, flow_in, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    indices: A `Tensor` of type `int32`.
    value: A `Tensor`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_scatter op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayScatter", handle=handle, indices=indices, value=value,
                              flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayScatter", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_scatter_eager_fallback(handle, indices, value, flow_in, name=None, ctx=None):
  raise RuntimeError("tensor_array_scatter op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_scatter_v2(handle, indices, value, flow_in, name=None):
  r"""Deprecated. Use TensorArrayScatterV3

  Args:
    handle: A `Tensor` of type `string`.
    indices: A `Tensor` of type `int32`.
    value: A `Tensor`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayScatterV2", name, _ctx._post_execution_callbacks, handle,
        indices, value, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_scatter_v2_eager_fallback(
            handle, indices, value, flow_in, name=name, ctx=_ctx)
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
        "TensorArrayScatterV2", handle=handle, indices=indices, value=value,
                                flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayScatterV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_scatter_v2_eager_fallback(handle, indices, value, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_scatter_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, indices, value, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArrayScatterV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayScatterV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_scatter_v3(handle, indices, value, flow_in, name=None):
  r"""Scatter the data from the input value into specific TensorArray elements.

  `indices` must be a vector, its length must match the first dim of `value`.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    indices: A `Tensor` of type `int32`.
      The locations at which to write the tensor elements.
    value: A `Tensor`. The concatenated tensor to write to the TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayScatterV3", name, _ctx._post_execution_callbacks, handle,
        indices, value, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_scatter_v3_eager_fallback(
            handle, indices, value, flow_in, name=name, ctx=_ctx)
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
        "TensorArrayScatterV3", handle=handle, indices=indices, value=value,
                                flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayScatterV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_scatter_v3_eager_fallback(handle, indices, value, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_scatter_v3
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  indices = _ops.convert_to_tensor(indices, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, indices, value, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArrayScatterV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayScatterV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_size(handle, flow_in, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_size op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArraySize", handle=handle, flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorArraySize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_size_eager_fallback(handle, flow_in, name=None, ctx=None):
  raise RuntimeError("tensor_array_size op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_size_v2(handle, flow_in, name=None):
  r"""Deprecated. Use TensorArraySizeV3

  Args:
    handle: A `Tensor` of type `string`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArraySizeV2", name, _ctx._post_execution_callbacks, handle,
        flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_size_v2_eager_fallback(
            handle, flow_in, name=name, ctx=_ctx)
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
        "TensorArraySizeV2", handle=handle, flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorArraySizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_size_v2_eager_fallback(handle, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_size_v2
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = None
  _result = _execute.execute(b"TensorArraySizeV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArraySizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_size_v3(handle, flow_in, name=None):
  r"""Get the current size of the TensorArray.

  Args:
    handle: A `Tensor` of type `resource`.
      The handle to a TensorArray (output of TensorArray or TensorArrayGrad).
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArraySizeV3", name, _ctx._post_execution_callbacks, handle,
        flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_size_v3_eager_fallback(
            handle, flow_in, name=name, ctx=_ctx)
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
        "TensorArraySizeV3", handle=handle, flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TensorArraySizeV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_size_v3_eager_fallback(handle, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_size_v3
  """
  _ctx = ctx if ctx else _context.context()
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, flow_in]
  _attrs = None
  _result = _execute.execute(b"TensorArraySizeV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArraySizeV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_split(handle, value, lengths, flow_in, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    value: A `Tensor`.
    lengths: A `Tensor` of type `int64`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_split op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArraySplit", handle=handle, value=value, lengths=lengths,
                            flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArraySplit", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_split_eager_fallback(handle, value, lengths, flow_in, name=None, ctx=None):
  raise RuntimeError("tensor_array_split op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_split_v2(handle, value, lengths, flow_in, name=None):
  r"""Deprecated. Use TensorArraySplitV3

  Args:
    handle: A `Tensor` of type `string`.
    value: A `Tensor`.
    lengths: A `Tensor` of type `int64`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArraySplitV2", name, _ctx._post_execution_callbacks, handle,
        value, lengths, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_split_v2_eager_fallback(
            handle, value, lengths, flow_in, name=name, ctx=_ctx)
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
        "TensorArraySplitV2", handle=handle, value=value, lengths=lengths,
                              flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArraySplitV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_split_v2_eager_fallback(handle, value, lengths, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_split_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  lengths = _ops.convert_to_tensor(lengths, _dtypes.int64)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, value, lengths, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArraySplitV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArraySplitV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_split_v3(handle, value, lengths, flow_in, name=None):
  r"""Split the data from the input value into TensorArray elements.

  Assuming that `lengths` takes on values

    ```(n0, n1, ..., n(T-1))```

  and that `value` has shape

    ```(n0 + n1 + ... + n(T-1) x d0 x d1 x ...)```,

  this splits values into a TensorArray with T tensors.

  TensorArray index t will be the subtensor of values with starting position

    ```(n0 + n1 + ... + n(t-1), 0, 0, ...)```

  and having size

    ```nt x d0 x d1 x ...```

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    value: A `Tensor`. The concatenated tensor to write to the TensorArray.
    lengths: A `Tensor` of type `int64`.
      The vector of lengths, how to split the rows of value into the
      TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArraySplitV3", name, _ctx._post_execution_callbacks, handle,
        value, lengths, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_split_v3_eager_fallback(
            handle, value, lengths, flow_in, name=name, ctx=_ctx)
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
        "TensorArraySplitV3", handle=handle, value=value, lengths=lengths,
                              flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArraySplitV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_split_v3_eager_fallback(handle, value, lengths, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_split_v3
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  lengths = _ops.convert_to_tensor(lengths, _dtypes.int64)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, value, lengths, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArraySplitV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArraySplitV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_unpack(handle, value, flow_in, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    value: A `Tensor`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_unpack op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayUnpack", handle=handle, value=value, flow_in=flow_in,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayUnpack", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_unpack_eager_fallback(handle, value, flow_in, name=None, ctx=None):
  raise RuntimeError("tensor_array_unpack op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_v2(size, dtype, element_shape=None, dynamic_size=False, clear_after_read=True, tensor_array_name="", name=None):
  r"""Deprecated. Use TensorArrayV3

  Args:
    size: A `Tensor` of type `int32`.
    dtype: A `tf.DType`.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
    dynamic_size: An optional `bool`. Defaults to `False`.
    clear_after_read: An optional `bool`. Defaults to `True`.
    tensor_array_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayV2", name, _ctx._post_execution_callbacks, size, "dtype",
        dtype, "element_shape", element_shape, "dynamic_size", dynamic_size,
        "clear_after_read", clear_after_read, "tensor_array_name",
        tensor_array_name)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_v2_eager_fallback(
            size, dtype=dtype, element_shape=element_shape,
            dynamic_size=dynamic_size, clear_after_read=clear_after_read,
            tensor_array_name=tensor_array_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  if dynamic_size is None:
    dynamic_size = False
  dynamic_size = _execute.make_bool(dynamic_size, "dynamic_size")
  if clear_after_read is None:
    clear_after_read = True
  clear_after_read = _execute.make_bool(clear_after_read, "clear_after_read")
  if tensor_array_name is None:
    tensor_array_name = ""
  tensor_array_name = _execute.make_str(tensor_array_name, "tensor_array_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayV2", size=size, dtype=dtype, element_shape=element_shape,
                         dynamic_size=dynamic_size,
                         clear_after_read=clear_after_read,
                         tensor_array_name=tensor_array_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"), "dynamic_size",
            _op.get_attr("dynamic_size"), "clear_after_read",
            _op.get_attr("clear_after_read"), "tensor_array_name",
            _op.get_attr("tensor_array_name"))
  _execute.record_gradient(
      "TensorArrayV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_v2_eager_fallback(size, dtype, element_shape=None, dynamic_size=False, clear_after_read=True, tensor_array_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_v2
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  if dynamic_size is None:
    dynamic_size = False
  dynamic_size = _execute.make_bool(dynamic_size, "dynamic_size")
  if clear_after_read is None:
    clear_after_read = True
  clear_after_read = _execute.make_bool(clear_after_read, "clear_after_read")
  if tensor_array_name is None:
    tensor_array_name = ""
  tensor_array_name = _execute.make_str(tensor_array_name, "tensor_array_name")
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [size]
  _attrs = ("dtype", dtype, "element_shape", element_shape, "dynamic_size",
  dynamic_size, "clear_after_read", clear_after_read, "tensor_array_name",
  tensor_array_name)
  _result = _execute.execute(b"TensorArrayV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_tensor_array_v3_outputs = ["handle", "flow"]
_TensorArrayV3Output = _collections.namedtuple(
    "TensorArrayV3", _tensor_array_v3_outputs)


def tensor_array_v3(size, dtype, element_shape=None, dynamic_size=False, clear_after_read=True, identical_element_shapes=False, tensor_array_name="", name=None):
  r"""An array of Tensors of given size.

  Write data via Write and read via Read or Pack.

  Args:
    size: A `Tensor` of type `int32`. The size of the array.
    dtype: A `tf.DType`. The type of the elements on the tensor_array.
    element_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `None`.
      The expected shape of an element, if known. Used to
      validate the shapes of TensorArray elements. If this shape is not
      fully specified, gathering zero-size TensorArrays is an error.
    dynamic_size: An optional `bool`. Defaults to `False`.
      A boolean that determines whether writes to the TensorArray
      are allowed to grow the size.  By default, this is not allowed.
    clear_after_read: An optional `bool`. Defaults to `True`.
      If true (default), Tensors in the TensorArray are cleared
      after being read.  This disables multiple read semantics but allows early
      release of memory.
    identical_element_shapes: An optional `bool`. Defaults to `False`.
      If true (default is false), then all
      elements in the TensorArray will be expected to have have identical shapes.
      This allows certain behaviors, like dynamically checking for
      consistent shapes on write, and being able to fill in properly
      shaped zero tensors on stack -- even if the element_shape attribute
      is not fully defined.
    tensor_array_name: An optional `string`. Defaults to `""`.
      Overrides the name used for the temporary tensor_array
      resource. Default value is the name of the 'TensorArray' op (which
      is guaranteed unique).
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (handle, flow).

    handle: A `Tensor` of type `resource`.
    flow: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayV3", name, _ctx._post_execution_callbacks, size, "dtype",
        dtype, "element_shape", element_shape, "dynamic_size", dynamic_size,
        "clear_after_read", clear_after_read, "identical_element_shapes",
        identical_element_shapes, "tensor_array_name", tensor_array_name)
      _result = _TensorArrayV3Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_v3_eager_fallback(
            size, dtype=dtype, element_shape=element_shape,
            dynamic_size=dynamic_size, clear_after_read=clear_after_read,
            identical_element_shapes=identical_element_shapes,
            tensor_array_name=tensor_array_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  if dynamic_size is None:
    dynamic_size = False
  dynamic_size = _execute.make_bool(dynamic_size, "dynamic_size")
  if clear_after_read is None:
    clear_after_read = True
  clear_after_read = _execute.make_bool(clear_after_read, "clear_after_read")
  if identical_element_shapes is None:
    identical_element_shapes = False
  identical_element_shapes = _execute.make_bool(identical_element_shapes, "identical_element_shapes")
  if tensor_array_name is None:
    tensor_array_name = ""
  tensor_array_name = _execute.make_str(tensor_array_name, "tensor_array_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayV3", size=size, dtype=dtype, element_shape=element_shape,
                         dynamic_size=dynamic_size,
                         clear_after_read=clear_after_read,
                         identical_element_shapes=identical_element_shapes,
                         tensor_array_name=tensor_array_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "element_shape",
            _op.get_attr("element_shape"), "dynamic_size",
            _op.get_attr("dynamic_size"), "clear_after_read",
            _op.get_attr("clear_after_read"), "identical_element_shapes",
            _op.get_attr("identical_element_shapes"), "tensor_array_name",
            _op.get_attr("tensor_array_name"))
  _execute.record_gradient(
      "TensorArrayV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayV3Output._make(_result)
  return _result



def tensor_array_v3_eager_fallback(size, dtype, element_shape=None, dynamic_size=False, clear_after_read=True, identical_element_shapes=False, tensor_array_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_v3
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if element_shape is None:
    element_shape = None
  element_shape = _execute.make_shape(element_shape, "element_shape")
  if dynamic_size is None:
    dynamic_size = False
  dynamic_size = _execute.make_bool(dynamic_size, "dynamic_size")
  if clear_after_read is None:
    clear_after_read = True
  clear_after_read = _execute.make_bool(clear_after_read, "clear_after_read")
  if identical_element_shapes is None:
    identical_element_shapes = False
  identical_element_shapes = _execute.make_bool(identical_element_shapes, "identical_element_shapes")
  if tensor_array_name is None:
    tensor_array_name = ""
  tensor_array_name = _execute.make_str(tensor_array_name, "tensor_array_name")
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [size]
  _attrs = ("dtype", dtype, "element_shape", element_shape, "dynamic_size",
  dynamic_size, "clear_after_read", clear_after_read,
  "identical_element_shapes", identical_element_shapes, "tensor_array_name",
  tensor_array_name)
  _result = _execute.execute(b"TensorArrayV3", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayV3", _inputs_flat, _attrs, _result, name)
  _result = _TensorArrayV3Output._make(_result)
  return _result


def tensor_array_write(handle, index, value, flow_in, name=None):
  r"""TODO: add doc.

  Args:
    handle: A `Tensor` of type mutable `string`.
    index: A `Tensor` of type `int32`.
    value: A `Tensor`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tensor_array_write op does not support eager execution. Arg 'handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorArrayWrite", handle=handle, index=index, value=value,
                            flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayWrite", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_write_eager_fallback(handle, index, value, flow_in, name=None, ctx=None):
  raise RuntimeError("tensor_array_write op does not support eager execution. Arg 'handle' is a ref.")

def tensor_array_write_v2(handle, index, value, flow_in, name=None):
  r"""Deprecated. Use TensorArrayGradV3

  Args:
    handle: A `Tensor` of type `string`.
    index: A `Tensor` of type `int32`.
    value: A `Tensor`.
    flow_in: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayWriteV2", name, _ctx._post_execution_callbacks, handle,
        index, value, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_write_v2_eager_fallback(
            handle, index, value, flow_in, name=name, ctx=_ctx)
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
        "TensorArrayWriteV2", handle=handle, index=index, value=value,
                              flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayWriteV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_write_v2_eager_fallback(handle, index, value, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_write_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.string)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, index, value, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArrayWriteV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayWriteV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_array_write_v3(handle, index, value, flow_in, name=None):
  r"""Push an element onto the tensor_array.

  Args:
    handle: A `Tensor` of type `resource`. The handle to a TensorArray.
    index: A `Tensor` of type `int32`.
      The position to write to inside the TensorArray.
    value: A `Tensor`. The tensor to write to the TensorArray.
    flow_in: A `Tensor` of type `float32`.
      A float scalar that enforces proper chaining of operations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorArrayWriteV3", name, _ctx._post_execution_callbacks, handle,
        index, value, flow_in)
      return _result
    except _core._FallbackException:
      try:
        return tensor_array_write_v3_eager_fallback(
            handle, index, value, flow_in, name=name, ctx=_ctx)
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
        "TensorArrayWriteV3", handle=handle, index=index, value=value,
                              flow_in=flow_in, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorArrayWriteV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_array_write_v3_eager_fallback(handle, index, value, flow_in, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_array_write_v3
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  handle = _ops.convert_to_tensor(handle, _dtypes.resource)
  index = _ops.convert_to_tensor(index, _dtypes.int32)
  flow_in = _ops.convert_to_tensor(flow_in, _dtypes.float32)
  _inputs_flat = [handle, index, value, flow_in]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorArrayWriteV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorArrayWriteV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def unstage(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None):
  r"""Op is similar to a lightweight Dequeue.

  The basic functionality is similar to dequeue with many fewer
  capabilities and options.  This Op is optimized for performance.

  Args:
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
    capacity: An optional `int` that is `>= 0`. Defaults to `0`.
    memory_limit: An optional `int` that is `>= 0`. Defaults to `0`.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Unstage",
        name, _ctx._post_execution_callbacks, "capacity", capacity,
        "memory_limit", memory_limit, "dtypes", dtypes, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return unstage_eager_fallback(
            capacity=capacity, memory_limit=memory_limit, dtypes=dtypes,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Unstage", dtypes=dtypes, capacity=capacity,
                   memory_limit=memory_limit, container=container,
                   shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("capacity", _op.get_attr("capacity"), "memory_limit",
            _op.get_attr("memory_limit"), "dtypes", _op.get_attr("dtypes"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "Unstage", _inputs_flat, _attrs, _result, name)
  return _result



def unstage_eager_fallback(dtypes, capacity=0, memory_limit=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unstage
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'unstage' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  if capacity is None:
    capacity = 0
  capacity = _execute.make_int(capacity, "capacity")
  if memory_limit is None:
    memory_limit = 0
  memory_limit = _execute.make_int(memory_limit, "memory_limit")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("capacity", capacity, "memory_limit", memory_limit, "dtypes",
  dtypes, "container", container, "shared_name", shared_name)
  _result = _execute.execute(b"Unstage", len(dtypes), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Unstage", _inputs_flat, _attrs, _result, name)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AccumulatorApplyGradient"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "local_step"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradient"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
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
#   name: "AccumulatorNumAccumulated"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "num_accumulated"
#     type: DT_INT32
#   }
# }
# op {
#   name: "AccumulatorSetGlobalStep"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "new_global_step"
#     type: DT_INT64
#   }
# }
# op {
#   name: "AccumulatorTakeGradient"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "num_required"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "average"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
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
#   name: "Barrier"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "BarrierClose"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "cancel_pending_enqueues"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "BarrierIncompleteSize"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
# }
# op {
#   name: "BarrierInsertMany"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "keys"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "component_index"
#     type: "int"
#   }
# }
# op {
#   name: "BarrierReadySize"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
# }
# op {
#   name: "BarrierTakeMany"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "num_elements"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "keys"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "allow_small_batch"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "wait_for_incomplete"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "ConditionalAccumulator"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "dtype"
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
#   attr {
#     name: "shape"
#     type: "shape"
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
#     name: "reduction_type"
#     type: "string"
#     default_value {
#       s: "MEAN"
#     }
#     allowed_values {
#       list {
#         s: "MEAN"
#         s: "SUM"
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "DeleteSessionTensor"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "DynamicPartition"
#   input_arg {
#     name: "data"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "partitions"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "outputs"
#     type_attr: "T"
#     number_attr: "num_partitions"
#   }
#   attr {
#     name: "num_partitions"
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
#   name: "DynamicStitch"
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "data"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "merged"
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
#   name: "FIFOQueue"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "FIFOQueueV2"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "FakeQueue"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   is_stateful: true
# }
# op {
#   name: "GetSessionHandle"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "GetSessionHandleV2"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "GetSessionTensor"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "MapClear"
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "MapIncompleteSize"
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "MapPeek"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "MapSize"
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "MapStage"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "fake_dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#   }
#   attr {
#     name: "fake_dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "MapUnstage"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "MapUnstageNoKey"
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "OrderedMapClear"
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "OrderedMapIncompleteSize"
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "OrderedMapPeek"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "OrderedMapSize"
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "OrderedMapStage"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "values"
#     type_list_attr: "fake_dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#   }
#   attr {
#     name: "fake_dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "OrderedMapUnstage"
#   input_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "OrderedMapUnstageNoKey"
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "key"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "PaddingFIFOQueue"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "PaddingFIFOQueueV2"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "ParallelDynamicStitch"
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#     number_attr: "N"
#   }
#   input_arg {
#     name: "data"
#     type_attr: "T"
#     number_attr: "N"
#   }
#   output_arg {
#     name: "merged"
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
#   name: "PriorityQueue"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "PriorityQueueV2"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
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
#   name: "QueueClose"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "cancel_pending_enqueues"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "QueueCloseV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "cancel_pending_enqueues"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueDequeue"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "QueueDequeueMany"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "n"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "QueueDequeueManyV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "n"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueDequeueUpTo"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "n"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "QueueDequeueUpToV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "n"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueDequeueV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "component_types"
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueEnqueue"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "components"
#     type_list_attr: "Tcomponents"
#   }
#   attr {
#     name: "Tcomponents"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "QueueEnqueueMany"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "components"
#     type_list_attr: "Tcomponents"
#   }
#   attr {
#     name: "Tcomponents"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "QueueEnqueueManyV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "components"
#     type_list_attr: "Tcomponents"
#   }
#   attr {
#     name: "Tcomponents"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueEnqueueV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "components"
#     type_list_attr: "Tcomponents"
#   }
#   attr {
#     name: "Tcomponents"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "timeout_ms"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueIsClosed"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "is_closed"
#     type: DT_BOOL
#   }
# }
# op {
#   name: "QueueIsClosedV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_closed"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "QueueSize"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
# }
# op {
#   name: "QueueSizeV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   is_stateful: true
# }
# op {
#   name: "RandomShuffleQueue"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "min_after_dequeue"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#   name: "RandomShuffleQueueV2"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "component_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "min_after_dequeue"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "seed2"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#   name: "RecordInput"
#   output_arg {
#     name: "records"
#     type: DT_STRING
#   }
#   attr {
#     name: "file_pattern"
#     type: "string"
#   }
#   attr {
#     name: "file_random_seed"
#     type: "int"
#     default_value {
#       i: 301
#     }
#   }
#   attr {
#     name: "file_shuffle_shift_ratio"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "file_buffer_size"
#     type: "int"
#     default_value {
#       i: 10000
#     }
#   }
#   attr {
#     name: "file_parallelism"
#     type: "int"
#     default_value {
#       i: 16
#     }
#   }
#   attr {
#     name: "batch_size"
#     type: "int"
#     default_value {
#       i: 32
#     }
#   }
#   attr {
#     name: "compression_type"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "SparseAccumulatorApplyGradient"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "local_step"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradient_indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "gradient_values"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "gradient_shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
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
#   attr {
#     name: "has_known_shape"
#     type: "bool"
#   }
# }
# op {
#   name: "SparseAccumulatorTakeGradient"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "num_required"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "values"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
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
#   name: "SparseConditionalAccumulator"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "dtype"
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
#   attr {
#     name: "shape"
#     type: "shape"
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
#     name: "reduction_type"
#     type: "string"
#     default_value {
#       s: "MEAN"
#     }
#     allowed_values {
#       list {
#         s: "MEAN"
#         s: "SUM"
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "Stack"
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "elem_type"
#     type: "type"
#   }
#   attr {
#     name: "stack_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "StackClose"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
# }
# op {
#   name: "StackCloseV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   is_stateful: true
# }
# op {
#   name: "StackPop"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "elem"
#     type_attr: "elem_type"
#   }
#   attr {
#     name: "elem_type"
#     type: "type"
#   }
# }
# op {
#   name: "StackPopV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "elem"
#     type_attr: "elem_type"
#   }
#   attr {
#     name: "elem_type"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "StackPush"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "elem"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "swap_memory"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "StackPushV2"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "elem"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "swap_memory"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "StackV2"
#   input_arg {
#     name: "max_size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "elem_type"
#     type: "type"
#   }
#   attr {
#     name: "stack_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "Stage"
#   input_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "StageClear"
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "StagePeek"
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
#   name: "StageSize"
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
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
#   name: "TensorArray"
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "dynamic_size"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "clear_after_read"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "tensor_array_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayV3"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayClose"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayCloseV3"
#   }
# }
# op {
#   name: "TensorArrayCloseV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayCloseV3"
#   }
# }
# op {
#   name: "TensorArrayCloseV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayConcat"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape_except0"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayGradV3"
#   }
# }
# op {
#   name: "TensorArrayConcatV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape_except0"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
# }
# op {
#   name: "TensorArrayConcatV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape_except0"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayGather"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayGatherV3"
#   }
# }
# op {
#   name: "TensorArrayGatherV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayGatherV3"
#   }
# }
# op {
#   name: "TensorArrayGatherV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayGrad"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "grad_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "source"
#     type: "string"
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayGradV3"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayGradV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "grad_handle"
#     type: DT_STRING
#   }
#   attr {
#     name: "source"
#     type: "string"
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayGradV3"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayGradV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "grad_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "source"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayGradWithShape"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "shape_to_prepend"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "grad_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "source"
#     type: "string"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayPack"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayGatherV3 with RangeOp"
#   }
# }
# op {
#   name: "TensorArrayRead"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayReadV3"
#   }
# }
# op {
#   name: "TensorArrayReadV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayReadV3"
#   }
# }
# op {
#   name: "TensorArrayReadV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "value"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayScatter"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 19
#     explanation: "Use TensorArrayGradV3"
#   }
# }
# op {
#   name: "TensorArrayScatterV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayScatterV3"
#   }
# }
# op {
#   name: "TensorArrayScatterV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArraySize"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArraySizeV3"
#   }
# }
# op {
#   name: "TensorArraySizeV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArraySizeV3"
#   }
# }
# op {
#   name: "TensorArraySizeV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArraySplit"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArraySplitV3"
#   }
# }
# op {
#   name: "TensorArraySplitV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArraySplitV3"
#   }
# }
# op {
#   name: "TensorArraySplitV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lengths"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayUnpack"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 20
#     explanation: "Use TensorArrayScatterV3 with RangeOp"
#   }
# }
# op {
#   name: "TensorArrayV2"
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   attr {
#     name: "dynamic_size"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "clear_after_read"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "tensor_array_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayV3"
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayV3"
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "flow"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "element_shape"
#     type: "shape"
#     default_value {
#       shape {
#         unknown_rank: true
#       }
#     }
#   }
#   attr {
#     name: "dynamic_size"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "clear_after_read"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "identical_element_shapes"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "tensor_array_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "TensorArrayWrite"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 16
#     explanation: "Use TensorArrayWriteV3"
#   }
# }
# op {
#   name: "TensorArrayWriteV2"
#   input_arg {
#     name: "handle"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TensorArrayWriteV3"
#   }
# }
# op {
#   name: "TensorArrayWriteV3"
#   input_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "index"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "flow_in"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "flow_out"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "Unstage"
#   output_arg {
#     name: "values"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "capacity"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "memory_limit"
#     type: "int"
#     default_value {
#       i: 0
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
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
_op_def_lib = _InitOpDefLibrary(b"\nr\n\030AccumulatorApplyGradient\022\r\n\006handle\030\007\200\001\001\022\016\n\nlocal_step\030\t\022\021\n\010gradient\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n?\n\031AccumulatorNumAccumulated\022\r\n\006handle\030\007\200\001\001\032\023\n\017num_accumulated\030\003\n>\n\030AccumulatorSetGlobalStep\022\r\n\006handle\030\007\200\001\001\022\023\n\017new_global_step\030\t\nr\n\027AccumulatorTakeGradient\022\r\n\006handle\030\007\200\001\001\022\020\n\014num_required\030\003\032\020\n\007average\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\255\001\n\007Barrier\032\r\n\006handle\030\007\200\001\001\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nB\n\014BarrierClose\022\r\n\006handle\030\007\200\001\001\"#\n\027cancel_pending_enqueues\022\004bool\032\002(\000\n0\n\025BarrierIncompleteSize\022\r\n\006handle\030\007\200\001\001\032\010\n\004size\030\003\n\\\n\021BarrierInsertMany\022\r\n\006handle\030\007\200\001\001\022\010\n\004keys\030\007\022\013\n\006values\"\001T\"\t\n\001T\022\004type\"\026\n\017component_index\022\003int\n+\n\020BarrierReadySize\022\r\n\006handle\030\007\200\001\001\032\010\n\004size\030\003\n\347\001\n\017BarrierTakeMany\022\r\n\006handle\030\007\200\001\001\022\020\n\014num_elements\030\003\032\013\n\007indices\030\t\032\010\n\004keys\030\007\032\031\n\006values2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\035\n\021allow_small_batch\022\004bool\032\002(\000\"\037\n\023wait_for_incomplete\022\004bool\032\002(\000\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n\305\001\n\026ConditionalAccumulator\032\r\n\006handle\030\007\200\001\001\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\016\n\005shape\022\005shape\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"/\n\016reduction_type\022\006string\032\006\022\004MEAN:\r\n\013\022\004MEAN\022\003SUM\210\001\001\n$\n\023DeleteSessionTensor\022\n\n\006handle\030\007\210\001\001\nq\n\020DynamicPartition\022\t\n\004data\"\001T\022\016\n\npartitions\030\003\032\034\n\007outputs\"\001T*\016num_partitions\"\031\n\016num_partitions\022\003int(\0010\001\"\t\n\001T\022\004type\nS\n\rDynamicStitch\022\016\n\007indices\030\003*\001N\022\014\n\004data\"\001T*\001N\032\013\n\006merged\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\n\257\001\n\tFIFOQueue\032\r\n\006handle\030\007\200\001\001\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\256\001\n\013FIFOQueueV2\032\n\n\006handle\030\024\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n+\n\tFakeQueue\022\014\n\010resource\030\024\032\r\n\006handle\030\007\200\001\001\210\001\001\n8\n\020GetSessionHandle\022\n\n\005value\"\001T\032\n\n\006handle\030\007\"\t\n\001T\022\004type\210\001\001\n:\n\022GetSessionHandleV2\022\n\n\005value\"\001T\032\n\n\006handle\030\024\"\t\n\001T\022\004type\210\001\001\n@\n\020GetSessionTensor\022\n\n\006handle\030\007\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\n\211\001\n\010MapClear\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\234\001\n\021MapIncompleteSize\032\010\n\004size\030\003\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\264\001\n\007MapPeek\022\007\n\003key\030\t\022\013\n\007indices\030\003\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\222\001\n\007MapSize\032\010\n\004size\030\003\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\325\001\n\010MapStage\022\007\n\003key\030\t\022\013\n\007indices\030\003\022\025\n\006values2\013fake_dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\035\n\013fake_dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\267\001\n\nMapUnstage\022\007\n\003key\030\t\022\013\n\007indices\030\003\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\274\001\n\017MapUnstageNoKey\022\013\n\007indices\030\003\032\007\n\003key\030\t\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\220\001\n\017OrderedMapClear\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\243\001\n\030OrderedMapIncompleteSize\032\010\n\004size\030\003\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\273\001\n\016OrderedMapPeek\022\007\n\003key\030\t\022\013\n\007indices\030\003\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\231\001\n\016OrderedMapSize\032\010\n\004size\030\003\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\334\001\n\017OrderedMapStage\022\007\n\003key\030\t\022\013\n\007indices\030\003\022\025\n\006values2\013fake_dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\035\n\013fake_dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\276\001\n\021OrderedMapUnstage\022\007\n\003key\030\t\022\013\n\007indices\030\003\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\303\001\n\026OrderedMapUnstageNoKey\022\013\n\007indices\030\003\032\007\n\003key\030\t\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\266\001\n\020PaddingFIFOQueue\032\r\n\006handle\030\007\200\001\001\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\265\001\n\022PaddingFIFOQueueV2\032\n\n\006handle\030\024\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n[\n\025ParallelDynamicStitch\022\016\n\007indices\030\003*\001N\022\014\n\004data\"\001T*\001N\032\013\n\006merged\"\001T\"\014\n\001N\022\003int(\0010\001\"\t\n\001T\022\004type\n\261\001\n\rPriorityQueue\032\r\n\006handle\030\007\200\001\001\"#\n\017component_types\022\nlist(type)\032\002\n\000(\001\"\027\n\006shapes\022\013list(shape)(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\260\001\n\017PriorityQueueV2\032\n\n\006handle\030\024\"#\n\017component_types\022\nlist(type)\032\002\n\000(\001\"\027\n\006shapes\022\013list(shape)(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n@\n\nQueueClose\022\r\n\006handle\030\007\200\001\001\"#\n\027cancel_pending_enqueues\022\004bool\032\002(\000\nB\n\014QueueCloseV2\022\n\n\006handle\030\024\"#\n\027cancel_pending_enqueues\022\004bool\032\002(\000\210\001\001\n\177\n\014QueueDequeue\022\r\n\006handle\030\007\200\001\001\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n\212\001\n\020QueueDequeueMany\022\r\n\006handle\030\007\200\001\001\022\005\n\001n\030\003\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n\214\001\n\022QueueDequeueManyV2\022\n\n\006handle\030\024\022\005\n\001n\030\003\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\212\001\n\020QueueDequeueUpTo\022\r\n\006handle\030\007\200\001\001\022\005\n\001n\030\003\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n\214\001\n\022QueueDequeueUpToV2\022\n\n\006handle\030\024\022\005\n\001n\030\003\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\201\001\n\016QueueDequeueV2\022\n\n\006handle\030\024\032\035\n\ncomponents2\017component_types\"!\n\017component_types\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\nw\n\014QueueEnqueue\022\r\n\006handle\030\007\200\001\001\022\031\n\ncomponents2\013Tcomponents\"\035\n\013Tcomponents\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n{\n\020QueueEnqueueMany\022\r\n\006handle\030\007\200\001\001\022\031\n\ncomponents2\013Tcomponents\"\035\n\013Tcomponents\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n}\n\022QueueEnqueueManyV2\022\n\n\006handle\030\024\022\031\n\ncomponents2\013Tcomponents\"\035\n\013Tcomponents\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\ny\n\016QueueEnqueueV2\022\n\n\006handle\030\024\022\031\n\ncomponents2\013Tcomponents\"\035\n\013Tcomponents\022\nlist(type)(\0010\001\"\036\n\ntimeout_ms\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n-\n\rQueueIsClosed\022\r\n\006handle\030\007\200\001\001\032\r\n\tis_closed\030\n\n/\n\017QueueIsClosedV2\022\n\n\006handle\030\024\032\r\n\tis_closed\030\n\210\001\001\n$\n\tQueueSize\022\r\n\006handle\030\007\200\001\001\032\010\n\004size\030\003\n&\n\013QueueSizeV2\022\n\n\006handle\030\024\032\010\n\004size\030\003\210\001\001\n\371\001\n\022RandomShuffleQueue\032\r\n\006handle\030\007\200\001\001\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\034\n\021min_after_dequeue\022\003int\032\002\030\000\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\370\001\n\024RandomShuffleQueueV2\032\n\n\006handle\030\024\"!\n\017component_types\022\nlist(type)(\0010\001\"\033\n\006shapes\022\013list(shape)\032\002\n\000(\001\"\034\n\010capacity\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\034\n\021min_after_dequeue\022\003int\032\002\030\000\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\357\001\n\013RecordInput\032\013\n\007records\030\007\"\026\n\014file_pattern\022\006string\"\034\n\020file_random_seed\022\003int\032\003\030\255\002\"(\n\030file_shuffle_shift_ratio\022\005float\032\005%\000\000\000\000\"\034\n\020file_buffer_size\022\003int\032\003\030\220N\"\033\n\020file_parallelism\022\003int\032\002\030\020\"\025\n\nbatch_size\022\003int\032\002\030 \"\036\n\020compression_type\022\006string\032\002\022\000\210\001\001\n\302\001\n\036SparseAccumulatorApplyGradient\022\r\n\006handle\030\007\200\001\001\022\016\n\nlocal_step\030\t\022\024\n\020gradient_indices\030\t\022\030\n\017gradient_values\"\005dtype\022\022\n\016gradient_shape\030\t\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\017has_known_shape\022\004bool\n\217\001\n\035SparseAccumulatorTakeGradient\022\r\n\006handle\030\007\200\001\001\022\020\n\014num_required\030\003\032\013\n\007indices\030\t\032\017\n\006values\"\005dtype\032\t\n\005shape\030\t\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\n\313\001\n\034SparseConditionalAccumulator\032\r\n\006handle\030\007\200\001\001\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\016\n\005shape\022\005shape\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"/\n\016reduction_type\022\006string\032\006\022\004MEAN:\r\n\013\022\004MEAN\022\003SUM\210\001\001\nF\n\005Stack\032\r\n\006handle\030\007\200\001\001\"\021\n\telem_type\022\004type\"\030\n\nstack_name\022\006string\032\002\022\000\210\001\001\n\033\n\nStackClose\022\r\n\006handle\030\007\200\001\001\n\035\n\014StackCloseV2\022\n\n\006handle\030\024\210\001\001\n?\n\010StackPop\022\r\n\006handle\030\007\200\001\001\032\021\n\004elem\"\telem_type\"\021\n\telem_type\022\004type\nA\n\nStackPopV2\022\n\n\006handle\030\024\032\021\n\004elem\"\telem_type\"\021\n\telem_type\022\004type\210\001\001\nV\n\tStackPush\022\r\n\006handle\030\007\200\001\001\022\t\n\004elem\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\027\n\013swap_memory\022\004bool\032\002(\000\nX\n\013StackPushV2\022\n\n\006handle\030\024\022\t\n\004elem\"\001T\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\027\n\013swap_memory\022\004bool\032\002(\000\210\001\001\nS\n\007StackV2\022\014\n\010max_size\030\003\032\n\n\006handle\030\024\"\021\n\telem_type\022\004type\"\030\n\nstack_name\022\006string\032\002\022\000\210\001\001\n\234\001\n\005Stage\022\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\213\001\n\nStageClear\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\253\001\n\tStagePeek\022\t\n\005index\030\003\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\224\001\n\tStageSize\032\010\n\004size\030\003\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\024\n\006dtypes\022\nlist(type)\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\306\001\n\013TensorArray\022\010\n\004size\030\003\032\r\n\006handle\030\007\200\001\001\"\r\n\005dtype\022\004type\"\030\n\014dynamic_size\022\004bool\032\002(\000\"\034\n\020clear_after_read\022\004bool\032\002(\001\"\037\n\021tensor_array_name\022\006string\032\002\022\000\"\034\n\relement_shape\022\005shape\032\004:\002\030\001B\025\010\020\022\021Use TensorArrayV3\210\001\001\n=\n\020TensorArrayClose\022\r\n\006handle\030\007\200\001\001B\032\010\020\022\026Use TensorArrayCloseV3\n<\n\022TensorArrayCloseV2\022\n\n\006handle\030\007B\032\010\032\022\026Use TensorArrayCloseV3\n#\n\022TensorArrayCloseV3\022\n\n\006handle\030\024\210\001\001\n\234\001\n\021TensorArrayConcat\022\r\n\006handle\030\007\200\001\001\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\032\013\n\007lengths\030\t\"\r\n\005dtype\022\004type\"$\n\025element_shape_except0\022\005shape\032\004:\002\030\001B\031\010\020\022\025Use TensorArrayGradV3\n\200\001\n\023TensorArrayConcatV2\022\n\n\006handle\030\007\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\032\013\n\007lengths\030\t\"\r\n\005dtype\022\004type\"$\n\025element_shape_except0\022\005shape\032\004:\002\030\001\n\203\001\n\023TensorArrayConcatV3\022\n\n\006handle\030\024\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\032\013\n\007lengths\030\t\"\r\n\005dtype\022\004type\"$\n\025element_shape_except0\022\005shape\032\004:\002\030\001\210\001\001\n\226\001\n\021TensorArrayGather\022\r\n\006handle\030\007\200\001\001\022\013\n\007indices\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001B\033\010\020\022\027Use TensorArrayGatherV3\n\225\001\n\023TensorArrayGatherV2\022\n\n\006handle\030\007\022\013\n\007indices\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001B\033\010\032\022\027Use TensorArrayGatherV3\n{\n\023TensorArrayGatherV3\022\n\n\006handle\030\024\022\013\n\007indices\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001\210\001\001\nn\n\017TensorArrayGrad\022\n\n\006handle\030\007\022\013\n\007flow_in\030\001\032\022\n\013grad_handle\030\007\200\001\001\"\020\n\006source\022\006stringB\031\010\020\022\025Use TensorArrayGradV3\210\001\001\nm\n\021TensorArrayGradV2\022\n\n\006handle\030\007\022\013\n\007flow_in\030\001\032\017\n\013grad_handle\030\007\"\020\n\006source\022\006stringB\031\010\032\022\025Use TensorArrayGradV3\210\001\001\n`\n\021TensorArrayGradV3\022\n\n\006handle\030\024\022\013\n\007flow_in\030\001\032\017\n\013grad_handle\030\024\032\014\n\010flow_out\030\001\"\020\n\006source\022\006string\210\001\001\n}\n\030TensorArrayGradWithShape\022\n\n\006handle\030\024\022\013\n\007flow_in\030\001\022\024\n\020shape_to_prepend\030\003\032\017\n\013grad_handle\030\024\032\014\n\010flow_out\030\001\"\020\n\006source\022\006string\210\001\001\n\224\001\n\017TensorArrayPack\022\r\n\006handle\030\007\200\001\001\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001B(\010\020\022$Use TensorArrayGatherV3 with RangeOp\nr\n\017TensorArrayRead\022\r\n\006handle\030\007\200\001\001\022\t\n\005index\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004typeB\031\010\020\022\025Use TensorArrayReadV3\nq\n\021TensorArrayReadV2\022\n\n\006handle\030\007\022\t\n\005index\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004typeB\031\010\032\022\025Use TensorArrayReadV3\nY\n\021TensorArrayReadV3\022\n\n\006handle\030\024\022\t\n\005index\030\003\022\013\n\007flow_in\030\001\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\n}\n\022TensorArrayScatter\022\r\n\006handle\030\007\200\001\001\022\013\n\007indices\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\031\010\023\022\025Use TensorArrayGradV3\n\177\n\024TensorArrayScatterV2\022\n\n\006handle\030\007\022\013\n\007indices\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\034\010\032\022\030Use TensorArrayScatterV3\nd\n\024TensorArrayScatterV3\022\n\n\006handle\030\024\022\013\n\007indices\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004type\210\001\001\nR\n\017TensorArraySize\022\r\n\006handle\030\007\200\001\001\022\013\n\007flow_in\030\001\032\010\n\004size\030\003B\031\010\020\022\025Use TensorArraySizeV3\nQ\n\021TensorArraySizeV2\022\n\n\006handle\030\007\022\013\n\007flow_in\030\001\032\010\n\004size\030\003B\031\010\032\022\025Use TensorArraySizeV3\n9\n\021TensorArraySizeV3\022\n\n\006handle\030\024\022\013\n\007flow_in\030\001\032\010\n\004size\030\003\210\001\001\n|\n\020TensorArraySplit\022\r\n\006handle\030\007\200\001\001\022\n\n\005value\"\001T\022\013\n\007lengths\030\t\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\032\010\020\022\026Use TensorArraySplitV3\n{\n\022TensorArraySplitV2\022\n\n\006handle\030\007\022\n\n\005value\"\001T\022\013\n\007lengths\030\t\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\032\010\032\022\026Use TensorArraySplitV3\nb\n\022TensorArraySplitV3\022\n\n\006handle\030\024\022\n\n\005value\"\001T\022\013\n\007lengths\030\t\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004type\210\001\001\n\177\n\021TensorArrayUnpack\022\r\n\006handle\030\007\200\001\001\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB)\010\024\022%Use TensorArrayScatterV3 with RangeOp\n\305\001\n\rTensorArrayV2\022\010\n\004size\030\003\032\n\n\006handle\030\007\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001\"\030\n\014dynamic_size\022\004bool\032\002(\000\"\034\n\020clear_after_read\022\004bool\032\002(\001\"\037\n\021tensor_array_name\022\006string\032\002\022\000B\025\010\032\022\021Use TensorArrayV3\210\001\001\n\336\001\n\rTensorArrayV3\022\010\n\004size\030\003\032\n\n\006handle\030\024\032\010\n\004flow\030\001\"\r\n\005dtype\022\004type\"\034\n\relement_shape\022\005shape\032\004:\002\030\001\"\030\n\014dynamic_size\022\004bool\032\002(\000\"\034\n\020clear_after_read\022\004bool\032\002(\001\"$\n\030identical_element_shapes\022\004bool\032\002(\000\"\037\n\021tensor_array_name\022\006string\032\002\022\000\210\001\001\nz\n\020TensorArrayWrite\022\r\n\006handle\030\007\200\001\001\022\t\n\005index\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\032\010\020\022\026Use TensorArrayWriteV3\ny\n\022TensorArrayWriteV2\022\n\n\006handle\030\007\022\t\n\005index\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004typeB\032\010\032\022\026Use TensorArrayWriteV3\n`\n\022TensorArrayWriteV3\022\n\n\006handle\030\024\022\t\n\005index\030\003\022\n\n\005value\"\001T\022\013\n\007flow_in\030\001\032\014\n\010flow_out\030\001\"\t\n\001T\022\004type\210\001\001\n\236\001\n\007Unstage\032\020\n\006values2\006dtypes\"\025\n\010capacity\022\003int\032\002\030\000(\001\"\031\n\014memory_limit\022\003int\032\002\030\000(\001\"\030\n\006dtypes\022\nlist(type)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001")
