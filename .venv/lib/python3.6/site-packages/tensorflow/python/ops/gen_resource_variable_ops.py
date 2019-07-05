"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: resource_variable_ops.cc
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


def assign_add_variable_op(resource, value, name=None):
  r"""Adds a value to the current value of a variable.

  Any ReadVariableOp with a control dependency on this op is guaranteed to
  see the incremented value or a subsequent newer one.

  Args:
    resource: A `Tensor` of type `resource`.
      handle to the resource in which to store the variable.
    value: A `Tensor`. the value by which the variable will be incremented.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AssignAddVariableOp", name, _ctx._post_execution_callbacks, resource,
        value)
      return _result
    except _core._FallbackException:
      try:
        return assign_add_variable_op_eager_fallback(
            resource, value, name=name, ctx=_ctx)
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
        "AssignAddVariableOp", resource=resource, value=value, name=name)
  return _op
  _result = None
  return _result



def assign_add_variable_op_eager_fallback(resource, value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function assign_add_variable_op
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (value,) = _execute.args_to_matching_eager([value], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, value]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"AssignAddVariableOp", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def assign_sub_variable_op(resource, value, name=None):
  r"""Subtracts a value from the current value of a variable.

  Any ReadVariableOp with a control dependency on this op is guaranteed to
  see the decremented value or a subsequent newer one.

  Args:
    resource: A `Tensor` of type `resource`.
      handle to the resource in which to store the variable.
    value: A `Tensor`. the value by which the variable will be incremented.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AssignSubVariableOp", name, _ctx._post_execution_callbacks, resource,
        value)
      return _result
    except _core._FallbackException:
      try:
        return assign_sub_variable_op_eager_fallback(
            resource, value, name=name, ctx=_ctx)
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
        "AssignSubVariableOp", resource=resource, value=value, name=name)
  return _op
  _result = None
  return _result



def assign_sub_variable_op_eager_fallback(resource, value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function assign_sub_variable_op
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (value,) = _execute.args_to_matching_eager([value], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, value]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"AssignSubVariableOp", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def assign_variable_op(resource, value, name=None):
  r"""Assigns a new value to a variable.

  Any ReadVariableOp with a control dependency on this op is guaranteed to return
  this value or a subsequent newer value of the variable.

  Args:
    resource: A `Tensor` of type `resource`.
      handle to the resource in which to store the variable.
    value: A `Tensor`. the value to set the new tensor to use.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AssignVariableOp", name, _ctx._post_execution_callbacks, resource,
        value)
      return _result
    except _core._FallbackException:
      try:
        return assign_variable_op_eager_fallback(
            resource, value, name=name, ctx=_ctx)
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
        "AssignVariableOp", resource=resource, value=value, name=name)
  return _op
  _result = None
  return _result



def assign_variable_op_eager_fallback(resource, value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function assign_variable_op
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (value,) = _execute.args_to_matching_eager([value], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, value]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"AssignVariableOp", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def consume_mutex_lock(mutex_lock, name=None):
  r"""This op consumes a lock created by `MutexLock`.

  This op exists to consume a tensor created by `MutexLock` (other than
  direct control dependencies).  It should be the only that consumes the tensor,
  and will raise an error if it is not.  Its only purpose is to keep the
  mutex lock tensor alive until it is consumed by this op.

  **NOTE**: This operation must run on the same device as its input.  This may
  be enforced via the `colocate_with` mechanism.

  Args:
    mutex_lock: A `Tensor` of type `variant`.
      A tensor returned by `MutexLock`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ConsumeMutexLock", name, _ctx._post_execution_callbacks, mutex_lock)
      return _result
    except _core._FallbackException:
      try:
        return consume_mutex_lock_eager_fallback(
            mutex_lock, name=name, ctx=_ctx)
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
        "ConsumeMutexLock", mutex_lock=mutex_lock, name=name)
  return _op
  _result = None
  return _result



def consume_mutex_lock_eager_fallback(mutex_lock, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function consume_mutex_lock
  """
  _ctx = ctx if ctx else _context.context()
  mutex_lock = _ops.convert_to_tensor(mutex_lock, _dtypes.variant)
  _inputs_flat = [mutex_lock]
  _attrs = None
  _result = _execute.execute(b"ConsumeMutexLock", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def destroy_resource_op(resource, ignore_lookup_error=True, name=None):
  r"""Deletes the resource specified by the handle.

  All subsequent operations using the resource will result in a NotFound
  error status.

  Args:
    resource: A `Tensor` of type `resource`. handle to the resource to delete.
    ignore_lookup_error: An optional `bool`. Defaults to `True`.
      whether to ignore the error when the resource
      doesn't exist.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DestroyResourceOp", name, _ctx._post_execution_callbacks, resource,
        "ignore_lookup_error", ignore_lookup_error)
      return _result
    except _core._FallbackException:
      try:
        return destroy_resource_op_eager_fallback(
            resource, ignore_lookup_error=ignore_lookup_error, name=name,
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
  if ignore_lookup_error is None:
    ignore_lookup_error = True
  ignore_lookup_error = _execute.make_bool(ignore_lookup_error, "ignore_lookup_error")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DestroyResourceOp", resource=resource,
                             ignore_lookup_error=ignore_lookup_error,
                             name=name)
  return _op
  _result = None
  return _result



def destroy_resource_op_eager_fallback(resource, ignore_lookup_error=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function destroy_resource_op
  """
  _ctx = ctx if ctx else _context.context()
  if ignore_lookup_error is None:
    ignore_lookup_error = True
  ignore_lookup_error = _execute.make_bool(ignore_lookup_error, "ignore_lookup_error")
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource]
  _attrs = ("ignore_lookup_error", ignore_lookup_error)
  _result = _execute.execute(b"DestroyResourceOp", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def mutex_lock(mutex, name=None):
  r"""Locks a mutex resource.  The output is the lock.  So long as the lock tensor

  is alive, any other request to use `MutexLock` with this mutex will wait.

  This is particularly useful for creating a critical section when used in
  conjunction with `MutexLockIdentity`:

  ```python

  mutex = mutex_v2(
    shared_name=handle_name, container=container, name=name)

  def execute_in_critical_section(fn, *args, **kwargs):
    lock = gen_resource_variable_ops.mutex_lock(mutex)

    with ops.control_dependencies([lock]):
      r = fn(*args, **kwargs)

    with ops.control_dependencies(nest.flatten(r)):
      with ops.colocate_with(mutex):
        ensure_lock_exists = mutex_lock_identity(lock)

      # Make sure that if any element of r is accessed, all of
      # them are executed together.
      r = nest.map_structure(tf.identity, r)

    with ops.control_dependencies([ensure_lock_exists]):
      return nest.map_structure(tf.identity, r)
  ```

  While `fn` is running in the critical section, no other functions which wish to
  use this critical section may run.

  Often the use case is that two executions of the same graph, in parallel,
  wish to run `fn`; and we wish to ensure that only one of them executes
  at a time.  This is especially important if `fn` modifies one or more
  variables at a time.

  It is also useful if two separate functions must share a resource, but we
  wish to ensure the usage is exclusive.

  Args:
    mutex: A `Tensor` of type `resource`. The mutex resource to lock.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MutexLock",
        name, _ctx._post_execution_callbacks, mutex)
      return _result
    except _core._FallbackException:
      try:
        return mutex_lock_eager_fallback(
            mutex, name=name, ctx=_ctx)
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
        "MutexLock", mutex=mutex, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MutexLock", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutex_lock_eager_fallback(mutex, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mutex_lock
  """
  _ctx = ctx if ctx else _context.context()
  mutex = _ops.convert_to_tensor(mutex, _dtypes.resource)
  _inputs_flat = [mutex]
  _attrs = None
  _result = _execute.execute(b"MutexLock", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MutexLock", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mutex_v2(container="", shared_name="", name=None):
  r"""Creates a Mutex resource that can be locked by `MutexLock`.

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this variable is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this variable is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MutexV2",
        name, _ctx._post_execution_callbacks, "container", container,
        "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return mutex_v2_eager_fallback(
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
        "MutexV2", container=container, shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "MutexV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutex_v2_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mutex_v2
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
  _result = _execute.execute(b"MutexV2", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "MutexV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def read_variable_op(resource, dtype, name=None):
  r"""Reads the value of a variable.

  The tensor returned by this operation is immutable.

  The value returned by this operation is guaranteed to be influenced by all the
  writes on which this operation depends directly or indirectly, and to not be
  influenced by any of the writes which depend directly or indirectly on this
  operation.

  Args:
    resource: A `Tensor` of type `resource`.
      handle to the resource in which to store the variable.
    dtype: A `tf.DType`. the dtype of the value.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReadVariableOp", name, _ctx._post_execution_callbacks, resource,
        "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return read_variable_op_eager_fallback(
            resource, dtype=dtype, name=name, ctx=_ctx)
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
        "ReadVariableOp", resource=resource, dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "ReadVariableOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def read_variable_op_eager_fallback(resource, dtype, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function read_variable_op
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource]
  _attrs = ("dtype", dtype)
  _result = _execute.execute(b"ReadVariableOp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReadVariableOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resource_gather(resource, indices, dtype, validate_indices=True, name=None):
  r"""Gather slices from the variable pointed to by `resource` according to `indices`.

  `indices` must be an integer tensor of any dimension (usually 0-D or 1-D).
  Produces an output tensor with shape `indices.shape + params.shape[1:]` where:

  ```python
      # Scalar indices
      output[:, ..., :] = params[indices, :, ... :]

      # Vector indices
      output[i, :, ..., :] = params[indices[i], :, ... :]

      # Higher rank indices
      output[i, ..., j, :, ... :] = params[indices[i, ..., j], :, ..., :]
  ```

  Args:
    resource: A `Tensor` of type `resource`.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    dtype: A `tf.DType`.
    validate_indices: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceGather", name, _ctx._post_execution_callbacks, resource,
        indices, "validate_indices", validate_indices, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return resource_gather_eager_fallback(
            resource, indices, validate_indices=validate_indices, dtype=dtype,
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
  dtype = _execute.make_type(dtype, "dtype")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceGather", resource=resource, indices=indices, dtype=dtype,
                          validate_indices=validate_indices, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("validate_indices", _op.get_attr("validate_indices"), "dtype",
            _op.get_attr("dtype"), "Tindices", _op.get_attr("Tindices"))
  _execute.record_gradient(
      "ResourceGather", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resource_gather_eager_fallback(resource, indices, dtype, validate_indices=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_gather
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if validate_indices is None:
    validate_indices = True
  validate_indices = _execute.make_bool(validate_indices, "validate_indices")
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices]
  _attrs = ("validate_indices", validate_indices, "dtype", dtype, "Tindices",
  _attr_Tindices)
  _result = _execute.execute(b"ResourceGather", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResourceGather", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resource_scatter_add(resource, indices, updates, name=None):
  r"""Adds sparse updates to the variable referenced by `resource`.

  This operation computes

      # Scalar indices
      ref[indices, ...] += updates[...]

      # Vector indices (for each i)
      ref[indices[i], ...] += updates[i, ...]

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] += updates[i, ..., j, ...]

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions add.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterAdd", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_add_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterAdd", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_add_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_add
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterAdd", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_div(resource, indices, updates, name=None):
  r"""Divides sparse updates into the variable referenced by `resource`.

  This operation computes

      # Scalar indices
      ref[indices, ...] /= updates[...]

      # Vector indices (for each i)
      ref[indices[i], ...] /= updates[i, ...]

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] /= updates[i, ..., j, ...]

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions multiply.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterDiv", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_div_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterDiv", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_div_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_div
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterDiv", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_max(resource, indices, updates, name=None):
  r"""Reduces sparse updates into the variable referenced by `resource` using the `max` operation.

  This operation computes

      # Scalar indices
      ref[indices, ...] = max(ref[indices, ...], updates[...])

      # Vector indices (for each i)
      ref[indices[i], ...] = max(ref[indices[i], ...], updates[i, ...])

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] = max(ref[indices[i, ..., j], ...], updates[i, ..., j, ...])

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions are combined.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterMax", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_max_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterMax", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_max_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_max
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterMax", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_min(resource, indices, updates, name=None):
  r"""Reduces sparse updates into the variable referenced by `resource` using the `min` operation.

  This operation computes

      # Scalar indices
      ref[indices, ...] = min(ref[indices, ...], updates[...])

      # Vector indices (for each i)
      ref[indices[i], ...] = min(ref[indices[i], ...], updates[i, ...])

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] = min(ref[indices[i, ..., j], ...], updates[i, ..., j, ...])

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions are combined.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterMin", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_min_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterMin", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_min_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_min
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterMin", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_mul(resource, indices, updates, name=None):
  r"""Multiplies sparse updates into the variable referenced by `resource`.

  This operation computes

      # Scalar indices
      ref[indices, ...] *= updates[...]

      # Vector indices (for each i)
      ref[indices[i], ...] *= updates[i, ...]

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] *= updates[i, ..., j, ...]

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions multiply.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterMul", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_mul_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterMul", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_mul_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_mul
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterMul", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_sub(resource, indices, updates, name=None):
  r"""Subtracts sparse updates from the variable referenced by `resource`.

  This operation computes

      # Scalar indices
      ref[indices, ...] -= updates[...]

      # Vector indices (for each i)
      ref[indices[i], ...] -= updates[i, ...]

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] -= updates[i, ..., j, ...]

  Duplicate entries are handled correctly: if multiple `indices` reference
  the same location, their contributions add.

  Requires `updates.shape = indices.shape + ref.shape[1:]` or `updates.shape = []`.

  <div style="width:70%; margin:auto; margin-bottom:10px; margin-top:20px;">
  <img style="width:100%" src='https://www.tensorflow.org/images/ScatterAdd.png' alt>
  </div>

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterSub", name, _ctx._post_execution_callbacks, resource,
        indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_sub_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterSub", resource=resource, indices=indices,
                              updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_sub_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_sub
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterSub", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_scatter_update(resource, indices, updates, name=None):
  r"""Assigns sparse updates to the variable referenced by `resource`.

  This operation computes

      # Scalar indices
      ref[indices, ...] = updates[...]

      # Vector indices (for each i)
      ref[indices[i], ...] = updates[i, ...]

      # High rank indices (for each i, ..., j)
      ref[indices[i, ..., j], ...] = updates[i, ..., j, ...]

  Args:
    resource: A `Tensor` of type `resource`. Should be from a `Variable` node.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A tensor of indices into the first dimension of `ref`.
    updates: A `Tensor`. A tensor of updated values to add to `ref`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceScatterUpdate", name, _ctx._post_execution_callbacks,
        resource, indices, updates)
      return _result
    except _core._FallbackException:
      try:
        return resource_scatter_update_eager_fallback(
            resource, indices, updates, name=name, ctx=_ctx)
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
        "ResourceScatterUpdate", resource=resource, indices=indices,
                                 updates=updates, name=name)
  return _op
  _result = None
  return _result



def resource_scatter_update_eager_fallback(resource, indices, updates, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_scatter_update
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (updates,) = _execute.args_to_matching_eager([updates], _ctx)
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource, indices, updates]
  _attrs = ("dtype", _attr_dtype, "Tindices", _attr_Tindices)
  _result = _execute.execute(b"ResourceScatterUpdate", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def var_handle_op(dtype, shape, container="", shared_name="", name=None):
  r"""Creates a handle to a Variable resource.

  Args:
    dtype: A `tf.DType`. the type of this variable. Must agree with the dtypes
      of all ops using this variable.
    shape: A `tf.TensorShape` or list of `ints`.
      The (possibly partially specified) shape of this variable.
    container: An optional `string`. Defaults to `""`.
      the container this variable is placed in.
    shared_name: An optional `string`. Defaults to `""`.
      the name by which this variable is referred to.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "VarHandleOp",
        name, _ctx._post_execution_callbacks, "container", container,
        "shared_name", shared_name, "dtype", dtype, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return var_handle_op_eager_fallback(
            container=container, shared_name=shared_name, dtype=dtype,
            shape=shape, name=name, ctx=_ctx)
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
  shape = _execute.make_shape(shape, "shape")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "VarHandleOp", dtype=dtype, shape=shape, container=container,
                       shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "dtype", _op.get_attr("dtype"),
            "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "VarHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def var_handle_op_eager_fallback(dtype, shape, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function var_handle_op
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name, "dtype",
  dtype, "shape", shape)
  _result = _execute.execute(b"VarHandleOp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "VarHandleOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def var_is_initialized_op(resource, name=None):
  r"""Checks whether a resource handle-based variable has been initialized.

  Args:
    resource: A `Tensor` of type `resource`. the input resource handle.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "VarIsInitializedOp", name, _ctx._post_execution_callbacks, resource)
      return _result
    except _core._FallbackException:
      try:
        return var_is_initialized_op_eager_fallback(
            resource, name=name, ctx=_ctx)
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
        "VarIsInitializedOp", resource=resource, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "VarIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def var_is_initialized_op_eager_fallback(resource, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function var_is_initialized_op
  """
  _ctx = ctx if ctx else _context.context()
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource]
  _attrs = None
  _result = _execute.execute(b"VarIsInitializedOp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "VarIsInitializedOp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def variable_shape(input, out_type=_dtypes.int32, name=None):
  r"""Returns the shape of the variable pointed to by `resource`.

  This operation returns a 1-D integer tensor representing the shape of `input`.

  For example:

  ```
  # 't' is [[[1, 1, 1], [2, 2, 2]], [[3, 3, 3], [4, 4, 4]]]
  shape(t) ==> [2, 2, 3]
  ```

  Args:
    input: A `Tensor` of type `resource`.
    out_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `out_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "VariableShape", name, _ctx._post_execution_callbacks, input,
        "out_type", out_type)
      return _result
    except _core._FallbackException:
      try:
        return variable_shape_eager_fallback(
            input, out_type=out_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "VariableShape", input=input, out_type=out_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("out_type", _op.get_attr("out_type"))
  _execute.record_gradient(
      "VariableShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def variable_shape_eager_fallback(input, out_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function variable_shape
  """
  _ctx = ctx if ctx else _context.context()
  if out_type is None:
    out_type = _dtypes.int32
  out_type = _execute.make_type(out_type, "out_type")
  input = _ops.convert_to_tensor(input, _dtypes.resource)
  _inputs_flat = [input]
  _attrs = ("out_type", out_type)
  _result = _execute.execute(b"VariableShape", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "VariableShape", _inputs_flat, _attrs, _result, name)
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
#   name: "AssignAddVariableOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
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
#   name: "AssignSubVariableOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
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
#   name: "AssignVariableOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
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
#   name: "ConsumeMutexLock"
#   input_arg {
#     name: "mutex_lock"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "DestroyResourceOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "ignore_lookup_error"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "MutexLock"
#   input_arg {
#     name: "mutex"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "mutex_lock"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "MutexV2"
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
#   name: "ReadVariableOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
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
#   name: "ResourceGather"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "validate_indices"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterAdd"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterDiv"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterMax"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterMin"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterMul"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterSub"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
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
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceScatterUpdate"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "updates"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#   }
#   attr {
#     name: "Tindices"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "VarHandleOp"
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
#   name: "VarIsInitializedOp"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "is_initialized"
#     type: DT_BOOL
#   }
#   is_stateful: true
# }
# op {
#   name: "VariableShape"
#   input_arg {
#     name: "input"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "output"
#     type_attr: "out_type"
#   }
#   attr {
#     name: "out_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nE\n\023AssignAddVariableOp\022\014\n\010resource\030\024\022\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\nE\n\023AssignSubVariableOp\022\014\n\010resource\030\024\022\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\nB\n\020AssignVariableOp\022\014\n\010resource\030\024\022\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\n%\n\020ConsumeMutexLock\022\016\n\nmutex_lock\030\025\210\001\001\nE\n\021DestroyResourceOp\022\014\n\010resource\030\024\"\037\n\023ignore_lookup_error\022\004bool\032\002(\001\210\001\001\n)\n\tMutexLock\022\t\n\005mutex\030\024\032\016\n\nmutex_lock\030\025\210\001\001\nN\n\007MutexV2\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n@\n\016ReadVariableOp\022\014\n\010resource\030\024\032\016\n\005value\"\005dtype\"\r\n\005dtype\022\004type\210\001\001\n\216\001\n\016ResourceGather\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\032\017\n\006output\"\005dtype\"\034\n\020validate_indices\022\004bool\032\002(\001\"\r\n\005dtype\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterAdd\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterDiv\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterMax\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterMin\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterMul\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\n\214\001\n\022ResourceScatterSub\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"$\n\005dtype\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\nx\n\025ResourceScatterUpdate\022\014\n\010resource\030\024\022\023\n\007indices\"\010Tindices\022\020\n\007updates\"\005dtype\"\r\n\005dtype\022\004type\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\210\001\001\nq\n\013VarHandleOp\032\014\n\010resource\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\210\001\001\n9\n\022VarIsInitializedOp\022\014\n\010resource\030\024\032\022\n\016is_initialized\030\n\210\001\001\nO\n\rVariableShape\022\t\n\005input\030\024\032\022\n\006output\"\010out_type\"\034\n\010out_type\022\004type\032\0020\003:\006\n\0042\002\003\t\210\001\001")
