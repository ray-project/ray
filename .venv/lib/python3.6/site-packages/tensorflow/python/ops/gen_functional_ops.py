"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: functional_ops.cc
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


def fake_param(dtype, shape, name=None):
  r"""  This op is used as a placeholder in If branch functions. It doesn't provide a
  valid output when run, so must either be removed (e.g. replaced with a
  function input) or guaranteed not to be used (e.g. if mirroring an
  intermediate output needed for the gradient computation of the other branch).

  Args:
    dtype: A `tf.DType`. The type of the output.
    shape: A `tf.TensorShape` or list of `ints`.
          The purported shape of the output. This is only used for shape inference;
          the output will not necessarily have this shape. Can be a partial shape.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "FakeParam",
        name, _ctx._post_execution_callbacks, "dtype", dtype, "shape", shape)
      return _result
    except _core._FallbackException:
      try:
        return fake_param_eager_fallback(
            dtype=dtype, shape=shape, name=name, ctx=_ctx)
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "FakeParam", dtype=dtype, shape=shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "shape", _op.get_attr("shape"))
  _execute.record_gradient(
      "FakeParam", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fake_param_eager_fallback(dtype, shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fake_param
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  shape = _execute.make_shape(shape, "shape")
  _inputs_flat = []
  _attrs = ("dtype", dtype, "shape", shape)
  _result = _execute.execute(b"FakeParam", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FakeParam", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _for(start, limit, delta, input, body, name=None):
  r"""  ```python
   output = input;
   for i in range(start, limit, delta)
     output = body(i, output);
  ```

  Args:
    start: A `Tensor` of type `int32`. The lower bound. An int32
    limit: A `Tensor` of type `int32`. The upper bound. An int32
    delta: A `Tensor` of type `int32`. The increment. An int32
    input: A list of `Tensor` objects.
      A list of input tensors whose types are T.
    body: A function decorated with @Defun.
          A function that takes a list of tensors (int32, T) and returns another
          list of tensors (T).
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "For", name,
        _ctx._post_execution_callbacks, start, limit, delta, input, "body",
        body)
      return _result
    except _core._FallbackException:
      try:
        return _for_eager_fallback(
            start, limit, delta, input, body=body, name=name, ctx=_ctx)
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
        "For", start=start, limit=limit, delta=delta, input=input, body=body,
               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "body", _op.get_attr("body"))
  _execute.record_gradient(
      "For", _inputs_flat, _attrs, _result, name)
  return _result



def _for_eager_fallback(start, limit, delta, input, body, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _for
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  start = _ops.convert_to_tensor(start, _dtypes.int32)
  limit = _ops.convert_to_tensor(limit, _dtypes.int32)
  delta = _ops.convert_to_tensor(delta, _dtypes.int32)
  _inputs_flat = [start, limit, delta] + list(input)
  _attrs = ("T", _attr_T, "body", body)
  _result = _execute.execute(b"For", len(input), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "For", _inputs_flat, _attrs, _result, name)
  return _result


def _if(cond, input, Tout, then_branch, else_branch, output_shapes=[], name=None):
  r"""output = cond ? then_branch(input) : else_branch(input)

  Args:
    cond: A `Tensor`.
            A Tensor. If the tensor is a scalar of non-boolean type, the
            scalar is converted to a boolean according to the
            following rule: if the scalar is a numerical value, non-zero means
            `True` and zero means False; if the scalar is a string, non-empty
            means `True` and empty means `False`. If the tensor is not a scalar,
            being empty means False and being non-empty means True.
    input: A list of `Tensor` objects. A list of input tensors.
    Tout: A list of `tf.DTypes`. A list of output types.
    then_branch: A function decorated with @Defun.
            A function that takes 'inputs' and returns a list of tensors, whose
            types are the same as what else_branch returns.
    else_branch: A function decorated with @Defun.
          A function that takes 'inputs' and returns a list of tensors, whose
          types are the same as what then_branch returns.
    output_shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "If", name,
        _ctx._post_execution_callbacks, cond, input, "Tout", Tout,
        "then_branch", then_branch, "else_branch", else_branch,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return _if_eager_fallback(
            cond, input, Tout=Tout, then_branch=then_branch,
            else_branch=else_branch, output_shapes=output_shapes, name=name,
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
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'if' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'if' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "If", cond=cond, input=input, Tout=Tout, then_branch=then_branch,
              else_branch=else_branch, output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("Tcond", _op.get_attr("Tcond"), "Tin", _op.get_attr("Tin"),
            "Tout", _op.get_attr("Tout"), "then_branch",
            _op.get_attr("then_branch"), "else_branch",
            _op.get_attr("else_branch"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "If", _inputs_flat, _attrs, _result, name)
  return _result



def _if_eager_fallback(cond, input, Tout, then_branch, else_branch, output_shapes=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _if
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'if' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'if' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Tcond, (cond,) = _execute.args_to_matching_eager([cond], _ctx)
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = [cond] + list(input)
  _attrs = ("Tcond", _attr_Tcond, "Tin", _attr_Tin, "Tout", Tout,
  "then_branch", then_branch, "else_branch", else_branch, "output_shapes",
  output_shapes)
  _result = _execute.execute(b"If", len(Tout), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "If", _inputs_flat, _attrs, _result, name)
  return _result


def partitioned_call(args, Tout, f, config="", config_proto="", executor_type="", name=None):
  r"""returns `f(inputs)`, where `f`'s body is placed and partitioned.

  Args:
    args: A list of `Tensor` objects. A list of input tensors.
    Tout: A list of `tf.DTypes`. A list of output types.
    f: A function decorated with @Defun.
            A function that takes 'args', a list of tensors, and returns 'output',
            another list of tensors. Input and output types are specified by 'Tin'
            and 'Tout'. The function body of f will be placed and partitioned across
            devices, setting this op apart from the regular Call op.
    config: An optional `string`. Defaults to `""`.
    config_proto: An optional `string`. Defaults to `""`.
    executor_type: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "PartitionedCall", name, _ctx._post_execution_callbacks, args, "Tout",
        Tout, "f", f, "config", config, "config_proto", config_proto,
        "executor_type", executor_type)
      return _result
    except _core._FallbackException:
      try:
        return partitioned_call_eager_fallback(
            args, Tout=Tout, f=f, config=config, config_proto=config_proto,
            executor_type=executor_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'partitioned_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if config is None:
    config = ""
  config = _execute.make_str(config, "config")
  if config_proto is None:
    config_proto = ""
  config_proto = _execute.make_str(config_proto, "config_proto")
  if executor_type is None:
    executor_type = ""
  executor_type = _execute.make_str(executor_type, "executor_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PartitionedCall", args=args, Tout=Tout, f=f, config=config,
                           config_proto=config_proto,
                           executor_type=executor_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"), "f",
            _op.get_attr("f"), "config", _op.get_attr("config"),
            "config_proto", _op.get_attr("config_proto"), "executor_type",
            _op.get_attr("executor_type"))
  _execute.record_gradient(
      "PartitionedCall", _inputs_flat, _attrs, _result, name)
  return _result



def partitioned_call_eager_fallback(args, Tout, f, config="", config_proto="", executor_type="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function partitioned_call
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'partitioned_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if config is None:
    config = ""
  config = _execute.make_str(config, "config")
  if config_proto is None:
    config_proto = ""
  config_proto = _execute.make_str(config_proto, "config_proto")
  if executor_type is None:
    executor_type = ""
  executor_type = _execute.make_str(executor_type, "executor_type")
  _attr_Tin, args = _execute.convert_to_mixed_eager_tensors(args, _ctx)
  _inputs_flat = list(args)
  _attrs = ("Tin", _attr_Tin, "Tout", Tout, "f", f, "config", config,
  "config_proto", config_proto, "executor_type", executor_type)
  _result = _execute.execute(b"PartitionedCall", len(Tout),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "PartitionedCall", _inputs_flat, _attrs, _result, name)
  return _result


def remote_call(target, args, Tout, f, name=None):
  r"""Runs function `f` on a remote device indicated by `target`.

  Args:
    target: A `Tensor` of type `string`.
      A fully specified device name where we want to run the function.
    args: A list of `Tensor` objects. A list of arguments for the function.
    Tout: A list of `tf.DTypes` that has length `>= 1`.
      The type list for the return values.
    f: A function decorated with @Defun. The function to run remotely.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RemoteCall",
        name, _ctx._post_execution_callbacks, target, args, "Tout", Tout, "f",
        f)
      return _result
    except _core._FallbackException:
      try:
        return remote_call_eager_fallback(
            target, args, Tout=Tout, f=f, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'remote_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "RemoteCall", target=target, args=args, Tout=Tout, f=f, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"), "f",
            _op.get_attr("f"))
  _execute.record_gradient(
      "RemoteCall", _inputs_flat, _attrs, _result, name)
  return _result



def remote_call_eager_fallback(target, args, Tout, f, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function remote_call
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'remote_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tin, args = _execute.convert_to_mixed_eager_tensors(args, _ctx)
  target = _ops.convert_to_tensor(target, _dtypes.string)
  _inputs_flat = [target] + list(args)
  _attrs = ("Tin", _attr_Tin, "Tout", Tout, "f", f)
  _result = _execute.execute(b"RemoteCall", len(Tout), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RemoteCall", _inputs_flat, _attrs, _result, name)
  return _result


def stateful_partitioned_call(args, Tout, f, config="", config_proto="", executor_type="", name=None):
  r"""returns `f(inputs)`, where `f`'s body is placed and partitioned.

  Args:
    args: A list of `Tensor` objects. A list of input tensors.
    Tout: A list of `tf.DTypes`. A list of output types.
    f: A function decorated with @Defun.
            A function that takes 'args', a list of tensors, and returns 'output',
            another list of tensors. Input and output types are specified by 'Tin'
            and 'Tout'. The function body of f will be placed and partitioned across
            devices, setting this op apart from the regular Call op. This op is
            stateful.
    config: An optional `string`. Defaults to `""`.
    config_proto: An optional `string`. Defaults to `""`.
    executor_type: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatefulPartitionedCall", name, _ctx._post_execution_callbacks, args,
        "Tout", Tout, "f", f, "config", config, "config_proto", config_proto,
        "executor_type", executor_type)
      return _result
    except _core._FallbackException:
      try:
        return stateful_partitioned_call_eager_fallback(
            args, Tout=Tout, f=f, config=config, config_proto=config_proto,
            executor_type=executor_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'stateful_partitioned_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if config is None:
    config = ""
  config = _execute.make_str(config, "config")
  if config_proto is None:
    config_proto = ""
  config_proto = _execute.make_str(config_proto, "config_proto")
  if executor_type is None:
    executor_type = ""
  executor_type = _execute.make_str(executor_type, "executor_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatefulPartitionedCall", args=args, Tout=Tout, f=f, config=config,
                                   config_proto=config_proto,
                                   executor_type=executor_type, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"), "f",
            _op.get_attr("f"), "config", _op.get_attr("config"),
            "config_proto", _op.get_attr("config_proto"), "executor_type",
            _op.get_attr("executor_type"))
  _execute.record_gradient(
      "StatefulPartitionedCall", _inputs_flat, _attrs, _result, name)
  return _result



def stateful_partitioned_call_eager_fallback(args, Tout, f, config="", config_proto="", executor_type="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateful_partitioned_call
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'stateful_partitioned_call' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  if config is None:
    config = ""
  config = _execute.make_str(config, "config")
  if config_proto is None:
    config_proto = ""
  config_proto = _execute.make_str(config_proto, "config_proto")
  if executor_type is None:
    executor_type = ""
  executor_type = _execute.make_str(executor_type, "executor_type")
  _attr_Tin, args = _execute.convert_to_mixed_eager_tensors(args, _ctx)
  _inputs_flat = list(args)
  _attrs = ("Tin", _attr_Tin, "Tout", Tout, "f", f, "config", config,
  "config_proto", config_proto, "executor_type", executor_type)
  _result = _execute.execute(b"StatefulPartitionedCall", len(Tout),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatefulPartitionedCall", _inputs_flat, _attrs, _result, name)
  return _result


def stateless_if(cond, input, Tout, then_branch, else_branch, name=None):
  r"""output = cond ? then_branch(input) : else_branch(input)

  Args:
    cond: A `Tensor`.
            A Tensor. If the tensor is a scalar of non-boolean type, the
            scalar is converted to a boolean according to the
            following rule: if the scalar is a numerical value, non-zero means
            `True` and zero means False; if the scalar is a string, non-empty
            means `True` and empty means `False`. If the tensor is not a scalar,
            being empty means False and being non-empty means True.

            This should only be used when the if then/else body functions do not
            have stateful ops.
    input: A list of `Tensor` objects. A list of input tensors.
    Tout: A list of `tf.DTypes`. A list of output types.
    then_branch: A function decorated with @Defun.
            A function that takes 'inputs' and returns a list of tensors, whose
            types are the same as what else_branch returns.
    else_branch: A function decorated with @Defun.
          A function that takes 'inputs' and returns a list of tensors, whose
          types are the same as what then_branch returns.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StatelessIf",
        name, _ctx._post_execution_callbacks, cond, input, "Tout", Tout,
        "then_branch", then_branch, "else_branch", else_branch)
      return _result
    except _core._FallbackException:
      try:
        return stateless_if_eager_fallback(
            cond, input, Tout=Tout, then_branch=then_branch,
            else_branch=else_branch, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'stateless_if' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "StatelessIf", cond=cond, input=input, Tout=Tout,
                       then_branch=then_branch, else_branch=else_branch,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tcond", _op.get_attr("Tcond"), "Tin", _op.get_attr("Tin"),
            "Tout", _op.get_attr("Tout"), "then_branch",
            _op.get_attr("then_branch"), "else_branch",
            _op.get_attr("else_branch"))
  _execute.record_gradient(
      "StatelessIf", _inputs_flat, _attrs, _result, name)
  return _result



def stateless_if_eager_fallback(cond, input, Tout, then_branch, else_branch, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_if
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'stateless_if' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tcond, (cond,) = _execute.args_to_matching_eager([cond], _ctx)
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = [cond] + list(input)
  _attrs = ("Tcond", _attr_Tcond, "Tin", _attr_Tin, "Tout", Tout,
  "then_branch", then_branch, "else_branch", else_branch)
  _result = _execute.execute(b"StatelessIf", len(Tout), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StatelessIf", _inputs_flat, _attrs, _result, name)
  return _result


def stateless_while(input, cond, body, name=None):
  r"""output = input; While (Cond(output)) { output = Body(output) }

  Args:
    input: A list of `Tensor` objects.
      A list of input tensors whose types are T.
    cond: A function decorated with @Defun.
            A function takes 'input' and returns a tensor.  If the tensor is
            a scalar of non-boolean, the scalar is converted to a boolean
            according to the following rule: if the scalar is a numerical
            value, non-zero means True and zero means False; if the scalar is
            a string, non-empty means True and empty means False. If the
            tensor is not a scalar, non-emptiness means True and False
            otherwise.

            This should only be used when the while condition and body functions
            do not have stateful ops.
    body: A function decorated with @Defun.
            A function that takes a list of tensors and returns another
            list of tensors. Both lists have the same types as specified
            by T.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StatelessWhile", name, _ctx._post_execution_callbacks, input, "cond",
        cond, "body", body)
      return _result
    except _core._FallbackException:
      try:
        return stateless_while_eager_fallback(
            input, cond=cond, body=body, name=name, ctx=_ctx)
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
        "StatelessWhile", input=input, cond=cond, body=body, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "cond", _op.get_attr("cond"), "body",
            _op.get_attr("body"))
  _execute.record_gradient(
      "StatelessWhile", _inputs_flat, _attrs, _result, name)
  return _result



def stateless_while_eager_fallback(input, cond, body, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function stateless_while
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("T", _attr_T, "cond", cond, "body", body)
  _result = _execute.execute(b"StatelessWhile", len(input),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StatelessWhile", _inputs_flat, _attrs, _result, name)
  return _result


def symbolic_gradient(input, Tout, f, name=None):
  r"""Computes the gradient function for function f via backpropagation.

  Args:
    input: A list of `Tensor` objects. a list of input tensors of size N + M;
    Tout: A list of `tf.DTypes` that has length `>= 1`.
      the type list for the input list.
    f: A function decorated with @Defun.
      The function we want to compute the gradient for.

      The function 'f' must be a numerical function which takes N inputs and
      produces M outputs. Its gradient function 'g', which is computed by
      this SymbolicGradient op is a function taking N + M inputs and
      produces N outputs.

      I.e. if we have
         (y1, y2, ..., y_M) = f(x1, x2, ..., x_N),
      then, g is
         (dL/dx1, dL/dx2, ..., dL/dx_N) = g(x1, x2, ..., x_N,
                                           dL/dy1, dL/dy2, ..., dL/dy_M),

      where L is a scalar-value function of (x1, x2, ..., xN) (e.g., the
      loss function). dL/dx_i is the partial derivative of L with respect
      to x_i.

      (Needs some math expert to say the comment above better.)
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `Tout`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SymbolicGradient", name, _ctx._post_execution_callbacks, input,
        "Tout", Tout, "f", f)
      return _result
    except _core._FallbackException:
      try:
        return symbolic_gradient_eager_fallback(
            input, Tout=Tout, f=f, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'symbolic_gradient' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _, _, _op = _op_def_lib._apply_op_helper(
        "SymbolicGradient", input=input, Tout=Tout, f=f, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"), "f",
            _op.get_attr("f"))
  _execute.record_gradient(
      "SymbolicGradient", _inputs_flat, _attrs, _result, name)
  return _result



def symbolic_gradient_eager_fallback(input, Tout, f, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function symbolic_gradient
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(Tout, (list, tuple)):
    raise TypeError(
        "Expected list for 'Tout' argument to "
        "'symbolic_gradient' Op, not %r." % Tout)
  Tout = [_execute.make_type(_t, "Tout") for _t in Tout]
  _attr_Tin, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("Tin", _attr_Tin, "Tout", Tout, "f", f)
  _result = _execute.execute(b"SymbolicGradient", len(Tout),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SymbolicGradient", _inputs_flat, _attrs, _result, name)
  return _result


def _while(input, cond, body, output_shapes=[], name=None):
  r"""output = input; While (Cond(output)) { output = Body(output) }

  Args:
    input: A list of `Tensor` objects.
      A list of input tensors whose types are T.
    cond: A function decorated with @Defun.
            A function takes 'input' and returns a tensor.  If the tensor is
            a scalar of non-boolean, the scalar is converted to a boolean
            according to the following rule: if the scalar is a numerical
            value, non-zero means True and zero means False; if the scalar is
            a string, non-empty means True and empty means False. If the
            tensor is not a scalar, non-emptiness means True and False
            otherwise.
    body: A function decorated with @Defun.
            A function that takes a list of tensors and returns another
            list of tensors. Both lists have the same types as specified
            by T.
    output_shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "While", name,
        _ctx._post_execution_callbacks, input, "cond", cond, "body", body,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return _while_eager_fallback(
            input, cond=cond, body=body, output_shapes=output_shapes,
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
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'while' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "While", input=input, cond=cond, body=body,
                 output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "cond", _op.get_attr("cond"), "body",
            _op.get_attr("body"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "While", _inputs_flat, _attrs, _result, name)
  return _result



def _while_eager_fallback(input, cond, body, output_shapes=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _while
  """
  _ctx = ctx if ctx else _context.context()
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'while' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_T, input = _execute.convert_to_mixed_eager_tensors(input, _ctx)
  _inputs_flat = list(input)
  _attrs = ("T", _attr_T, "cond", cond, "body", body, "output_shapes",
  output_shapes)
  _result = _execute.execute(b"While", len(input), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "While", _inputs_flat, _attrs, _result, name)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "FakeParam"
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
# }
# op {
#   name: "For"
#   input_arg {
#     name: "start"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "limit"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "delta"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "input"
#     type_list_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "body"
#     type: "func"
#   }
# }
# op {
#   name: "If"
#   input_arg {
#     name: "cond"
#     type_attr: "Tcond"
#   }
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tcond"
#     type: "type"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "then_branch"
#     type: "func"
#   }
#   attr {
#     name: "else_branch"
#     type: "func"
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "PartitionedCall"
#   input_arg {
#     name: "args"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "config_proto"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "executor_type"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "RemoteCall"
#   input_arg {
#     name: "target"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "args"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   is_stateful: true
# }
# op {
#   name: "StatefulPartitionedCall"
#   input_arg {
#     name: "args"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "config"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "config_proto"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "executor_type"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "StatelessIf"
#   input_arg {
#     name: "cond"
#     type_attr: "Tcond"
#   }
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tcond"
#     type: "type"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "then_branch"
#     type: "func"
#   }
#   attr {
#     name: "else_branch"
#     type: "func"
#   }
# }
# op {
#   name: "StatelessWhile"
#   input_arg {
#     name: "input"
#     type_list_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "cond"
#     type: "func"
#   }
#   attr {
#     name: "body"
#     type: "func"
#   }
# }
# op {
#   name: "SymbolicGradient"
#   input_arg {
#     name: "input"
#     type_list_attr: "Tin"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "Tout"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
# }
# op {
#   name: "While"
#   input_arg {
#     name: "input"
#     type_list_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "cond"
#     type: "func"
#   }
#   attr {
#     name: "body"
#     type: "func"
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n;\n\tFakeParam\032\017\n\006output\"\005dtype\"\r\n\005dtype\022\004type\"\016\n\005shape\022\005shape\n`\n\003For\022\t\n\005start\030\003\022\t\n\005limit\030\003\022\t\n\005delta\030\003\022\n\n\005input2\001T\032\013\n\006output2\001T\"\021\n\001T\022\nlist(type)(\001\"\014\n\004body\022\004func\n\272\001\n\002If\022\r\n\004cond\"\005Tcond\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\r\n\005Tcond\022\004type\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\"\023\n\013then_branch\022\004func\"\023\n\013else_branch\022\004func\" \n\routput_shapes\022\013list(shape)\032\002\n\000\210\001\001\n\263\001\n\017PartitionedCall\022\013\n\004args2\003Tin\032\016\n\006output2\004Tout\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\"\t\n\001f\022\004func\"\024\n\006config\022\006string\032\002\022\000\"\032\n\014config_proto\022\006string\032\002\022\000\"\033\n\rexecutor_type\022\006string\032\002\022\000\nr\n\nRemoteCall\022\n\n\006target\030\007\022\013\n\004args2\003Tin\032\016\n\006output2\004Tout\"\025\n\003Tin\022\nlist(type)(\0010\001\"\026\n\004Tout\022\nlist(type)(\0010\001\"\t\n\001f\022\004func\210\001\001\n\276\001\n\027StatefulPartitionedCall\022\013\n\004args2\003Tin\032\016\n\006output2\004Tout\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\"\t\n\001f\022\004func\"\024\n\006config\022\006string\032\002\022\000\"\032\n\014config_proto\022\006string\032\002\022\000\"\033\n\rexecutor_type\022\006string\032\002\022\000\210\001\001\n\236\001\n\013StatelessIf\022\r\n\004cond\"\005Tcond\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\r\n\005Tcond\022\004type\"\023\n\003Tin\022\nlist(type)(\001\"\024\n\004Tout\022\nlist(type)(\001\"\023\n\013then_branch\022\004func\"\023\n\013else_branch\022\004func\nX\n\016StatelessWhile\022\n\n\005input2\001T\032\013\n\006output2\001T\"\021\n\001T\022\nlist(type)(\001\"\014\n\004cond\022\004func\"\014\n\004body\022\004func\nj\n\020SymbolicGradient\022\014\n\005input2\003Tin\032\016\n\006output2\004Tout\"\025\n\003Tin\022\nlist(type)(\0010\001\"\026\n\004Tout\022\nlist(type)(\0010\001\"\t\n\001f\022\004func\nt\n\005While\022\n\n\005input2\001T\032\013\n\006output2\001T\"\021\n\001T\022\nlist(type)(\001\"\014\n\004cond\022\004func\"\014\n\004body\022\004func\" \n\routput_shapes\022\013list(shape)\032\002\n\000\210\001\001")
