"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gru_ops.cc
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


_gru_block_cell_outputs = ["r", "u", "c", "h"]
_GRUBlockCellOutput = _collections.namedtuple(
    "GRUBlockCell", _gru_block_cell_outputs)


@_dispatch.add_dispatch_list
@tf_export('gru_block_cell')
def gru_block_cell(x, h_prev, w_ru, w_c, b_ru, b_c, name=None):
  r"""Computes the GRU cell forward propagation for 1 time step.

  Args
      x: Input to the GRU cell.
      h_prev: State input from the previous GRU cell.
      w_ru: Weight matrix for the reset and update gate.
      w_c: Weight matrix for the cell connection gate.
      b_ru: Bias vector for the reset and update gate.
      b_c: Bias vector for the cell connection gate.

  Returns
      r: Output of the reset gate.
      u: Output of the update gate.
      c: Output of the cell connection gate.
      h: Current state of the GRU cell.

  Note on notation of the variables:

  Concatenation of a and b is represented by a_b
  Element-wise dot product of a and b is represented by ab
  Element-wise dot product is represented by \circ
  Matrix multiplication is represented by *

  Biases are initialized with :
  `b_ru` - constant_initializer(1.0)
  `b_c` - constant_initializer(0.0)

  This kernel op implements the following mathematical equations:

  ```
  x_h_prev = [x, h_prev]

  [r_bar u_bar] = x_h_prev * w_ru + b_ru

  r = sigmoid(r_bar)
  u = sigmoid(u_bar)

  h_prevr = h_prev \circ r

  x_h_prevr = [x h_prevr]

  c_bar = x_h_prevr * w_c + b_c
  c = tanh(c_bar)

  h = (1-u) \circ c + u \circ h_prev
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`.
    h_prev: A `Tensor`. Must have the same type as `x`.
    w_ru: A `Tensor`. Must have the same type as `x`.
    w_c: A `Tensor`. Must have the same type as `x`.
    b_ru: A `Tensor`. Must have the same type as `x`.
    b_c: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (r, u, c, h).

    r: A `Tensor`. Has the same type as `x`.
    u: A `Tensor`. Has the same type as `x`.
    c: A `Tensor`. Has the same type as `x`.
    h: A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "GRUBlockCell",
        name, _ctx._post_execution_callbacks, x, h_prev, w_ru, w_c, b_ru, b_c)
      _result = _GRUBlockCellOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return gru_block_cell_eager_fallback(
            x, h_prev, w_ru, w_c, b_ru, b_c, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gru_block_cell, x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c,
                              b_ru=b_ru, b_c=b_c, name=name)
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
        "GRUBlockCell", x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c, b_ru=b_ru,
                        b_c=b_c, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gru_block_cell, x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c, b_ru=b_ru,
                          b_c=b_c, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GRUBlockCell", _inputs_flat, _attrs, _result, name)
  _result = _GRUBlockCellOutput._make(_result)
  return _result



def gru_block_cell_eager_fallback(x, h_prev, w_ru, w_c, b_ru, b_c, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gru_block_cell
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, h_prev, w_ru, w_c, b_ru, b_c], _ctx)
  (x, h_prev, w_ru, w_c, b_ru, b_c) = _inputs_T
  _inputs_flat = [x, h_prev, w_ru, w_c, b_ru, b_c]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GRUBlockCell", 4, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GRUBlockCell", _inputs_flat, _attrs, _result, name)
  _result = _GRUBlockCellOutput._make(_result)
  return _result

_ops.RegisterShape("GRUBlockCell")(None)


_gru_block_cell_grad_outputs = ["d_x", "d_h_prev", "d_c_bar", "d_r_bar_u_bar"]
_GRUBlockCellGradOutput = _collections.namedtuple(
    "GRUBlockCellGrad", _gru_block_cell_grad_outputs)


@_dispatch.add_dispatch_list
@tf_export('gru_block_cell_grad')
def gru_block_cell_grad(x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h, name=None):
  r"""Computes the GRU cell back-propagation for 1 time step.

  Args
      x: Input to the GRU cell.
      h_prev: State input from the previous GRU cell.
      w_ru: Weight matrix for the reset and update gate.
      w_c: Weight matrix for the cell connection gate.
      b_ru: Bias vector for the reset and update gate.
      b_c: Bias vector for the cell connection gate.
      r: Output of the reset gate.
      u: Output of the update gate.
      c: Output of the cell connection gate.
      d_h: Gradients of the h_new wrt to objective function.

  Returns
      d_x: Gradients of the x wrt to objective function.
      d_h_prev: Gradients of the h wrt to objective function.
      d_c_bar Gradients of the c_bar wrt to objective function.
      d_r_bar_u_bar Gradients of the r_bar & u_bar wrt to objective function.

  This kernel op implements the following mathematical equations:

  Note on notation of the variables:

  Concatenation of a and b is represented by a_b
  Element-wise dot product of a and b is represented by ab
  Element-wise dot product is represented by \circ
  Matrix multiplication is represented by *

  Additional notes for clarity:

  `w_ru` can be segmented into 4 different matrices.
  ```
  w_ru = [w_r_x w_u_x
          w_r_h_prev w_u_h_prev]
  ```
  Similarly, `w_c` can be segmented into 2 different matrices.
  ```
  w_c = [w_c_x w_c_h_prevr]
  ```
  Same goes for biases.
  ```
  b_ru = [b_ru_x b_ru_h]
  b_c = [b_c_x b_c_h]
  ```
  Another note on notation:
  ```
  d_x = d_x_component_1 + d_x_component_2

  where d_x_component_1 = d_r_bar * w_r_x^T + d_u_bar * w_r_x^T
  and d_x_component_2 = d_c_bar * w_c_x^T

  d_h_prev = d_h_prev_component_1 + d_h_prevr \circ r + d_h \circ u
  where d_h_prev_componenet_1 = d_r_bar * w_r_h_prev^T + d_u_bar * w_r_h_prev^T
  ```

  Mathematics behind the Gradients below:
  ```
  d_c_bar = d_h \circ (1-u) \circ (1-c \circ c)
  d_u_bar = d_h \circ (h-c) \circ u \circ (1-u)

  d_r_bar_u_bar = [d_r_bar d_u_bar]

  [d_x_component_1 d_h_prev_component_1] = d_r_bar_u_bar * w_ru^T

  [d_x_component_2 d_h_prevr] = d_c_bar * w_c^T

  d_x = d_x_component_1 + d_x_component_2

  d_h_prev = d_h_prev_component_1 + d_h_prevr \circ r + u
  ```
  Below calculation is performed in the python wrapper for the Gradients
  (not in the gradient kernel.)
  ```
  d_w_ru = x_h_prevr^T * d_c_bar

  d_w_c = x_h_prev^T * d_r_bar_u_bar

  d_b_ru = sum of d_r_bar_u_bar along axis = 0

  d_b_c = sum of d_c_bar along axis = 0
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `float32`.
    h_prev: A `Tensor`. Must have the same type as `x`.
    w_ru: A `Tensor`. Must have the same type as `x`.
    w_c: A `Tensor`. Must have the same type as `x`.
    b_ru: A `Tensor`. Must have the same type as `x`.
    b_c: A `Tensor`. Must have the same type as `x`.
    r: A `Tensor`. Must have the same type as `x`.
    u: A `Tensor`. Must have the same type as `x`.
    c: A `Tensor`. Must have the same type as `x`.
    d_h: A `Tensor`. Must have the same type as `x`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (d_x, d_h_prev, d_c_bar, d_r_bar_u_bar).

    d_x: A `Tensor`. Has the same type as `x`.
    d_h_prev: A `Tensor`. Has the same type as `x`.
    d_c_bar: A `Tensor`. Has the same type as `x`.
    d_r_bar_u_bar: A `Tensor`. Has the same type as `x`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GRUBlockCellGrad", name, _ctx._post_execution_callbacks, x, h_prev,
        w_ru, w_c, b_ru, b_c, r, u, c, d_h)
      _result = _GRUBlockCellGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return gru_block_cell_grad_eager_fallback(
            x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              gru_block_cell_grad, x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c,
                                   b_ru=b_ru, b_c=b_c, r=r, u=u, c=c, d_h=d_h,
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
        "GRUBlockCellGrad", x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c, b_ru=b_ru,
                            b_c=b_c, r=r, u=u, c=c, d_h=d_h, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          gru_block_cell_grad, x=x, h_prev=h_prev, w_ru=w_ru, w_c=w_c,
                               b_ru=b_ru, b_c=b_c, r=r, u=u, c=c, d_h=d_h,
                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "GRUBlockCellGrad", _inputs_flat, _attrs, _result, name)
  _result = _GRUBlockCellGradOutput._make(_result)
  return _result



def gru_block_cell_grad_eager_fallback(x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function gru_block_cell_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h], _ctx)
  (x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h) = _inputs_T
  _inputs_flat = [x, h_prev, w_ru, w_c, b_ru, b_c, r, u, c, d_h]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"GRUBlockCellGrad", 4, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GRUBlockCellGrad", _inputs_flat, _attrs, _result, name)
  _result = _GRUBlockCellGradOutput._make(_result)
  return _result

_ops.RegisterShape("GRUBlockCellGrad")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "GRUBlockCell"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w_ru"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w_c"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_ru"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_c"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "r"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "u"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "c"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "h"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "GRUBlockCellGrad"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w_ru"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w_c"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_ru"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b_c"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "r"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "u"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "c"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "d_h"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_x"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_h_prev"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_c_bar"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "d_r_bar_u_bar"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\177\n\014GRUBlockCell\022\006\n\001x\"\001T\022\013\n\006h_prev\"\001T\022\t\n\004w_ru\"\001T\022\010\n\003w_c\"\001T\022\t\n\004b_ru\"\001T\022\010\n\003b_c\"\001T\032\006\n\001r\"\001T\032\006\n\001u\"\001T\032\006\n\001c\"\001T\032\006\n\001h\"\001T\"\020\n\001T\022\004type:\005\n\0032\001\001\n\300\001\n\020GRUBlockCellGrad\022\006\n\001x\"\001T\022\013\n\006h_prev\"\001T\022\t\n\004w_ru\"\001T\022\010\n\003w_c\"\001T\022\t\n\004b_ru\"\001T\022\010\n\003b_c\"\001T\022\006\n\001r\"\001T\022\006\n\001u\"\001T\022\006\n\001c\"\001T\022\010\n\003d_h\"\001T\032\010\n\003d_x\"\001T\032\r\n\010d_h_prev\"\001T\032\014\n\007d_c_bar\"\001T\032\022\n\rd_r_bar_u_bar\"\001T\"\020\n\001T\022\004type:\005\n\0032\001\001")
