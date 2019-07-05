"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: lstm_ops.cc
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


_block_lstm_outputs = ["i", "cs", "f", "o", "ci", "co", "h"]
_BlockLSTMOutput = _collections.namedtuple(
    "BlockLSTM", _block_lstm_outputs)


@_dispatch.add_dispatch_list
@tf_export('block_lstm')
def block_lstm(seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, forget_bias=1, cell_clip=3, use_peephole=False, name=None):
  r"""Computes the LSTM cell forward propagation for all the time steps.

  This is equivalent to applying LSTMBlockCell in a loop, like so:

  ```python
  for x1 in unpack(x):
    i1, cs1, f1, o1, ci1, co1, h1 = LSTMBlock(
      x1, cs_prev, h_prev, w, wci, wcf, wco, b)
    cs_prev = cs1
    h_prev = h1
    i.append(i1)
    cs.append(cs1)
    f.append(f1)
    o.append(o1)
    ci.append(ci1)
    co.append(co1)
    h.append(h1)
  return pack(i), pack(cs), pack(f), pack(o), pack(ci), pack(ch), pack(h)
  ```

  Args:
    seq_len_max: A `Tensor` of type `int64`.
      Maximum time length actually used by this input. Outputs are padded
      with zeros beyond this length.
    x: A `Tensor`. Must be one of the following types: `half`, `float32`.
      The sequence input to the LSTM, shape (timelen, batch_size, num_inputs).
    cs_prev: A `Tensor`. Must have the same type as `x`.
      Value of the initial cell state.
    h_prev: A `Tensor`. Must have the same type as `x`.
      Initial output of cell (to be used for peephole).
    w: A `Tensor`. Must have the same type as `x`. The weight matrix.
    wci: A `Tensor`. Must have the same type as `x`.
      The weight matrix for input gate peephole connection.
    wcf: A `Tensor`. Must have the same type as `x`.
      The weight matrix for forget gate peephole connection.
    wco: A `Tensor`. Must have the same type as `x`.
      The weight matrix for output gate peephole connection.
    b: A `Tensor`. Must have the same type as `x`. The bias vector.
    forget_bias: An optional `float`. Defaults to `1`. The forget gate bias.
    cell_clip: An optional `float`. Defaults to `3`.
      Value to clip the 'cs' value to.
    use_peephole: An optional `bool`. Defaults to `False`.
      Whether to use peephole weights.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (i, cs, f, o, ci, co, h).

    i: A `Tensor`. Has the same type as `x`. The input gate over the whole time sequence.
    cs: A `Tensor`. Has the same type as `x`. The cell state before the tanh over the whole time sequence.
    f: A `Tensor`. Has the same type as `x`. The forget gate over the whole time sequence.
    o: A `Tensor`. Has the same type as `x`. The output gate over the whole time sequence.
    ci: A `Tensor`. Has the same type as `x`. The cell input over the whole time sequence.
    co: A `Tensor`. Has the same type as `x`. The cell after the tanh over the whole time sequence.
    h: A `Tensor`. Has the same type as `x`. The output h vector over the whole time sequence.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "BlockLSTM",
        name, _ctx._post_execution_callbacks, seq_len_max, x, cs_prev, h_prev,
        w, wci, wcf, wco, b, "forget_bias", forget_bias, "cell_clip",
        cell_clip, "use_peephole", use_peephole)
      _result = _BlockLSTMOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return block_lstm_eager_fallback(
            seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b,
            forget_bias=forget_bias, cell_clip=cell_clip,
            use_peephole=use_peephole, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              block_lstm, seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                          h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco, b=b,
                          forget_bias=forget_bias, cell_clip=cell_clip,
                          use_peephole=use_peephole, name=name)
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
  if forget_bias is None:
    forget_bias = 1
  forget_bias = _execute.make_float(forget_bias, "forget_bias")
  if cell_clip is None:
    cell_clip = 3
  cell_clip = _execute.make_float(cell_clip, "cell_clip")
  if use_peephole is None:
    use_peephole = False
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BlockLSTM", seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                     h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco, b=b,
                     forget_bias=forget_bias, cell_clip=cell_clip,
                     use_peephole=use_peephole, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          block_lstm, seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                      h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco, b=b,
                      forget_bias=forget_bias, cell_clip=cell_clip,
                      use_peephole=use_peephole, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("forget_bias", _op.get_attr("forget_bias"), "cell_clip",
            _op.get_attr("cell_clip"), "use_peephole",
            _op.get_attr("use_peephole"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "BlockLSTM", _inputs_flat, _attrs, _result, name)
  _result = _BlockLSTMOutput._make(_result)
  return _result



def block_lstm_eager_fallback(seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, forget_bias=1, cell_clip=3, use_peephole=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function block_lstm
  """
  _ctx = ctx if ctx else _context.context()
  if forget_bias is None:
    forget_bias = 1
  forget_bias = _execute.make_float(forget_bias, "forget_bias")
  if cell_clip is None:
    cell_clip = 3
  cell_clip = _execute.make_float(cell_clip, "cell_clip")
  if use_peephole is None:
    use_peephole = False
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, cs_prev, h_prev, w, wci, wcf, wco, b], _ctx)
  (x, cs_prev, h_prev, w, wci, wcf, wco, b) = _inputs_T
  seq_len_max = _ops.convert_to_tensor(seq_len_max, _dtypes.int64)
  _inputs_flat = [seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b]
  _attrs = ("forget_bias", forget_bias, "cell_clip", cell_clip,
  "use_peephole", use_peephole, "T", _attr_T)
  _result = _execute.execute(b"BlockLSTM", 7, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BlockLSTM", _inputs_flat, _attrs, _result, name)
  _result = _BlockLSTMOutput._make(_result)
  return _result

_ops.RegisterShape("BlockLSTM")(None)


_block_lstm_grad_outputs = ["x_grad", "cs_prev_grad", "h_prev_grad", "w_grad",
                           "wci_grad", "wcf_grad", "wco_grad", "b_grad"]
_BlockLSTMGradOutput = _collections.namedtuple(
    "BlockLSTMGrad", _block_lstm_grad_outputs)


@_dispatch.add_dispatch_list
@tf_export('block_lstm_grad')
def block_lstm_grad(seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad, h_grad, use_peephole, name=None):
  r"""Computes the LSTM cell backward propagation for the entire time sequence.

  This implementation is to be used in conjunction of LSTMBlock.

  Args:
    seq_len_max: A `Tensor` of type `int64`.
      Maximum time length actually used by this input. Outputs are padded
      with zeros beyond this length.
    x: A `Tensor`. Must be one of the following types: `half`, `float32`.
      The sequence input to the LSTM, shape (timelen, batch_size, num_inputs).
    cs_prev: A `Tensor`. Must have the same type as `x`.
      Value of the initial cell state.
    h_prev: A `Tensor`. Must have the same type as `x`.
      Initial output of cell (to be used for peephole).
    w: A `Tensor`. Must have the same type as `x`. The weight matrix.
    wci: A `Tensor`. Must have the same type as `x`.
      The weight matrix for input gate peephole connection.
    wcf: A `Tensor`. Must have the same type as `x`.
      The weight matrix for forget gate peephole connection.
    wco: A `Tensor`. Must have the same type as `x`.
      The weight matrix for output gate peephole connection.
    b: A `Tensor`. Must have the same type as `x`. The bias vector.
    i: A `Tensor`. Must have the same type as `x`.
      The input gate over the whole time sequence.
    cs: A `Tensor`. Must have the same type as `x`.
      The cell state before the tanh over the whole time sequence.
    f: A `Tensor`. Must have the same type as `x`.
      The forget gate over the whole time sequence.
    o: A `Tensor`. Must have the same type as `x`.
      The output gate over the whole time sequence.
    ci: A `Tensor`. Must have the same type as `x`.
      The cell input over the whole time sequence.
    co: A `Tensor`. Must have the same type as `x`.
      The cell after the tanh over the whole time sequence.
    h: A `Tensor`. Must have the same type as `x`.
      The output h vector over the whole time sequence.
    cs_grad: A `Tensor`. Must have the same type as `x`.
      The current gradient of cs.
    h_grad: A `Tensor`. Must have the same type as `x`.
      The gradient of h vector.
    use_peephole: A `bool`. Whether to use peephole weights.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (x_grad, cs_prev_grad, h_prev_grad, w_grad, wci_grad, wcf_grad, wco_grad, b_grad).

    x_grad: A `Tensor`. Has the same type as `x`. The gradient of x to be back-propped.
    cs_prev_grad: A `Tensor`. Has the same type as `x`. The gradient of cs_prev to be back-propped.
    h_prev_grad: A `Tensor`. Has the same type as `x`. The gradient of h_prev to be back-propped.
    w_grad: A `Tensor`. Has the same type as `x`. The gradient for w to be back-propped.
    wci_grad: A `Tensor`. Has the same type as `x`. The gradient for wci to be back-propped.
    wcf_grad: A `Tensor`. Has the same type as `x`. The gradient for wcf to be back-propped.
    wco_grad: A `Tensor`. Has the same type as `x`. The gradient for wco to be back-propped.
    b_grad: A `Tensor`. Has the same type as `x`. The gradient for w to be back-propped.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BlockLSTMGrad", name, _ctx._post_execution_callbacks, seq_len_max, x,
        cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad,
        h_grad, "use_peephole", use_peephole)
      _result = _BlockLSTMGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return block_lstm_grad_eager_fallback(
            seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o,
            ci, co, h, cs_grad, h_grad, use_peephole=use_peephole, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              block_lstm_grad, seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                               h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco,
                               b=b, i=i, cs=cs, f=f, o=o, ci=ci, co=co, h=h,
                               cs_grad=cs_grad, h_grad=h_grad,
                               use_peephole=use_peephole, name=name)
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
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BlockLSTMGrad", seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                         h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco, b=b,
                         i=i, cs=cs, f=f, o=o, ci=ci, co=co, h=h,
                         cs_grad=cs_grad, h_grad=h_grad,
                         use_peephole=use_peephole, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          block_lstm_grad, seq_len_max=seq_len_max, x=x, cs_prev=cs_prev,
                           h_prev=h_prev, w=w, wci=wci, wcf=wcf, wco=wco, b=b,
                           i=i, cs=cs, f=f, o=o, ci=ci, co=co, h=h,
                           cs_grad=cs_grad, h_grad=h_grad,
                           use_peephole=use_peephole, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("use_peephole", _op.get_attr("use_peephole"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "BlockLSTMGrad", _inputs_flat, _attrs, _result, name)
  _result = _BlockLSTMGradOutput._make(_result)
  return _result



def block_lstm_grad_eager_fallback(seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad, h_grad, use_peephole, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function block_lstm_grad
  """
  _ctx = ctx if ctx else _context.context()
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad, h_grad], _ctx)
  (x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad, h_grad) = _inputs_T
  seq_len_max = _ops.convert_to_tensor(seq_len_max, _dtypes.int64)
  _inputs_flat = [seq_len_max, x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, h, cs_grad, h_grad]
  _attrs = ("use_peephole", use_peephole, "T", _attr_T)
  _result = _execute.execute(b"BlockLSTMGrad", 8, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BlockLSTMGrad", _inputs_flat, _attrs, _result, name)
  _result = _BlockLSTMGradOutput._make(_result)
  return _result

_ops.RegisterShape("BlockLSTMGrad")(None)


_lstm_block_cell_outputs = ["i", "cs", "f", "o", "ci", "co", "h"]
_LSTMBlockCellOutput = _collections.namedtuple(
    "LSTMBlockCell", _lstm_block_cell_outputs)


@_dispatch.add_dispatch_list
@tf_export('lstm_block_cell')
def lstm_block_cell(x, cs_prev, h_prev, w, wci, wcf, wco, b, forget_bias=1, cell_clip=3, use_peephole=False, name=None):
  r"""Computes the LSTM cell forward propagation for 1 time step.

  This implementation uses 1 weight matrix and 1 bias vector, and there's an
  optional peephole connection.

  This kernel op implements the following mathematical equations:

  ```python
  xh = [x, h_prev]
  [i, f, ci, o] = xh * w + b
  f = f + forget_bias

  if not use_peephole:
    wci = wcf = wco = 0

  i = sigmoid(cs_prev * wci + i)
  f = sigmoid(cs_prev * wcf + f)
  ci = tanh(ci)

  cs = ci .* i + cs_prev .* f
  cs = clip(cs, cell_clip)

  o = sigmoid(cs * wco + o)
  co = tanh(cs)
  h = co .* o
  ```

  Args:
    x: A `Tensor`. Must be one of the following types: `half`, `float32`.
      The input to the LSTM cell, shape (batch_size, num_inputs).
    cs_prev: A `Tensor`. Must have the same type as `x`.
      Value of the cell state at previous time step.
    h_prev: A `Tensor`. Must have the same type as `x`.
      Output of the previous cell at previous time step.
    w: A `Tensor`. Must have the same type as `x`. The weight matrix.
    wci: A `Tensor`. Must have the same type as `x`.
      The weight matrix for input gate peephole connection.
    wcf: A `Tensor`. Must have the same type as `x`.
      The weight matrix for forget gate peephole connection.
    wco: A `Tensor`. Must have the same type as `x`.
      The weight matrix for output gate peephole connection.
    b: A `Tensor`. Must have the same type as `x`. The bias vector.
    forget_bias: An optional `float`. Defaults to `1`. The forget gate bias.
    cell_clip: An optional `float`. Defaults to `3`.
      Value to clip the 'cs' value to.
    use_peephole: An optional `bool`. Defaults to `False`.
      Whether to use peephole weights.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (i, cs, f, o, ci, co, h).

    i: A `Tensor`. Has the same type as `x`. The input gate.
    cs: A `Tensor`. Has the same type as `x`. The cell state before the tanh.
    f: A `Tensor`. Has the same type as `x`. The forget gate.
    o: A `Tensor`. Has the same type as `x`. The output gate.
    ci: A `Tensor`. Has the same type as `x`. The cell input.
    co: A `Tensor`. Has the same type as `x`. The cell after the tanh.
    h: A `Tensor`. Has the same type as `x`. The output h vector.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LSTMBlockCell", name, _ctx._post_execution_callbacks, x, cs_prev,
        h_prev, w, wci, wcf, wco, b, "forget_bias", forget_bias, "cell_clip",
        cell_clip, "use_peephole", use_peephole)
      _result = _LSTMBlockCellOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return lstm_block_cell_eager_fallback(
            x, cs_prev, h_prev, w, wci, wcf, wco, b, forget_bias=forget_bias,
            cell_clip=cell_clip, use_peephole=use_peephole, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lstm_block_cell, x=x, cs_prev=cs_prev, h_prev=h_prev, w=w,
                               wci=wci, wcf=wcf, wco=wco, b=b,
                               forget_bias=forget_bias, cell_clip=cell_clip,
                               use_peephole=use_peephole, name=name)
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
  if forget_bias is None:
    forget_bias = 1
  forget_bias = _execute.make_float(forget_bias, "forget_bias")
  if cell_clip is None:
    cell_clip = 3
  cell_clip = _execute.make_float(cell_clip, "cell_clip")
  if use_peephole is None:
    use_peephole = False
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LSTMBlockCell", x=x, cs_prev=cs_prev, h_prev=h_prev, w=w, wci=wci,
                         wcf=wcf, wco=wco, b=b, forget_bias=forget_bias,
                         cell_clip=cell_clip, use_peephole=use_peephole,
                         name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lstm_block_cell, x=x, cs_prev=cs_prev, h_prev=h_prev, w=w, wci=wci,
                           wcf=wcf, wco=wco, b=b, forget_bias=forget_bias,
                           cell_clip=cell_clip, use_peephole=use_peephole,
                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("forget_bias", _op.get_attr("forget_bias"), "cell_clip",
            _op.get_attr("cell_clip"), "use_peephole",
            _op.get_attr("use_peephole"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "LSTMBlockCell", _inputs_flat, _attrs, _result, name)
  _result = _LSTMBlockCellOutput._make(_result)
  return _result



def lstm_block_cell_eager_fallback(x, cs_prev, h_prev, w, wci, wcf, wco, b, forget_bias=1, cell_clip=3, use_peephole=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lstm_block_cell
  """
  _ctx = ctx if ctx else _context.context()
  if forget_bias is None:
    forget_bias = 1
  forget_bias = _execute.make_float(forget_bias, "forget_bias")
  if cell_clip is None:
    cell_clip = 3
  cell_clip = _execute.make_float(cell_clip, "cell_clip")
  if use_peephole is None:
    use_peephole = False
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, cs_prev, h_prev, w, wci, wcf, wco, b], _ctx)
  (x, cs_prev, h_prev, w, wci, wcf, wco, b) = _inputs_T
  _inputs_flat = [x, cs_prev, h_prev, w, wci, wcf, wco, b]
  _attrs = ("forget_bias", forget_bias, "cell_clip", cell_clip,
  "use_peephole", use_peephole, "T", _attr_T)
  _result = _execute.execute(b"LSTMBlockCell", 7, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LSTMBlockCell", _inputs_flat, _attrs, _result, name)
  _result = _LSTMBlockCellOutput._make(_result)
  return _result

_ops.RegisterShape("LSTMBlockCell")(None)


_lstm_block_cell_grad_outputs = ["cs_prev_grad", "dicfo", "wci_grad",
                                "wcf_grad", "wco_grad"]
_LSTMBlockCellGradOutput = _collections.namedtuple(
    "LSTMBlockCellGrad", _lstm_block_cell_grad_outputs)


@_dispatch.add_dispatch_list
@tf_export('lstm_block_cell_grad')
def lstm_block_cell_grad(x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad, use_peephole, name=None):
  r"""Computes the LSTM cell backward propagation for 1 timestep.

  This implementation is to be used in conjunction of LSTMBlockCell.

  Args:
    x: A `Tensor`. Must be one of the following types: `half`, `float32`.
      The input to the LSTM cell, shape (batch_size, num_inputs).
    cs_prev: A `Tensor`. Must have the same type as `x`.
      The previous cell state.
    h_prev: A `Tensor`. Must have the same type as `x`. The previous h state.
    w: A `Tensor`. Must have the same type as `x`. The weight matrix.
    wci: A `Tensor`. Must have the same type as `x`.
      The weight matrix for input gate peephole connection.
    wcf: A `Tensor`. Must have the same type as `x`.
      The weight matrix for forget gate peephole connection.
    wco: A `Tensor`. Must have the same type as `x`.
      The weight matrix for output gate peephole connection.
    b: A `Tensor`. Must have the same type as `x`. The bias vector.
    i: A `Tensor`. Must have the same type as `x`. The input gate.
    cs: A `Tensor`. Must have the same type as `x`.
      The cell state before the tanh.
    f: A `Tensor`. Must have the same type as `x`. The forget gate.
    o: A `Tensor`. Must have the same type as `x`. The output gate.
    ci: A `Tensor`. Must have the same type as `x`. The cell input.
    co: A `Tensor`. Must have the same type as `x`. The cell after the tanh.
    cs_grad: A `Tensor`. Must have the same type as `x`.
      The current gradient of cs.
    h_grad: A `Tensor`. Must have the same type as `x`.
      The gradient of h vector.
    use_peephole: A `bool`. Whether the cell uses peephole connections.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (cs_prev_grad, dicfo, wci_grad, wcf_grad, wco_grad).

    cs_prev_grad: A `Tensor`. Has the same type as `x`. The gradient of cs to be back-propped.
    dicfo: A `Tensor`. Has the same type as `x`. The derivative wrt to [i, cs, f, o].
    wci_grad: A `Tensor`. Has the same type as `x`. The gradient for wci to be back-propped.
    wcf_grad: A `Tensor`. Has the same type as `x`. The gradient for wcf to be back-propped.
    wco_grad: A `Tensor`. Has the same type as `x`. The gradient for wco to be back-propped.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LSTMBlockCellGrad", name, _ctx._post_execution_callbacks, x, cs_prev,
        h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad,
        "use_peephole", use_peephole)
      _result = _LSTMBlockCellGradOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return lstm_block_cell_grad_eager_fallback(
            x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co,
            cs_grad, h_grad, use_peephole=use_peephole, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              lstm_block_cell_grad, x=x, cs_prev=cs_prev, h_prev=h_prev, w=w,
                                    wci=wci, wcf=wcf, wco=wco, b=b, i=i,
                                    cs=cs, f=f, o=o, ci=ci, co=co,
                                    cs_grad=cs_grad, h_grad=h_grad,
                                    use_peephole=use_peephole, name=name)
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
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "LSTMBlockCellGrad", x=x, cs_prev=cs_prev, h_prev=h_prev, w=w,
                             wci=wci, wcf=wcf, wco=wco, b=b, i=i, cs=cs, f=f,
                             o=o, ci=ci, co=co, cs_grad=cs_grad,
                             h_grad=h_grad, use_peephole=use_peephole,
                             name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          lstm_block_cell_grad, x=x, cs_prev=cs_prev, h_prev=h_prev, w=w,
                                wci=wci, wcf=wcf, wco=wco, b=b, i=i, cs=cs,
                                f=f, o=o, ci=ci, co=co, cs_grad=cs_grad,
                                h_grad=h_grad, use_peephole=use_peephole,
                                name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("use_peephole", _op.get_attr("use_peephole"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "LSTMBlockCellGrad", _inputs_flat, _attrs, _result, name)
  _result = _LSTMBlockCellGradOutput._make(_result)
  return _result



def lstm_block_cell_grad_eager_fallback(x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad, use_peephole, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lstm_block_cell_grad
  """
  _ctx = ctx if ctx else _context.context()
  use_peephole = _execute.make_bool(use_peephole, "use_peephole")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad], _ctx)
  (x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad) = _inputs_T
  _inputs_flat = [x, cs_prev, h_prev, w, wci, wcf, wco, b, i, cs, f, o, ci, co, cs_grad, h_grad]
  _attrs = ("use_peephole", use_peephole, "T", _attr_T)
  _result = _execute.execute(b"LSTMBlockCellGrad", 5, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LSTMBlockCellGrad", _inputs_flat, _attrs, _result, name)
  _result = _LSTMBlockCellGradOutput._make(_result)
  return _result

_ops.RegisterShape("LSTMBlockCellGrad")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BlockLSTM"
#   input_arg {
#     name: "seq_len_max"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wcf"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wco"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "i"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "cs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "f"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "o"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "ci"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "co"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "h"
#     type_attr: "T"
#   }
#   attr {
#     name: "forget_bias"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "cell_clip"
#     type: "float"
#     default_value {
#       f: 3
#     }
#   }
#   attr {
#     name: "use_peephole"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "BlockLSTMGrad"
#   input_arg {
#     name: "seq_len_max"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wcf"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wco"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "i"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "f"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "o"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "ci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "co"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "x_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "cs_prev_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "h_prev_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "w_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wci_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wcf_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wco_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "b_grad"
#     type_attr: "T"
#   }
#   attr {
#     name: "use_peephole"
#     type: "bool"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "LSTMBlockCell"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wcf"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wco"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "i"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "cs"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "f"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "o"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "ci"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "co"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "h"
#     type_attr: "T"
#   }
#   attr {
#     name: "forget_bias"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "cell_clip"
#     type: "float"
#     default_value {
#       f: 3
#     }
#   }
#   attr {
#     name: "use_peephole"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "LSTMBlockCellGrad"
#   input_arg {
#     name: "x"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_prev"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "w"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wcf"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "wco"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "b"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "i"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "f"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "o"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "ci"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "co"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "cs_grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "h_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "cs_prev_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "dicfo"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wci_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wcf_grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "wco_grad"
#     type_attr: "T"
#   }
#   attr {
#     name: "use_peephole"
#     type: "bool"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\215\002\n\tBlockLSTM\022\017\n\013seq_len_max\030\t\022\006\n\001x\"\001T\022\014\n\007cs_prev\"\001T\022\013\n\006h_prev\"\001T\022\006\n\001w\"\001T\022\010\n\003wci\"\001T\022\010\n\003wcf\"\001T\022\010\n\003wco\"\001T\022\006\n\001b\"\001T\032\006\n\001i\"\001T\032\007\n\002cs\"\001T\032\006\n\001f\"\001T\032\006\n\001o\"\001T\032\007\n\002ci\"\001T\032\007\n\002co\"\001T\032\006\n\001h\"\001T\"\033\n\013forget_bias\022\005float\032\005%\000\000\200?\"\031\n\tcell_clip\022\005float\032\005%\000\000@@\"\030\n\014use_peephole\022\004bool\032\002(\000\"\021\n\001T\022\004type:\006\n\0042\002\023\001\n\351\002\n\rBlockLSTMGrad\022\017\n\013seq_len_max\030\t\022\006\n\001x\"\001T\022\014\n\007cs_prev\"\001T\022\013\n\006h_prev\"\001T\022\006\n\001w\"\001T\022\010\n\003wci\"\001T\022\010\n\003wcf\"\001T\022\010\n\003wco\"\001T\022\006\n\001b\"\001T\022\006\n\001i\"\001T\022\007\n\002cs\"\001T\022\006\n\001f\"\001T\022\006\n\001o\"\001T\022\007\n\002ci\"\001T\022\007\n\002co\"\001T\022\006\n\001h\"\001T\022\014\n\007cs_grad\"\001T\022\013\n\006h_grad\"\001T\032\013\n\006x_grad\"\001T\032\021\n\014cs_prev_grad\"\001T\032\020\n\013h_prev_grad\"\001T\032\013\n\006w_grad\"\001T\032\r\n\010wci_grad\"\001T\032\r\n\010wcf_grad\"\001T\032\r\n\010wco_grad\"\001T\032\013\n\006b_grad\"\001T\"\024\n\014use_peephole\022\004bool\"\021\n\001T\022\004type:\006\n\0042\002\023\001\n\200\002\n\rLSTMBlockCell\022\006\n\001x\"\001T\022\014\n\007cs_prev\"\001T\022\013\n\006h_prev\"\001T\022\006\n\001w\"\001T\022\010\n\003wci\"\001T\022\010\n\003wcf\"\001T\022\010\n\003wco\"\001T\022\006\n\001b\"\001T\032\006\n\001i\"\001T\032\007\n\002cs\"\001T\032\006\n\001f\"\001T\032\006\n\001o\"\001T\032\007\n\002ci\"\001T\032\007\n\002co\"\001T\032\006\n\001h\"\001T\"\033\n\013forget_bias\022\005float\032\005%\000\000\200?\"\031\n\tcell_clip\022\005float\032\005%\000\000@@\"\030\n\014use_peephole\022\004bool\032\002(\000\"\021\n\001T\022\004type:\006\n\0042\002\023\001\n\247\002\n\021LSTMBlockCellGrad\022\006\n\001x\"\001T\022\014\n\007cs_prev\"\001T\022\013\n\006h_prev\"\001T\022\006\n\001w\"\001T\022\010\n\003wci\"\001T\022\010\n\003wcf\"\001T\022\010\n\003wco\"\001T\022\006\n\001b\"\001T\022\006\n\001i\"\001T\022\007\n\002cs\"\001T\022\006\n\001f\"\001T\022\006\n\001o\"\001T\022\007\n\002ci\"\001T\022\007\n\002co\"\001T\022\014\n\007cs_grad\"\001T\022\013\n\006h_grad\"\001T\032\021\n\014cs_prev_grad\"\001T\032\n\n\005dicfo\"\001T\032\r\n\010wci_grad\"\001T\032\r\n\010wcf_grad\"\001T\032\r\n\010wco_grad\"\001T\"\024\n\014use_peephole\022\004bool\"\021\n\001T\022\004type:\006\n\0042\002\023\001")
