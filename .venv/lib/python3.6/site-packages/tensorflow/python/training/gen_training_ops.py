"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: training_ops.cc
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


def apply_ada_max(var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AdaMax algorithm.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  v_t <- max(beta2 * v_{t-1}, abs(g))
  variable <- variable - learning_rate / (1 - beta1^t) * m_t / (v_t + epsilon)

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    m: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    v: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    beta1_power: A `Tensor`. Must have the same type as `var`.
      Must be a scalar.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    beta1: A `Tensor`. Must have the same type as `var`.
      Momentum factor. Must be a scalar.
    beta2: A `Tensor`. Must have the same type as `var`.
      Momentum factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, m, and v tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_ada_max op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAdaMax", var=var, m=m, v=v, beta1_power=beta1_power, lr=lr,
                       beta1=beta1, beta2=beta2, epsilon=epsilon, grad=grad,
                       use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyAdaMax", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_ada_max_eager_fallback(var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_ada_max op does not support eager execution. Arg 'out' is a ref.")

def apply_adadelta(var, accum, accum_update, lr, rho, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the adadelta scheme.

  accum = rho() * accum + (1 - rho()) * grad.square();
  update = (update_accum + epsilon).sqrt() * (accum + epsilon()).rsqrt() * grad;
  update_accum = rho() * update_accum + (1 - rho()) * update.square();
  var -= update;

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    accum_update: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Constant factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var, accum and update_accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_adadelta op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAdadelta", var=var, accum=accum, accum_update=accum_update,
                         lr=lr, rho=rho, epsilon=epsilon, grad=grad,
                         use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyAdadelta", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_adadelta_eager_fallback(var, accum, accum_update, lr, rho, epsilon, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_adadelta op does not support eager execution. Arg 'out' is a ref.")

def apply_adagrad(var, accum, lr, grad, use_locking=False, update_slots=True, name=None):
  r"""Update '*var' according to the adagrad scheme.

  accum += grad * grad
  var -= lr * grad * (1 / sqrt(accum))

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    update_slots: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_adagrad op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAdagrad", var=var, accum=accum, lr=lr, grad=grad,
                        use_locking=use_locking, update_slots=update_slots,
                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"), "update_slots",
            _op.get_attr("update_slots"))
  _execute.record_gradient(
      "ApplyAdagrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_adagrad_eager_fallback(var, accum, lr, grad, use_locking=False, update_slots=True, name=None, ctx=None):
  raise RuntimeError("apply_adagrad op does not support eager execution. Arg 'out' is a ref.")

def apply_adagrad_da(var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, use_locking=False, name=None):
  r"""Update '*var' according to the proximal adagrad scheme.

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    gradient_accumulator: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    gradient_squared_accumulator: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    global_step: A `Tensor` of type `int64`.
      Training step number. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_adagrad_da op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAdagradDA", var=var, gradient_accumulator=gradient_accumulator,
                          gradient_squared_accumulator=gradient_squared_accumulator,
                          grad=grad, lr=lr, l1=l1, l2=l2,
                          global_step=global_step, use_locking=use_locking,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyAdagradDA", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_adagrad_da_eager_fallback(var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_adagrad_da op does not support eager execution. Arg 'out' is a ref.")

def apply_adam(var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, use_nesterov=False, name=None):
  r"""Update '*var' according to the Adam algorithm.

  $$lr_t := \text{learning\_rate} * \sqrt{1 - beta_2^t} / (1 - beta_1^t)$$
  $$m_t := beta_1 * m_{t-1} + (1 - beta_1) * g$$
  $$v_t := beta_2 * v_{t-1} + (1 - beta_2) * g * g$$
  $$variable := variable - lr_t * m_t / (\sqrt{v_t} + \epsilon)$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    m: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    v: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    beta1_power: A `Tensor`. Must have the same type as `var`.
      Must be a scalar.
    beta2_power: A `Tensor`. Must have the same type as `var`.
      Must be a scalar.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    beta1: A `Tensor`. Must have the same type as `var`.
      Momentum factor. Must be a scalar.
    beta2: A `Tensor`. Must have the same type as `var`.
      Momentum factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, m, and v tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, uses the nesterov update.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_adam op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAdam", var=var, m=m, v=v, beta1_power=beta1_power,
                     beta2_power=beta2_power, lr=lr, beta1=beta1, beta2=beta2,
                     epsilon=epsilon, grad=grad, use_locking=use_locking,
                     use_nesterov=use_nesterov, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"), "use_nesterov",
            _op.get_attr("use_nesterov"))
  _execute.record_gradient(
      "ApplyAdam", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_adam_eager_fallback(var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, use_nesterov=False, name=None, ctx=None):
  raise RuntimeError("apply_adam op does not support eager execution. Arg 'out' is a ref.")

def apply_add_sign(var, m, lr, alpha, sign_decay, beta, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AddSign update.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  update <- (alpha + sign_decay * sign(g) *sign(m)) * g
  variable <- variable - lr_t * update

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    m: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    alpha: A `Tensor`. Must have the same type as `var`. Must be a scalar.
    sign_decay: A `Tensor`. Must have the same type as `var`.
      Must be a scalar.
    beta: A `Tensor`. Must have the same type as `var`. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and m tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_add_sign op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyAddSign", var=var, m=m, lr=lr, alpha=alpha,
                        sign_decay=sign_decay, beta=beta, grad=grad,
                        use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyAddSign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_add_sign_eager_fallback(var, m, lr, alpha, sign_decay, beta, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_add_sign op does not support eager execution. Arg 'out' is a ref.")

def apply_centered_rms_prop(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the centered RMSProp algorithm.

  The centered RMSProp algorithm uses an estimate of the centered second moment
  (i.e., the variance) for normalization, as opposed to regular RMSProp, which
  uses the (uncentered) second moment. This often helps with training, but is
  slightly more expensive in terms of computation and memory.

  Note that in dense implementation of this algorithm, mg, ms, and mom will
  update even if the grad is zero, but in this sparse implementation, mg, ms,
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  mean_grad = decay * mean_grad + (1-decay) * gradient

  Delta = learning_rate * gradient / sqrt(mean_square + epsilon - mean_grad ** 2)

  mg <- rho * mg_{t-1} + (1-rho) * grad
  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms - mg * mg + epsilon)
  var <- var - mom

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    mg: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    ms: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    mom: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `var`.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, mg, ms, and mom tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_centered_rms_prop op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyCenteredRMSProp", var=var, mg=mg, ms=ms, mom=mom, lr=lr,
                                rho=rho, momentum=momentum, epsilon=epsilon,
                                grad=grad, use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyCenteredRMSProp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_centered_rms_prop_eager_fallback(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_centered_rms_prop op does not support eager execution. Arg 'out' is a ref.")

def apply_ftrl(var, accum, linear, grad, lr, l1, l2, lr_power, use_locking=False, name=None):
  r"""Update '*var' according to the Ftrl-proximal scheme.

  accum_new = accum + grad * grad
  linear += grad + (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    linear: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regulariation. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regulariation. Must be a scalar.
    lr_power: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_ftrl op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyFtrl", var=var, accum=accum, linear=linear, grad=grad, lr=lr,
                     l1=l1, l2=l2, lr_power=lr_power, use_locking=use_locking,
                     name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyFtrl", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_ftrl_eager_fallback(var, accum, linear, grad, lr, l1, l2, lr_power, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_ftrl op does not support eager execution. Arg 'out' is a ref.")

def apply_ftrl_v2(var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None):
  r"""Update '*var' according to the Ftrl-proximal scheme.

  grad_with_shrinkage = grad + 2 * l2_shrinkage * var
  accum_new = accum + grad_with_shrinkage * grad_with_shrinkage
  linear += grad_with_shrinkage +
      (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    linear: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regulariation. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 shrinkage regulariation. Must be a scalar.
    l2_shrinkage: A `Tensor`. Must have the same type as `var`.
    lr_power: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_ftrl_v2 op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyFtrlV2", var=var, accum=accum, linear=linear, grad=grad, lr=lr,
                       l1=l1, l2=l2, l2_shrinkage=l2_shrinkage,
                       lr_power=lr_power, use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyFtrlV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_ftrl_v2_eager_fallback(var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_ftrl_v2 op does not support eager execution. Arg 'out' is a ref.")

def apply_gradient_descent(var, alpha, delta, use_locking=False, name=None):
  r"""Update '*var' by subtracting 'alpha' * 'delta' from it.

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    alpha: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    delta: A `Tensor`. Must have the same type as `var`. The change.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_gradient_descent op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyGradientDescent", var=var, alpha=alpha, delta=delta,
                                use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyGradientDescent", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_gradient_descent_eager_fallback(var, alpha, delta, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_gradient_descent op does not support eager execution. Arg 'out' is a ref.")

def apply_momentum(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update '*var' according to the momentum scheme. Set use_nesterov = True if you

  want to use Nesterov momentum.

  accum = accum * momentum + grad
  var -= lr * accum

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    momentum: A `Tensor`. Must have the same type as `var`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var - lr * momentum * accum, so in the end, the var you get is actually
      var - lr * momentum * accum.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_momentum op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyMomentum", var=var, accum=accum, lr=lr, grad=grad,
                         momentum=momentum, use_locking=use_locking,
                         use_nesterov=use_nesterov, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"), "use_nesterov",
            _op.get_attr("use_nesterov"))
  _execute.record_gradient(
      "ApplyMomentum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_momentum_eager_fallback(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  raise RuntimeError("apply_momentum op does not support eager execution. Arg 'out' is a ref.")

def apply_power_sign(var, m, lr, logbase, sign_decay, beta, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AddSign update.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  update <- exp(logbase * sign_decay * sign(g) * sign(m_t)) * g
  variable <- variable - lr_t * update

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    m: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    logbase: A `Tensor`. Must have the same type as `var`. Must be a scalar.
    sign_decay: A `Tensor`. Must have the same type as `var`.
      Must be a scalar.
    beta: A `Tensor`. Must have the same type as `var`. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and m tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_power_sign op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyPowerSign", var=var, m=m, lr=lr, logbase=logbase,
                          sign_decay=sign_decay, beta=beta, grad=grad,
                          use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyPowerSign", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_power_sign_eager_fallback(var, m, lr, logbase, sign_decay, beta, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_power_sign op does not support eager execution. Arg 'out' is a ref.")

def apply_proximal_adagrad(var, accum, lr, l1, l2, grad, use_locking=False, name=None):
  r"""Update '*var' and '*accum' according to FOBOS with Adagrad learning rate.

  accum += grad * grad
  prox_v = var - lr * grad * (1 / sqrt(accum))
  var = sign(prox_v)/(1+lr*l2) * max{|prox_v|-lr*l1,0}

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_proximal_adagrad op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyProximalAdagrad", var=var, accum=accum, lr=lr, l1=l1, l2=l2,
                                grad=grad, use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyProximalAdagrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_proximal_adagrad_eager_fallback(var, accum, lr, l1, l2, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_proximal_adagrad op does not support eager execution. Arg 'out' is a ref.")

def apply_proximal_gradient_descent(var, alpha, l1, l2, delta, use_locking=False, name=None):
  r"""Update '*var' as FOBOS algorithm with fixed learning rate.

  prox_v = var - alpha * delta
  var = sign(prox_v)/(1+alpha*l2) * max{|prox_v|-alpha*l1,0}

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    alpha: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    delta: A `Tensor`. Must have the same type as `var`. The change.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_proximal_gradient_descent op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyProximalGradientDescent", var=var, alpha=alpha, l1=l1, l2=l2,
                                        delta=delta, use_locking=use_locking,
                                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyProximalGradientDescent", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_proximal_gradient_descent_eager_fallback(var, alpha, l1, l2, delta, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_proximal_gradient_descent op does not support eager execution. Arg 'out' is a ref.")

def apply_rms_prop(var, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the RMSProp algorithm.

  Note that in dense implementation of this algorithm, ms and mom will
  update even if the grad is zero, but in this sparse implementation, ms
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon)

  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)
  var <- var - mom

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    ms: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    mom: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `var`.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, ms, and mom tensors is protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("apply_rms_prop op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ApplyRMSProp", var=var, ms=ms, mom=mom, lr=lr, rho=rho,
                        momentum=momentum, epsilon=epsilon, grad=grad,
                        use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "use_locking",
            _op.get_attr("use_locking"))
  _execute.record_gradient(
      "ApplyRMSProp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def apply_rms_prop_eager_fallback(var, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None, ctx=None):
  raise RuntimeError("apply_rms_prop op does not support eager execution. Arg 'out' is a ref.")

def resource_apply_ada_max(var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AdaMax algorithm.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  v_t <- max(beta2 * v_{t-1}, abs(g))
  variable <- variable - learning_rate / (1 - beta1^t) * m_t / (v_t + epsilon)

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    m: A `Tensor` of type `resource`. Should be from a Variable().
    v: A `Tensor` of type `resource`. Should be from a Variable().
    beta1_power: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Must be a scalar.
    lr: A `Tensor`. Must have the same type as `beta1_power`.
      Scaling factor. Must be a scalar.
    beta1: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    beta2: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `beta1_power`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `beta1_power`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, m, and v tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdaMax", name, _ctx._post_execution_callbacks, var, m,
        v, beta1_power, lr, beta1, beta2, epsilon, grad, "use_locking",
        use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_ada_max_eager_fallback(
            var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdaMax", var=var, m=m, v=v, beta1_power=beta1_power,
                               lr=lr, beta1=beta1, beta2=beta2,
                               epsilon=epsilon, grad=grad,
                               use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_ada_max_eager_fallback(var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_ada_max
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([beta1_power, lr, beta1, beta2, epsilon, grad], _ctx)
  (beta1_power, lr, beta1, beta2, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  m = _ops.convert_to_tensor(m, _dtypes.resource)
  v = _ops.convert_to_tensor(v, _dtypes.resource)
  _inputs_flat = [var, m, v, beta1_power, lr, beta1, beta2, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyAdaMax", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_adadelta(var, accum, accum_update, lr, rho, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the adadelta scheme.

  accum = rho() * accum + (1 - rho()) * grad.square();
  update = (update_accum + epsilon).sqrt() * (accum + epsilon()).rsqrt() * grad;
  update_accum = rho() * update_accum + (1 - rho()) * update.square();
  var -= update;

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    accum_update: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Constant factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var, accum and update_accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdadelta", name, _ctx._post_execution_callbacks, var,
        accum, accum_update, lr, rho, epsilon, grad, "use_locking",
        use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_adadelta_eager_fallback(
            var, accum, accum_update, lr, rho, epsilon, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdadelta", var=var, accum=accum,
                                 accum_update=accum_update, lr=lr, rho=rho,
                                 epsilon=epsilon, grad=grad,
                                 use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_adadelta_eager_fallback(var, accum, accum_update, lr, rho, epsilon, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_adadelta
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, epsilon, grad], _ctx)
  (lr, rho, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  accum_update = _ops.convert_to_tensor(accum_update, _dtypes.resource)
  _inputs_flat = [var, accum, accum_update, lr, rho, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyAdadelta", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_adagrad(var, accum, lr, grad, use_locking=False, update_slots=True, name=None):
  r"""Update '*var' according to the adagrad scheme.

  accum += grad * grad
  var -= lr * grad * (1 / sqrt(accum))

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    update_slots: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdagrad", name, _ctx._post_execution_callbacks, var,
        accum, lr, grad, "use_locking", use_locking, "update_slots",
        update_slots)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_adagrad_eager_fallback(
            var, accum, lr, grad, use_locking=use_locking,
            update_slots=update_slots, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdagrad", var=var, accum=accum, lr=lr, grad=grad,
                                use_locking=use_locking,
                                update_slots=update_slots, name=name)
  return _op
  _result = None
  return _result



def resource_apply_adagrad_eager_fallback(var, accum, lr, grad, use_locking=False, update_slots=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_adagrad
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad], _ctx)
  (lr, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking, "update_slots",
  update_slots)
  _result = _execute.execute(b"ResourceApplyAdagrad", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_adagrad_da(var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, use_locking=False, name=None):
  r"""Update '*var' according to the proximal adagrad scheme.

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    gradient_accumulator: A `Tensor` of type `resource`.
      Should be from a Variable().
    gradient_squared_accumulator: A `Tensor` of type `resource`.
      Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    lr: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 regularization. Must be a scalar.
    global_step: A `Tensor` of type `int64`.
      Training step number. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdagradDA", name, _ctx._post_execution_callbacks, var,
        gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2,
        global_step, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_adagrad_da_eager_fallback(
            var, gradient_accumulator, gradient_squared_accumulator, grad, lr,
            l1, l2, global_step, use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdagradDA", var=var,
                                  gradient_accumulator=gradient_accumulator,
                                  gradient_squared_accumulator=gradient_squared_accumulator,
                                  grad=grad, lr=lr, l1=l1, l2=l2,
                                  global_step=global_step,
                                  use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_adagrad_da_eager_fallback(var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_adagrad_da
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2], _ctx)
  (grad, lr, l1, l2) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  gradient_accumulator = _ops.convert_to_tensor(gradient_accumulator, _dtypes.resource)
  gradient_squared_accumulator = _ops.convert_to_tensor(gradient_squared_accumulator, _dtypes.resource)
  global_step = _ops.convert_to_tensor(global_step, _dtypes.int64)
  _inputs_flat = [var, gradient_accumulator, gradient_squared_accumulator, grad, lr, l1, l2, global_step]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyAdagradDA", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_adam(var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, use_nesterov=False, name=None):
  r"""Update '*var' according to the Adam algorithm.

  $$lr_t := \text{learning\_rate} * \sqrt{1 - beta_2^t} / (1 - beta_1^t)$$
  $$m_t := beta_1 * m_{t-1} + (1 - beta_1) * g$$
  $$v_t := beta_2 * v_{t-1} + (1 - beta_2) * g * g$$
  $$variable := variable - lr_t * m_t / (\sqrt{v_t} + \epsilon)$$

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    m: A `Tensor` of type `resource`. Should be from a Variable().
    v: A `Tensor` of type `resource`. Should be from a Variable().
    beta1_power: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Must be a scalar.
    beta2_power: A `Tensor`. Must have the same type as `beta1_power`.
      Must be a scalar.
    lr: A `Tensor`. Must have the same type as `beta1_power`.
      Scaling factor. Must be a scalar.
    beta1: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    beta2: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `beta1_power`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `beta1_power`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, m, and v tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, uses the nesterov update.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdam", name, _ctx._post_execution_callbacks, var, m, v,
        beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad,
        "use_locking", use_locking, "use_nesterov", use_nesterov)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_adam_eager_fallback(
            var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon,
            grad, use_locking=use_locking, use_nesterov=use_nesterov,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdam", var=var, m=m, v=v, beta1_power=beta1_power,
                             beta2_power=beta2_power, lr=lr, beta1=beta1,
                             beta2=beta2, epsilon=epsilon, grad=grad,
                             use_locking=use_locking,
                             use_nesterov=use_nesterov, name=name)
  return _op
  _result = None
  return _result



def resource_apply_adam_eager_fallback(var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, use_nesterov=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_adam
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad], _ctx)
  (beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  m = _ops.convert_to_tensor(m, _dtypes.resource)
  v = _ops.convert_to_tensor(v, _dtypes.resource)
  _inputs_flat = [var, m, v, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking, "use_nesterov",
  use_nesterov)
  _result = _execute.execute(b"ResourceApplyAdam", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_adam_with_amsgrad(var, m, v, vhat, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the Adam algorithm.

  $$lr_t := \text{learning\_rate} * \sqrt{1 - beta_2^t} / (1 - beta_1^t)$$
  $$m_t := beta_1 * m_{t-1} + (1 - beta_1) * g$$
  $$v_t := beta_2 * v_{t-1} + (1 - beta_2) * g * g$$
  $$vhat_t := max{vhat_{t-1}, v_t}$$
  $$variable := variable - lr_t * m_t / (\sqrt{vhat_t} + \epsilon)$$

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    m: A `Tensor` of type `resource`. Should be from a Variable().
    v: A `Tensor` of type `resource`. Should be from a Variable().
    vhat: A `Tensor` of type `resource`. Should be from a Variable().
    beta1_power: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Must be a scalar.
    beta2_power: A `Tensor`. Must have the same type as `beta1_power`.
      Must be a scalar.
    lr: A `Tensor`. Must have the same type as `beta1_power`.
      Scaling factor. Must be a scalar.
    beta1: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    beta2: A `Tensor`. Must have the same type as `beta1_power`.
      Momentum factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `beta1_power`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `beta1_power`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, m, and v tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAdamWithAmsgrad", name, _ctx._post_execution_callbacks,
        var, m, v, vhat, beta1_power, beta2_power, lr, beta1, beta2, epsilon,
        grad, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_adam_with_amsgrad_eager_fallback(
            var, m, v, vhat, beta1_power, beta2_power, lr, beta1, beta2,
            epsilon, grad, use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAdamWithAmsgrad", var=var, m=m, v=v, vhat=vhat,
                                        beta1_power=beta1_power,
                                        beta2_power=beta2_power, lr=lr,
                                        beta1=beta1, beta2=beta2,
                                        epsilon=epsilon, grad=grad,
                                        use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_adam_with_amsgrad_eager_fallback(var, m, v, vhat, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_adam_with_amsgrad
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad], _ctx)
  (beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  m = _ops.convert_to_tensor(m, _dtypes.resource)
  v = _ops.convert_to_tensor(v, _dtypes.resource)
  vhat = _ops.convert_to_tensor(vhat, _dtypes.resource)
  _inputs_flat = [var, m, v, vhat, beta1_power, beta2_power, lr, beta1, beta2, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyAdamWithAmsgrad", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_add_sign(var, m, lr, alpha, sign_decay, beta, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AddSign update.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  update <- (alpha + sign_decay * sign(g) *sign(m)) * g
  variable <- variable - lr_t * update

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    m: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    alpha: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    sign_decay: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    beta: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and m tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyAddSign", name, _ctx._post_execution_callbacks, var, m,
        lr, alpha, sign_decay, beta, grad, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_add_sign_eager_fallback(
            var, m, lr, alpha, sign_decay, beta, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyAddSign", var=var, m=m, lr=lr, alpha=alpha,
                                sign_decay=sign_decay, beta=beta, grad=grad,
                                use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_add_sign_eager_fallback(var, m, lr, alpha, sign_decay, beta, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_add_sign
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, alpha, sign_decay, beta, grad], _ctx)
  (lr, alpha, sign_decay, beta, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  m = _ops.convert_to_tensor(m, _dtypes.resource)
  _inputs_flat = [var, m, lr, alpha, sign_decay, beta, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyAddSign", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_centered_rms_prop(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the centered RMSProp algorithm.

  The centered RMSProp algorithm uses an estimate of the centered second moment
  (i.e., the variance) for normalization, as opposed to regular RMSProp, which
  uses the (uncentered) second moment. This often helps with training, but is
  slightly more expensive in terms of computation and memory.

  Note that in dense implementation of this algorithm, mg, ms, and mom will
  update even if the grad is zero, but in this sparse implementation, mg, ms,
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  mean_grad = decay * mean_grad + (1-decay) * gradient

  Delta = learning_rate * gradient / sqrt(mean_square + epsilon - mean_grad ** 2)

  mg <- rho * mg_{t-1} + (1-rho) * grad
  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms - mg * mg + epsilon)
  var <- var - mom

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    mg: A `Tensor` of type `resource`. Should be from a Variable().
    ms: A `Tensor` of type `resource`. Should be from a Variable().
    mom: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `lr`.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, mg, ms, and mom tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyCenteredRMSProp", name, _ctx._post_execution_callbacks,
        var, mg, ms, mom, lr, rho, momentum, epsilon, grad, "use_locking",
        use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_centered_rms_prop_eager_fallback(
            var, mg, ms, mom, lr, rho, momentum, epsilon, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyCenteredRMSProp", var=var, mg=mg, ms=ms, mom=mom, lr=lr,
                                        rho=rho, momentum=momentum,
                                        epsilon=epsilon, grad=grad,
                                        use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_centered_rms_prop_eager_fallback(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_centered_rms_prop
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, momentum, epsilon, grad], _ctx)
  (lr, rho, momentum, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  mg = _ops.convert_to_tensor(mg, _dtypes.resource)
  ms = _ops.convert_to_tensor(ms, _dtypes.resource)
  mom = _ops.convert_to_tensor(mom, _dtypes.resource)
  _inputs_flat = [var, mg, ms, mom, lr, rho, momentum, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyCenteredRMSProp", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_ftrl(var, accum, linear, grad, lr, l1, l2, lr_power, use_locking=False, name=None):
  r"""Update '*var' according to the Ftrl-proximal scheme.

  accum_new = accum + grad * grad
  linear += grad - (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    linear: A `Tensor` of type `resource`. Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    lr: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regulariation. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 regulariation. Must be a scalar.
    lr_power: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyFtrl", name, _ctx._post_execution_callbacks, var, accum,
        linear, grad, lr, l1, l2, lr_power, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_ftrl_eager_fallback(
            var, accum, linear, grad, lr, l1, l2, lr_power,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyFtrl", var=var, accum=accum, linear=linear, grad=grad,
                             lr=lr, l1=l1, l2=l2, lr_power=lr_power,
                             use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_ftrl_eager_fallback(var, accum, linear, grad, lr, l1, l2, lr_power, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_ftrl
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2, lr_power], _ctx)
  (grad, lr, l1, l2, lr_power) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  linear = _ops.convert_to_tensor(linear, _dtypes.resource)
  _inputs_flat = [var, accum, linear, grad, lr, l1, l2, lr_power]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyFtrl", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_ftrl_v2(var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None):
  r"""Update '*var' according to the Ftrl-proximal scheme.

  grad_with_shrinkage = grad + 2 * l2_shrinkage * var
  accum_new = accum + grad_with_shrinkage * grad_with_shrinkage
  linear += grad_with_shrinkage +
      (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    linear: A `Tensor` of type `resource`. Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    lr: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regulariation. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 shrinkage regulariation. Must be a scalar.
    l2_shrinkage: A `Tensor`. Must have the same type as `grad`.
    lr_power: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyFtrlV2", name, _ctx._post_execution_callbacks, var,
        accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_ftrl_v2_eager_fallback(
            var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyFtrlV2", var=var, accum=accum, linear=linear, grad=grad,
                               lr=lr, l1=l1, l2=l2, l2_shrinkage=l2_shrinkage,
                               lr_power=lr_power, use_locking=use_locking,
                               name=name)
  return _op
  _result = None
  return _result



def resource_apply_ftrl_v2_eager_fallback(var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_ftrl_v2
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2, l2_shrinkage, lr_power], _ctx)
  (grad, lr, l1, l2, l2_shrinkage, lr_power) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  linear = _ops.convert_to_tensor(linear, _dtypes.resource)
  _inputs_flat = [var, accum, linear, grad, lr, l1, l2, l2_shrinkage, lr_power]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyFtrlV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_gradient_descent(var, alpha, delta, use_locking=False, name=None):
  r"""Update '*var' by subtracting 'alpha' * 'delta' from it.

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    alpha: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    delta: A `Tensor`. Must have the same type as `alpha`. The change.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyGradientDescent", name, _ctx._post_execution_callbacks,
        var, alpha, delta, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_gradient_descent_eager_fallback(
            var, alpha, delta, use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyGradientDescent", var=var, alpha=alpha, delta=delta,
                                        use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_gradient_descent_eager_fallback(var, alpha, delta, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_gradient_descent
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([alpha, delta], _ctx)
  (alpha, delta) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  _inputs_flat = [var, alpha, delta]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyGradientDescent", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_keras_momentum(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update '*var' according to the momentum scheme. Set use_nesterov = True if you

  want to use Nesterov momentum.

  accum = accum * momentum - lr * grad
  var += accum

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    momentum: A `Tensor`. Must have the same type as `lr`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var + momentum * accum, so in the end, the var you get is actually
      var + momentum * accum.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyKerasMomentum", name, _ctx._post_execution_callbacks,
        var, accum, lr, grad, momentum, "use_locking", use_locking,
        "use_nesterov", use_nesterov)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_keras_momentum_eager_fallback(
            var, accum, lr, grad, momentum, use_locking=use_locking,
            use_nesterov=use_nesterov, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyKerasMomentum", var=var, accum=accum, lr=lr, grad=grad,
                                      momentum=momentum,
                                      use_locking=use_locking,
                                      use_nesterov=use_nesterov, name=name)
  return _op
  _result = None
  return _result



def resource_apply_keras_momentum_eager_fallback(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_keras_momentum
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad, momentum], _ctx)
  (lr, grad, momentum) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad, momentum]
  _attrs = ("T", _attr_T, "use_locking", use_locking, "use_nesterov",
  use_nesterov)
  _result = _execute.execute(b"ResourceApplyKerasMomentum", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_momentum(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update '*var' according to the momentum scheme. Set use_nesterov = True if you

  want to use Nesterov momentum.

  accum = accum * momentum + grad
  var -= lr * accum

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    momentum: A `Tensor`. Must have the same type as `lr`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var - lr * momentum * accum, so in the end, the var you get is actually
      var - lr * momentum * accum.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyMomentum", name, _ctx._post_execution_callbacks, var,
        accum, lr, grad, momentum, "use_locking", use_locking, "use_nesterov",
        use_nesterov)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_momentum_eager_fallback(
            var, accum, lr, grad, momentum, use_locking=use_locking,
            use_nesterov=use_nesterov, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyMomentum", var=var, accum=accum, lr=lr, grad=grad,
                                 momentum=momentum, use_locking=use_locking,
                                 use_nesterov=use_nesterov, name=name)
  return _op
  _result = None
  return _result



def resource_apply_momentum_eager_fallback(var, accum, lr, grad, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_momentum
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad, momentum], _ctx)
  (lr, grad, momentum) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad, momentum]
  _attrs = ("T", _attr_T, "use_locking", use_locking, "use_nesterov",
  use_nesterov)
  _result = _execute.execute(b"ResourceApplyMomentum", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_apply_power_sign(var, m, lr, logbase, sign_decay, beta, grad, use_locking=False, name=None):
  r"""Update '*var' according to the AddSign update.

  m_t <- beta1 * m_{t-1} + (1 - beta1) * g
  update <- exp(logbase * sign_decay * sign(g) * sign(m_t)) * g
  variable <- variable - lr_t * update

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    m: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    logbase: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    sign_decay: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    beta: A `Tensor`. Must have the same type as `lr`. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and m tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyPowerSign", name, _ctx._post_execution_callbacks, var,
        m, lr, logbase, sign_decay, beta, grad, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_power_sign_eager_fallback(
            var, m, lr, logbase, sign_decay, beta, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyPowerSign", var=var, m=m, lr=lr, logbase=logbase,
                                  sign_decay=sign_decay, beta=beta, grad=grad,
                                  use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_power_sign_eager_fallback(var, m, lr, logbase, sign_decay, beta, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_power_sign
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, logbase, sign_decay, beta, grad], _ctx)
  (lr, logbase, sign_decay, beta, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  m = _ops.convert_to_tensor(m, _dtypes.resource)
  _inputs_flat = [var, m, lr, logbase, sign_decay, beta, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyPowerSign", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_proximal_adagrad(var, accum, lr, l1, l2, grad, use_locking=False, name=None):
  r"""Update '*var' and '*accum' according to FOBOS with Adagrad learning rate.

  accum += grad * grad
  prox_v = var - lr * grad * (1 / sqrt(accum))
  var = sign(prox_v)/(1+lr*l2) * max{|prox_v|-lr*l1,0}

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `lr`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `lr`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyProximalAdagrad", name, _ctx._post_execution_callbacks,
        var, accum, lr, l1, l2, grad, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_proximal_adagrad_eager_fallback(
            var, accum, lr, l1, l2, grad, use_locking=use_locking, name=name,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyProximalAdagrad", var=var, accum=accum, lr=lr, l1=l1,
                                        l2=l2, grad=grad,
                                        use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_proximal_adagrad_eager_fallback(var, accum, lr, l1, l2, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_proximal_adagrad
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, l1, l2, grad], _ctx)
  (lr, l1, l2, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, l1, l2, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyProximalAdagrad", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_proximal_gradient_descent(var, alpha, l1, l2, delta, use_locking=False, name=None):
  r"""Update '*var' as FOBOS algorithm with fixed learning rate.

  prox_v = var - alpha * delta
  var = sign(prox_v)/(1+alpha*l2) * max{|prox_v|-alpha*l1,0}

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    alpha: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `alpha`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `alpha`.
      L2 regularization. Must be a scalar.
    delta: A `Tensor`. Must have the same type as `alpha`. The change.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyProximalGradientDescent", name,
        _ctx._post_execution_callbacks, var, alpha, l1, l2, delta,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_proximal_gradient_descent_eager_fallback(
            var, alpha, l1, l2, delta, use_locking=use_locking, name=name,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyProximalGradientDescent", var=var, alpha=alpha, l1=l1,
                                                l2=l2, delta=delta,
                                                use_locking=use_locking,
                                                name=name)
  return _op
  _result = None
  return _result



def resource_apply_proximal_gradient_descent_eager_fallback(var, alpha, l1, l2, delta, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_proximal_gradient_descent
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([alpha, l1, l2, delta], _ctx)
  (alpha, l1, l2, delta) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  _inputs_flat = [var, alpha, l1, l2, delta]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyProximalGradientDescent", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_apply_rms_prop(var, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None):
  r"""Update '*var' according to the RMSProp algorithm.

  Note that in dense implementation of this algorithm, ms and mom will
  update even if the grad is zero, but in this sparse implementation, ms
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon)

  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)
  var <- var - mom

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    ms: A `Tensor` of type `resource`. Should be from a Variable().
    mom: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `lr`.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, ms, and mom tensors is protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceApplyRMSProp", name, _ctx._post_execution_callbacks, var, ms,
        mom, lr, rho, momentum, epsilon, grad, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_apply_rms_prop_eager_fallback(
            var, ms, mom, lr, rho, momentum, epsilon, grad,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceApplyRMSProp", var=var, ms=ms, mom=mom, lr=lr, rho=rho,
                                momentum=momentum, epsilon=epsilon, grad=grad,
                                use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_apply_rms_prop_eager_fallback(var, ms, mom, lr, rho, momentum, epsilon, grad, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_apply_rms_prop
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, momentum, epsilon, grad], _ctx)
  (lr, rho, momentum, epsilon, grad) = _inputs_T
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  ms = _ops.convert_to_tensor(ms, _dtypes.resource)
  mom = _ops.convert_to_tensor(mom, _dtypes.resource)
  _inputs_flat = [var, ms, mom, lr, rho, momentum, epsilon, grad]
  _attrs = ("T", _attr_T, "use_locking", use_locking)
  _result = _execute.execute(b"ResourceApplyRMSProp", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def resource_sparse_apply_adadelta(var, accum, accum_update, lr, rho, epsilon, grad, indices, use_locking=False, name=None):
  r"""var: Should be from a Variable().

  Args:
    var: A `Tensor` of type `resource`.
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    accum_update: A `Tensor` of type `resource`.
      : Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Learning rate. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Constant factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyAdadelta", name, _ctx._post_execution_callbacks,
        var, accum, accum_update, lr, rho, epsilon, grad, indices,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_adadelta_eager_fallback(
            var, accum, accum_update, lr, rho, epsilon, grad, indices,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyAdadelta", var=var, accum=accum,
                                       accum_update=accum_update, lr=lr,
                                       rho=rho, epsilon=epsilon, grad=grad,
                                       indices=indices,
                                       use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_adadelta_eager_fallback(var, accum, accum_update, lr, rho, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_adadelta
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, epsilon, grad], _ctx)
  (lr, rho, epsilon, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  accum_update = _ops.convert_to_tensor(accum_update, _dtypes.resource)
  _inputs_flat = [var, accum, accum_update, lr, rho, epsilon, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyAdadelta", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_adagrad(var, accum, lr, grad, indices, use_locking=False, update_slots=True, name=None):
  r"""Update relevant entries in '*var' and '*accum' according to the adagrad scheme.

  That is for rows we have grad for, we update var and accum as follows:
  accum += grad * grad
  var -= lr * grad * (1 / sqrt(accum))

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Learning rate. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    update_slots: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyAdagrad", name, _ctx._post_execution_callbacks,
        var, accum, lr, grad, indices, "use_locking", use_locking,
        "update_slots", update_slots)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_adagrad_eager_fallback(
            var, accum, lr, grad, indices, use_locking=use_locking,
            update_slots=update_slots, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyAdagrad", var=var, accum=accum, lr=lr, grad=grad,
                                      indices=indices,
                                      use_locking=use_locking,
                                      update_slots=update_slots, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_adagrad_eager_fallback(var, accum, lr, grad, indices, use_locking=False, update_slots=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_adagrad
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad], _ctx)
  (lr, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking, "update_slots", update_slots)
  _result = _execute.execute(b"ResourceSparseApplyAdagrad", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_adagrad_da(var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, use_locking=False, name=None):
  r"""Update entries in '*var' and '*accum' according to the proximal adagrad scheme.

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    gradient_accumulator: A `Tensor` of type `resource`.
      Should be from a Variable().
    gradient_squared_accumulator: A `Tensor` of type `resource`.
      Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `grad`.
      Learning rate. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 regularization. Must be a scalar.
    global_step: A `Tensor` of type `int64`.
      Training step number. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyAdagradDA", name, _ctx._post_execution_callbacks,
        var, gradient_accumulator, gradient_squared_accumulator, grad,
        indices, lr, l1, l2, global_step, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_adagrad_da_eager_fallback(
            var, gradient_accumulator, gradient_squared_accumulator, grad,
            indices, lr, l1, l2, global_step, use_locking=use_locking,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyAdagradDA", var=var,
                                        gradient_accumulator=gradient_accumulator,
                                        gradient_squared_accumulator=gradient_squared_accumulator,
                                        grad=grad, indices=indices, lr=lr,
                                        l1=l1, l2=l2, global_step=global_step,
                                        use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_adagrad_da_eager_fallback(var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_adagrad_da
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2], _ctx)
  (grad, lr, l1, l2) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  gradient_accumulator = _ops.convert_to_tensor(gradient_accumulator, _dtypes.resource)
  gradient_squared_accumulator = _ops.convert_to_tensor(gradient_squared_accumulator, _dtypes.resource)
  global_step = _ops.convert_to_tensor(global_step, _dtypes.int64)
  _inputs_flat = [var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyAdagradDA", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_centered_rms_prop(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None):
  r"""Update '*var' according to the centered RMSProp algorithm.

  The centered RMSProp algorithm uses an estimate of the centered second moment
  (i.e., the variance) for normalization, as opposed to regular RMSProp, which
  uses the (uncentered) second moment. This often helps with training, but is
  slightly more expensive in terms of computation and memory.

  Note that in dense implementation of this algorithm, mg, ms, and mom will
  update even if the grad is zero, but in this sparse implementation, mg, ms,
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  mean_grad = decay * mean_grad + (1-decay) * gradient
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon - mean_grad ** 2)

  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)
  var <- var - mom

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    mg: A `Tensor` of type `resource`. Should be from a Variable().
    ms: A `Tensor` of type `resource`. Should be from a Variable().
    mom: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `lr`.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var, ms and mom.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, mg, ms, and mom tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyCenteredRMSProp", name,
        _ctx._post_execution_callbacks, var, mg, ms, mom, lr, rho, momentum,
        epsilon, grad, indices, "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_centered_rms_prop_eager_fallback(
            var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyCenteredRMSProp", var=var, mg=mg, ms=ms, mom=mom,
                                              lr=lr, rho=rho,
                                              momentum=momentum,
                                              epsilon=epsilon, grad=grad,
                                              indices=indices,
                                              use_locking=use_locking,
                                              name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_centered_rms_prop_eager_fallback(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_centered_rms_prop
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, momentum, epsilon, grad], _ctx)
  (lr, rho, momentum, epsilon, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  mg = _ops.convert_to_tensor(mg, _dtypes.resource)
  ms = _ops.convert_to_tensor(ms, _dtypes.resource)
  mom = _ops.convert_to_tensor(mom, _dtypes.resource)
  _inputs_flat = [var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyCenteredRMSProp", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_ftrl(var, accum, linear, grad, indices, lr, l1, l2, lr_power, use_locking=False, name=None):
  r"""Update relevant entries in '*var' according to the Ftrl-proximal scheme.

  That is for rows we have grad for, we update var, accum and linear as follows:
  accum_new = accum + grad * grad
  linear += grad + (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    linear: A `Tensor` of type `resource`. Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 regularization. Must be a scalar.
    lr_power: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyFtrl", name, _ctx._post_execution_callbacks, var,
        accum, linear, grad, indices, lr, l1, l2, lr_power, "use_locking",
        use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_ftrl_eager_fallback(
            var, accum, linear, grad, indices, lr, l1, l2, lr_power,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyFtrl", var=var, accum=accum, linear=linear,
                                   grad=grad, indices=indices, lr=lr, l1=l1,
                                   l2=l2, lr_power=lr_power,
                                   use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_ftrl_eager_fallback(var, accum, linear, grad, indices, lr, l1, l2, lr_power, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_ftrl
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2, lr_power], _ctx)
  (grad, lr, l1, l2, lr_power) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  linear = _ops.convert_to_tensor(linear, _dtypes.resource)
  _inputs_flat = [var, accum, linear, grad, indices, lr, l1, l2, lr_power]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyFtrl", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_ftrl_v2(var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None):
  r"""Update relevant entries in '*var' according to the Ftrl-proximal scheme.

  That is for rows we have grad for, we update var, accum and linear as follows:
  grad_with_shrinkage = grad + 2 * l2_shrinkage * var
  accum_new = accum + grad_with_shrinkage * grad_with_shrinkage
  linear += grad_with_shrinkage +
      (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    linear: A `Tensor` of type `resource`. Should be from a Variable().
    grad: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `grad`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `grad`.
      L2 shrinkage regulariation. Must be a scalar.
    l2_shrinkage: A `Tensor`. Must have the same type as `grad`.
    lr_power: A `Tensor`. Must have the same type as `grad`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyFtrlV2", name, _ctx._post_execution_callbacks,
        var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_ftrl_v2_eager_fallback(
            var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage,
            lr_power, use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyFtrlV2", var=var, accum=accum, linear=linear,
                                     grad=grad, indices=indices, lr=lr, l1=l1,
                                     l2=l2, l2_shrinkage=l2_shrinkage,
                                     lr_power=lr_power,
                                     use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_ftrl_v2_eager_fallback(var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_ftrl_v2
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([grad, lr, l1, l2, l2_shrinkage, lr_power], _ctx)
  (grad, lr, l1, l2, l2_shrinkage, lr_power) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  linear = _ops.convert_to_tensor(linear, _dtypes.resource)
  _inputs_flat = [var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyFtrlV2", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_keras_momentum(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update relevant entries in '*var' and '*accum' according to the momentum scheme.

  Set use_nesterov = True if you want to use Nesterov momentum.

  That is for rows we have grad for, we update var and accum as follows:

  accum = accum * momentum - lr * grad
  var += accum

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Learning rate. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    momentum: A `Tensor`. Must have the same type as `lr`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var + momentum * accum, so in the end, the var you get is actually
      var + momentum * accum.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyKerasMomentum", name,
        _ctx._post_execution_callbacks, var, accum, lr, grad, indices,
        momentum, "use_locking", use_locking, "use_nesterov", use_nesterov)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_keras_momentum_eager_fallback(
            var, accum, lr, grad, indices, momentum, use_locking=use_locking,
            use_nesterov=use_nesterov, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyKerasMomentum", var=var, accum=accum, lr=lr,
                                            grad=grad, indices=indices,
                                            momentum=momentum,
                                            use_locking=use_locking,
                                            use_nesterov=use_nesterov,
                                            name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_keras_momentum_eager_fallback(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_keras_momentum
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad, momentum], _ctx)
  (lr, grad, momentum) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad, indices, momentum]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking, "use_nesterov", use_nesterov)
  _result = _execute.execute(b"ResourceSparseApplyKerasMomentum", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_momentum(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update relevant entries in '*var' and '*accum' according to the momentum scheme.

  Set use_nesterov = True if you want to use Nesterov momentum.

  That is for rows we have grad for, we update var and accum as follows:

  accum = accum * momentum + grad
  var -= lr * accum

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Learning rate. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    momentum: A `Tensor`. Must have the same type as `lr`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var - lr * momentum * accum, so in the end, the var you get is actually
      var - lr * momentum * accum.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyMomentum", name, _ctx._post_execution_callbacks,
        var, accum, lr, grad, indices, momentum, "use_locking", use_locking,
        "use_nesterov", use_nesterov)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_momentum_eager_fallback(
            var, accum, lr, grad, indices, momentum, use_locking=use_locking,
            use_nesterov=use_nesterov, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyMomentum", var=var, accum=accum, lr=lr, grad=grad,
                                       indices=indices, momentum=momentum,
                                       use_locking=use_locking,
                                       use_nesterov=use_nesterov, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_momentum_eager_fallback(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_momentum
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, grad, momentum], _ctx)
  (lr, grad, momentum) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, grad, indices, momentum]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking, "use_nesterov", use_nesterov)
  _result = _execute.execute(b"ResourceSparseApplyMomentum", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_proximal_adagrad(var, accum, lr, l1, l2, grad, indices, use_locking=False, name=None):
  r"""Sparse update entries in '*var' and '*accum' according to FOBOS algorithm.

  That is for rows we have grad for, we update var and accum as follows:
  accum += grad * grad
  prox_v = var
  prox_v -= lr * grad * (1 / sqrt(accum))
  var = sign(prox_v)/(1+lr*l2) * max{|prox_v|-lr*l1,0}

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    accum: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Learning rate. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `lr`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `lr`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyProximalAdagrad", name,
        _ctx._post_execution_callbacks, var, accum, lr, l1, l2, grad, indices,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_proximal_adagrad_eager_fallback(
            var, accum, lr, l1, l2, grad, indices, use_locking=use_locking,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyProximalAdagrad", var=var, accum=accum, lr=lr,
                                              l1=l1, l2=l2, grad=grad,
                                              indices=indices,
                                              use_locking=use_locking,
                                              name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_proximal_adagrad_eager_fallback(var, accum, lr, l1, l2, grad, indices, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_proximal_adagrad
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, l1, l2, grad], _ctx)
  (lr, l1, l2, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  accum = _ops.convert_to_tensor(accum, _dtypes.resource)
  _inputs_flat = [var, accum, lr, l1, l2, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyProximalAdagrad", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_proximal_gradient_descent(var, alpha, l1, l2, grad, indices, use_locking=False, name=None):
  r"""Sparse update '*var' as FOBOS algorithm with fixed learning rate.

  That is for rows we have grad for, we update var as follows:
  prox_v = var - alpha * grad
  var = sign(prox_v)/(1+alpha*l2) * max{|prox_v|-alpha*l1,0}

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    alpha: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `alpha`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `alpha`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `alpha`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyProximalGradientDescent", name,
        _ctx._post_execution_callbacks, var, alpha, l1, l2, grad, indices,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_proximal_gradient_descent_eager_fallback(
            var, alpha, l1, l2, grad, indices, use_locking=use_locking,
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
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyProximalGradientDescent", var=var, alpha=alpha,
                                                      l1=l1, l2=l2, grad=grad,
                                                      indices=indices,
                                                      use_locking=use_locking,
                                                      name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_proximal_gradient_descent_eager_fallback(var, alpha, l1, l2, grad, indices, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_proximal_gradient_descent
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([alpha, l1, l2, grad], _ctx)
  (alpha, l1, l2, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  _inputs_flat = [var, alpha, l1, l2, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyProximalGradientDescent", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def resource_sparse_apply_rms_prop(var, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None):
  r"""Update '*var' according to the RMSProp algorithm.

  Note that in dense implementation of this algorithm, ms and mom will
  update even if the grad is zero, but in this sparse implementation, ms
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon)

  ms <- rho * ms_{t-1} + (1-rho) * grad * grad
  mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)
  var <- var - mom

  Args:
    var: A `Tensor` of type `resource`. Should be from a Variable().
    ms: A `Tensor` of type `resource`. Should be from a Variable().
    mom: A `Tensor` of type `resource`. Should be from a Variable().
    lr: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `lr`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `lr`.
    epsilon: A `Tensor`. Must have the same type as `lr`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `lr`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var, ms and mom.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, ms, and mom tensors is protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResourceSparseApplyRMSProp", name, _ctx._post_execution_callbacks,
        var, ms, mom, lr, rho, momentum, epsilon, grad, indices,
        "use_locking", use_locking)
      return _result
    except _core._FallbackException:
      try:
        return resource_sparse_apply_rms_prop_eager_fallback(
            var, ms, mom, lr, rho, momentum, epsilon, grad, indices,
            use_locking=use_locking, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResourceSparseApplyRMSProp", var=var, ms=ms, mom=mom, lr=lr, rho=rho,
                                      momentum=momentum, epsilon=epsilon,
                                      grad=grad, indices=indices,
                                      use_locking=use_locking, name=name)
  return _op
  _result = None
  return _result



def resource_sparse_apply_rms_prop_eager_fallback(var, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resource_sparse_apply_rms_prop
  """
  _ctx = ctx if ctx else _context.context()
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([lr, rho, momentum, epsilon, grad], _ctx)
  (lr, rho, momentum, epsilon, grad) = _inputs_T
  _attr_Tindices, (indices,) = _execute.args_to_matching_eager([indices], _ctx)
  var = _ops.convert_to_tensor(var, _dtypes.resource)
  ms = _ops.convert_to_tensor(ms, _dtypes.resource)
  mom = _ops.convert_to_tensor(mom, _dtypes.resource)
  _inputs_flat = [var, ms, mom, lr, rho, momentum, epsilon, grad, indices]
  _attrs = ("T", _attr_T, "Tindices", _attr_Tindices, "use_locking",
  use_locking)
  _result = _execute.execute(b"ResourceSparseApplyRMSProp", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def sparse_apply_adadelta(var, accum, accum_update, lr, rho, epsilon, grad, indices, use_locking=False, name=None):
  r"""var: Should be from a Variable().

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    accum_update: A mutable `Tensor`. Must have the same type as `var`.
      : Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Learning rate. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay factor. Must be a scalar.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Constant factor. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_adadelta op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyAdadelta", var=var, accum=accum,
                               accum_update=accum_update, lr=lr, rho=rho,
                               epsilon=epsilon, grad=grad, indices=indices,
                               use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyAdadelta", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_adadelta_eager_fallback(var, accum, accum_update, lr, rho, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_adadelta op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_adagrad(var, accum, lr, grad, indices, use_locking=False, update_slots=True, name=None):
  r"""Update relevant entries in '*var' and '*accum' according to the adagrad scheme.

  That is for rows we have grad for, we update var and accum as follows:
  $$accum += grad * grad$$
  $$var -= lr * grad * (1 / sqrt(accum))$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Learning rate. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    update_slots: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_adagrad op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if update_slots is None:
    update_slots = True
  update_slots = _execute.make_bool(update_slots, "update_slots")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyAdagrad", var=var, accum=accum, lr=lr, grad=grad,
                              indices=indices, use_locking=use_locking,
                              update_slots=update_slots, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"), "update_slots",
            _op.get_attr("update_slots"))
  _execute.record_gradient(
      "SparseApplyAdagrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_adagrad_eager_fallback(var, accum, lr, grad, indices, use_locking=False, update_slots=True, name=None, ctx=None):
  raise RuntimeError("sparse_apply_adagrad op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_adagrad_da(var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, use_locking=False, name=None):
  r"""Update entries in '*var' and '*accum' according to the proximal adagrad scheme.

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    gradient_accumulator: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    gradient_squared_accumulator: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `var`.
      Learning rate. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    global_step: A `Tensor` of type `int64`.
      Training step number. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_adagrad_da op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyAdagradDA", var=var,
                                gradient_accumulator=gradient_accumulator,
                                gradient_squared_accumulator=gradient_squared_accumulator,
                                grad=grad, indices=indices, lr=lr, l1=l1,
                                l2=l2, global_step=global_step,
                                use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyAdagradDA", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_adagrad_da_eager_fallback(var, gradient_accumulator, gradient_squared_accumulator, grad, indices, lr, l1, l2, global_step, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_adagrad_da op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_centered_rms_prop(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None):
  r"""Update '*var' according to the centered RMSProp algorithm.

  The centered RMSProp algorithm uses an estimate of the centered second moment
  (i.e., the variance) for normalization, as opposed to regular RMSProp, which
  uses the (uncentered) second moment. This often helps with training, but is
  slightly more expensive in terms of computation and memory.

  Note that in dense implementation of this algorithm, mg, ms, and mom will
  update even if the grad is zero, but in this sparse implementation, mg, ms,
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  mean_grad = decay * mean_grad + (1-decay) * gradient
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon - mean_grad ** 2)

  $$ms <- rho * ms_{t-1} + (1-rho) * grad * grad$$
  $$mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)$$
  $$var <- var - mom$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    mg: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    ms: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    mom: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `var`.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var, ms and mom.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, mg, ms, and mom tensors is
      protected by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_centered_rms_prop op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyCenteredRMSProp", var=var, mg=mg, ms=ms, mom=mom, lr=lr,
                                      rho=rho, momentum=momentum,
                                      epsilon=epsilon, grad=grad,
                                      indices=indices,
                                      use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyCenteredRMSProp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_centered_rms_prop_eager_fallback(var, mg, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_centered_rms_prop op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_ftrl(var, accum, linear, grad, indices, lr, l1, l2, lr_power, use_locking=False, name=None):
  r"""Update relevant entries in '*var' according to the Ftrl-proximal scheme.

  That is for rows we have grad for, we update var, accum and linear as follows:
  $$accum_new = accum + grad * grad$$
  $$linear += grad + (accum_{new}^{-lr_{power}} - accum^{-lr_{power}} / lr * var$$
  $$quadratic = 1.0 / (accum_{new}^{lr_{power}} * lr) + 2 * l2$$
  $$var = (sign(linear) * l1 - linear) / quadratic\ if\ |linear| > l1\ else\ 0.0$$
  $$accum = accum_{new}$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    linear: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    lr_power: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_ftrl op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyFtrl", var=var, accum=accum, linear=linear, grad=grad,
                           indices=indices, lr=lr, l1=l1, l2=l2,
                           lr_power=lr_power, use_locking=use_locking,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyFtrl", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_ftrl_eager_fallback(var, accum, linear, grad, indices, lr, l1, l2, lr_power, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_ftrl op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_ftrl_v2(var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None):
  r"""Update relevant entries in '*var' according to the Ftrl-proximal scheme.

  That is for rows we have grad for, we update var, accum and linear as follows:
  grad_with_shrinkage = grad + 2 * l2_shrinkage * var
  accum_new = accum + grad_with_shrinkage * grad_with_shrinkage
  linear += grad_with_shrinkage +
      (accum_new^(-lr_power) - accum^(-lr_power)) / lr * var
  quadratic = 1.0 / (accum_new^(lr_power) * lr) + 2 * l2
  var = (sign(linear) * l1 - linear) / quadratic if |linear| > l1 else 0.0
  accum = accum_new

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    linear: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 shrinkage regulariation. Must be a scalar.
    l2_shrinkage: A `Tensor`. Must have the same type as `var`.
    lr_power: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_ftrl_v2 op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyFtrlV2", var=var, accum=accum, linear=linear, grad=grad,
                             indices=indices, lr=lr, l1=l1, l2=l2,
                             l2_shrinkage=l2_shrinkage, lr_power=lr_power,
                             use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyFtrlV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_ftrl_v2_eager_fallback(var, accum, linear, grad, indices, lr, l1, l2, l2_shrinkage, lr_power, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_ftrl_v2 op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_momentum(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None):
  r"""Update relevant entries in '*var' and '*accum' according to the momentum scheme.

  Set use_nesterov = True if you want to use Nesterov momentum.

  That is for rows we have grad for, we update var and accum as follows:

  $$accum = accum * momentum + grad$$
  $$var -= lr * accum$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Learning rate. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    momentum: A `Tensor`. Must have the same type as `var`.
      Momentum. Must be a scalar.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var and accum tensors will be protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    use_nesterov: An optional `bool`. Defaults to `False`.
      If `True`, the tensor passed to compute grad will be
      var - lr * momentum * accum, so in the end, the var you get is actually
      var - lr * momentum * accum.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_momentum op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  if use_nesterov is None:
    use_nesterov = False
  use_nesterov = _execute.make_bool(use_nesterov, "use_nesterov")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyMomentum", var=var, accum=accum, lr=lr, grad=grad,
                               indices=indices, momentum=momentum,
                               use_locking=use_locking,
                               use_nesterov=use_nesterov, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"), "use_nesterov",
            _op.get_attr("use_nesterov"))
  _execute.record_gradient(
      "SparseApplyMomentum", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_momentum_eager_fallback(var, accum, lr, grad, indices, momentum, use_locking=False, use_nesterov=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_momentum op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_proximal_adagrad(var, accum, lr, l1, l2, grad, indices, use_locking=False, name=None):
  r"""Sparse update entries in '*var' and '*accum' according to FOBOS algorithm.

  That is for rows we have grad for, we update var and accum as follows:
  $$accum += grad * grad$$
  $$prox_v = var$$
  $$prox_v -= lr * grad * (1 / sqrt(accum))$$
  $$var = sign(prox_v)/(1+lr*l2) * max{|prox_v|-lr*l1,0}$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    accum: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Learning rate. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, updating of the var and accum tensors will be protected by
      a lock; otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_proximal_adagrad op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyProximalAdagrad", var=var, accum=accum, lr=lr, l1=l1,
                                      l2=l2, grad=grad, indices=indices,
                                      use_locking=use_locking, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyProximalAdagrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_proximal_adagrad_eager_fallback(var, accum, lr, l1, l2, grad, indices, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_proximal_adagrad op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_proximal_gradient_descent(var, alpha, l1, l2, grad, indices, use_locking=False, name=None):
  r"""Sparse update '*var' as FOBOS algorithm with fixed learning rate.

  That is for rows we have grad for, we update var as follows:
  $$prox_v = var - alpha * grad$$
  $$var = sign(prox_v)/(1+alpha*l2) * max{|prox_v|-alpha*l1,0}$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    alpha: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    l1: A `Tensor`. Must have the same type as `var`.
      L1 regularization. Must be a scalar.
    l2: A `Tensor`. Must have the same type as `var`.
      L2 regularization. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var and accum.
    use_locking: An optional `bool`. Defaults to `False`.
      If True, the subtraction will be protected by a lock;
      otherwise the behavior is undefined, but may exhibit less contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_proximal_gradient_descent op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyProximalGradientDescent", var=var, alpha=alpha, l1=l1,
                                              l2=l2, grad=grad,
                                              indices=indices,
                                              use_locking=use_locking,
                                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyProximalGradientDescent", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_proximal_gradient_descent_eager_fallback(var, alpha, l1, l2, grad, indices, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_proximal_gradient_descent op does not support eager execution. Arg 'out' is a ref.")

def sparse_apply_rms_prop(var, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None):
  r"""Update '*var' according to the RMSProp algorithm.

  Note that in dense implementation of this algorithm, ms and mom will
  update even if the grad is zero, but in this sparse implementation, ms
  and mom will not update in iterations during which the grad is zero.

  mean_square = decay * mean_square + (1-decay) * gradient ** 2
  Delta = learning_rate * gradient / sqrt(mean_square + epsilon)

  $$ms <- rho * ms_{t-1} + (1-rho) * grad * grad$$
  $$mom <- momentum * mom_{t-1} + lr * grad / sqrt(ms + epsilon)$$
  $$var <- var - mom$$

  Args:
    var: A mutable `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `complex64`, `int64`, `qint8`, `quint8`, `qint32`, `bfloat16`, `uint16`, `complex128`, `half`, `uint32`, `uint64`.
      Should be from a Variable().
    ms: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    mom: A mutable `Tensor`. Must have the same type as `var`.
      Should be from a Variable().
    lr: A `Tensor`. Must have the same type as `var`.
      Scaling factor. Must be a scalar.
    rho: A `Tensor`. Must have the same type as `var`.
      Decay rate. Must be a scalar.
    momentum: A `Tensor`. Must have the same type as `var`.
    epsilon: A `Tensor`. Must have the same type as `var`.
      Ridge term. Must be a scalar.
    grad: A `Tensor`. Must have the same type as `var`. The gradient.
    indices: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      A vector of indices into the first dimension of var, ms and mom.
    use_locking: An optional `bool`. Defaults to `False`.
      If `True`, updating of the var, ms, and mom tensors is protected
      by a lock; otherwise the behavior is undefined, but may exhibit less
      contention.
    name: A name for the operation (optional).

  Returns:
    A mutable `Tensor`. Has the same type as `var`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("sparse_apply_rms_prop op does not support eager execution. Arg 'out' is a ref.")
  # Add nodes to the TensorFlow graph.
  if use_locking is None:
    use_locking = False
  use_locking = _execute.make_bool(use_locking, "use_locking")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SparseApplyRMSProp", var=var, ms=ms, mom=mom, lr=lr, rho=rho,
                              momentum=momentum, epsilon=epsilon, grad=grad,
                              indices=indices, use_locking=use_locking,
                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tindices", _op.get_attr("Tindices"),
            "use_locking", _op.get_attr("use_locking"))
  _execute.record_gradient(
      "SparseApplyRMSProp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_apply_rms_prop_eager_fallback(var, ms, mom, lr, rho, momentum, epsilon, grad, indices, use_locking=False, name=None, ctx=None):
  raise RuntimeError("sparse_apply_rms_prop op does not support eager execution. Arg 'out' is a ref.")
def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "ApplyAdaMax"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "beta1_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyAdadelta"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum_update"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyAdagrad"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "update_slots"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "ApplyAdagradDA"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "gradient_accumulator"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "gradient_squared_accumulator"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "global_step"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyAdam"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "v"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "beta1_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyAddSign"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sign_decay"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyCenteredRMSProp"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mg"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "ms"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mom"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyFtrl"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "linear"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyFtrlV2"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "linear"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_shrinkage"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyGradientDescent"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "delta"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyMomentum"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyPowerSign"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "m"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "logbase"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sign_decay"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyProximalAdagrad"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyProximalGradientDescent"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "delta"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ApplyRMSProp"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "ms"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mom"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResourceApplyAdaMax"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "m"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "v"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "beta1_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAdadelta"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum_update"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAdagrad"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "update_slots"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAdagradDA"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "gradient_accumulator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "gradient_squared_accumulator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "global_step"
#     type: DT_INT64
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAdam"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "m"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "v"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "beta1_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAdamWithAmsgrad"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "m"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "v"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "vhat"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "beta1_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2_power"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyAddSign"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "m"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sign_decay"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyCenteredRMSProp"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mg"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "ms"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mom"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyFtrl"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "linear"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyFtrlV2"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "linear"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_shrinkage"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyGradientDescent"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "delta"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyKerasMomentum"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyMomentum"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyPowerSign"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "m"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "logbase"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sign_decay"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "beta"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyProximalAdagrad"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyProximalGradientDescent"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "delta"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceApplyRMSProp"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "ms"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mom"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyAdadelta"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum_update"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyAdagrad"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "update_slots"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyAdagradDA"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "gradient_accumulator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "gradient_squared_accumulator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "global_step"
#     type: DT_INT64
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyCenteredRMSProp"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mg"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "ms"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mom"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyFtrl"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "linear"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyFtrlV2"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "linear"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_shrinkage"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyKerasMomentum"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "momentum"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyMomentum"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "momentum"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyProximalAdagrad"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "accum"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyProximalGradientDescent"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ResourceSparseApplyRMSProp"
#   input_arg {
#     name: "var"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "ms"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "mom"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "SparseApplyAdadelta"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum_update"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyAdagrad"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "update_slots"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "SparseApplyAdagradDA"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "gradient_accumulator"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "gradient_squared_accumulator"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "global_step"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyCenteredRMSProp"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mg"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "ms"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mom"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyFtrl"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "linear"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyFtrlV2"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "linear"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2_shrinkage"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "lr_power"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyMomentum"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "use_nesterov"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyProximalAdagrad"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "accum"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyProximalGradientDescent"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l1"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "l2"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SparseApplyRMSProp"
#   input_arg {
#     name: "var"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "ms"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "mom"
#     type_attr: "T"
#     is_ref: true
#   }
#   input_arg {
#     name: "lr"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "rho"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "momentum"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "epsilon"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "grad"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "indices"
#     type_attr: "Tindices"
#   }
#   output_arg {
#     name: "out"
#     type_attr: "T"
#     is_ref: true
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
#   attr {
#     name: "use_locking"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\304\001\n\013ApplyAdaMax\022\013\n\003var\"\001T\200\001\001\022\t\n\001m\"\001T\200\001\001\022\t\n\001v\"\001T\200\001\001\022\020\n\013beta1_power\"\001T\022\007\n\002lr\"\001T\022\n\n\005beta1\"\001T\022\n\n\005beta2\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\265\001\n\rApplyAdadelta\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\024\n\014accum_update\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\240\001\n\014ApplyAdagrad\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014update_slots\022\004bool\032\002(\001\n\340\001\n\016ApplyAdagradDA\022\013\n\003var\"\001T\200\001\001\022\034\n\024gradient_accumulator\"\001T\200\001\001\022$\n\034gradient_squared_accumulator\"\001T\200\001\001\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\017\n\013global_step\030\t\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\356\001\n\tApplyAdam\022\013\n\003var\"\001T\200\001\001\022\t\n\001m\"\001T\200\001\001\022\t\n\001v\"\001T\200\001\001\022\020\n\013beta1_power\"\001T\022\020\n\013beta2_power\"\001T\022\007\n\002lr\"\001T\022\n\n\005beta1\"\001T\022\n\n\005beta2\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\n\252\001\n\014ApplyAddSign\022\013\n\003var\"\001T\200\001\001\022\t\n\001m\"\001T\200\001\001\022\007\n\002lr\"\001T\022\n\n\005alpha\"\001T\022\017\n\nsign_decay\"\001T\022\t\n\004beta\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\313\001\n\024ApplyCenteredRMSProp\022\013\n\003var\"\001T\200\001\001\022\n\n\002mg\"\001T\200\001\001\022\n\n\002ms\"\001T\200\001\001\022\013\n\003mom\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\264\001\n\tApplyFtrl\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\016\n\006linear\"\001T\200\001\001\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\r\n\010lr_power\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\311\001\n\013ApplyFtrlV2\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\016\n\006linear\"\001T\200\001\001\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\021\n\014l2_shrinkage\"\001T\022\r\n\010lr_power\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\203\001\n\024ApplyGradientDescent\022\013\n\003var\"\001T\200\001\001\022\n\n\005alpha\"\001T\022\n\n\005delta\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\260\001\n\rApplyMomentum\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\r\n\010momentum\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\n\256\001\n\016ApplyPowerSign\022\013\n\003var\"\001T\200\001\001\022\t\n\001m\"\001T\200\001\001\022\007\n\002lr\"\001T\022\014\n\007logbase\"\001T\022\017\n\nsign_decay\"\001T\022\t\n\004beta\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\240\001\n\024ApplyProximalAdagrad\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\235\001\n\034ApplyProximalGradientDescent\022\013\n\003var\"\001T\200\001\001\022\n\n\005alpha\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\n\n\005delta\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\267\001\n\014ApplyRMSProp\022\013\n\003var\"\001T\200\001\001\022\n\n\002ms\"\001T\200\001\001\022\013\n\003mom\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\n\266\001\n\023ResourceApplyAdaMax\022\007\n\003var\030\024\022\005\n\001m\030\024\022\005\n\001v\030\024\022\020\n\013beta1_power\"\001T\022\007\n\002lr\"\001T\022\n\n\005beta1\"\001T\022\n\n\005beta2\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\247\001\n\025ResourceApplyAdadelta\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\020\n\014accum_update\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\226\001\n\024ResourceApplyAdagrad\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014update_slots\022\004bool\032\002(\001\210\001\001\n\322\001\n\026ResourceApplyAdagradDA\022\007\n\003var\030\024\022\030\n\024gradient_accumulator\030\024\022 \n\034gradient_squared_accumulator\030\024\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\017\n\013global_step\030\t\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\340\001\n\021ResourceApplyAdam\022\007\n\003var\030\024\022\005\n\001m\030\024\022\005\n\001v\030\024\022\020\n\013beta1_power\"\001T\022\020\n\013beta2_power\"\001T\022\007\n\002lr\"\001T\022\n\n\005beta1\"\001T\022\n\n\005beta2\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\210\001\001\n\333\001\n\034ResourceApplyAdamWithAmsgrad\022\007\n\003var\030\024\022\005\n\001m\030\024\022\005\n\001v\030\024\022\010\n\004vhat\030\024\022\020\n\013beta1_power\"\001T\022\020\n\013beta2_power\"\001T\022\007\n\002lr\"\001T\022\n\n\005beta1\"\001T\022\n\n\005beta2\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\240\001\n\024ResourceApplyAddSign\022\007\n\003var\030\024\022\005\n\001m\030\024\022\007\n\002lr\"\001T\022\n\n\005alpha\"\001T\022\017\n\nsign_decay\"\001T\022\t\n\004beta\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\271\001\n\034ResourceApplyCenteredRMSProp\022\007\n\003var\030\024\022\006\n\002mg\030\024\022\006\n\002ms\030\024\022\007\n\003mom\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\246\001\n\021ResourceApplyFtrl\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\n\n\006linear\030\024\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\r\n\010lr_power\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\273\001\n\023ResourceApplyFtrlV2\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\n\n\006linear\030\024\022\t\n\004grad\"\001T\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\021\n\014l2_shrinkage\"\001T\022\r\n\010lr_power\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n}\n\034ResourceApplyGradientDescent\022\007\n\003var\030\024\022\n\n\005alpha\"\001T\022\n\n\005delta\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\253\001\n\032ResourceApplyKerasMomentum\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\r\n\010momentum\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\210\001\001\n\246\001\n\025ResourceApplyMomentum\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\r\n\010momentum\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\210\001\001\n\244\001\n\026ResourceApplyPowerSign\022\007\n\003var\030\024\022\005\n\001m\030\024\022\007\n\002lr\"\001T\022\014\n\007logbase\"\001T\022\017\n\nsign_decay\"\001T\022\t\n\004beta\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\226\001\n\034ResourceApplyProximalAdagrad\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\227\001\n$ResourceApplyProximalGradientDescent\022\007\n\003var\030\024\022\n\n\005alpha\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\n\n\005delta\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\251\001\n\024ResourceApplyRMSProp\022\007\n\003var\030\024\022\006\n\002ms\030\024\022\007\n\003mom\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\334\001\n\033ResourceSparseApplyAdadelta\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\020\n\014accum_update\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\313\001\n\032ResourceSparseApplyAdagrad\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014update_slots\022\004bool\032\002(\001\210\001\001\n\207\002\n\034ResourceSparseApplyAdagradDA\022\007\n\003var\030\024\022\030\n\024gradient_accumulator\030\024\022 \n\034gradient_squared_accumulator\030\024\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\017\n\013global_step\030\t\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\356\001\n\"ResourceSparseApplyCenteredRMSProp\022\007\n\003var\030\024\022\006\n\002mg\030\024\022\006\n\002ms\030\024\022\007\n\003mom\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\333\001\n\027ResourceSparseApplyFtrl\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\n\n\006linear\030\024\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\r\n\010lr_power\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\360\001\n\031ResourceSparseApplyFtrlV2\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\n\n\006linear\030\024\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\021\n\014l2_shrinkage\"\001T\022\r\n\010lr_power\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\340\001\n ResourceSparseApplyKerasMomentum\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\r\n\010momentum\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\210\001\001\n\333\001\n\033ResourceSparseApplyMomentum\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\r\n\010momentum\"\001T\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\210\001\001\n\313\001\n\"ResourceSparseApplyProximalAdagrad\022\007\n\003var\030\024\022\t\n\005accum\030\024\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\313\001\n*ResourceSparseApplyProximalGradientDescent\022\007\n\003var\030\024\022\n\n\005alpha\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\336\001\n\032ResourceSparseApplyRMSProp\022\007\n\003var\030\024\022\006\n\002ms\030\024\022\007\n\003mom\030\024\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\210\001\001\n\352\001\n\023SparseApplyAdadelta\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\024\n\014accum_update\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\325\001\n\022SparseApplyAdagrad\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014update_slots\022\004bool\032\002(\001\n\225\002\n\024SparseApplyAdagradDA\022\013\n\003var\"\001T\200\001\001\022\034\n\024gradient_accumulator\"\001T\200\001\001\022$\n\034gradient_squared_accumulator\"\001T\200\001\001\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\017\n\013global_step\030\t\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\200\002\n\032SparseApplyCenteredRMSProp\022\013\n\003var\"\001T\200\001\001\022\n\n\002mg\"\001T\200\001\001\022\n\n\002ms\"\001T\200\001\001\022\013\n\003mom\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\351\001\n\017SparseApplyFtrl\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\016\n\006linear\"\001T\200\001\001\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\r\n\010lr_power\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\376\001\n\021SparseApplyFtrlV2\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\016\n\006linear\"\001T\200\001\001\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\021\n\014l2_shrinkage\"\001T\022\r\n\010lr_power\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\345\001\n\023SparseApplyMomentum\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\022\r\n\010momentum\"\001T\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\"\030\n\014use_nesterov\022\004bool\032\002(\000\n\325\001\n\032SparseApplyProximalAdagrad\022\013\n\003var\"\001T\200\001\001\022\r\n\005accum\"\001T\200\001\001\022\007\n\002lr\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\321\001\n\"SparseApplyProximalGradientDescent\022\013\n\003var\"\001T\200\001\001\022\n\n\005alpha\"\001T\022\007\n\002l1\"\001T\022\007\n\002l2\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000\n\354\001\n\022SparseApplyRMSProp\022\013\n\003var\"\001T\200\001\001\022\n\n\002ms\"\001T\200\001\001\022\013\n\003mom\"\001T\200\001\001\022\007\n\002lr\"\001T\022\010\n\003rho\"\001T\022\r\n\010momentum\"\001T\022\014\n\007epsilon\"\001T\022\t\n\004grad\"\001T\022\023\n\007indices\"\010Tindices\032\013\n\003out\"\001T\200\001\001\" \n\001T\022\004type:\025\n\0232\021\001\002\003\004\005\006\010\t\013\014\r\016\021\022\023\026\027\"\030\n\010Tindices\022\004type:\006\n\0042\002\003\t\"\027\n\013use_locking\022\004bool\032\002(\000")
