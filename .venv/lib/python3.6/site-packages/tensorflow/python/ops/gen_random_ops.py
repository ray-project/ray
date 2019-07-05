"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: random_ops.cc
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


def multinomial(logits, num_samples, seed=0, seed2=0, output_dtype=_dtypes.int64, name=None):
  r"""Draws samples from a multinomial distribution.

  Args:
    logits: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      2-D Tensor with shape `[batch_size, num_classes]`.  Each slice `[i, :]`
      represents the unnormalized log probabilities for all classes.
    num_samples: A `Tensor` of type `int32`.
      0-D.  Number of independent samples to draw for each row slice.
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 is set to be non-zero, the internal random number
      generator is seeded by the given seed.  Otherwise, a random seed is used.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    output_dtype: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `output_dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Multinomial",
        name, _ctx._post_execution_callbacks, logits, num_samples, "seed",
        seed, "seed2", seed2, "output_dtype", output_dtype)
      return _result
    except _core._FallbackException:
      try:
        return multinomial_eager_fallback(
            logits, num_samples, seed=seed, seed2=seed2,
            output_dtype=output_dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if output_dtype is None:
    output_dtype = _dtypes.int64
  output_dtype = _execute.make_type(output_dtype, "output_dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Multinomial", logits=logits, num_samples=num_samples, seed=seed,
                       seed2=seed2, output_dtype=output_dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "T",
            _op.get_attr("T"), "output_dtype", _op.get_attr("output_dtype"))
  _execute.record_gradient(
      "Multinomial", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def multinomial_eager_fallback(logits, num_samples, seed=0, seed2=0, output_dtype=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multinomial
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if output_dtype is None:
    output_dtype = _dtypes.int64
  output_dtype = _execute.make_type(output_dtype, "output_dtype")
  _attr_T, (logits,) = _execute.args_to_matching_eager([logits], _ctx)
  num_samples = _ops.convert_to_tensor(num_samples, _dtypes.int32)
  _inputs_flat = [logits, num_samples]
  _attrs = ("seed", seed, "seed2", seed2, "T", _attr_T, "output_dtype",
  output_dtype)
  _result = _execute.execute(b"Multinomial", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Multinomial", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def parameterized_truncated_normal(shape, means, stdevs, minvals, maxvals, seed=0, seed2=0, name=None):
  r"""Outputs random values from a normal distribution. The parameters may each be a

  scalar which applies to the entire output, or a vector of length shape[0] which
  stores the parameters for each batch.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor. Batches are indexed by the 0th dimension.
    means: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      The mean parameter of each batch.
    stdevs: A `Tensor`. Must have the same type as `means`.
      The standard deviation parameter of each batch. Must be greater than 0.
    minvals: A `Tensor`. Must have the same type as `means`.
      The minimum cutoff. May be -infinity.
    maxvals: A `Tensor`. Must have the same type as `means`.
      The maximum cutoff. May be +infinity, and must be more than the minval
      for each batch.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `means`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ParameterizedTruncatedNormal", name, _ctx._post_execution_callbacks,
        shape, means, stdevs, minvals, maxvals, "seed", seed, "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return parameterized_truncated_normal_eager_fallback(
            shape, means, stdevs, minvals, maxvals, seed=seed, seed2=seed2,
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ParameterizedTruncatedNormal", shape=shape, means=means,
                                        stdevs=stdevs, minvals=minvals,
                                        maxvals=maxvals, seed=seed,
                                        seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"),
            "dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "ParameterizedTruncatedNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def parameterized_truncated_normal_eager_fallback(shape, means, stdevs, minvals, maxvals, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function parameterized_truncated_normal
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_dtype, _inputs_dtype = _execute.args_to_matching_eager([means, stdevs, minvals, maxvals], _ctx)
  (means, stdevs, minvals, maxvals) = _inputs_dtype
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _inputs_flat = [shape, means, stdevs, minvals, maxvals]
  _attrs = ("seed", seed, "seed2", seed2, "dtype", _attr_dtype, "T", _attr_T)
  _result = _execute.execute(b"ParameterizedTruncatedNormal", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ParameterizedTruncatedNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_gamma(shape, alpha, seed=0, seed2=0, name=None):
  r"""Outputs random values from the Gamma distribution(s) described by alpha.

  This op uses the algorithm by Marsaglia et al. to acquire samples via
  transformation-rejection from pairs of uniform and normal random variables.
  See http://dl.acm.org/citation.cfm?id=358414

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D integer tensor. Shape of independent samples to draw from each
      distribution described by the shape parameters given in alpha.
    alpha: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
      A tensor in which each scalar is a "shape" parameter describing the
      associated gamma distribution.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `alpha`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RandomGamma",
        name, _ctx._post_execution_callbacks, shape, alpha, "seed", seed,
        "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return random_gamma_eager_fallback(
            shape, alpha, seed=seed, seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomGamma", shape=shape, alpha=alpha, seed=seed, seed2=seed2,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "S",
            _op.get_attr("S"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "RandomGamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_gamma_eager_fallback(shape, alpha, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_gamma
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_S, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _attr_T, (alpha,) = _execute.args_to_matching_eager([alpha], _ctx)
  _inputs_flat = [shape, alpha]
  _attrs = ("seed", seed, "seed2", seed2, "S", _attr_S, "T", _attr_T)
  _result = _execute.execute(b"RandomGamma", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomGamma", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_gamma_grad(alpha, sample, name=None):
  r"""Computes the derivative of a Gamma random sample w.r.t. `alpha`.

  Args:
    alpha: A `Tensor`. Must be one of the following types: `float32`, `float64`.
    sample: A `Tensor`. Must have the same type as `alpha`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `alpha`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomGammaGrad", name, _ctx._post_execution_callbacks, alpha,
        sample)
      return _result
    except _core._FallbackException:
      try:
        return random_gamma_grad_eager_fallback(
            alpha, sample, name=name, ctx=_ctx)
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
        "RandomGammaGrad", alpha=alpha, sample=sample, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RandomGammaGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_gamma_grad_eager_fallback(alpha, sample, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_gamma_grad
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([alpha, sample], _ctx)
  (alpha, sample) = _inputs_T
  _inputs_flat = [alpha, sample]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"RandomGammaGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomGammaGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_poisson(shape, rate, seed=0, seed2=0, name=None):
  r"""Use RandomPoissonV2 instead.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
    rate: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`.
    seed: An optional `int`. Defaults to `0`.
    seed2: An optional `int`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `rate`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomPoisson", name, _ctx._post_execution_callbacks, shape, rate,
        "seed", seed, "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return random_poisson_eager_fallback(
            shape, rate, seed=seed, seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomPoisson", shape=shape, rate=rate, seed=seed, seed2=seed2,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "S",
            _op.get_attr("S"), "dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "RandomPoisson", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_poisson_eager_fallback(shape, rate, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_poisson
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_S, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _attr_dtype, (rate,) = _execute.args_to_matching_eager([rate], _ctx)
  _inputs_flat = [shape, rate]
  _attrs = ("seed", seed, "seed2", seed2, "S", _attr_S, "dtype", _attr_dtype)
  _result = _execute.execute(b"RandomPoisson", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomPoisson", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_poisson_v2(shape, rate, seed=0, seed2=0, dtype=_dtypes.int64, name=None):
  r"""Outputs random values from the Poisson distribution(s) described by rate.

  This op uses two algorithms, depending on rate. If rate >= 10, then
  the algorithm by Hormann is used to acquire samples via
  transformation-rejection.
  See http://www.sciencedirect.com/science/article/pii/0167668793909974.

  Otherwise, Knuth's algorithm is used to acquire samples via multiplying uniform
  random variables.
  See Donald E. Knuth (1969). Seminumerical Algorithms. The Art of Computer
  Programming, Volume 2. Addison Wesley

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      1-D integer tensor. Shape of independent samples to draw from each
      distribution described by the shape parameters given in rate.
    rate: A `Tensor`. Must be one of the following types: `half`, `float32`, `float64`, `int32`, `int64`.
      A tensor in which each scalar is a "rate" parameter describing the
      associated poisson distribution.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    dtype: An optional `tf.DType` from: `tf.half, tf.float32, tf.float64, tf.int32, tf.int64`. Defaults to `tf.int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomPoissonV2", name, _ctx._post_execution_callbacks, shape, rate,
        "seed", seed, "seed2", seed2, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return random_poisson_v2_eager_fallback(
            shape, rate, seed=seed, seed2=seed2, dtype=dtype, name=name,
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if dtype is None:
    dtype = _dtypes.int64
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomPoissonV2", shape=shape, rate=rate, seed=seed, seed2=seed2,
                           dtype=dtype, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "S",
            _op.get_attr("S"), "R", _op.get_attr("R"), "dtype",
            _op.get_attr("dtype"))
  _execute.record_gradient(
      "RandomPoissonV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_poisson_v2_eager_fallback(shape, rate, seed=0, seed2=0, dtype=_dtypes.int64, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_poisson_v2
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if dtype is None:
    dtype = _dtypes.int64
  dtype = _execute.make_type(dtype, "dtype")
  _attr_S, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _attr_R, (rate,) = _execute.args_to_matching_eager([rate], _ctx, _dtypes.float64)
  _inputs_flat = [shape, rate]
  _attrs = ("seed", seed, "seed2", seed2, "S", _attr_S, "R", _attr_R, "dtype",
  dtype)
  _result = _execute.execute(b"RandomPoissonV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomPoissonV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_shuffle(value, seed=0, seed2=0, name=None):
  r"""Randomly shuffles a tensor along its first dimension.

    The tensor is shuffled along dimension 0, such that each `value[j]` is mapped
    to one and only one `output[i]`. For example, a mapping that might occur for a
    3x2 tensor is:

  ```
  [[1, 2],       [[5, 6],
   [3, 4],  ==>   [1, 2],
   [5, 6]]        [3, 4]]
  ```

  Args:
    value: A `Tensor`. The tensor to be shuffled.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomShuffle", name, _ctx._post_execution_callbacks, value, "seed",
        seed, "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return random_shuffle_eager_fallback(
            value, seed=seed, seed2=seed2, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomShuffle", value=value, seed=seed, seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "RandomShuffle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_shuffle_eager_fallback(value, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_shuffle
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (value,) = _execute.args_to_matching_eager([value], _ctx)
  _inputs_flat = [value]
  _attrs = ("seed", seed, "seed2", seed2, "T", _attr_T)
  _result = _execute.execute(b"RandomShuffle", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomShuffle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_standard_normal(shape, dtype, seed=0, seed2=0, name=None):
  r"""Outputs random values from a normal distribution.

  The generated values will have mean 0 and standard deviation 1.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    dtype: A `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`.
      The type of the output.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomStandardNormal", name, _ctx._post_execution_callbacks, shape,
        "seed", seed, "seed2", seed2, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return random_standard_normal_eager_fallback(
            shape, seed=seed, seed2=seed2, dtype=dtype, name=name, ctx=_ctx)
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomStandardNormal", shape=shape, dtype=dtype, seed=seed,
                                seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"),
            "dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "RandomStandardNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_standard_normal_eager_fallback(shape, dtype, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_standard_normal
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _inputs_flat = [shape]
  _attrs = ("seed", seed, "seed2", seed2, "dtype", dtype, "T", _attr_T)
  _result = _execute.execute(b"RandomStandardNormal", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomStandardNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_uniform(shape, dtype, seed=0, seed2=0, name=None):
  r"""Outputs random values from a uniform distribution.

  The generated values follow a uniform distribution in the range `[0, 1)`. The
  lower bound 0 is included in the range, while the upper bound 1 is excluded.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    dtype: A `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`.
      The type of the output.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomUniform", name, _ctx._post_execution_callbacks, shape, "seed",
        seed, "seed2", seed2, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return random_uniform_eager_fallback(
            shape, seed=seed, seed2=seed2, dtype=dtype, name=name, ctx=_ctx)
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomUniform", shape=shape, dtype=dtype, seed=seed, seed2=seed2,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"),
            "dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "RandomUniform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_uniform_eager_fallback(shape, dtype, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_uniform
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _inputs_flat = [shape]
  _attrs = ("seed", seed, "seed2", seed2, "dtype", dtype, "T", _attr_T)
  _result = _execute.execute(b"RandomUniform", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomUniform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_uniform_int(shape, minval, maxval, seed=0, seed2=0, name=None):
  r"""Outputs random integers from a uniform distribution.

  The generated values are uniform integers in the range `[minval, maxval)`.
  The lower bound `minval` is included in the range, while the upper bound
  `maxval` is excluded.

  The random integers are slightly biased unless `maxval - minval` is an exact
  power of two.  The bias is small for values of `maxval - minval` significantly
  smaller than the range of the output (either `2^32` or `2^64`).

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    minval: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      0-D.  Inclusive lower bound on the generated integers.
    maxval: A `Tensor`. Must have the same type as `minval`.
      0-D.  Exclusive upper bound on the generated integers.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `minval`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RandomUniformInt", name, _ctx._post_execution_callbacks, shape,
        minval, maxval, "seed", seed, "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return random_uniform_int_eager_fallback(
            shape, minval, maxval, seed=seed, seed2=seed2, name=name,
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RandomUniformInt", shape=shape, minval=minval, maxval=maxval,
                            seed=seed, seed2=seed2, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"),
            "Tout", _op.get_attr("Tout"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "RandomUniformInt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_uniform_int_eager_fallback(shape, minval, maxval, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_uniform_int
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_Tout, _inputs_Tout = _execute.args_to_matching_eager([minval, maxval], _ctx)
  (minval, maxval) = _inputs_Tout
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _inputs_flat = [shape, minval, maxval]
  _attrs = ("seed", seed, "seed2", seed2, "Tout", _attr_Tout, "T", _attr_T)
  _result = _execute.execute(b"RandomUniformInt", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomUniformInt", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def truncated_normal(shape, dtype, seed=0, seed2=0, name=None):
  r"""Outputs random values from a truncated normal distribution.

  The generated values follow a normal distribution with mean 0 and standard
  deviation 1, except that values whose magnitude is more than 2 standard
  deviations from the mean are dropped and re-picked.

  Args:
    shape: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      The shape of the output tensor.
    dtype: A `tf.DType` from: `tf.half, tf.bfloat16, tf.float32, tf.float64`.
      The type of the output.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TruncatedNormal", name, _ctx._post_execution_callbacks, shape,
        "seed", seed, "seed2", seed2, "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return truncated_normal_eager_fallback(
            shape, seed=seed, seed2=seed2, dtype=dtype, name=name, ctx=_ctx)
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
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TruncatedNormal", shape=shape, dtype=dtype, seed=seed, seed2=seed2,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("seed", _op.get_attr("seed"), "seed2", _op.get_attr("seed2"),
            "dtype", _op.get_attr("dtype"), "T", _op.get_attr("T"))
  _execute.record_gradient(
      "TruncatedNormal", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def truncated_normal_eager_fallback(shape, dtype, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function truncated_normal
  """
  _ctx = ctx if ctx else _context.context()
  dtype = _execute.make_type(dtype, "dtype")
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (shape,) = _execute.args_to_matching_eager([shape], _ctx)
  _inputs_flat = [shape]
  _attrs = ("seed", seed, "seed2", seed2, "dtype", dtype, "T", _attr_T)
  _result = _execute.execute(b"TruncatedNormal", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TruncatedNormal", _inputs_flat, _attrs, _result, name)
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
#   name: "Multinomial"
#   input_arg {
#     name: "logits"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "num_samples"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type_attr: "output_dtype"
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
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_UINT16
#         type: DT_HALF
#         type: DT_UINT32
#         type: DT_UINT64
#       }
#     }
#   }
#   attr {
#     name: "output_dtype"
#     type: "type"
#     default_value {
#       type: DT_INT64
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
# op {
#   name: "ParameterizedTruncatedNormal"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "means"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "stdevs"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "minvals"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "maxvals"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "T"
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
#   name: "RandomGamma"
#   input_arg {
#     name: "shape"
#     type_attr: "S"
#   }
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
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
#     name: "S"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "RandomGammaGrad"
#   input_arg {
#     name: "alpha"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "sample"
#     type_attr: "T"
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
#       }
#     }
#   }
# }
# op {
#   name: "RandomPoisson"
#   input_arg {
#     name: "shape"
#     type_attr: "S"
#   }
#   input_arg {
#     name: "rate"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "S"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   deprecation {
#     version: 25
#     explanation: "Replaced by RandomPoissonV2"
#   }
#   is_stateful: true
# }
# op {
#   name: "RandomPoissonV2"
#   input_arg {
#     name: "shape"
#     type_attr: "S"
#   }
#   input_arg {
#     name: "rate"
#     type_attr: "R"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "S"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "R"
#     type: "type"
#     default_value {
#       type: DT_DOUBLE
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_INT64
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "RandomShuffle"
#   input_arg {
#     name: "value"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
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
#     name: "T"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "RandomStandardNormal"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "T"
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
#   name: "RandomUniform"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "T"
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
#   name: "RandomUniformInt"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "minval"
#     type_attr: "Tout"
#   }
#   input_arg {
#     name: "maxval"
#     type_attr: "Tout"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "Tout"
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
#     name: "Tout"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
#   attr {
#     name: "T"
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
#   name: "TruncatedNormal"
#   input_arg {
#     name: "shape"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "dtype"
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
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "T"
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
_op_def_lib = _InitOpDefLibrary(b"\n\250\001\n\013Multinomial\022\013\n\006logits\"\001T\022\017\n\013num_samples\030\003\032\026\n\006output\"\014output_dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\" \n\014output_dtype\022\004type\032\0020\t:\006\n\0042\002\003\t\210\001\001\n\322\001\n\034ParameterizedTruncatedNormal\022\n\n\005shape\"\001T\022\016\n\005means\"\005dtype\022\017\n\006stdevs\"\005dtype\022\020\n\007minvals\"\005dtype\022\020\n\007maxvals\"\005dtype\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\005dtype\022\004type:\010\n\0062\004\023\016\001\002\"\021\n\001T\022\004type:\006\n\0042\002\003\t\210\001\001\n\177\n\013RandomGamma\022\n\n\005shape\"\001S\022\n\n\005alpha\"\001T\032\013\n\006output\"\001T\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\021\n\001S\022\004type:\006\n\0042\002\003\t\"\022\n\001T\022\004type:\007\n\0052\003\023\001\002\210\001\001\nJ\n\017RandomGammaGrad\022\n\n\005alpha\"\001T\022\013\n\006sample\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\n\255\001\n\rRandomPoisson\022\n\n\005shape\"\001S\022\r\n\004rate\"\005dtype\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\021\n\001S\022\004type:\006\n\0042\002\003\t\"\026\n\005dtype\022\004type:\007\n\0052\003\023\001\002B\037\010\031\022\033Replaced by RandomPoissonV2\210\001\001\n\252\001\n\017RandomPoissonV2\022\n\n\005shape\"\001S\022\t\n\004rate\"\001R\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\021\n\001S\022\004type:\006\n\0042\002\003\t\"\030\n\001R\022\004type\032\0020\002:\t\n\0072\005\023\001\002\003\t\"\034\n\005dtype\022\004type\032\0020\t:\t\n\0072\005\023\001\002\003\t\210\001\001\nY\n\rRandomShuffle\022\n\n\005value\"\001T\032\013\n\006output\"\001T\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\t\n\001T\022\004type\210\001\001\n\205\001\n\024RandomStandardNormal\022\n\n\005shape\"\001T\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\005dtype\022\004type:\010\n\0062\004\023\016\001\002\"\021\n\001T\022\004type:\006\n\0042\002\003\t\210\001\001\n~\n\rRandomUniform\022\n\n\005shape\"\001T\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\005dtype\022\004type:\010\n\0062\004\023\016\001\002\"\021\n\001T\022\004type:\006\n\0042\002\003\t\210\001\001\n\235\001\n\020RandomUniformInt\022\n\n\005shape\"\001T\022\016\n\006minval\"\004Tout\022\016\n\006maxval\"\004Tout\032\016\n\006output\"\004Tout\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\024\n\004Tout\022\004type:\006\n\0042\002\003\t\"\021\n\001T\022\004type:\006\n\0042\002\003\t\210\001\001\n\200\001\n\017TruncatedNormal\022\n\n\005shape\"\001T\032\017\n\006output\"\005dtype\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\027\n\005dtype\022\004type:\010\n\0062\004\023\016\001\002\"\021\n\001T\022\004type:\006\n\0042\002\003\t\210\001\001")
