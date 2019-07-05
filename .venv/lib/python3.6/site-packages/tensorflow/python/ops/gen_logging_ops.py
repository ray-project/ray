"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: logging_ops.cc
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


def _assert(condition, data, summarize=3, name=None):
  r"""Asserts that the given condition is true.

  If `condition` evaluates to false, print the list of tensors in `data`.
  `summarize` determines how many entries of the tensors to print.

  Args:
    condition: A `Tensor` of type `bool`. The condition to evaluate.
    data: A list of `Tensor` objects.
      The tensors to print out when condition is false.
    summarize: An optional `int`. Defaults to `3`.
      Print this many entries of each tensor.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Assert", name,
        _ctx._post_execution_callbacks, condition, data, "summarize",
        summarize)
      return _result
    except _core._FallbackException:
      try:
        return _assert_eager_fallback(
            condition, data, summarize=summarize, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Assert", condition=condition, data=data, summarize=summarize,
                  name=name)
  return _op
  _result = None
  return _result



def _assert_eager_fallback(condition, data, summarize=3, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _assert
  """
  _ctx = ctx if ctx else _context.context()
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _attr_T, data = _execute.convert_to_mixed_eager_tensors(data, _ctx)
  condition = _ops.convert_to_tensor(condition, _dtypes.bool)
  _inputs_flat = [condition] + list(data)
  _attrs = ("T", _attr_T, "summarize", summarize)
  _result = _execute.execute(b"Assert", 0, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _result = None
  return _result


def audio_summary(tag, tensor, sample_rate, max_outputs=3, name=None):
  r"""Outputs a `Summary` protocol buffer with audio.

  The summary has up to `max_outputs` summary values containing audio. The
  audio is built from `tensor` which must be 3-D with shape `[batch_size,
  frames, channels]` or 2-D with shape `[batch_size, frames]`. The values are
  assumed to be in the range of `[-1.0, 1.0]` with a sample rate of `sample_rate`.

  The `tag` argument is a scalar `Tensor` of type `string`.  It is used to
  build the `tag` of the summary values:

  *  If `max_outputs` is 1, the summary value tag is '*tag*/audio'.
  *  If `max_outputs` is greater than 1, the summary value tags are
     generated sequentially as '*tag*/audio/0', '*tag*/audio/1', etc.

  Args:
    tag: A `Tensor` of type `string`.
      Scalar. Used to build the `tag` attribute of the summary values.
    tensor: A `Tensor` of type `float32`. 2-D of shape `[batch_size, frames]`.
    sample_rate: A `float`. The sample rate of the signal in hertz.
    max_outputs: An optional `int` that is `>= 1`. Defaults to `3`.
      Max number of batch elements to generate audio for.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AudioSummary",
        name, _ctx._post_execution_callbacks, tag, tensor, "sample_rate",
        sample_rate, "max_outputs", max_outputs)
      return _result
    except _core._FallbackException:
      try:
        return audio_summary_eager_fallback(
            tag, tensor, sample_rate=sample_rate, max_outputs=max_outputs,
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
  sample_rate = _execute.make_float(sample_rate, "sample_rate")
  if max_outputs is None:
    max_outputs = 3
  max_outputs = _execute.make_int(max_outputs, "max_outputs")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AudioSummary", tag=tag, tensor=tensor, sample_rate=sample_rate,
                        max_outputs=max_outputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("sample_rate", _op.get_attr("sample_rate"), "max_outputs",
            _op.get_attr("max_outputs"))
  _execute.record_gradient(
      "AudioSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def audio_summary_eager_fallback(tag, tensor, sample_rate, max_outputs=3, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function audio_summary
  """
  _ctx = ctx if ctx else _context.context()
  sample_rate = _execute.make_float(sample_rate, "sample_rate")
  if max_outputs is None:
    max_outputs = 3
  max_outputs = _execute.make_int(max_outputs, "max_outputs")
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  tensor = _ops.convert_to_tensor(tensor, _dtypes.float32)
  _inputs_flat = [tag, tensor]
  _attrs = ("sample_rate", sample_rate, "max_outputs", max_outputs)
  _result = _execute.execute(b"AudioSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AudioSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def audio_summary_v2(tag, tensor, sample_rate, max_outputs=3, name=None):
  r"""Outputs a `Summary` protocol buffer with audio.

  The summary has up to `max_outputs` summary values containing audio. The
  audio is built from `tensor` which must be 3-D with shape `[batch_size,
  frames, channels]` or 2-D with shape `[batch_size, frames]`. The values are
  assumed to be in the range of `[-1.0, 1.0]` with a sample rate of `sample_rate`.

  The `tag` argument is a scalar `Tensor` of type `string`.  It is used to
  build the `tag` of the summary values:

  *  If `max_outputs` is 1, the summary value tag is '*tag*/audio'.
  *  If `max_outputs` is greater than 1, the summary value tags are
     generated sequentially as '*tag*/audio/0', '*tag*/audio/1', etc.

  Args:
    tag: A `Tensor` of type `string`.
      Scalar. Used to build the `tag` attribute of the summary values.
    tensor: A `Tensor` of type `float32`. 2-D of shape `[batch_size, frames]`.
    sample_rate: A `Tensor` of type `float32`.
      The sample rate of the signal in hertz.
    max_outputs: An optional `int` that is `>= 1`. Defaults to `3`.
      Max number of batch elements to generate audio for.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AudioSummaryV2", name, _ctx._post_execution_callbacks, tag, tensor,
        sample_rate, "max_outputs", max_outputs)
      return _result
    except _core._FallbackException:
      try:
        return audio_summary_v2_eager_fallback(
            tag, tensor, sample_rate, max_outputs=max_outputs, name=name,
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
  if max_outputs is None:
    max_outputs = 3
  max_outputs = _execute.make_int(max_outputs, "max_outputs")
  _, _, _op = _op_def_lib._apply_op_helper(
        "AudioSummaryV2", tag=tag, tensor=tensor, sample_rate=sample_rate,
                          max_outputs=max_outputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_outputs", _op.get_attr("max_outputs"))
  _execute.record_gradient(
      "AudioSummaryV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def audio_summary_v2_eager_fallback(tag, tensor, sample_rate, max_outputs=3, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function audio_summary_v2
  """
  _ctx = ctx if ctx else _context.context()
  if max_outputs is None:
    max_outputs = 3
  max_outputs = _execute.make_int(max_outputs, "max_outputs")
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  tensor = _ops.convert_to_tensor(tensor, _dtypes.float32)
  sample_rate = _ops.convert_to_tensor(sample_rate, _dtypes.float32)
  _inputs_flat = [tag, tensor, sample_rate]
  _attrs = ("max_outputs", max_outputs)
  _result = _execute.execute(b"AudioSummaryV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AudioSummaryV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def histogram_summary(tag, values, name=None):
  r"""Outputs a `Summary` protocol buffer with a histogram.

  The generated
  [`Summary`](https://www.tensorflow.org/code/tensorflow/core/framework/summary.proto)
  has one summary value containing a histogram for `values`.

  This op reports an `InvalidArgument` error if any value is not finite.

  Args:
    tag: A `Tensor` of type `string`.
      Scalar.  Tag to use for the `Summary.Value`.
    values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      Any shape. Values to use to build the histogram.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "HistogramSummary", name, _ctx._post_execution_callbacks, tag, values)
      return _result
    except _core._FallbackException:
      try:
        return histogram_summary_eager_fallback(
            tag, values, name=name, ctx=_ctx)
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
        "HistogramSummary", tag=tag, values=values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "HistogramSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def histogram_summary_eager_fallback(tag, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function histogram_summary
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (values,) = _execute.args_to_matching_eager([values], _ctx, _dtypes.float32)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  _inputs_flat = [tag, values]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"HistogramSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "HistogramSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def image_summary(tag, tensor, max_images=3, bad_color=_execute.make_tensor("""dtype: DT_UINT8 tensor_shape { dim { size: 4 } } int_val: 255 int_val: 0 int_val: 0 int_val: 255""", "bad_color"), name=None):
  r"""Outputs a `Summary` protocol buffer with images.

  The summary has up to `max_images` summary values containing images. The
  images are built from `tensor` which must be 4-D with shape `[batch_size,
  height, width, channels]` and where `channels` can be:

  *  1: `tensor` is interpreted as Grayscale.
  *  3: `tensor` is interpreted as RGB.
  *  4: `tensor` is interpreted as RGBA.

  The images have the same number of channels as the input tensor. For float
  input, the values are normalized one image at a time to fit in the range
  `[0, 255]`.  `uint8` values are unchanged.  The op uses two different
  normalization algorithms:

  *  If the input values are all positive, they are rescaled so the largest one
     is 255.

  *  If any input value is negative, the values are shifted so input value 0.0
     is at 127.  They are then rescaled so that either the smallest value is 0,
     or the largest one is 255.

  The `tag` argument is a scalar `Tensor` of type `string`.  It is used to
  build the `tag` of the summary values:

  *  If `max_images` is 1, the summary value tag is '*tag*/image'.
  *  If `max_images` is greater than 1, the summary value tags are
     generated sequentially as '*tag*/image/0', '*tag*/image/1', etc.

  The `bad_color` argument is the color to use in the generated images for
  non-finite input values.  It is a `uint8` 1-D tensor of length `channels`.
  Each element must be in the range `[0, 255]` (It represents the value of a
  pixel in the output image).  Non-finite values in the input tensor are
  replaced by this tensor in the output image.  The default value is the color
  red.

  Args:
    tag: A `Tensor` of type `string`.
      Scalar. Used to build the `tag` attribute of the summary values.
    tensor: A `Tensor`. Must be one of the following types: `uint8`, `float32`, `half`, `float64`.
      4-D of shape `[batch_size, height, width, channels]` where
      `channels` is 1, 3, or 4.
    max_images: An optional `int` that is `>= 1`. Defaults to `3`.
      Max number of batch elements to generate images for.
    bad_color: An optional `tf.TensorProto`. Defaults to `dtype: DT_UINT8 tensor_shape { dim { size: 4 } } int_val: 255 int_val: 0 int_val: 0 int_val: 255`.
      Color to use for pixels with non-finite values.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ImageSummary",
        name, _ctx._post_execution_callbacks, tag, tensor, "max_images",
        max_images, "bad_color", bad_color)
      return _result
    except _core._FallbackException:
      try:
        return image_summary_eager_fallback(
            tag, tensor, max_images=max_images, bad_color=bad_color,
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
  if max_images is None:
    max_images = 3
  max_images = _execute.make_int(max_images, "max_images")
  if bad_color is None:
    bad_color = _execute.make_tensor("""dtype: DT_UINT8 tensor_shape { dim { size: 4 } } int_val: 255 int_val: 0 int_val: 0 int_val: 255""", "bad_color")
  bad_color = _execute.make_tensor(bad_color, "bad_color")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ImageSummary", tag=tag, tensor=tensor, max_images=max_images,
                        bad_color=bad_color, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("max_images", _op.get_attr("max_images"), "T", _op.get_attr("T"),
            "bad_color", _op.get_attr("bad_color"))
  _execute.record_gradient(
      "ImageSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def image_summary_eager_fallback(tag, tensor, max_images=3, bad_color=_execute.make_tensor("""dtype: DT_UINT8 tensor_shape { dim { size: 4 } } int_val: 255 int_val: 0 int_val: 0 int_val: 255""", "bad_color"), name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function image_summary
  """
  _ctx = ctx if ctx else _context.context()
  if max_images is None:
    max_images = 3
  max_images = _execute.make_int(max_images, "max_images")
  if bad_color is None:
    bad_color = _execute.make_tensor("""dtype: DT_UINT8 tensor_shape { dim { size: 4 } } int_val: 255 int_val: 0 int_val: 0 int_val: 255""", "bad_color")
  bad_color = _execute.make_tensor(bad_color, "bad_color")
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx, _dtypes.float32)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  _inputs_flat = [tag, tensor]
  _attrs = ("max_images", max_images, "T", _attr_T, "bad_color", bad_color)
  _result = _execute.execute(b"ImageSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ImageSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def merge_summary(inputs, name=None):
  r"""Merges summaries.

  This op creates a
  [`Summary`](https://www.tensorflow.org/code/tensorflow/core/framework/summary.proto)
  protocol buffer that contains the union of all the values in the input
  summaries.

  When the Op is run, it reports an `InvalidArgument` error if multiple values
  in the summaries to merge use the same tag.

  Args:
    inputs: A list of at least 1 `Tensor` objects with type `string`.
      Can be of any shape.  Each must contain serialized `Summary` protocol
      buffers.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MergeSummary",
        name, _ctx._post_execution_callbacks, inputs)
      return _result
    except _core._FallbackException:
      try:
        return merge_summary_eager_fallback(
            inputs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
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
        "'merge_summary' Op, not %r." % inputs)
  _attr_N = len(inputs)
  _, _, _op = _op_def_lib._apply_op_helper(
        "MergeSummary", inputs=inputs, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"))
  _execute.record_gradient(
      "MergeSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def merge_summary_eager_fallback(inputs, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function merge_summary
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'merge_summary' Op, not %r." % inputs)
  _attr_N = len(inputs)
  inputs = _ops.convert_n_to_tensor(inputs, _dtypes.string)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N)
  _result = _execute.execute(b"MergeSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MergeSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def _print(input, data, message="", first_n=-1, summarize=3, name=None):
  r"""Prints a list of tensors.

  Passes `input` through to `output` and prints `data` when evaluating.

  Args:
    input: A `Tensor`. The tensor passed to `output`
    data: A list of `Tensor` objects.
      A list of tensors to print out when op is evaluated.
    message: An optional `string`. Defaults to `""`.
      A string, prefix of the error message.
    first_n: An optional `int`. Defaults to `-1`.
      Only log `first_n` number of times. -1 disables logging.
    summarize: An optional `int`. Defaults to `3`.
      Only print this many entries of each tensor.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Print", name,
        _ctx._post_execution_callbacks, input, data, "message", message,
        "first_n", first_n, "summarize", summarize)
      return _result
    except _core._FallbackException:
      try:
        return _print_eager_fallback(
            input, data, message=message, first_n=first_n,
            summarize=summarize, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if message is None:
    message = ""
  message = _execute.make_str(message, "message")
  if first_n is None:
    first_n = -1
  first_n = _execute.make_int(first_n, "first_n")
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Print", input=input, data=data, message=message, first_n=first_n,
                 summarize=summarize, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "U", _op.get_attr("U"), "message",
            _op.get_attr("message"), "first_n", _op.get_attr("first_n"),
            "summarize", _op.get_attr("summarize"))
  _execute.record_gradient(
      "Print", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def _print_eager_fallback(input, data, message="", first_n=-1, summarize=3, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function _print
  """
  _ctx = ctx if ctx else _context.context()
  if message is None:
    message = ""
  message = _execute.make_str(message, "message")
  if first_n is None:
    first_n = -1
  first_n = _execute.make_int(first_n, "first_n")
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _attr_U, data = _execute.convert_to_mixed_eager_tensors(data, _ctx)
  _inputs_flat = [input] + list(data)
  _attrs = ("T", _attr_T, "U", _attr_U, "message", message, "first_n",
  first_n, "summarize", summarize)
  _result = _execute.execute(b"Print", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Print", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def print_v2(input, output_stream="stderr", name=None):
  r"""Prints a string scalar.

  Prints a string scalar to the desired output_stream.

  Args:
    input: A `Tensor` of type `string`. The string scalar to print.
    output_stream: An optional `string`. Defaults to `"stderr"`.
      A string specifying the output stream or logging level to print to.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "PrintV2",
        name, _ctx._post_execution_callbacks, input, "output_stream",
        output_stream)
      return _result
    except _core._FallbackException:
      try:
        return print_v2_eager_fallback(
            input, output_stream=output_stream, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if output_stream is None:
    output_stream = "stderr"
  output_stream = _execute.make_str(output_stream, "output_stream")
  _, _, _op = _op_def_lib._apply_op_helper(
        "PrintV2", input=input, output_stream=output_stream, name=name)
  return _op
  _result = None
  return _result



def print_v2_eager_fallback(input, output_stream="stderr", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function print_v2
  """
  _ctx = ctx if ctx else _context.context()
  if output_stream is None:
    output_stream = "stderr"
  output_stream = _execute.make_str(output_stream, "output_stream")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("output_stream", output_stream)
  _result = _execute.execute(b"PrintV2", 0, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _result = None
  return _result


def scalar_summary(tags, values, name=None):
  r"""Outputs a `Summary` protocol buffer with scalar values.

  The input `tags` and `values` must have the same shape.  The generated summary
  has a summary value for each tag-value pair in `tags` and `values`.

  Args:
    tags: A `Tensor` of type `string`. Tags for the summary.
    values: A `Tensor`. Must be one of the following types: `float32`, `float64`, `int32`, `uint8`, `int16`, `int8`, `int64`, `bfloat16`, `uint16`, `half`, `uint32`, `uint64`.
      Same shape as `tags.  Values for the summary.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ScalarSummary", name, _ctx._post_execution_callbacks, tags, values)
      return _result
    except _core._FallbackException:
      try:
        return scalar_summary_eager_fallback(
            tags, values, name=name, ctx=_ctx)
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
        "ScalarSummary", tags=tags, values=values, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "ScalarSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def scalar_summary_eager_fallback(tags, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function scalar_summary
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (values,) = _execute.args_to_matching_eager([values], _ctx)
  tags = _ops.convert_to_tensor(tags, _dtypes.string)
  _inputs_flat = [tags, values]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"ScalarSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ScalarSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_summary(tensor, description="", labels=[], display_name="", name=None):
  r"""Outputs a `Summary` protocol buffer with a tensor.

  This op is being phased out in favor of TensorSummaryV2, which lets callers pass
  a tag as well as a serialized SummaryMetadata proto string that contains
  plugin-specific data. We will keep this op to maintain backwards compatibility.

  Args:
    tensor: A `Tensor`. A tensor to serialize.
    description: An optional `string`. Defaults to `""`.
      A json-encoded SummaryDescription proto.
    labels: An optional list of `strings`. Defaults to `[]`.
      An unused list of strings.
    display_name: An optional `string`. Defaults to `""`. An unused string.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorSummary", name, _ctx._post_execution_callbacks, tensor,
        "description", description, "labels", labels, "display_name",
        display_name)
      return _result
    except _core._FallbackException:
      try:
        return tensor_summary_eager_fallback(
            tensor, description=description, labels=labels,
            display_name=display_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if description is None:
    description = ""
  description = _execute.make_str(description, "description")
  if labels is None:
    labels = []
  if not isinstance(labels, (list, tuple)):
    raise TypeError(
        "Expected list for 'labels' argument to "
        "'tensor_summary' Op, not %r." % labels)
  labels = [_execute.make_str(_s, "labels") for _s in labels]
  if display_name is None:
    display_name = ""
  display_name = _execute.make_str(display_name, "display_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorSummary", tensor=tensor, description=description,
                         labels=labels, display_name=display_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "description",
            _op.get_attr("description"), "labels", _op.get_attr("labels"),
            "display_name", _op.get_attr("display_name"))
  _execute.record_gradient(
      "TensorSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_summary_eager_fallback(tensor, description="", labels=[], display_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_summary
  """
  _ctx = ctx if ctx else _context.context()
  if description is None:
    description = ""
  description = _execute.make_str(description, "description")
  if labels is None:
    labels = []
  if not isinstance(labels, (list, tuple)):
    raise TypeError(
        "Expected list for 'labels' argument to "
        "'tensor_summary' Op, not %r." % labels)
  labels = [_execute.make_str(_s, "labels") for _s in labels]
  if display_name is None:
    display_name = ""
  display_name = _execute.make_str(display_name, "display_name")
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  _inputs_flat = [tensor]
  _attrs = ("T", _attr_T, "description", description, "labels", labels,
  "display_name", display_name)
  _result = _execute.execute(b"TensorSummary", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_summary_v2(tag, tensor, serialized_summary_metadata, name=None):
  r"""Outputs a `Summary` protocol buffer with a tensor and per-plugin data.

  Args:
    tag: A `Tensor` of type `string`.
      A string attached to this summary. Used for organization in TensorBoard.
    tensor: A `Tensor`. A tensor to serialize.
    serialized_summary_metadata: A `Tensor` of type `string`.
      A serialized SummaryMetadata proto. Contains plugin
      data.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TensorSummaryV2", name, _ctx._post_execution_callbacks, tag, tensor,
        serialized_summary_metadata)
      return _result
    except _core._FallbackException:
      try:
        return tensor_summary_v2_eager_fallback(
            tag, tensor, serialized_summary_metadata, name=name, ctx=_ctx)
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
        "TensorSummaryV2", tag=tag, tensor=tensor,
                           serialized_summary_metadata=serialized_summary_metadata,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "TensorSummaryV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_summary_v2_eager_fallback(tag, tensor, serialized_summary_metadata, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_summary_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (tensor,) = _execute.args_to_matching_eager([tensor], _ctx)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  serialized_summary_metadata = _ops.convert_to_tensor(serialized_summary_metadata, _dtypes.string)
  _inputs_flat = [tag, tensor, serialized_summary_metadata]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"TensorSummaryV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorSummaryV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('timestamp')
def timestamp(name=None):
  r"""Provides the time since epoch in seconds.

  Returns the timestamp as a `float64` for seconds since the Unix epoch.

  Note: the timestamp is computed when the op is executed, not when it is added
  to the graph.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Timestamp",
        name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return timestamp_eager_fallback(
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              timestamp, name=name)
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
        "Timestamp", name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          timestamp, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "Timestamp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def timestamp_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function timestamp
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"Timestamp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Timestamp", _inputs_flat, _attrs, _result, name)
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
#   name: "Assert"
#   input_arg {
#     name: "condition"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "data"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "summarize"
#     type: "int"
#     default_value {
#       i: 3
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "AudioSummary"
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "sample_rate"
#     type: "float"
#   }
#   attr {
#     name: "max_outputs"
#     type: "int"
#     default_value {
#       i: 3
#     }
#     has_minimum: true
#     minimum: 1
#   }
#   deprecation {
#     version: 15
#     explanation: "Use AudioSummaryV2."
#   }
# }
# op {
#   name: "AudioSummaryV2"
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sample_rate"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "max_outputs"
#     type: "int"
#     default_value {
#       i: 3
#     }
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "HistogramSummary"
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
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
# }
# op {
#   name: "ImageSummary"
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "max_images"
#     type: "int"
#     default_value {
#       i: 3
#     }
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_FLOAT
#         type: DT_HALF
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "bad_color"
#     type: "tensor"
#     default_value {
#       tensor {
#         dtype: DT_UINT8
#         tensor_shape {
#           dim {
#             size: 4
#           }
#         }
#         int_val: 255
#         int_val: 0
#         int_val: 0
#         int_val: 255
#       }
#     }
#   }
# }
# op {
#   name: "MergeSummary"
#   input_arg {
#     name: "inputs"
#     type: DT_STRING
#     number_attr: "N"
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "Print"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "data"
#     type_list_attr: "U"
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
#     name: "U"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "message"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "first_n"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "summarize"
#     type: "int"
#     default_value {
#       i: 3
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "PrintV2"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   attr {
#     name: "output_stream"
#     type: "string"
#     default_value {
#       s: "stderr"
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ScalarSummary"
#   input_arg {
#     name: "tags"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
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
# }
# op {
#   name: "TensorSummary"
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
#   attr {
#     name: "description"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "labels"
#     type: "list(string)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "display_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "TensorSummaryV2"
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "serialized_summary_metadata"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "type"
#   }
# }
# op {
#   name: "Timestamp"
#   output_arg {
#     name: "ts"
#     type: DT_DOUBLE
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\nP\n\006Assert\022\r\n\tcondition\030\n\022\t\n\004data2\001T\"\023\n\001T\022\nlist(type)(\0010\001\"\024\n\tsummarize\022\003int\032\002\030\003\210\001\001\n{\n\014AudioSummary\022\007\n\003tag\030\007\022\n\n\006tensor\030\001\032\013\n\007summary\030\007\"\024\n\013sample_rate\022\005float\"\032\n\013max_outputs\022\003int\032\002\030\003(\0010\001B\027\010\017\022\023Use AudioSummaryV2.\n_\n\016AudioSummaryV2\022\007\n\003tag\030\007\022\n\n\006tensor\030\001\022\017\n\013sample_rate\030\001\032\013\n\007summary\030\007\"\032\n\013max_outputs\022\003int\032\002\030\003(\0010\001\nV\n\020HistogramSummary\022\007\n\003tag\030\007\022\013\n\006values\"\001T\032\013\n\007summary\030\007\"\037\n\001T\022\004type\032\0020\001:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\216\001\n\014ImageSummary\022\007\n\003tag\030\007\022\013\n\006tensor\"\001T\032\013\n\007summary\030\007\"\031\n\nmax_images\022\003int\032\002\030\003(\0010\001\"\027\n\001T\022\004type\032\0020\001:\010\n\0062\004\004\001\023\002\"\'\n\tbad_color\022\006tensor\032\022B\020\010\004\022\004\022\002\010\004:\006\377\001\000\000\377\001\n8\n\014MergeSummary\022\r\n\006inputs\030\007*\001N\032\013\n\007summary\030\007\"\014\n\001N\022\003int(\0010\001\n\226\001\n\005Print\022\n\n\005input\"\001T\022\t\n\004data2\001U\032\013\n\006output\"\001T\"\t\n\001T\022\004type\"\021\n\001U\022\nlist(type)(\001\"\025\n\007message\022\006string\032\002\022\000\"\033\n\007first_n\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\024\n\tsummarize\022\003int\032\002\030\003\210\001\001\n:\n\007PrintV2\022\t\n\005input\030\007\"!\n\routput_stream\022\006string\032\010\022\006stderr\210\001\001\nP\n\rScalarSummary\022\010\n\004tags\030\007\022\013\n\006values\"\001T\032\013\n\007summary\030\007\"\033\n\001T\022\004type:\020\n\0162\014\001\002\003\004\005\006\t\016\021\023\026\027\n\207\001\n\rTensorSummary\022\013\n\006tensor\"\001T\032\013\n\007summary\030\007\"\t\n\001T\022\004type\"\031\n\013description\022\006string\032\002\022\000\"\032\n\006labels\022\014list(string)\032\002\n\000\"\032\n\014display_name\022\006string\032\002\022\000\n`\n\017TensorSummaryV2\022\007\n\003tag\030\007\022\013\n\006tensor\"\001T\022\037\n\033serialized_summary_metadata\030\007\032\013\n\007summary\030\007\"\t\n\001T\022\004type\n\026\n\tTimestamp\032\006\n\002ts\030\002\210\001\001")
