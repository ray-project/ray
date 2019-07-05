"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: audio_ops.cc
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
@tf_export('audio_spectrogram')
def audio_spectrogram(input, window_size, stride, magnitude_squared=False, name=None):
  r"""Produces a visualization of audio data over time.

  Spectrograms are a standard way of representing audio information as a series of
  slices of frequency information, one slice for each window of time. By joining
  these together into a sequence, they form a distinctive fingerprint of the sound
  over time.

  This op expects to receive audio data as an input, stored as floats in the range
  -1 to 1, together with a window width in samples, and a stride specifying how
  far to move the window between slices. From this it generates a three
  dimensional output. The lowest dimension has an amplitude value for each
  frequency during that time slice. The next dimension is time, with successive
  frequency slices. The final dimension is for the channels in the input, so a
  stereo audio input would have two here for example.

  This means the layout when converted and saved as an image is rotated 90 degrees
  clockwise from a typical spectrogram. Time is descending down the Y axis, and
  the frequency decreases from left to right.

  Each value in the result represents the square root of the sum of the real and
  imaginary parts of an FFT on the current window of samples. In this way, the
  lowest dimension represents the power of each frequency in the current window,
  and adjacent windows are concatenated in the next dimension.

  To get a more intuitive and visual look at what this operation does, you can run
  tensorflow/examples/wav_to_spectrogram to read in an audio file and save out the
  resulting spectrogram as a PNG image.

  Args:
    input: A `Tensor` of type `float32`. Float representation of audio data.
    window_size: An `int`.
      How wide the input window is in samples. For the highest efficiency
      this should be a power of two, but other values are accepted.
    stride: An `int`.
      How widely apart the center of adjacent sample windows should be.
    magnitude_squared: An optional `bool`. Defaults to `False`.
      Whether to return the squared magnitude or just the
      magnitude. Using squared magnitude can avoid extra calculations.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AudioSpectrogram", name, _ctx._post_execution_callbacks, input,
        "window_size", window_size, "stride", stride, "magnitude_squared",
        magnitude_squared)
      return _result
    except _core._FallbackException:
      try:
        return audio_spectrogram_eager_fallback(
            input, window_size=window_size, stride=stride,
            magnitude_squared=magnitude_squared, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              audio_spectrogram, input=input, window_size=window_size,
                                 stride=stride,
                                 magnitude_squared=magnitude_squared,
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
  window_size = _execute.make_int(window_size, "window_size")
  stride = _execute.make_int(stride, "stride")
  if magnitude_squared is None:
    magnitude_squared = False
  magnitude_squared = _execute.make_bool(magnitude_squared, "magnitude_squared")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "AudioSpectrogram", input=input, window_size=window_size,
                            stride=stride,
                            magnitude_squared=magnitude_squared, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          audio_spectrogram, input=input, window_size=window_size,
                             stride=stride,
                             magnitude_squared=magnitude_squared, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("window_size", _op.get_attr("window_size"), "stride",
            _op.get_attr("stride"), "magnitude_squared",
            _op.get_attr("magnitude_squared"))
  _execute.record_gradient(
      "AudioSpectrogram", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def audio_spectrogram_eager_fallback(input, window_size, stride, magnitude_squared=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function audio_spectrogram
  """
  _ctx = ctx if ctx else _context.context()
  window_size = _execute.make_int(window_size, "window_size")
  stride = _execute.make_int(stride, "stride")
  if magnitude_squared is None:
    magnitude_squared = False
  magnitude_squared = _execute.make_bool(magnitude_squared, "magnitude_squared")
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  _inputs_flat = [input]
  _attrs = ("window_size", window_size, "stride", stride, "magnitude_squared",
  magnitude_squared)
  _result = _execute.execute(b"AudioSpectrogram", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AudioSpectrogram", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_decode_wav_outputs = ["audio", "sample_rate"]
_DecodeWavOutput = _collections.namedtuple(
    "DecodeWav", _decode_wav_outputs)


@_dispatch.add_dispatch_list
@tf_export('decode_wav')
def decode_wav(contents, desired_channels=-1, desired_samples=-1, name=None):
  r"""Decode a 16-bit PCM WAV file to a float tensor.

  The -32768 to 32767 signed 16-bit values will be scaled to -1.0 to 1.0 in float.

  When desired_channels is set, if the input contains fewer channels than this
  then the last channel will be duplicated to give the requested number, else if
  the input has more channels than requested then the additional channels will be
  ignored.

  If desired_samples is set, then the audio will be cropped or padded with zeroes
  to the requested length.

  The first output contains a Tensor with the content of the audio samples. The
  lowest dimension will be the number of channels, and the second will be the
  number of samples. For example, a ten-sample-long stereo WAV file should give an
  output shape of [10, 2].

  Args:
    contents: A `Tensor` of type `string`.
      The WAV-encoded audio, usually from a file.
    desired_channels: An optional `int`. Defaults to `-1`.
      Number of sample channels wanted.
    desired_samples: An optional `int`. Defaults to `-1`.
      Length of audio requested.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (audio, sample_rate).

    audio: A `Tensor` of type `float32`.
    sample_rate: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeWav",
        name, _ctx._post_execution_callbacks, contents, "desired_channels",
        desired_channels, "desired_samples", desired_samples)
      _result = _DecodeWavOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return decode_wav_eager_fallback(
            contents, desired_channels=desired_channels,
            desired_samples=desired_samples, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decode_wav, contents=contents,
                          desired_channels=desired_channels,
                          desired_samples=desired_samples, name=name)
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
  if desired_channels is None:
    desired_channels = -1
  desired_channels = _execute.make_int(desired_channels, "desired_channels")
  if desired_samples is None:
    desired_samples = -1
  desired_samples = _execute.make_int(desired_samples, "desired_samples")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeWav", contents=contents, desired_channels=desired_channels,
                     desired_samples=desired_samples, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decode_wav, contents=contents, desired_channels=desired_channels,
                      desired_samples=desired_samples, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("desired_channels", _op.get_attr("desired_channels"),
            "desired_samples", _op.get_attr("desired_samples"))
  _execute.record_gradient(
      "DecodeWav", _inputs_flat, _attrs, _result, name)
  _result = _DecodeWavOutput._make(_result)
  return _result



def decode_wav_eager_fallback(contents, desired_channels=-1, desired_samples=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_wav
  """
  _ctx = ctx if ctx else _context.context()
  if desired_channels is None:
    desired_channels = -1
  desired_channels = _execute.make_int(desired_channels, "desired_channels")
  if desired_samples is None:
    desired_samples = -1
  desired_samples = _execute.make_int(desired_samples, "desired_samples")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = ("desired_channels", desired_channels, "desired_samples",
  desired_samples)
  _result = _execute.execute(b"DecodeWav", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeWav", _inputs_flat, _attrs, _result, name)
  _result = _DecodeWavOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('encode_wav')
def encode_wav(audio, sample_rate, name=None):
  r"""Encode audio data using the WAV file format.

  This operation will generate a string suitable to be saved out to create a .wav
  audio file. It will be encoded in the 16-bit PCM format. It takes in float
  values in the range -1.0f to 1.0f, and any outside that value will be clamped to
  that range.

  `audio` is a 2-D float Tensor of shape `[length, channels]`.
  `sample_rate` is a scalar Tensor holding the rate to use (e.g. 44100).

  Args:
    audio: A `Tensor` of type `float32`. 2-D with shape `[length, channels]`.
    sample_rate: A `Tensor` of type `int32`.
      Scalar containing the sample frequency.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodeWav",
        name, _ctx._post_execution_callbacks, audio, sample_rate)
      return _result
    except _core._FallbackException:
      try:
        return encode_wav_eager_fallback(
            audio, sample_rate, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_wav, audio=audio, sample_rate=sample_rate, name=name)
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
        "EncodeWav", audio=audio, sample_rate=sample_rate, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_wav, audio=audio, sample_rate=sample_rate, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "EncodeWav", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_wav_eager_fallback(audio, sample_rate, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_wav
  """
  _ctx = ctx if ctx else _context.context()
  audio = _ops.convert_to_tensor(audio, _dtypes.float32)
  sample_rate = _ops.convert_to_tensor(sample_rate, _dtypes.int32)
  _inputs_flat = [audio, sample_rate]
  _attrs = None
  _result = _execute.execute(b"EncodeWav", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeWav", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('mfcc')
def mfcc(spectrogram, sample_rate, upper_frequency_limit=4000, lower_frequency_limit=20, filterbank_channel_count=40, dct_coefficient_count=13, name=None):
  r"""Transforms a spectrogram into a form that's useful for speech recognition.

  Mel Frequency Cepstral Coefficients are a way of representing audio data that's
  been effective as an input feature for machine learning. They are created by
  taking the spectrum of a spectrogram (a 'cepstrum'), and discarding some of the
  higher frequencies that are less significant to the human ear. They have a long
  history in the speech recognition world, and https://en.wikipedia.org/wiki/Mel-frequency_cepstrum
  is a good resource to learn more.

  Args:
    spectrogram: A `Tensor` of type `float32`.
      Typically produced by the Spectrogram op, with magnitude_squared
      set to true.
    sample_rate: A `Tensor` of type `int32`.
      How many samples per second the source audio used.
    upper_frequency_limit: An optional `float`. Defaults to `4000`.
      The highest frequency to use when calculating the
      ceptstrum.
    lower_frequency_limit: An optional `float`. Defaults to `20`.
      The lowest frequency to use when calculating the
      ceptstrum.
    filterbank_channel_count: An optional `int`. Defaults to `40`.
      Resolution of the Mel bank used internally.
    dct_coefficient_count: An optional `int`. Defaults to `13`.
      How many output channels to produce per time slice.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Mfcc", name,
        _ctx._post_execution_callbacks, spectrogram, sample_rate,
        "upper_frequency_limit", upper_frequency_limit,
        "lower_frequency_limit", lower_frequency_limit,
        "filterbank_channel_count", filterbank_channel_count,
        "dct_coefficient_count", dct_coefficient_count)
      return _result
    except _core._FallbackException:
      try:
        return mfcc_eager_fallback(
            spectrogram, sample_rate,
            upper_frequency_limit=upper_frequency_limit,
            lower_frequency_limit=lower_frequency_limit,
            filterbank_channel_count=filterbank_channel_count,
            dct_coefficient_count=dct_coefficient_count, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              mfcc, spectrogram=spectrogram, sample_rate=sample_rate,
                    upper_frequency_limit=upper_frequency_limit,
                    lower_frequency_limit=lower_frequency_limit,
                    filterbank_channel_count=filterbank_channel_count,
                    dct_coefficient_count=dct_coefficient_count, name=name)
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
  if upper_frequency_limit is None:
    upper_frequency_limit = 4000
  upper_frequency_limit = _execute.make_float(upper_frequency_limit, "upper_frequency_limit")
  if lower_frequency_limit is None:
    lower_frequency_limit = 20
  lower_frequency_limit = _execute.make_float(lower_frequency_limit, "lower_frequency_limit")
  if filterbank_channel_count is None:
    filterbank_channel_count = 40
  filterbank_channel_count = _execute.make_int(filterbank_channel_count, "filterbank_channel_count")
  if dct_coefficient_count is None:
    dct_coefficient_count = 13
  dct_coefficient_count = _execute.make_int(dct_coefficient_count, "dct_coefficient_count")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "Mfcc", spectrogram=spectrogram, sample_rate=sample_rate,
                upper_frequency_limit=upper_frequency_limit,
                lower_frequency_limit=lower_frequency_limit,
                filterbank_channel_count=filterbank_channel_count,
                dct_coefficient_count=dct_coefficient_count, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          mfcc, spectrogram=spectrogram, sample_rate=sample_rate,
                upper_frequency_limit=upper_frequency_limit,
                lower_frequency_limit=lower_frequency_limit,
                filterbank_channel_count=filterbank_channel_count,
                dct_coefficient_count=dct_coefficient_count, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("upper_frequency_limit", _op.get_attr("upper_frequency_limit"),
            "lower_frequency_limit", _op.get_attr("lower_frequency_limit"),
            "filterbank_channel_count",
            _op.get_attr("filterbank_channel_count"), "dct_coefficient_count",
            _op.get_attr("dct_coefficient_count"))
  _execute.record_gradient(
      "Mfcc", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mfcc_eager_fallback(spectrogram, sample_rate, upper_frequency_limit=4000, lower_frequency_limit=20, filterbank_channel_count=40, dct_coefficient_count=13, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mfcc
  """
  _ctx = ctx if ctx else _context.context()
  if upper_frequency_limit is None:
    upper_frequency_limit = 4000
  upper_frequency_limit = _execute.make_float(upper_frequency_limit, "upper_frequency_limit")
  if lower_frequency_limit is None:
    lower_frequency_limit = 20
  lower_frequency_limit = _execute.make_float(lower_frequency_limit, "lower_frequency_limit")
  if filterbank_channel_count is None:
    filterbank_channel_count = 40
  filterbank_channel_count = _execute.make_int(filterbank_channel_count, "filterbank_channel_count")
  if dct_coefficient_count is None:
    dct_coefficient_count = 13
  dct_coefficient_count = _execute.make_int(dct_coefficient_count, "dct_coefficient_count")
  spectrogram = _ops.convert_to_tensor(spectrogram, _dtypes.float32)
  sample_rate = _ops.convert_to_tensor(sample_rate, _dtypes.int32)
  _inputs_flat = [spectrogram, sample_rate]
  _attrs = ("upper_frequency_limit", upper_frequency_limit,
  "lower_frequency_limit", lower_frequency_limit, "filterbank_channel_count",
  filterbank_channel_count, "dct_coefficient_count", dct_coefficient_count)
  _result = _execute.execute(b"Mfcc", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Mfcc", _inputs_flat, _attrs, _result, name)
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
#   name: "AudioSpectrogram"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "spectrogram"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "window_size"
#     type: "int"
#   }
#   attr {
#     name: "stride"
#     type: "int"
#   }
#   attr {
#     name: "magnitude_squared"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "DecodeWav"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "audio"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "sample_rate"
#     type: DT_INT32
#   }
#   attr {
#     name: "desired_channels"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "desired_samples"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "EncodeWav"
#   input_arg {
#     name: "audio"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sample_rate"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
# }
# op {
#   name: "Mfcc"
#   input_arg {
#     name: "spectrogram"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "sample_rate"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "upper_frequency_limit"
#     type: "float"
#     default_value {
#       f: 4000
#     }
#   }
#   attr {
#     name: "lower_frequency_limit"
#     type: "float"
#     default_value {
#       f: 20
#     }
#   }
#   attr {
#     name: "filterbank_channel_count"
#     type: "int"
#     default_value {
#       i: 40
#     }
#   }
#   attr {
#     name: "dct_coefficient_count"
#     type: "int"
#     default_value {
#       i: 13
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\np\n\020AudioSpectrogram\022\t\n\005input\030\001\032\017\n\013spectrogram\030\001\"\022\n\013window_size\022\003int\"\r\n\006stride\022\003int\"\035\n\021magnitude_squared\022\004bool\032\002(\000\n\200\001\n\tDecodeWav\022\014\n\010contents\030\007\032\t\n\005audio\030\001\032\017\n\013sample_rate\030\003\"$\n\020desired_channels\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"#\n\017desired_samples\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n5\n\tEncodeWav\022\t\n\005audio\030\001\022\017\n\013sample_rate\030\003\032\014\n\010contents\030\007\n\311\001\n\004Mfcc\022\017\n\013spectrogram\030\001\022\017\n\013sample_rate\030\003\032\n\n\006output\030\001\"%\n\025upper_frequency_limit\022\005float\032\005%\000\000zE\"%\n\025lower_frequency_limit\022\005float\032\005%\000\000\240A\"#\n\030filterbank_channel_count\022\003int\032\002\030(\" \n\025dct_coefficient_count\022\003int\032\002\030\r")
