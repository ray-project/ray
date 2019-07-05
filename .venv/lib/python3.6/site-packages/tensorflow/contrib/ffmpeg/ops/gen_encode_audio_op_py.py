"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: encode_audio_op_py.cc
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
@tf_export('encode_audio')
def encode_audio(sampled_audio, file_format, samples_per_second, bits_per_second=192000, name=None):
  r"""Processes a `Tensor` containing sampled audio with the number of channels

  and length of the audio specified by the dimensions of the `Tensor`. The
  audio is converted into a string that, when saved to disk, will be equivalent
  to the audio in the specified audio format.

  The input audio has one row of the tensor for each channel in the audio file.
  Each channel contains audio samples starting at the beginning of the audio and
  having `1/samples_per_second` time between them. The output file will contain
  all of the audio channels contained in the tensor.

  Args:
    sampled_audio: A `Tensor` of type `float32`.
      A rank 2 tensor containing all tracks of the audio. Dimension 0
      is time and dimension 1 is the channel.
    file_format: A `string`.
      A string describing the audio file format. This must be "wav".
    samples_per_second: An `int`.
      The number of samples per second that the audio should have.
    bits_per_second: An optional `int`. Defaults to `192000`.
      The approximate bitrate of the encoded audio file. This is
      ignored by the "wav" file format.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`. The binary audio file contents.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodeAudio",
        name, _ctx._post_execution_callbacks, sampled_audio, "file_format",
        file_format, "samples_per_second", samples_per_second,
        "bits_per_second", bits_per_second)
      return _result
    except _core._FallbackException:
      try:
        return encode_audio_eager_fallback(
            sampled_audio, file_format=file_format,
            samples_per_second=samples_per_second,
            bits_per_second=bits_per_second, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_audio, sampled_audio=sampled_audio,
                            file_format=file_format,
                            samples_per_second=samples_per_second,
                            bits_per_second=bits_per_second, name=name)
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
  file_format = _execute.make_str(file_format, "file_format")
  samples_per_second = _execute.make_int(samples_per_second, "samples_per_second")
  if bits_per_second is None:
    bits_per_second = 192000
  bits_per_second = _execute.make_int(bits_per_second, "bits_per_second")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EncodeAudio", sampled_audio=sampled_audio, file_format=file_format,
                       samples_per_second=samples_per_second,
                       bits_per_second=bits_per_second, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_audio, sampled_audio=sampled_audio, file_format=file_format,
                        samples_per_second=samples_per_second,
                        bits_per_second=bits_per_second, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("file_format", _op.get_attr("file_format"), "samples_per_second",
            _op.get_attr("samples_per_second"), "bits_per_second",
            _op.get_attr("bits_per_second"))
  _execute.record_gradient(
      "EncodeAudio", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_audio_eager_fallback(sampled_audio, file_format, samples_per_second, bits_per_second=192000, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_audio
  """
  _ctx = ctx if ctx else _context.context()
  file_format = _execute.make_str(file_format, "file_format")
  samples_per_second = _execute.make_int(samples_per_second, "samples_per_second")
  if bits_per_second is None:
    bits_per_second = 192000
  bits_per_second = _execute.make_int(bits_per_second, "bits_per_second")
  sampled_audio = _ops.convert_to_tensor(sampled_audio, _dtypes.float32)
  _inputs_flat = [sampled_audio]
  _attrs = ("file_format", file_format, "samples_per_second",
  samples_per_second, "bits_per_second", bits_per_second)
  _result = _execute.execute(b"EncodeAudio", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeAudio", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('encode_audio_v2')
def encode_audio_v2(sampled_audio, file_format, samples_per_second, bits_per_second, name=None):
  r"""Processes a `Tensor` containing sampled audio with the number of channels

  and length of the audio specified by the dimensions of the `Tensor`. The
  audio is converted into a string that, when saved to disk, will be equivalent
  to the audio in the specified audio format.

  The input audio has one row of the tensor for each channel in the audio file.
  Each channel contains audio samples starting at the beginning of the audio and
  having `1/samples_per_second` time between them. The output file will contain
  all of the audio channels contained in the tensor.

  Args:
    sampled_audio: A `Tensor` of type `float32`.
      A rank-2 float tensor containing all tracks of the audio.
      Dimension 0 is time and dimension 1 is the channel.
    file_format: A `Tensor` of type `string`.
      A string or rank-0 string tensor describing the audio file
      format. This value must be `"wav"`.
    samples_per_second: A `Tensor` of type `int32`.
      The number of samples per second that the audio should
      have, as an int or rank-0 `int32` tensor. This value must be
      positive.
    bits_per_second: A `Tensor` of type `int32`.
      The approximate bitrate of the encoded audio file, as
      an int or rank-0 `int32` tensor. This is ignored by the "wav" file
      format.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
    The binary audio file contents, as a rank-0 string tensor.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "EncodeAudioV2", name, _ctx._post_execution_callbacks, sampled_audio,
        file_format, samples_per_second, bits_per_second)
      return _result
    except _core._FallbackException:
      try:
        return encode_audio_v2_eager_fallback(
            sampled_audio, file_format, samples_per_second, bits_per_second,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_audio_v2, sampled_audio=sampled_audio,
                               file_format=file_format,
                               samples_per_second=samples_per_second,
                               bits_per_second=bits_per_second, name=name)
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
        "EncodeAudioV2", sampled_audio=sampled_audio, file_format=file_format,
                         samples_per_second=samples_per_second,
                         bits_per_second=bits_per_second, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_audio_v2, sampled_audio=sampled_audio,
                           file_format=file_format,
                           samples_per_second=samples_per_second,
                           bits_per_second=bits_per_second, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "EncodeAudioV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_audio_v2_eager_fallback(sampled_audio, file_format, samples_per_second, bits_per_second, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_audio_v2
  """
  _ctx = ctx if ctx else _context.context()
  sampled_audio = _ops.convert_to_tensor(sampled_audio, _dtypes.float32)
  file_format = _ops.convert_to_tensor(file_format, _dtypes.string)
  samples_per_second = _ops.convert_to_tensor(samples_per_second, _dtypes.int32)
  bits_per_second = _ops.convert_to_tensor(bits_per_second, _dtypes.int32)
  _inputs_flat = [sampled_audio, file_format, samples_per_second, bits_per_second]
  _attrs = None
  _result = _execute.execute(b"EncodeAudioV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeAudioV2", _inputs_flat, _attrs, _result, name)
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
#   name: "EncodeAudio"
#   input_arg {
#     name: "sampled_audio"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   attr {
#     name: "file_format"
#     type: "string"
#   }
#   attr {
#     name: "samples_per_second"
#     type: "int"
#   }
#   attr {
#     name: "bits_per_second"
#     type: "int"
#     default_value {
#       i: 192000
#     }
#   }
# }
# op {
#   name: "EncodeAudioV2"
#   input_arg {
#     name: "sampled_audio"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "file_format"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "samples_per_second"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "bits_per_second"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n~\n\013EncodeAudio\022\021\n\rsampled_audio\030\001\032\014\n\010contents\030\007\"\025\n\013file_format\022\006string\"\031\n\022samples_per_second\022\003int\"\034\n\017bits_per_second\022\003int\032\004\030\200\334\013\nn\n\rEncodeAudioV2\022\021\n\rsampled_audio\030\001\022\017\n\013file_format\030\007\022\026\n\022samples_per_second\030\003\022\023\n\017bits_per_second\030\003\032\014\n\010contents\030\007")
