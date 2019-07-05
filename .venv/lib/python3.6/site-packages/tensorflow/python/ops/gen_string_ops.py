"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: string_ops.cc
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
@tf_export('dtypes.as_string', 'as_string')
def as_string(input, precision=-1, scientific=False, shortest=False, width=-1, fill="", name=None):
  r"""Converts each entry in the given tensor to strings.  Supports many numeric

  types and boolean.

  Args:
    input: A `Tensor`. Must be one of the following types: `int8`, `int16`, `int32`, `int64`, `complex64`, `complex128`, `float32`, `float64`, `bool`.
    precision: An optional `int`. Defaults to `-1`.
      The post-decimal precision to use for floating point numbers.
      Only used if precision > -1.
    scientific: An optional `bool`. Defaults to `False`.
      Use scientific notation for floating point numbers.
    shortest: An optional `bool`. Defaults to `False`.
      Use shortest representation (either scientific or standard) for
      floating point numbers.
    width: An optional `int`. Defaults to `-1`.
      Pad pre-decimal numbers to this width.
      Applies to both floating point and integer numbers.
      Only used if width > -1.
    fill: An optional `string`. Defaults to `""`.
      The value to pad if width > -1.  If empty, pads with spaces.
      Another typical value is '0'.  String cannot be longer than 1 character.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AsString",
        name, _ctx._post_execution_callbacks, input, "precision", precision,
        "scientific", scientific, "shortest", shortest, "width", width,
        "fill", fill)
      return _result
    except _core._FallbackException:
      try:
        return as_string_eager_fallback(
            input, precision=precision, scientific=scientific,
            shortest=shortest, width=width, fill=fill, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              as_string, input=input, precision=precision,
                         scientific=scientific, shortest=shortest,
                         width=width, fill=fill, name=name)
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
  if precision is None:
    precision = -1
  precision = _execute.make_int(precision, "precision")
  if scientific is None:
    scientific = False
  scientific = _execute.make_bool(scientific, "scientific")
  if shortest is None:
    shortest = False
  shortest = _execute.make_bool(shortest, "shortest")
  if width is None:
    width = -1
  width = _execute.make_int(width, "width")
  if fill is None:
    fill = ""
  fill = _execute.make_str(fill, "fill")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "AsString", input=input, precision=precision, scientific=scientific,
                    shortest=shortest, width=width, fill=fill, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          as_string, input=input, precision=precision, scientific=scientific,
                     shortest=shortest, width=width, fill=fill, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "precision", _op.get_attr("precision"),
            "scientific", _op.get_attr("scientific"), "shortest",
            _op.get_attr("shortest"), "width", _op.get_attr("width"), "fill",
            _op.get_attr("fill"))
  _execute.record_gradient(
      "AsString", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def as_string_eager_fallback(input, precision=-1, scientific=False, shortest=False, width=-1, fill="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function as_string
  """
  _ctx = ctx if ctx else _context.context()
  if precision is None:
    precision = -1
  precision = _execute.make_int(precision, "precision")
  if scientific is None:
    scientific = False
  scientific = _execute.make_bool(scientific, "scientific")
  if shortest is None:
    shortest = False
  shortest = _execute.make_bool(shortest, "shortest")
  if width is None:
    width = -1
  width = _execute.make_int(width, "width")
  if fill is None:
    fill = ""
  fill = _execute.make_str(fill, "fill")
  _attr_T, (input,) = _execute.args_to_matching_eager([input], _ctx)
  _inputs_flat = [input]
  _attrs = ("T", _attr_T, "precision", precision, "scientific", scientific,
  "shortest", shortest, "width", width, "fill", fill)
  _result = _execute.execute(b"AsString", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AsString", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('io.decode_base64', v1=['io.decode_base64', 'decode_base64'])
@deprecated_endpoints('decode_base64')
def decode_base64(input, name=None):
  r"""Decode web-safe base64-encoded strings.

  Input may or may not have padding at the end. See EncodeBase64 for padding.
  Web-safe means that input must use - and _ instead of + and /.

  Args:
    input: A `Tensor` of type `string`. Base64 strings to decode.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeBase64",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return decode_base64_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              decode_base64, input=input, name=name)
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
        "DecodeBase64", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          decode_base64, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "DecodeBase64", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_base64_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_base64
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"DecodeBase64", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeBase64", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('io.encode_base64', v1=['io.encode_base64', 'encode_base64'])
@deprecated_endpoints('encode_base64')
def encode_base64(input, pad=False, name=None):
  r"""Encode strings into web-safe base64 format.

  Refer to the following article for more information on base64 format:
  en.wikipedia.org/wiki/Base64. Base64 strings may have padding with '=' at the
  end so that the encoded has length multiple of 4. See Padding section of the
  link above.

  Web-safe means that the encoder uses - and _ instead of + and /.

  Args:
    input: A `Tensor` of type `string`. Strings to be encoded.
    pad: An optional `bool`. Defaults to `False`.
      Bool whether padding is applied at the ends.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodeBase64",
        name, _ctx._post_execution_callbacks, input, "pad", pad)
      return _result
    except _core._FallbackException:
      try:
        return encode_base64_eager_fallback(
            input, pad=pad, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_base64, input=input, pad=pad, name=name)
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
  if pad is None:
    pad = False
  pad = _execute.make_bool(pad, "pad")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EncodeBase64", input=input, pad=pad, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_base64, input=input, pad=pad, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("pad", _op.get_attr("pad"))
  _execute.record_gradient(
      "EncodeBase64", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_base64_eager_fallback(input, pad=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_base64
  """
  _ctx = ctx if ctx else _context.context()
  if pad is None:
    pad = False
  pad = _execute.make_bool(pad, "pad")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("pad", pad)
  _result = _execute.execute(b"EncodeBase64", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeBase64", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reduce_join(inputs, reduction_indices, keep_dims=False, separator="", name=None):
  r"""Joins a string Tensor across the given dimensions.

  Computes the string join across dimensions in the given string Tensor of shape
  `[\\(d_0, d_1, ..., d_{n-1}\\)]`.  Returns a new Tensor created by joining the input
  strings with the given separator (default: empty string).  Negative indices are
  counted backwards from the end, with `-1` being equivalent to `n - 1`.  If
  indices are not specified, joins across all dimensions beginning from `n - 1`
  through `0`.

  For example:

  ```python
  # tensor `a` is [["a", "b"], ["c", "d"]]
  tf.reduce_join(a, 0) ==> ["ac", "bd"]
  tf.reduce_join(a, 1) ==> ["ab", "cd"]
  tf.reduce_join(a, -2) = tf.reduce_join(a, 0) ==> ["ac", "bd"]
  tf.reduce_join(a, -1) = tf.reduce_join(a, 1) ==> ["ab", "cd"]
  tf.reduce_join(a, 0, keep_dims=True) ==> [["ac", "bd"]]
  tf.reduce_join(a, 1, keep_dims=True) ==> [["ab"], ["cd"]]
  tf.reduce_join(a, 0, separator=".") ==> ["a.c", "b.d"]
  tf.reduce_join(a, [0, 1]) ==> "acbd"
  tf.reduce_join(a, [1, 0]) ==> "abcd"
  tf.reduce_join(a, []) ==> [["a", "b"], ["c", "d"]]
  tf.reduce_join(a) = tf.reduce_join(a, [1, 0]) ==> "abcd"
  ```

  Args:
    inputs: A `Tensor` of type `string`.
      The input to be joined.  All reduced indices must have non-zero size.
    reduction_indices: A `Tensor` of type `int32`.
      The dimensions to reduce over.  Dimensions are reduced in the
      order specified.  Omitting `reduction_indices` is equivalent to passing
      `[n-1, n-2, ..., 0]`.  Negative indices from `-n` to `-1` are supported.
    keep_dims: An optional `bool`. Defaults to `False`.
      If `True`, retain reduced dimensions with length `1`.
    separator: An optional `string`. Defaults to `""`.
      The separator to use when joining.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ReduceJoin",
        name, _ctx._post_execution_callbacks, inputs, reduction_indices,
        "keep_dims", keep_dims, "separator", separator)
      return _result
    except _core._FallbackException:
      try:
        return reduce_join_eager_fallback(
            inputs, reduction_indices, keep_dims=keep_dims,
            separator=separator, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  if separator is None:
    separator = ""
  separator = _execute.make_str(separator, "separator")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReduceJoin", inputs=inputs, reduction_indices=reduction_indices,
                      keep_dims=keep_dims, separator=separator, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("keep_dims", _op.get_attr("keep_dims"), "separator",
            _op.get_attr("separator"))
  _execute.record_gradient(
      "ReduceJoin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reduce_join_eager_fallback(inputs, reduction_indices, keep_dims=False, separator="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reduce_join
  """
  _ctx = ctx if ctx else _context.context()
  if keep_dims is None:
    keep_dims = False
  keep_dims = _execute.make_bool(keep_dims, "keep_dims")
  if separator is None:
    separator = ""
  separator = _execute.make_str(separator, "separator")
  inputs = _ops.convert_to_tensor(inputs, _dtypes.string)
  reduction_indices = _ops.convert_to_tensor(reduction_indices, _dtypes.int32)
  _inputs_flat = [inputs, reduction_indices]
  _attrs = ("keep_dims", keep_dims, "separator", separator)
  _result = _execute.execute(b"ReduceJoin", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReduceJoin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def regex_full_match(input, pattern, name=None):
  r"""Check if the input matches the regex pattern.

  The input is a string tensor of any shape. The pattern is a scalar
  string tensor which is applied to every element of the input tensor.
  The boolean values (True or False) of the output tensor indicate
  if the input matches the regex pattern provided.

  The pattern follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)

  Args:
    input: A `Tensor` of type `string`.
      A string tensor of the text to be processed.
    pattern: A `Tensor` of type `string`.
      A scalar string tensor containing the regular expression to match the input.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "RegexFullMatch", name, _ctx._post_execution_callbacks, input,
        pattern)
      return _result
    except _core._FallbackException:
      try:
        return regex_full_match_eager_fallback(
            input, pattern, name=name, ctx=_ctx)
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
        "RegexFullMatch", input=input, pattern=pattern, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "RegexFullMatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def regex_full_match_eager_fallback(input, pattern, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function regex_full_match
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.string)
  pattern = _ops.convert_to_tensor(pattern, _dtypes.string)
  _inputs_flat = [input, pattern]
  _attrs = None
  _result = _execute.execute(b"RegexFullMatch", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RegexFullMatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def regex_replace(input, pattern, rewrite, replace_global=True, name=None):
  r"""Replaces the match of pattern in input with rewrite.

  It follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)

  Args:
    input: A `Tensor` of type `string`. The text to be processed.
    pattern: A `Tensor` of type `string`.
      The regular expression to match the input.
    rewrite: A `Tensor` of type `string`.
      The rewrite to be applied to the matched expresion.
    replace_global: An optional `bool`. Defaults to `True`.
      If True, the replacement is global, otherwise the replacement
      is done only on the first match.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RegexReplace",
        name, _ctx._post_execution_callbacks, input, pattern, rewrite,
        "replace_global", replace_global)
      return _result
    except _core._FallbackException:
      try:
        return regex_replace_eager_fallback(
            input, pattern, rewrite, replace_global=replace_global, name=name,
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
  if replace_global is None:
    replace_global = True
  replace_global = _execute.make_bool(replace_global, "replace_global")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RegexReplace", input=input, pattern=pattern, rewrite=rewrite,
                        replace_global=replace_global, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("replace_global", _op.get_attr("replace_global"))
  _execute.record_gradient(
      "RegexReplace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def regex_replace_eager_fallback(input, pattern, rewrite, replace_global=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function regex_replace
  """
  _ctx = ctx if ctx else _context.context()
  if replace_global is None:
    replace_global = True
  replace_global = _execute.make_bool(replace_global, "replace_global")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  pattern = _ops.convert_to_tensor(pattern, _dtypes.string)
  rewrite = _ops.convert_to_tensor(rewrite, _dtypes.string)
  _inputs_flat = [input, pattern, rewrite]
  _attrs = ("replace_global", replace_global)
  _result = _execute.execute(b"RegexReplace", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RegexReplace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def static_regex_full_match(input, pattern, name=None):
  r"""Check if the input matches the regex pattern.

  The input is a string tensor of any shape. The pattern is the
  regular expression to be matched with every element of the input tensor.
  The boolean values (True or False) of the output tensor indicate
  if the input matches the regex pattern provided.

  The pattern follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)

  Args:
    input: A `Tensor` of type `string`.
      A string tensor of the text to be processed.
    pattern: A `string`. The regular expression to match the input.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StaticRegexFullMatch", name, _ctx._post_execution_callbacks, input,
        "pattern", pattern)
      return _result
    except _core._FallbackException:
      try:
        return static_regex_full_match_eager_fallback(
            input, pattern=pattern, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  pattern = _execute.make_str(pattern, "pattern")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StaticRegexFullMatch", input=input, pattern=pattern, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("pattern", _op.get_attr("pattern"))
  _execute.record_gradient(
      "StaticRegexFullMatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def static_regex_full_match_eager_fallback(input, pattern, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function static_regex_full_match
  """
  _ctx = ctx if ctx else _context.context()
  pattern = _execute.make_str(pattern, "pattern")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("pattern", pattern)
  _result = _execute.execute(b"StaticRegexFullMatch", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StaticRegexFullMatch", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def static_regex_replace(input, pattern, rewrite, replace_global=True, name=None):
  r"""Replaces the match of pattern in input with rewrite.

  It follows the re2 syntax (https://github.com/google/re2/wiki/Syntax)

  Args:
    input: A `Tensor` of type `string`. The text to be processed.
    pattern: A `string`. The regular expression to match the input.
    rewrite: A `string`. The rewrite to be applied to the matched expresion.
    replace_global: An optional `bool`. Defaults to `True`.
      If True, the replacement is global, otherwise the replacement
      is done only on the first match.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StaticRegexReplace", name, _ctx._post_execution_callbacks, input,
        "pattern", pattern, "rewrite", rewrite, "replace_global",
        replace_global)
      return _result
    except _core._FallbackException:
      try:
        return static_regex_replace_eager_fallback(
            input, pattern=pattern, rewrite=rewrite,
            replace_global=replace_global, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  pattern = _execute.make_str(pattern, "pattern")
  rewrite = _execute.make_str(rewrite, "rewrite")
  if replace_global is None:
    replace_global = True
  replace_global = _execute.make_bool(replace_global, "replace_global")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StaticRegexReplace", input=input, pattern=pattern, rewrite=rewrite,
                              replace_global=replace_global, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("pattern", _op.get_attr("pattern"), "rewrite",
            _op.get_attr("rewrite"), "replace_global",
            _op.get_attr("replace_global"))
  _execute.record_gradient(
      "StaticRegexReplace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def static_regex_replace_eager_fallback(input, pattern, rewrite, replace_global=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function static_regex_replace
  """
  _ctx = ctx if ctx else _context.context()
  pattern = _execute.make_str(pattern, "pattern")
  rewrite = _execute.make_str(rewrite, "rewrite")
  if replace_global is None:
    replace_global = True
  replace_global = _execute.make_bool(replace_global, "replace_global")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("pattern", pattern, "rewrite", rewrite, "replace_global",
  replace_global)
  _result = _execute.execute(b"StaticRegexReplace", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StaticRegexReplace", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def string_format(inputs, template="%s", placeholder="%s", summarize=3, name=None):
  r"""Formats a string template using a list of tensors.

  Formats a string template using a list of tensors, pretty-printing tensor summaries.

  Args:
    inputs: A list of `Tensor` objects.
      The list of tensors to format into the placeholder string.
    template: An optional `string`. Defaults to `"%s"`.
      A string, the template to format tensor summaries into.
    placeholder: An optional `string`. Defaults to `"%s"`.
      A string, at each placeholder in the template a subsequent tensor summary will be inserted.
    summarize: An optional `int`. Defaults to `3`.
      When formatting the tensor summaries print the first and last summarize entries of each tensor dimension.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StringFormat",
        name, _ctx._post_execution_callbacks, inputs, "template", template,
        "placeholder", placeholder, "summarize", summarize)
      return _result
    except _core._FallbackException:
      try:
        return string_format_eager_fallback(
            inputs, template=template, placeholder=placeholder,
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
  if template is None:
    template = "%s"
  template = _execute.make_str(template, "template")
  if placeholder is None:
    placeholder = "%s"
  placeholder = _execute.make_str(placeholder, "placeholder")
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StringFormat", inputs=inputs, template=template,
                        placeholder=placeholder, summarize=summarize,
                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "template", _op.get_attr("template"),
            "placeholder", _op.get_attr("placeholder"), "summarize",
            _op.get_attr("summarize"))
  _execute.record_gradient(
      "StringFormat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_format_eager_fallback(inputs, template="%s", placeholder="%s", summarize=3, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_format
  """
  _ctx = ctx if ctx else _context.context()
  if template is None:
    template = "%s"
  template = _execute.make_str(template, "template")
  if placeholder is None:
    placeholder = "%s"
  placeholder = _execute.make_str(placeholder, "placeholder")
  if summarize is None:
    summarize = 3
  summarize = _execute.make_int(summarize, "summarize")
  _attr_T, inputs = _execute.convert_to_mixed_eager_tensors(inputs, _ctx)
  _inputs_flat = list(inputs)
  _attrs = ("T", _attr_T, "template", template, "placeholder", placeholder,
  "summarize", summarize)
  _result = _execute.execute(b"StringFormat", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringFormat", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.join', v1=['strings.join', 'string_join'])
@deprecated_endpoints('string_join')
def string_join(inputs, separator="", name=None):
  r"""Joins the strings in the given list of string tensors into one tensor;

  with the given separator (default is an empty separator).

  Args:
    inputs: A list of at least 1 `Tensor` objects with type `string`.
      A list of string tensors.  The tensors must all have the same shape,
      or be scalars.  Scalars may be mixed in; these will be broadcast to the shape
      of non-scalar inputs.
    separator: An optional `string`. Defaults to `""`.
      string, an optional join separator.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StringJoin",
        name, _ctx._post_execution_callbacks, inputs, "separator", separator)
      return _result
    except _core._FallbackException:
      try:
        return string_join_eager_fallback(
            inputs, separator=separator, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              string_join, inputs=inputs, separator=separator, name=name)
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
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'string_join' Op, not %r." % inputs)
  _attr_N = len(inputs)
  if separator is None:
    separator = ""
  separator = _execute.make_str(separator, "separator")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StringJoin", inputs=inputs, separator=separator, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          string_join, inputs=inputs, separator=separator, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("N", _op.get_attr("N"), "separator", _op.get_attr("separator"))
  _execute.record_gradient(
      "StringJoin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_join_eager_fallback(inputs, separator="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_join
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(inputs, (list, tuple)):
    raise TypeError(
        "Expected list for 'inputs' argument to "
        "'string_join' Op, not %r." % inputs)
  _attr_N = len(inputs)
  if separator is None:
    separator = ""
  separator = _execute.make_str(separator, "separator")
  inputs = _ops.convert_n_to_tensor(inputs, _dtypes.string)
  _inputs_flat = list(inputs)
  _attrs = ("N", _attr_N, "separator", separator)
  _result = _execute.execute(b"StringJoin", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringJoin", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def string_length(input, unit="BYTE", name=None):
  r"""String lengths of `input`.

  Computes the length of each string given in the input tensor.

  Args:
    input: A `Tensor` of type `string`.
      The string for which to compute the length.
    unit: An optional `string` from: `"BYTE", "UTF8_CHAR"`. Defaults to `"BYTE"`.
      The unit that is counted to compute string length.  One of: `"BYTE"` (for
      the number of bytes in each string) or `"UTF8_CHAR"` (for the number of UTF-8
      encoded Unicode code points in each string).  Results are undefined
      if `unit=UTF8_CHAR` and the `input` strings do not contain structurally
      valid UTF-8.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StringLength",
        name, _ctx._post_execution_callbacks, input, "unit", unit)
      return _result
    except _core._FallbackException:
      try:
        return string_length_eager_fallback(
            input, unit=unit, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if unit is None:
    unit = "BYTE"
  unit = _execute.make_str(unit, "unit")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StringLength", input=input, unit=unit, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("unit", _op.get_attr("unit"))
  _execute.record_gradient(
      "StringLength", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_length_eager_fallback(input, unit="BYTE", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_length
  """
  _ctx = ctx if ctx else _context.context()
  if unit is None:
    unit = "BYTE"
  unit = _execute.make_str(unit, "unit")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("unit", unit)
  _result = _execute.execute(b"StringLength", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringLength", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_string_split_outputs = ["indices", "values", "shape"]
_StringSplitOutput = _collections.namedtuple(
    "StringSplit", _string_split_outputs)


def string_split(input, delimiter, skip_empty=True, name=None):
  r"""Split elements of `input` based on `delimiter` into a `SparseTensor`.

  Let N be the size of source (typically N will be the batch size). Split each
  element of `input` based on `delimiter` and return a `SparseTensor`
  containing the splitted tokens. Empty tokens are ignored.

  `delimiter` can be empty, or a string of split characters. If `delimiter` is an
   empty string, each element of `input` is split into individual single-byte
   character strings, including splitting of UTF-8 multibyte sequences. Otherwise
   every character of `delimiter` is a potential split point.

  For example:
    N = 2, input[0] is 'hello world' and input[1] is 'a b c', then the output
    will be

    indices = [0, 0;
               0, 1;
               1, 0;
               1, 1;
               1, 2]
    shape = [2, 3]
    values = ['hello', 'world', 'a', 'b', 'c']

  Args:
    input: A `Tensor` of type `string`. 1-D. Strings to split.
    delimiter: A `Tensor` of type `string`.
      0-D. Delimiter characters (bytes), or empty string.
    skip_empty: An optional `bool`. Defaults to `True`.
      A `bool`. If `True`, skip the empty strings from the result.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (indices, values, shape).

    indices: A `Tensor` of type `int64`.
    values: A `Tensor` of type `string`.
    shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StringSplit",
        name, _ctx._post_execution_callbacks, input, delimiter, "skip_empty",
        skip_empty)
      _result = _StringSplitOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return string_split_eager_fallback(
            input, delimiter, skip_empty=skip_empty, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if skip_empty is None:
    skip_empty = True
  skip_empty = _execute.make_bool(skip_empty, "skip_empty")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StringSplit", input=input, delimiter=delimiter,
                       skip_empty=skip_empty, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("skip_empty", _op.get_attr("skip_empty"))
  _execute.record_gradient(
      "StringSplit", _inputs_flat, _attrs, _result, name)
  _result = _StringSplitOutput._make(_result)
  return _result



def string_split_eager_fallback(input, delimiter, skip_empty=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_split
  """
  _ctx = ctx if ctx else _context.context()
  if skip_empty is None:
    skip_empty = True
  skip_empty = _execute.make_bool(skip_empty, "skip_empty")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  delimiter = _ops.convert_to_tensor(delimiter, _dtypes.string)
  _inputs_flat = [input, delimiter]
  _attrs = ("skip_empty", skip_empty)
  _result = _execute.execute(b"StringSplit", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringSplit", _inputs_flat, _attrs, _result, name)
  _result = _StringSplitOutput._make(_result)
  return _result


_string_split_v2_outputs = ["indices", "values", "shape"]
_StringSplitV2Output = _collections.namedtuple(
    "StringSplitV2", _string_split_v2_outputs)


def string_split_v2(input, sep, maxsplit=-1, name=None):
  r"""Split elements of `source` based on `sep` into a `SparseTensor`.

  Let N be the size of source (typically N will be the batch size). Split each
  element of `source` based on `sep` and return a `SparseTensor`
  containing the split tokens. Empty tokens are ignored.

  For example, N = 2, source[0] is 'hello world' and source[1] is 'a b c',
  then the output will be
  ```
  st.indices = [0, 0;
                0, 1;
                1, 0;
                1, 1;
                1, 2]
  st.shape = [2, 3]
  st.values = ['hello', 'world', 'a', 'b', 'c']
  ```

  If `sep` is given, consecutive delimiters are not grouped together and are
  deemed to delimit empty strings. For example, source of `"1<>2<><>3"` and
  sep of `"<>"` returns `["1", "2", "", "3"]`. If `sep` is None or an empty
  string, consecutive whitespace are regarded as a single separator, and the
  result will contain no empty strings at the startor end if the string has
  leading or trailing whitespace.

  Note that the above mentioned behavior matches python's str.split.

  Args:
    input: A `Tensor` of type `string`.
      `1-D` string `Tensor`, the strings to split.
    sep: A `Tensor` of type `string`.
      `0-D` string `Tensor`, the delimiter character.
    maxsplit: An optional `int`. Defaults to `-1`.
      An `int`. If `maxsplit > 0`, limit of the split of the result.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (indices, values, shape).

    indices: A `Tensor` of type `int64`.
    values: A `Tensor` of type `string`.
    shape: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StringSplitV2", name, _ctx._post_execution_callbacks, input, sep,
        "maxsplit", maxsplit)
      _result = _StringSplitV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return string_split_v2_eager_fallback(
            input, sep, maxsplit=maxsplit, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if maxsplit is None:
    maxsplit = -1
  maxsplit = _execute.make_int(maxsplit, "maxsplit")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StringSplitV2", input=input, sep=sep, maxsplit=maxsplit, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("maxsplit", _op.get_attr("maxsplit"))
  _execute.record_gradient(
      "StringSplitV2", _inputs_flat, _attrs, _result, name)
  _result = _StringSplitV2Output._make(_result)
  return _result



def string_split_v2_eager_fallback(input, sep, maxsplit=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_split_v2
  """
  _ctx = ctx if ctx else _context.context()
  if maxsplit is None:
    maxsplit = -1
  maxsplit = _execute.make_int(maxsplit, "maxsplit")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  sep = _ops.convert_to_tensor(sep, _dtypes.string)
  _inputs_flat = [input, sep]
  _attrs = ("maxsplit", maxsplit)
  _result = _execute.execute(b"StringSplitV2", 3, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringSplitV2", _inputs_flat, _attrs, _result, name)
  _result = _StringSplitV2Output._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.strip', v1=['strings.strip', 'string_strip'])
@deprecated_endpoints('string_strip')
def string_strip(input, name=None):
  r"""Strip leading and trailing whitespaces from the Tensor.

  Args:
    input: A `Tensor` of type `string`. A string `Tensor` of any shape.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "StringStrip",
        name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return string_strip_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              string_strip, input=input, name=name)
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
        "StringStrip", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          string_strip, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "StringStrip", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_strip_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_strip
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"StringStrip", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringStrip", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def string_to_hash_bucket(string_tensor, num_buckets, name=None):
  r"""Converts each string in the input Tensor to its hash mod by a number of buckets.

  The hash function is deterministic on the content of the string within the
  process.

  Note that the hash function may change from time to time.
  This functionality will be deprecated and it's recommended to use
  `tf.string_to_hash_bucket_fast()` or `tf.string_to_hash_bucket_strong()`.

  Args:
    string_tensor: A `Tensor` of type `string`.
    num_buckets: An `int` that is `>= 1`. The number of buckets.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StringToHashBucket", name, _ctx._post_execution_callbacks,
        string_tensor, "num_buckets", num_buckets)
      return _result
    except _core._FallbackException:
      try:
        return string_to_hash_bucket_eager_fallback(
            string_tensor, num_buckets=num_buckets, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  _, _, _op = _op_def_lib._apply_op_helper(
        "StringToHashBucket", string_tensor=string_tensor,
                              num_buckets=num_buckets, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_buckets", _op.get_attr("num_buckets"))
  _execute.record_gradient(
      "StringToHashBucket", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_to_hash_bucket_eager_fallback(string_tensor, num_buckets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_to_hash_bucket
  """
  _ctx = ctx if ctx else _context.context()
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  string_tensor = _ops.convert_to_tensor(string_tensor, _dtypes.string)
  _inputs_flat = [string_tensor]
  _attrs = ("num_buckets", num_buckets)
  _result = _execute.execute(b"StringToHashBucket", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "StringToHashBucket", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.to_hash_bucket_fast', v1=['strings.to_hash_bucket_fast', 'string_to_hash_bucket_fast'])
@deprecated_endpoints('string_to_hash_bucket_fast')
def string_to_hash_bucket_fast(input, num_buckets, name=None):
  r"""Converts each string in the input Tensor to its hash mod by a number of buckets.

  The hash function is deterministic on the content of the string within the
  process and will never change. However, it is not suitable for cryptography.
  This function may be used when CPU time is scarce and inputs are trusted or
  unimportant. There is a risk of adversaries constructing inputs that all hash
  to the same bucket. To prevent this problem, use a strong hash function with
  `tf.string_to_hash_bucket_strong`.

  Args:
    input: A `Tensor` of type `string`. The strings to assign a hash bucket.
    num_buckets: An `int` that is `>= 1`. The number of buckets.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StringToHashBucketFast", name, _ctx._post_execution_callbacks, input,
        "num_buckets", num_buckets)
      return _result
    except _core._FallbackException:
      try:
        return string_to_hash_bucket_fast_eager_fallback(
            input, num_buckets=num_buckets, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              string_to_hash_bucket_fast, input=input,
                                          num_buckets=num_buckets, name=name)
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
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StringToHashBucketFast", input=input, num_buckets=num_buckets,
                                  name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          string_to_hash_bucket_fast, input=input, num_buckets=num_buckets,
                                      name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_buckets", _op.get_attr("num_buckets"))
  _execute.record_gradient(
      "StringToHashBucketFast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_to_hash_bucket_fast_eager_fallback(input, num_buckets, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_to_hash_bucket_fast
  """
  _ctx = ctx if ctx else _context.context()
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("num_buckets", num_buckets)
  _result = _execute.execute(b"StringToHashBucketFast", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StringToHashBucketFast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.to_hash_bucket_strong', v1=['strings.to_hash_bucket_strong', 'string_to_hash_bucket_strong'])
@deprecated_endpoints('string_to_hash_bucket_strong')
def string_to_hash_bucket_strong(input, num_buckets, key, name=None):
  r"""Converts each string in the input Tensor to its hash mod by a number of buckets.

  The hash function is deterministic on the content of the string within the
  process. The hash function is a keyed hash function, where attribute `key`
  defines the key of the hash function. `key` is an array of 2 elements.

  A strong hash is important when inputs may be malicious, e.g. URLs with
  additional components. Adversaries could try to make their inputs hash to the
  same bucket for a denial-of-service attack or to skew the results. A strong
  hash prevents this by making it difficult, if not infeasible, to compute inputs
  that hash to the same bucket. This comes at a cost of roughly 4x higher compute
  time than `tf.string_to_hash_bucket_fast`.

  Args:
    input: A `Tensor` of type `string`. The strings to assign a hash bucket.
    num_buckets: An `int` that is `>= 1`. The number of buckets.
    key: A list of `ints`.
      The key for the keyed hash function passed as a list of two uint64
      elements.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "StringToHashBucketStrong", name, _ctx._post_execution_callbacks,
        input, "num_buckets", num_buckets, "key", key)
      return _result
    except _core._FallbackException:
      try:
        return string_to_hash_bucket_strong_eager_fallback(
            input, num_buckets=num_buckets, key=key, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              string_to_hash_bucket_strong, input=input,
                                            num_buckets=num_buckets, key=key,
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
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  if not isinstance(key, (list, tuple)):
    raise TypeError(
        "Expected list for 'key' argument to "
        "'string_to_hash_bucket_strong' Op, not %r." % key)
  key = [_execute.make_int(_i, "key") for _i in key]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "StringToHashBucketStrong", input=input, num_buckets=num_buckets,
                                    key=key, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          string_to_hash_bucket_strong, input=input, num_buckets=num_buckets,
                                        key=key, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_buckets", _op.get_attr("num_buckets"), "key",
            _op.get_attr("key"))
  _execute.record_gradient(
      "StringToHashBucketStrong", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def string_to_hash_bucket_strong_eager_fallback(input, num_buckets, key, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function string_to_hash_bucket_strong
  """
  _ctx = ctx if ctx else _context.context()
  num_buckets = _execute.make_int(num_buckets, "num_buckets")
  if not isinstance(key, (list, tuple)):
    raise TypeError(
        "Expected list for 'key' argument to "
        "'string_to_hash_bucket_strong' Op, not %r." % key)
  key = [_execute.make_int(_i, "key") for _i in key]
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("num_buckets", num_buckets, "key", key)
  _result = _execute.execute(b"StringToHashBucketStrong", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "StringToHashBucketStrong", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def substr(input, pos, len, unit="BYTE", name=None):
  r"""Return substrings from `Tensor` of strings.

  For each string in the input `Tensor`, creates a substring starting at index
  `pos` with a total length of `len`.

  If `len` defines a substring that would extend beyond the length of the input
  string, then as many characters as possible are used.

  A negative `pos` indicates distance within the string backwards from the end.

  If `pos` specifies an index which is out of range for any of the input strings,
  then an `InvalidArgumentError` is thrown.

  `pos` and `len` must have the same shape, otherwise a `ValueError` is thrown on
  Op creation.

  *NOTE*: `Substr` supports broadcasting up to two dimensions. More about
  broadcasting
  [here](http://docs.scipy.org/doc/numpy/user/basics.broadcasting.html)

  ---

  Examples

  Using scalar `pos` and `len`:

  ```python
  input = [b'Hello', b'World']
  position = 1
  length = 3

  output = [b'ell', b'orl']
  ```

  Using `pos` and `len` with same shape as `input`:

  ```python
  input = [[b'ten', b'eleven', b'twelve'],
           [b'thirteen', b'fourteen', b'fifteen'],
           [b'sixteen', b'seventeen', b'eighteen']]
  position = [[1, 2, 3],
              [1, 2, 3],
              [1, 2, 3]]
  length =   [[2, 3, 4],
              [4, 3, 2],
              [5, 5, 5]]

  output = [[b'en', b'eve', b'lve'],
            [b'hirt', b'urt', b'te'],
            [b'ixtee', b'vente', b'hteen']]
  ```

  Broadcasting `pos` and `len` onto `input`:

  ```
  input = [[b'ten', b'eleven', b'twelve'],
           [b'thirteen', b'fourteen', b'fifteen'],
           [b'sixteen', b'seventeen', b'eighteen'],
           [b'nineteen', b'twenty', b'twentyone']]
  position = [1, 2, 3]
  length =   [1, 2, 3]

  output = [[b'e', b'ev', b'lve'],
            [b'h', b'ur', b'tee'],
            [b'i', b've', b'hte'],
            [b'i', b'en', b'nty']]
  ```

  Broadcasting `input` onto `pos` and `len`:

  ```
  input = b'thirteen'
  position = [1, 5, 7]
  length =   [3, 2, 1]

  output = [b'hir', b'ee', b'n']
  ```

  Args:
    input: A `Tensor` of type `string`. Tensor of strings
    pos: A `Tensor`. Must be one of the following types: `int32`, `int64`.
      Scalar defining the position of first character in each substring
    len: A `Tensor`. Must have the same type as `pos`.
      Scalar defining the number of characters to include in each substring
    unit: An optional `string` from: `"BYTE", "UTF8_CHAR"`. Defaults to `"BYTE"`.
      The unit that is used to create the substring.  One of: `"BYTE"` (for
      defining position and length by bytes) or `"UTF8_CHAR"` (for the UTF-8
      encoded Unicode code points).  The default is `"BYTE"`. Results are undefined if
      `unit=UTF8_CHAR` and the `input` strings do not contain structurally valid
      UTF-8.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Substr", name,
        _ctx._post_execution_callbacks, input, pos, len, "unit", unit)
      return _result
    except _core._FallbackException:
      try:
        return substr_eager_fallback(
            input, pos, len, unit=unit, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if unit is None:
    unit = "BYTE"
  unit = _execute.make_str(unit, "unit")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Substr", input=input, pos=pos, len=len, unit=unit, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "unit", _op.get_attr("unit"))
  _execute.record_gradient(
      "Substr", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def substr_eager_fallback(input, pos, len, unit="BYTE", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function substr
  """
  _ctx = ctx if ctx else _context.context()
  if unit is None:
    unit = "BYTE"
  unit = _execute.make_str(unit, "unit")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([pos, len], _ctx)
  (pos, len) = _inputs_T
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input, pos, len]
  _attrs = ("T", _attr_T, "unit", unit)
  _result = _execute.execute(b"Substr", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Substr", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_unicode_decode_outputs = ["row_splits", "char_values"]
_UnicodeDecodeOutput = _collections.namedtuple(
    "UnicodeDecode", _unicode_decode_outputs)


def unicode_decode(input, input_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None):
  r"""Decodes each string in `input` into a sequence of Unicode code points.

  The character codepoints for all strings are returned using a single vector
  `char_values`, with strings expanded to characters in row-major order.

  The `row_splits` tensor indicates where the codepoints for
  each input string begin and end within the `char_values` tensor.
  In particular, the values for the `i`th
  string (in row-major order) are stored in the slice
  `[row_splits[i]:row_splits[i+1]]`. Thus:

  * `char_values[row_splits[i]+j]` is the Unicode codepoint for the `j`th
    character in the `i`th string (in row-major order).
  * `row_splits[i+1] - row_splits[i]` is the number of characters in the `i`th
    string (in row-major order).

  Args:
    input: A `Tensor` of type `string`.
      The text to be decoded. Can have any shape. Note that the output is flattened
      to a vector of char values.
    input_encoding: A `string`.
      Text encoding of the input strings. This is any of the encodings supported
      by ICU ucnv algorithmic converters. Examples: `"UTF-16", "US ASCII", "UTF-8"`.
    errors: An optional `string` from: `"strict", "replace", "ignore"`. Defaults to `"replace"`.
      Error handling policy when there is invalid formatting found in the input.
      The value of 'strict' will cause the operation to produce a InvalidArgument
      error on any invalid input formatting. A value of 'replace' (the default) will
      cause the operation to replace any invalid formatting in the input with the
      `replacement_char` codepoint. A value of 'ignore' will cause the operation to
      skip any invalid formatting in the input and produce no corresponding output
      character.
    replacement_char: An optional `int`. Defaults to `65533`.
      The replacement character codepoint to be used in place of any invalid
      formatting in the input when `errors='replace'`. Any valid unicode codepoint may
      be used. The default value is the default unicode replacement character is
      0xFFFD or U+65533.)
    replace_control_characters: An optional `bool`. Defaults to `False`.
      Whether to replace the C0 control characters (00-1F) with the
      `replacement_char`. Default is false.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (row_splits, char_values).

    row_splits: A `Tensor` of type `int64`.
    char_values: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnicodeDecode", name, _ctx._post_execution_callbacks, input,
        "input_encoding", input_encoding, "errors", errors,
        "replacement_char", replacement_char, "replace_control_characters",
        replace_control_characters)
      _result = _UnicodeDecodeOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unicode_decode_eager_fallback(
            input, input_encoding=input_encoding, errors=errors,
            replacement_char=replacement_char,
            replace_control_characters=replace_control_characters, name=name,
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
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UnicodeDecode", input=input, input_encoding=input_encoding,
                         errors=errors, replacement_char=replacement_char,
                         replace_control_characters=replace_control_characters,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("input_encoding", _op.get_attr("input_encoding"), "errors",
            _op.get_attr("errors"), "replacement_char",
            _op.get_attr("replacement_char"), "replace_control_characters",
            _op.get_attr("replace_control_characters"))
  _execute.record_gradient(
      "UnicodeDecode", _inputs_flat, _attrs, _result, name)
  _result = _UnicodeDecodeOutput._make(_result)
  return _result



def unicode_decode_eager_fallback(input, input_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unicode_decode
  """
  _ctx = ctx if ctx else _context.context()
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("input_encoding", input_encoding, "errors", errors,
  "replacement_char", replacement_char, "replace_control_characters",
  replace_control_characters)
  _result = _execute.execute(b"UnicodeDecode", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnicodeDecode", _inputs_flat, _attrs, _result, name)
  _result = _UnicodeDecodeOutput._make(_result)
  return _result


_unicode_decode_with_offsets_outputs = ["row_splits", "char_values",
                                       "char_to_byte_starts"]
_UnicodeDecodeWithOffsetsOutput = _collections.namedtuple(
    "UnicodeDecodeWithOffsets", _unicode_decode_with_offsets_outputs)


def unicode_decode_with_offsets(input, input_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None):
  r"""Decodes each string in `input` into a sequence of Unicode code points.

  The character codepoints for all strings are returned using a single vector
  `char_values`, with strings expanded to characters in row-major order.
  Similarly, the character start byte offsets are returned using a single vector
  `char_to_byte_starts`, with strings expanded in row-major order.

  The `row_splits` tensor indicates where the codepoints and start offsets for
  each input string begin and end within the `char_values` and
  `char_to_byte_starts` tensors.  In particular, the values for the `i`th
  string (in row-major order) are stored in the slice
  `[row_splits[i]:row_splits[i+1]]`. Thus:

  * `char_values[row_splits[i]+j]` is the Unicode codepoint for the `j`th
    character in the `i`th string (in row-major order).
  * `char_to_bytes_starts[row_splits[i]+j]` is the start byte offset for the `j`th
    character in the `i`th string (in row-major order).
  * `row_splits[i+1] - row_splits[i]` is the number of characters in the `i`th
    string (in row-major order).

  Args:
    input: A `Tensor` of type `string`.
      The text to be decoded. Can have any shape. Note that the output is flattened
      to a vector of char values.
    input_encoding: A `string`.
      Text encoding of the input strings. This is any of the encodings supported
      by ICU ucnv algorithmic converters. Examples: `"UTF-16", "US ASCII", "UTF-8"`.
    errors: An optional `string` from: `"strict", "replace", "ignore"`. Defaults to `"replace"`.
      Error handling policy when there is invalid formatting found in the input.
      The value of 'strict' will cause the operation to produce a InvalidArgument
      error on any invalid input formatting. A value of 'replace' (the default) will
      cause the operation to replace any invalid formatting in the input with the
      `replacement_char` codepoint. A value of 'ignore' will cause the operation to
      skip any invalid formatting in the input and produce no corresponding output
      character.
    replacement_char: An optional `int`. Defaults to `65533`.
      The replacement character codepoint to be used in place of any invalid
      formatting in the input when `errors='replace'`. Any valid unicode codepoint may
      be used. The default value is the default unicode replacement character is
      0xFFFD or U+65533.)
    replace_control_characters: An optional `bool`. Defaults to `False`.
      Whether to replace the C0 control characters (00-1F) with the
      `replacement_char`. Default is false.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (row_splits, char_values, char_to_byte_starts).

    row_splits: A `Tensor` of type `int64`.
    char_values: A `Tensor` of type `int32`.
    char_to_byte_starts: A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnicodeDecodeWithOffsets", name, _ctx._post_execution_callbacks,
        input, "input_encoding", input_encoding, "errors", errors,
        "replacement_char", replacement_char, "replace_control_characters",
        replace_control_characters)
      _result = _UnicodeDecodeWithOffsetsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return unicode_decode_with_offsets_eager_fallback(
            input, input_encoding=input_encoding, errors=errors,
            replacement_char=replacement_char,
            replace_control_characters=replace_control_characters, name=name,
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
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UnicodeDecodeWithOffsets", input=input,
                                    input_encoding=input_encoding,
                                    errors=errors,
                                    replacement_char=replacement_char,
                                    replace_control_characters=replace_control_characters,
                                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("input_encoding", _op.get_attr("input_encoding"), "errors",
            _op.get_attr("errors"), "replacement_char",
            _op.get_attr("replacement_char"), "replace_control_characters",
            _op.get_attr("replace_control_characters"))
  _execute.record_gradient(
      "UnicodeDecodeWithOffsets", _inputs_flat, _attrs, _result, name)
  _result = _UnicodeDecodeWithOffsetsOutput._make(_result)
  return _result



def unicode_decode_with_offsets_eager_fallback(input, input_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unicode_decode_with_offsets
  """
  _ctx = ctx if ctx else _context.context()
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("input_encoding", input_encoding, "errors", errors,
  "replacement_char", replacement_char, "replace_control_characters",
  replace_control_characters)
  _result = _execute.execute(b"UnicodeDecodeWithOffsets", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "UnicodeDecodeWithOffsets", _inputs_flat, _attrs, _result, name)
  _result = _UnicodeDecodeWithOffsetsOutput._make(_result)
  return _result


def unicode_encode(input_values, input_splits, output_encoding, errors="replace", replacement_char=65533, name=None):
  r"""Encode a tensor of ints into unicode strings.

  Returns a vector of strings, where `output[i]` is constructed by encoding the
  Unicode codepoints in `input_values[input_splits[i]:input_splits[i+1]]`
  using `output_encoding`.

  ---

  Example:

  ```
  input_values = [72, 101, 108, 108, 111, 87, 111, 114, 108, 100]
  input_splits = [0, 5, 10]
  output_encoding = 'UTF-8'

  output = ['Hello', 'World']
  ```

  Args:
    input_values: A `Tensor` of type `int32`.
      A 1D tensor containing the unicode codepoints that should be encoded.
    input_splits: A `Tensor` of type `int64`.
      A 1D tensor specifying how the unicode codepoints should be split into strings.
      In particular, `output[i]` is constructed by encoding the codepoints in the
      slice `input_values[input_splits[i]:input_splits[i+1]]`.
    output_encoding: A `string` from: `"UTF-8", "UTF-16-BE", "UTF-32-BE"`.
      Unicode encoding of the output strings. Valid encodings are: `"UTF-8",
      "UTF-16-BE", and "UTF-32-BE"`.
    errors: An optional `string` from: `"ignore", "replace", "strict"`. Defaults to `"replace"`.
      Error handling policy when there is invalid formatting found in the input.
      The value of 'strict' will cause the operation to produce a InvalidArgument
      error on any invalid input formatting. A value of 'replace' (the default) will
      cause the operation to replace any invalid formatting in the input with the
      `replacement_char` codepoint. A value of 'ignore' will cause the operation to
      skip any invalid formatting in the input and produce no corresponding output
      character.
    replacement_char: An optional `int`. Defaults to `65533`.
      The replacement character codepoint to be used in place of any invalid
      formatting in the input when `errors='replace'`. Any valid unicode codepoint may
      be used. The default value is the default unicode replacement character is
      0xFFFD (U+65533).
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnicodeEncode", name, _ctx._post_execution_callbacks, input_values,
        input_splits, "errors", errors, "output_encoding", output_encoding,
        "replacement_char", replacement_char)
      return _result
    except _core._FallbackException:
      try:
        return unicode_encode_eager_fallback(
            input_values, input_splits, errors=errors,
            output_encoding=output_encoding,
            replacement_char=replacement_char, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  output_encoding = _execute.make_str(output_encoding, "output_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  _, _, _op = _op_def_lib._apply_op_helper(
        "UnicodeEncode", input_values=input_values, input_splits=input_splits,
                         output_encoding=output_encoding, errors=errors,
                         replacement_char=replacement_char, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("errors", _op.get_attr("errors"), "output_encoding",
            _op.get_attr("output_encoding"), "replacement_char",
            _op.get_attr("replacement_char"))
  _execute.record_gradient(
      "UnicodeEncode", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unicode_encode_eager_fallback(input_values, input_splits, output_encoding, errors="replace", replacement_char=65533, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unicode_encode
  """
  _ctx = ctx if ctx else _context.context()
  output_encoding = _execute.make_str(output_encoding, "output_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  input_values = _ops.convert_to_tensor(input_values, _dtypes.int32)
  input_splits = _ops.convert_to_tensor(input_splits, _dtypes.int64)
  _inputs_flat = [input_values, input_splits]
  _attrs = ("errors", errors, "output_encoding", output_encoding,
  "replacement_char", replacement_char)
  _result = _execute.execute(b"UnicodeEncode", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnicodeEncode", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.unicode_script')
def unicode_script(input, name=None):
  r"""Determine the script codes of a given tensor of Unicode integer code points.

  This operation converts Unicode code points to script codes corresponding to
  each code point. Script codes correspond to International Components for
  Unicode (ICU) UScriptCode values. See http://icu-project.org/apiref/icu4c/uscript_8h.html.
  Returns -1 (USCRIPT_INVALID_CODE) for invalid codepoints. Output shape will
  match input shape.

  Args:
    input: A `Tensor` of type `int32`. A Tensor of int32 Unicode code points.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnicodeScript", name, _ctx._post_execution_callbacks, input)
      return _result
    except _core._FallbackException:
      try:
        return unicode_script_eager_fallback(
            input, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unicode_script, input=input, name=name)
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
        "UnicodeScript", input=input, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unicode_script, input=input, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "UnicodeScript", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unicode_script_eager_fallback(input, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unicode_script
  """
  _ctx = ctx if ctx else _context.context()
  input = _ops.convert_to_tensor(input, _dtypes.int32)
  _inputs_flat = [input]
  _attrs = None
  _result = _execute.execute(b"UnicodeScript", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnicodeScript", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('strings.unicode_transcode')
def unicode_transcode(input, input_encoding, output_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None):
  r"""Transcode the input text from a source encoding to a destination encoding.

  The input is a string tensor of any shape. The output is a string tensor of
  the same shape containing the transcoded strings. Output strings are always
  valid unicode. If the input contains invalid encoding positions, the
  `errors` attribute sets the policy for how to deal with them. If the default
  error-handling policy is used, invalid formatting will be substituted in the
  output by the `replacement_char`. If the errors policy is to `ignore`, any
  invalid encoding positions in the input are skipped and not included in the
  output. If it set to `strict` then any invalid formatting will result in an
  InvalidArgument error.

  This operation can be used with `output_encoding = input_encoding` to enforce
  correct formatting for inputs even if they are already in the desired encoding.

  If the input is prefixed by a Byte Order Mark needed to determine encoding
  (e.g. if the encoding is UTF-16 and the BOM indicates big-endian), then that
  BOM will be consumed and not emitted into the output. If the input encoding
  is marked with an explicit endianness (e.g. UTF-16-BE), then the BOM is
  interpreted as a non-breaking-space and is preserved in the output (including
  always for UTF-8).

  The end result is that if the input is marked as an explicit endianness the
  transcoding is faithful to all codepoints in the source. If it is not marked
  with an explicit endianness, the BOM is not considered part of the string itself
  but as metadata, and so is not preserved in the output.

  Args:
    input: A `Tensor` of type `string`.
      The text to be processed. Can have any shape.
    input_encoding: A `string`.
      Text encoding of the input strings. This is any of the encodings supported
      by ICU ucnv algorithmic converters. Examples: `"UTF-16", "US ASCII", "UTF-8"`.
    output_encoding: A `string` from: `"UTF-8", "UTF-16-BE", "UTF-32-BE"`.
      The unicode encoding to use in the output. Must be one of
      `"UTF-8", "UTF-16-BE", "UTF-32-BE"`. Multi-byte encodings will be big-endian.
    errors: An optional `string` from: `"strict", "replace", "ignore"`. Defaults to `"replace"`.
      Error handling policy when there is invalid formatting found in the input.
      The value of 'strict' will cause the operation to produce a InvalidArgument
      error on any invalid input formatting. A value of 'replace' (the default) will
      cause the operation to replace any invalid formatting in the input with the
      `replacement_char` codepoint. A value of 'ignore' will cause the operation to
      skip any invalid formatting in the input and produce no corresponding output
      character.
    replacement_char: An optional `int`. Defaults to `65533`.
      The replacement character codepoint to be used in place of any invalid
      formatting in the input when `errors='replace'`. Any valid unicode codepoint may
      be used. The default value is the default unicode replacement character is
      0xFFFD or U+65533.)

      Note that for UTF-8, passing a replacement character expressible in 1 byte, such
      as ' ', will preserve string alignment to the source since invalid bytes will be
      replaced with a 1-byte replacement. For UTF-16-BE and UTF-16-LE, any 1 or 2 byte
      replacement character will preserve byte alignment to the source.
    replace_control_characters: An optional `bool`. Defaults to `False`.
      Whether to replace the C0 control characters (00-1F) with the
      `replacement_char`. Default is false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnicodeTranscode", name, _ctx._post_execution_callbacks, input,
        "input_encoding", input_encoding, "output_encoding", output_encoding,
        "errors", errors, "replacement_char", replacement_char,
        "replace_control_characters", replace_control_characters)
      return _result
    except _core._FallbackException:
      try:
        return unicode_transcode_eager_fallback(
            input, input_encoding=input_encoding,
            output_encoding=output_encoding, errors=errors,
            replacement_char=replacement_char,
            replace_control_characters=replace_control_characters, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              unicode_transcode, input=input, input_encoding=input_encoding,
                                 output_encoding=output_encoding,
                                 errors=errors,
                                 replacement_char=replacement_char,
                                 replace_control_characters=replace_control_characters,
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
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  output_encoding = _execute.make_str(output_encoding, "output_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "UnicodeTranscode", input=input, input_encoding=input_encoding,
                            output_encoding=output_encoding, errors=errors,
                            replacement_char=replacement_char,
                            replace_control_characters=replace_control_characters,
                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          unicode_transcode, input=input, input_encoding=input_encoding,
                             output_encoding=output_encoding, errors=errors,
                             replacement_char=replacement_char,
                             replace_control_characters=replace_control_characters,
                             name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("input_encoding", _op.get_attr("input_encoding"),
            "output_encoding", _op.get_attr("output_encoding"), "errors",
            _op.get_attr("errors"), "replacement_char",
            _op.get_attr("replacement_char"), "replace_control_characters",
            _op.get_attr("replace_control_characters"))
  _execute.record_gradient(
      "UnicodeTranscode", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unicode_transcode_eager_fallback(input, input_encoding, output_encoding, errors="replace", replacement_char=65533, replace_control_characters=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unicode_transcode
  """
  _ctx = ctx if ctx else _context.context()
  input_encoding = _execute.make_str(input_encoding, "input_encoding")
  output_encoding = _execute.make_str(output_encoding, "output_encoding")
  if errors is None:
    errors = "replace"
  errors = _execute.make_str(errors, "errors")
  if replacement_char is None:
    replacement_char = 65533
  replacement_char = _execute.make_int(replacement_char, "replacement_char")
  if replace_control_characters is None:
    replace_control_characters = False
  replace_control_characters = _execute.make_bool(replace_control_characters, "replace_control_characters")
  input = _ops.convert_to_tensor(input, _dtypes.string)
  _inputs_flat = [input]
  _attrs = ("input_encoding", input_encoding, "output_encoding",
  output_encoding, "errors", errors, "replacement_char", replacement_char,
  "replace_control_characters", replace_control_characters)
  _result = _execute.execute(b"UnicodeTranscode", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnicodeTranscode", _inputs_flat, _attrs, _result, name)
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
#   name: "AsString"
#   input_arg {
#     name: "input"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_COMPLEX64
#         type: DT_COMPLEX128
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_BOOL
#       }
#     }
#   }
#   attr {
#     name: "precision"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "scientific"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "shortest"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "width"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "fill"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "DecodeBase64"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
# }
# op {
#   name: "EncodeBase64"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "pad"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ReduceJoin"
#   input_arg {
#     name: "inputs"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "reduction_indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "keep_dims"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "separator"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "RegexFullMatch"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "pattern"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_BOOL
#   }
# }
# op {
#   name: "RegexReplace"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "pattern"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "rewrite"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "replace_global"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "StaticRegexFullMatch"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_BOOL
#   }
#   attr {
#     name: "pattern"
#     type: "string"
#   }
# }
# op {
#   name: "StaticRegexReplace"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "pattern"
#     type: "string"
#   }
#   attr {
#     name: "rewrite"
#     type: "string"
#   }
#   attr {
#     name: "replace_global"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "StringFormat"
#   input_arg {
#     name: "inputs"
#     type_list_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "template"
#     type: "string"
#     default_value {
#       s: "%s"
#     }
#   }
#   attr {
#     name: "placeholder"
#     type: "string"
#     default_value {
#       s: "%s"
#     }
#   }
#   attr {
#     name: "summarize"
#     type: "int"
#     default_value {
#       i: 3
#     }
#   }
# }
# op {
#   name: "StringJoin"
#   input_arg {
#     name: "inputs"
#     type: DT_STRING
#     number_attr: "N"
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "separator"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "StringLength"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT32
#   }
#   attr {
#     name: "unit"
#     type: "string"
#     default_value {
#       s: "BYTE"
#     }
#     allowed_values {
#       list {
#         s: "BYTE"
#         s: "UTF8_CHAR"
#       }
#     }
#   }
# }
# op {
#   name: "StringSplit"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "delimiter"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "values"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "skip_empty"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "StringSplitV2"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "sep"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "values"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "shape"
#     type: DT_INT64
#   }
#   attr {
#     name: "maxsplit"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
# }
# op {
#   name: "StringStrip"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
# }
# op {
#   name: "StringToHashBucket"
#   input_arg {
#     name: "string_tensor"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT64
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "StringToHashBucketFast"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT64
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "StringToHashBucketStrong"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT64
#   }
#   attr {
#     name: "num_buckets"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "key"
#     type: "list(int)"
#   }
# }
# op {
#   name: "Substr"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "pos"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "len"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
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
#   attr {
#     name: "unit"
#     type: "string"
#     default_value {
#       s: "BYTE"
#     }
#     allowed_values {
#       list {
#         s: "BYTE"
#         s: "UTF8_CHAR"
#       }
#     }
#   }
# }
# op {
#   name: "UnicodeDecode"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "row_splits"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "char_values"
#     type: DT_INT32
#   }
#   attr {
#     name: "input_encoding"
#     type: "string"
#   }
#   attr {
#     name: "errors"
#     type: "string"
#     default_value {
#       s: "replace"
#     }
#     allowed_values {
#       list {
#         s: "strict"
#         s: "replace"
#         s: "ignore"
#       }
#     }
#   }
#   attr {
#     name: "replacement_char"
#     type: "int"
#     default_value {
#       i: 65533
#     }
#   }
#   attr {
#     name: "replace_control_characters"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "UnicodeDecodeWithOffsets"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "row_splits"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "char_values"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "char_to_byte_starts"
#     type: DT_INT64
#   }
#   attr {
#     name: "input_encoding"
#     type: "string"
#   }
#   attr {
#     name: "errors"
#     type: "string"
#     default_value {
#       s: "replace"
#     }
#     allowed_values {
#       list {
#         s: "strict"
#         s: "replace"
#         s: "ignore"
#       }
#     }
#   }
#   attr {
#     name: "replacement_char"
#     type: "int"
#     default_value {
#       i: 65533
#     }
#   }
#   attr {
#     name: "replace_control_characters"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "UnicodeEncode"
#   input_arg {
#     name: "input_values"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "input_splits"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "errors"
#     type: "string"
#     default_value {
#       s: "replace"
#     }
#     allowed_values {
#       list {
#         s: "ignore"
#         s: "replace"
#         s: "strict"
#       }
#     }
#   }
#   attr {
#     name: "output_encoding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "UTF-8"
#         s: "UTF-16-BE"
#         s: "UTF-32-BE"
#       }
#     }
#   }
#   attr {
#     name: "replacement_char"
#     type: "int"
#     default_value {
#       i: 65533
#     }
#   }
# }
# op {
#   name: "UnicodeScript"
#   input_arg {
#     name: "input"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_INT32
#   }
# }
# op {
#   name: "UnicodeTranscode"
#   input_arg {
#     name: "input"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "output"
#     type: DT_STRING
#   }
#   attr {
#     name: "input_encoding"
#     type: "string"
#   }
#   attr {
#     name: "output_encoding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "UTF-8"
#         s: "UTF-16-BE"
#         s: "UTF-32-BE"
#       }
#     }
#   }
#   attr {
#     name: "errors"
#     type: "string"
#     default_value {
#       s: "replace"
#     }
#     allowed_values {
#       list {
#         s: "strict"
#         s: "replace"
#         s: "ignore"
#       }
#     }
#   }
#   attr {
#     name: "replacement_char"
#     type: "int"
#     default_value {
#       i: 65533
#     }
#   }
#   attr {
#     name: "replace_control_characters"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\270\001\n\010AsString\022\n\n\005input\"\001T\032\n\n\006output\030\007\"\030\n\001T\022\004type:\r\n\0132\t\006\005\003\t\010\022\001\002\n\"\035\n\tprecision\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\026\n\nscientific\022\004bool\032\002(\000\"\024\n\010shortest\022\004bool\032\002(\000\"\031\n\005width\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\022\n\004fill\022\006string\032\002\022\000\n%\n\014DecodeBase64\022\t\n\005input\030\007\032\n\n\006output\030\007\n6\n\014EncodeBase64\022\t\n\005input\030\007\032\n\n\006output\030\007\"\017\n\003pad\022\004bool\032\002(\000\nk\n\nReduceJoin\022\n\n\006inputs\030\007\022\025\n\021reduction_indices\030\003\032\n\n\006output\030\007\"\025\n\tkeep_dims\022\004bool\032\002(\000\"\027\n\tseparator\022\006string\032\002\022\000\n4\n\016RegexFullMatch\022\t\n\005input\030\007\022\013\n\007pattern\030\007\032\n\n\006output\030\n\n[\n\014RegexReplace\022\t\n\005input\030\007\022\013\n\007pattern\030\007\022\013\n\007rewrite\030\007\032\n\n\006output\030\007\"\032\n\016replace_global\022\004bool\032\002(\001\n@\n\024StaticRegexFullMatch\022\t\n\005input\030\007\032\n\n\006output\030\n\"\021\n\007pattern\022\006string\nm\n\022StaticRegexReplace\022\t\n\005input\030\007\032\n\n\006output\030\007\"\021\n\007pattern\022\006string\"\021\n\007rewrite\022\006string\"\032\n\016replace_global\022\004bool\032\002(\001\n\207\001\n\014StringFormat\022\013\n\006inputs2\001T\032\n\n\006output\030\007\"\021\n\001T\022\nlist(type)(\001\"\030\n\010template\022\006string\032\004\022\002%s\"\033\n\013placeholder\022\006string\032\004\022\002%s\"\024\n\tsummarize\022\003int\032\002\030\003\nN\n\nStringJoin\022\r\n\006inputs\030\007*\001N\032\n\n\006output\030\007\"\014\n\001N\022\003int(\0010\001\"\027\n\tseparator\022\006string\032\002\022\000\nR\n\014StringLength\022\t\n\005input\030\007\032\n\n\006output\030\003\"+\n\004unit\022\006string\032\006\022\004BYTE:\023\n\021\022\004BYTE\022\tUTF8_CHAR\nc\n\013StringSplit\022\t\n\005input\030\007\022\r\n\tdelimiter\030\007\032\013\n\007indices\030\t\032\n\n\006values\030\007\032\t\n\005shape\030\t\"\026\n\nskip_empty\022\004bool\032\002(\001\ne\n\rStringSplitV2\022\t\n\005input\030\007\022\007\n\003sep\030\007\032\013\n\007indices\030\t\032\n\n\006values\030\007\032\t\n\005shape\030\t\"\034\n\010maxsplit\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\n$\n\013StringStrip\022\t\n\005input\030\007\032\n\n\006output\030\007\nK\n\022StringToHashBucket\022\021\n\rstring_tensor\030\007\032\n\n\006output\030\t\"\026\n\013num_buckets\022\003int(\0010\001\nG\n\026StringToHashBucketFast\022\t\n\005input\030\007\032\n\n\006output\030\t\"\026\n\013num_buckets\022\003int(\0010\001\n[\n\030StringToHashBucketStrong\022\t\n\005input\030\007\032\n\n\006output\030\t\"\026\n\013num_buckets\022\003int(\0010\001\"\020\n\003key\022\tlist(int)\ns\n\006Substr\022\t\n\005input\030\007\022\010\n\003pos\"\001T\022\010\n\003len\"\001T\032\n\n\006output\030\007\"\021\n\001T\022\004type:\006\n\0042\002\003\t\"+\n\004unit\022\006string\032\006\022\004BYTE:\023\n\021\022\004BYTE\022\tUTF8_CHAR\n\326\001\n\rUnicodeDecode\022\t\n\005input\030\007\032\016\n\nrow_splits\030\t\032\017\n\013char_values\030\003\"\030\n\016input_encoding\022\006string\"8\n\006errors\022\006string\032\t\022\007replace:\033\n\031\022\006strict\022\007replace\022\006ignore\"\035\n\020replacement_char\022\003int\032\004\030\375\377\003\"&\n\032replace_control_characters\022\004bool\032\002(\000\n\372\001\n\030UnicodeDecodeWithOffsets\022\t\n\005input\030\007\032\016\n\nrow_splits\030\t\032\017\n\013char_values\030\003\032\027\n\023char_to_byte_starts\030\t\"\030\n\016input_encoding\022\006string\"8\n\006errors\022\006string\032\t\022\007replace:\033\n\031\022\006strict\022\007replace\022\006ignore\"\035\n\020replacement_char\022\003int\032\004\030\375\377\003\"&\n\032replace_control_characters\022\004bool\032\002(\000\n\324\001\n\rUnicodeEncode\022\020\n\014input_values\030\003\022\020\n\014input_splits\030\t\032\n\n\006output\030\007\"8\n\006errors\022\006string\032\t\022\007replace:\033\n\031\022\006ignore\022\007replace\022\006strict\":\n\017output_encoding\022\006string:\037\n\035\022\005UTF-8\022\tUTF-16-BE\022\tUTF-32-BE\"\035\n\020replacement_char\022\003int\032\004\030\375\377\003\n&\n\rUnicodeScript\022\t\n\005input\030\003\032\n\n\006output\030\003\n\200\002\n\020UnicodeTranscode\022\t\n\005input\030\007\032\n\n\006output\030\007\"\030\n\016input_encoding\022\006string\":\n\017output_encoding\022\006string:\037\n\035\022\005UTF-8\022\tUTF-16-BE\022\tUTF-32-BE\"8\n\006errors\022\006string\032\t\022\007replace:\033\n\031\022\006strict\022\007replace\022\006ignore\"\035\n\020replacement_char\022\003int\032\004\030\375\377\003\"&\n\032replace_control_characters\022\004bool\032\002(\000")
