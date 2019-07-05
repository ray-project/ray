"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: fused_conv2d_bias_activation_op.cc
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
@tf_export('fused_conv2d_bias_activation')
def fused_conv2d_bias_activation(conv_input, filter, bias, side_input, conv_input_scale, side_input_scale, strides, padding, data_format="NHWC", filter_format="HWIO", activation_mode="Relu", dilations=[1, 1, 1, 1], name=None):
  r"""    Computes a fused kernel which implements: 2-D convolution, adds side input,

      with separate scaling on convolution and side inputs, then adds bias and
      applies the RELU activation function to the result. Supports both float and
      qint8 data formats. In the case of qint8, the output is clipped to [0..127].

      conv_input: A tensor with format as specified by `data_format` (see below).
      filter: A tensor with format depending on `data_format` as follows:
          "NHWC", "NCHW":
               `float [ filter_height, filter_width, in_channels, out_channels ]`
          "NCHW_VECT_C":
               `qint8 [ out_channels, in_channels, filter_height, filter_width ]`
      bias: 1-D float tensor with size matching the `out_channels` dimension of
          `filter`.
          Note: this tensor is still float, even if other inputs are qint8.
      side_input: A tensor with format as specified by `data_format` (see below).
          This tensor will be ignored and can be [] if side_input_scale == 0.
          Otherwise, the size of each dimension must match the `output` tensor.
      conv_input_scale: scalar float value to be multiplied by `conv_input`.
          (conceptually.. in reality it is applied after convolution).
      side_input_scale: scalar float value to be multiplied by `side_input`.
      output: A tensor with format as specified by `data_format` (see below).
          The dimension sizes are determined automatically based on other inputs
          and attributes.
      T: The element data type of `conv_input`, `side_input` and `output` tensors.
          Note: must match with the `data_format`.
      Tbias: The element data type of `bias`.
      strides: 1-D tensor of length 4.  The stride of the sliding window for each
          dimension of `input`. The dimension order is determined by the value of
          `data_format`, see below for details.
          Note: the stride for batch and channel dimensions must be 1.
      padding: The type of padding algorithm to use.
      data_format: A string specifying the data format of `conv_input`,
          `side_input` and `output` tensors with the following options:
          "NHWC": `float [ batch, height, width, channels ]`
          "NCHW": `float [ batch, channels, height, width ]`
          "NCHW_VECT_C":
              `qint8 [ batch, channels / 4, height, width, channels % 4 ]`
          Note: for "NCHW_VECT_C", `channels` must be a multiple of 4.
      filter_format: A string specifying the data format of `filter`,
          "HWIO": `float [ kernel_height, kernel_width, input_channels,
                           output_channels ]`
          "OIHW_VECT_I":
              `qint8 [ output_channels, input_channels / 4,
                       kernel_height, kernel_width, input_channels % 4 ]`
      activation_mode: The activation applied to the output.
          Must be "Relu" or "None".
      dilations: 1-D tensor of length 4.  The dilation factor for each dimension
          of `input`. If set to k > 1, there will be k-1 skipped cells between
          each filter element on that dimension. The dimension order is determined
          by the value of `data_format`, see above for details. Dilations in the
          batch and depth dimensions must be 1.

  Args:
    conv_input: A `Tensor`. Must be one of the following types: `float32`, `half`, `qint8`.
    filter: A `Tensor`. Must have the same type as `conv_input`.
    bias: A `Tensor`. Must be one of the following types: `float32`, `half`.
    side_input: A `Tensor`. Must have the same type as `conv_input`.
    conv_input_scale: A `Tensor` of type `float32`.
    side_input_scale: A `Tensor` of type `float32`.
    strides: A list of `ints`.
    padding: A `string` from: `"SAME", "VALID"`.
    data_format: An optional `string` from: `"NHWC", "NCHW", "NCHW_VECT_C"`. Defaults to `"NHWC"`.
    filter_format: An optional `string` from: `"HWIO", "OIHW", "OIHW_VECT_I"`. Defaults to `"HWIO"`.
    activation_mode: An optional `string` from: `"Relu", "None"`. Defaults to `"Relu"`.
    dilations: An optional list of `ints`. Defaults to `[1, 1, 1, 1]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `conv_input`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FusedConv2DBiasActivation", name, _ctx._post_execution_callbacks,
        conv_input, filter, bias, side_input, conv_input_scale,
        side_input_scale, "strides", strides, "padding", padding,
        "data_format", data_format, "filter_format", filter_format,
        "activation_mode", activation_mode, "dilations", dilations)
      return _result
    except _core._FallbackException:
      try:
        return fused_conv2d_bias_activation_eager_fallback(
            conv_input, filter, bias, side_input, conv_input_scale,
            side_input_scale, strides=strides, padding=padding,
            data_format=data_format, filter_format=filter_format,
            activation_mode=activation_mode, dilations=dilations, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              fused_conv2d_bias_activation, conv_input=conv_input,
                                            filter=filter, bias=bias,
                                            side_input=side_input,
                                            conv_input_scale=conv_input_scale,
                                            side_input_scale=side_input_scale,
                                            strides=strides, padding=padding,
                                            data_format=data_format,
                                            filter_format=filter_format,
                                            activation_mode=activation_mode,
                                            dilations=dilations, name=name)
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
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_conv2d_bias_activation' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if filter_format is None:
    filter_format = "HWIO"
  filter_format = _execute.make_str(filter_format, "filter_format")
  if activation_mode is None:
    activation_mode = "Relu"
  activation_mode = _execute.make_str(activation_mode, "activation_mode")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'fused_conv2d_bias_activation' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "FusedConv2DBiasActivation", conv_input=conv_input, filter=filter,
                                     bias=bias, side_input=side_input,
                                     conv_input_scale=conv_input_scale,
                                     side_input_scale=side_input_scale,
                                     strides=strides, padding=padding,
                                     data_format=data_format,
                                     filter_format=filter_format,
                                     activation_mode=activation_mode,
                                     dilations=dilations, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          fused_conv2d_bias_activation, conv_input=conv_input, filter=filter,
                                        bias=bias, side_input=side_input,
                                        conv_input_scale=conv_input_scale,
                                        side_input_scale=side_input_scale,
                                        strides=strides, padding=padding,
                                        data_format=data_format,
                                        filter_format=filter_format,
                                        activation_mode=activation_mode,
                                        dilations=dilations, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "Tbias", _op.get_attr("Tbias"), "strides",
            _op.get_attr("strides"), "padding", _op.get_attr("padding"),
            "data_format", _op.get_attr("data_format"), "filter_format",
            _op.get_attr("filter_format"), "activation_mode",
            _op.get_attr("activation_mode"), "dilations",
            _op.get_attr("dilations"))
  _execute.record_gradient(
      "FusedConv2DBiasActivation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fused_conv2d_bias_activation_eager_fallback(conv_input, filter, bias, side_input, conv_input_scale, side_input_scale, strides, padding, data_format="NHWC", filter_format="HWIO", activation_mode="Relu", dilations=[1, 1, 1, 1], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fused_conv2d_bias_activation
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(strides, (list, tuple)):
    raise TypeError(
        "Expected list for 'strides' argument to "
        "'fused_conv2d_bias_activation' Op, not %r." % strides)
  strides = [_execute.make_int(_i, "strides") for _i in strides]
  padding = _execute.make_str(padding, "padding")
  if data_format is None:
    data_format = "NHWC"
  data_format = _execute.make_str(data_format, "data_format")
  if filter_format is None:
    filter_format = "HWIO"
  filter_format = _execute.make_str(filter_format, "filter_format")
  if activation_mode is None:
    activation_mode = "Relu"
  activation_mode = _execute.make_str(activation_mode, "activation_mode")
  if dilations is None:
    dilations = [1, 1, 1, 1]
  if not isinstance(dilations, (list, tuple)):
    raise TypeError(
        "Expected list for 'dilations' argument to "
        "'fused_conv2d_bias_activation' Op, not %r." % dilations)
  dilations = [_execute.make_int(_i, "dilations") for _i in dilations]
  _attr_T, _inputs_T = _execute.args_to_matching_eager([conv_input, filter, side_input], _ctx)
  (conv_input, filter, side_input) = _inputs_T
  _attr_Tbias, (bias,) = _execute.args_to_matching_eager([bias], _ctx)
  conv_input_scale = _ops.convert_to_tensor(conv_input_scale, _dtypes.float32)
  side_input_scale = _ops.convert_to_tensor(side_input_scale, _dtypes.float32)
  _inputs_flat = [conv_input, filter, bias, side_input, conv_input_scale, side_input_scale]
  _attrs = ("T", _attr_T, "Tbias", _attr_Tbias, "strides", strides, "padding",
  padding, "data_format", data_format, "filter_format", filter_format,
  "activation_mode", activation_mode, "dilations", dilations)
  _result = _execute.execute(b"FusedConv2DBiasActivation", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FusedConv2DBiasActivation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("FusedConv2DBiasActivation")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "FusedConv2DBiasActivation"
#   input_arg {
#     name: "conv_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "filter"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "bias"
#     type_attr: "Tbias"
#   }
#   input_arg {
#     name: "side_input"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "conv_input_scale"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "side_input_scale"
#     type: DT_FLOAT
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
#         type: DT_HALF
#         type: DT_QINT8
#       }
#     }
#   }
#   attr {
#     name: "Tbias"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_HALF
#       }
#     }
#   }
#   attr {
#     name: "strides"
#     type: "list(int)"
#   }
#   attr {
#     name: "padding"
#     type: "string"
#     allowed_values {
#       list {
#         s: "SAME"
#         s: "VALID"
#       }
#     }
#   }
#   attr {
#     name: "data_format"
#     type: "string"
#     default_value {
#       s: "NHWC"
#     }
#     allowed_values {
#       list {
#         s: "NHWC"
#         s: "NCHW"
#         s: "NCHW_VECT_C"
#       }
#     }
#   }
#   attr {
#     name: "filter_format"
#     type: "string"
#     default_value {
#       s: "HWIO"
#     }
#     allowed_values {
#       list {
#         s: "HWIO"
#         s: "OIHW"
#         s: "OIHW_VECT_I"
#       }
#     }
#   }
#   attr {
#     name: "activation_mode"
#     type: "string"
#     default_value {
#       s: "Relu"
#     }
#     allowed_values {
#       list {
#         s: "Relu"
#         s: "None"
#       }
#     }
#   }
#   attr {
#     name: "dilations"
#     type: "list(int)"
#     default_value {
#       list {
#         i: 1
#         i: 1
#         i: 1
#         i: 1
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\306\003\n\031FusedConv2DBiasActivation\022\017\n\nconv_input\"\001T\022\013\n\006filter\"\001T\022\r\n\004bias\"\005Tbias\022\017\n\nside_input\"\001T\022\024\n\020conv_input_scale\030\001\022\024\n\020side_input_scale\030\001\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\001\023\013\"\025\n\005Tbias\022\004type:\006\n\0042\002\001\023\"\024\n\007strides\022\tlist(int)\"\"\n\007padding\022\006string:\017\n\r\022\004SAME\022\005VALID\":\n\013data_format\022\006string\032\006\022\004NHWC:\033\n\031\022\004NHWC\022\004NCHW\022\013NCHW_VECT_C\"<\n\rfilter_format\022\006string\032\006\022\004HWIO:\033\n\031\022\004HWIO\022\004OIHW\022\013OIHW_VECT_I\"1\n\017activation_mode\022\006string\032\006\022\004Relu:\016\n\014\022\004Relu\022\004None\" \n\tdilations\022\tlist(int)\032\010\n\006\032\004\001\001\001\001")
