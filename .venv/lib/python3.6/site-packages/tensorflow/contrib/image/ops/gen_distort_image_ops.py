"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: distort_image_ops.cc
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
@tf_export('adjust_hsv_in_yiq')
def adjust_hsv_in_yiq(images, delta_h, scale_s, scale_v, name=None):
  r"""Adjust the YIQ hue of one or more images.

  `images` is a tensor of at least 3 dimensions.  The last dimension is
  interpreted as channels, and must be three.

  We used linear transformation described in:
   beesbuzz.biz/code/hsv_color_transforms.php
  The input image is considered in the RGB colorspace. Conceptually, the RGB
  colors are first mapped into YIQ space, rotated around the Y channel by
  delta_h in radians, multiplying the chrominance channels (I, Q)  by scale_s,
  multiplying all channels (Y, I, Q)  by scale_v, and then remapped back to RGB
  colorspace. Each operation described above is a linear transformation.

  Args:
    images: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int16`, `int32`, `int64`, `half`, `float32`, `float64`.
      Images to adjust.  At least 3-D.
    delta_h: A `Tensor` of type `float32`.
      A float scale that represents the hue rotation amount, in radians.
      Although delta_h can be any float value.
    scale_s: A `Tensor` of type `float32`.
      A float scale that represents the factor to multiply the saturation by.
      scale_s needs to be non-negative.
    scale_v: A `Tensor` of type `float32`.
      A float scale that represents the factor to multiply the value by.
      scale_v needs to be non-negative.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
    The hsv-adjusted image or images. No clipping will be done in this op.
    The client can clip them using additional ops in their graph.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AdjustHsvInYiq", name, _ctx._post_execution_callbacks, images,
        delta_h, scale_s, scale_v)
      return _result
    except _core._FallbackException:
      try:
        return adjust_hsv_in_yiq_eager_fallback(
            images, delta_h, scale_s, scale_v, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              adjust_hsv_in_yiq, images=images, delta_h=delta_h,
                                 scale_s=scale_s, scale_v=scale_v, name=name)
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
        "AdjustHsvInYiq", images=images, delta_h=delta_h, scale_s=scale_s,
                          scale_v=scale_v, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          adjust_hsv_in_yiq, images=images, delta_h=delta_h, scale_s=scale_s,
                             scale_v=scale_v, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "AdjustHsvInYiq", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def adjust_hsv_in_yiq_eager_fallback(images, delta_h, scale_s, scale_v, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function adjust_hsv_in_yiq
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  delta_h = _ops.convert_to_tensor(delta_h, _dtypes.float32)
  scale_s = _ops.convert_to_tensor(scale_s, _dtypes.float32)
  scale_v = _ops.convert_to_tensor(scale_v, _dtypes.float32)
  _inputs_flat = [images, delta_h, scale_s, scale_v]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"AdjustHsvInYiq", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AdjustHsvInYiq", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("AdjustHsvInYiq")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AdjustHsvInYiq"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "delta_h"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "scale_s"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "scale_v"
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
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_INT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\nj\n\016AdjustHsvInYiq\022\013\n\006images\"\001T\022\013\n\007delta_h\030\001\022\013\n\007scale_s\030\001\022\013\n\007scale_v\030\001\032\013\n\006output\"\001T\"\027\n\001T\022\004type:\014\n\n2\010\004\006\005\003\t\023\001\002")
