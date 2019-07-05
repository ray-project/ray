"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: single_image_random_dot_stereograms_ops.cc
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
@tf_export('single_image_random_dot_stereograms')
def single_image_random_dot_stereograms(depth_values, hidden_surface_removal=True, convergence_dots_size=8, dots_per_inch=72, eye_separation=2.5, mu=0.3333, normalize=True, normalize_max=-100, normalize_min=100, border_level=0, number_colors=256, output_image_shape=[1024, 768, 1], output_data_window=[1022, 757], name=None):
  r"""Outputs a single image random dot stereogram for export via encode_PNG/JPG OP.

  Given the 2-D tensor 'depth_values' with encoded Z values, this operation will
  encode 3-D data into a 2-D image.  The output of this Op is suitable for the
  encode_PNG/JPG ops.  Be careful with image compression as this may corrupt the
  encode 3-D data within the image.

  This Op is based upon:
  'http://www.learningace.com/doc/4331582/b6ab058d1e206d68ab60e4e1ead2fe6e/sirds-paper'

  Example use which outputs a SIRDS image as picture_out.png:
  ```python
  img=[[1,2,3,3,2,1],
       [1,2,3,4,5,2],
       [1,2,3,4,5,3],
       [1,2,3,4,5,4],
       [6,5,4,4,5,5]]

  session = tf.InteractiveSession()

  sirds = single_image_random_dot_stereograms(img,convergence_dots_size=8,number_colors=256,normalize=True)

  out = sirds.eval()

  png = tf.image.encode_png(out).eval()

  with open('picture_out.png', 'wb') as f:
      f.write(png)
  ```

  Args:
    depth_values: A `Tensor`. Must be one of the following types: `float64`, `float32`, `int64`, `int32`.
      Z values of data to encode into 'output_data_window' window,
      lower values are further away {0.0 floor(far), 1.0 ceiling(near) after normalization}, must be 2-D tensor
    hidden_surface_removal: An optional `bool`. Defaults to `True`.
      Activate hidden surface removal
    convergence_dots_size: An optional `int`. Defaults to `8`.
      Black dot size in pixels to help view converge image, drawn on bottom of image
    dots_per_inch: An optional `int`. Defaults to `72`.
      Output device in dots/inch
    eye_separation: An optional `float`. Defaults to `2.5`.
      Separation between eyes in inches
    mu: An optional `float`. Defaults to `0.3333`.
      Depth of field, Fraction of viewing distance (eg. 1/3 = .3333)
    normalize: An optional `bool`. Defaults to `True`.
      Normalize input data to [0.0, 1.0]
    normalize_max: An optional `float`. Defaults to `-100`.
      Fix MAX value for Normalization - if < MIN, autoscale
    normalize_min: An optional `float`. Defaults to `100`.
      Fix MIN value for Normalization - if > MAX, autoscale
    border_level: An optional `float`. Defaults to `0`.
      Value of border depth 0.0 {far} to 1.0 {near}
    number_colors: An optional `int`. Defaults to `256`.
      2 (Black & White),256 (grayscale), and Numbers > 256 (Full Color) are all that are supported currently
    output_image_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[1024, 768, 1]`.
      Output size of returned image in X,Y, Channels 1-grayscale, 3 color (1024, 768, 1),
      channels will be updated to 3 if 'number_colors' > 256
    output_data_window: An optional `tf.TensorShape` or list of `ints`. Defaults to `[1022, 757]`.
      Size of "DATA" window, must be equal to or smaller than 'output_image_shape', will be centered
      and use 'convergence_dots_size' for best fit to avoid overlap if possible
    name: A name for the operation (optional).

  Returns:
    A tensor of size 'output_image_shape' with the encoded 'depth_values'
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SingleImageRandomDotStereograms", name,
        _ctx._post_execution_callbacks, depth_values,
        "hidden_surface_removal", hidden_surface_removal,
        "convergence_dots_size", convergence_dots_size, "dots_per_inch",
        dots_per_inch, "eye_separation", eye_separation, "mu", mu,
        "normalize", normalize, "normalize_max", normalize_max,
        "normalize_min", normalize_min, "border_level", border_level,
        "number_colors", number_colors, "output_image_shape",
        output_image_shape, "output_data_window", output_data_window)
      return _result
    except _core._FallbackException:
      try:
        return single_image_random_dot_stereograms_eager_fallback(
            depth_values, hidden_surface_removal=hidden_surface_removal,
            convergence_dots_size=convergence_dots_size,
            dots_per_inch=dots_per_inch, eye_separation=eye_separation, mu=mu,
            normalize=normalize, normalize_max=normalize_max,
            normalize_min=normalize_min, border_level=border_level,
            number_colors=number_colors,
            output_image_shape=output_image_shape,
            output_data_window=output_data_window, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              single_image_random_dot_stereograms, depth_values=depth_values,
                                                   hidden_surface_removal=hidden_surface_removal,
                                                   convergence_dots_size=convergence_dots_size,
                                                   dots_per_inch=dots_per_inch,
                                                   eye_separation=eye_separation,
                                                   mu=mu, normalize=normalize,
                                                   normalize_max=normalize_max,
                                                   normalize_min=normalize_min,
                                                   border_level=border_level,
                                                   number_colors=number_colors,
                                                   output_image_shape=output_image_shape,
                                                   output_data_window=output_data_window,
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
  if hidden_surface_removal is None:
    hidden_surface_removal = True
  hidden_surface_removal = _execute.make_bool(hidden_surface_removal, "hidden_surface_removal")
  if convergence_dots_size is None:
    convergence_dots_size = 8
  convergence_dots_size = _execute.make_int(convergence_dots_size, "convergence_dots_size")
  if dots_per_inch is None:
    dots_per_inch = 72
  dots_per_inch = _execute.make_int(dots_per_inch, "dots_per_inch")
  if eye_separation is None:
    eye_separation = 2.5
  eye_separation = _execute.make_float(eye_separation, "eye_separation")
  if mu is None:
    mu = 0.3333
  mu = _execute.make_float(mu, "mu")
  if normalize is None:
    normalize = True
  normalize = _execute.make_bool(normalize, "normalize")
  if normalize_max is None:
    normalize_max = -100
  normalize_max = _execute.make_float(normalize_max, "normalize_max")
  if normalize_min is None:
    normalize_min = 100
  normalize_min = _execute.make_float(normalize_min, "normalize_min")
  if border_level is None:
    border_level = 0
  border_level = _execute.make_float(border_level, "border_level")
  if number_colors is None:
    number_colors = 256
  number_colors = _execute.make_int(number_colors, "number_colors")
  if output_image_shape is None:
    output_image_shape = [1024, 768, 1]
  output_image_shape = _execute.make_shape(output_image_shape, "output_image_shape")
  if output_data_window is None:
    output_data_window = [1022, 757]
  output_data_window = _execute.make_shape(output_data_window, "output_data_window")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "SingleImageRandomDotStereograms", depth_values=depth_values,
                                           hidden_surface_removal=hidden_surface_removal,
                                           convergence_dots_size=convergence_dots_size,
                                           dots_per_inch=dots_per_inch,
                                           eye_separation=eye_separation,
                                           mu=mu, normalize=normalize,
                                           normalize_max=normalize_max,
                                           normalize_min=normalize_min,
                                           border_level=border_level,
                                           number_colors=number_colors,
                                           output_image_shape=output_image_shape,
                                           output_data_window=output_data_window,
                                           name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          single_image_random_dot_stereograms, depth_values=depth_values,
                                               hidden_surface_removal=hidden_surface_removal,
                                               convergence_dots_size=convergence_dots_size,
                                               dots_per_inch=dots_per_inch,
                                               eye_separation=eye_separation,
                                               mu=mu, normalize=normalize,
                                               normalize_max=normalize_max,
                                               normalize_min=normalize_min,
                                               border_level=border_level,
                                               number_colors=number_colors,
                                               output_image_shape=output_image_shape,
                                               output_data_window=output_data_window,
                                               name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "hidden_surface_removal",
            _op.get_attr("hidden_surface_removal"), "convergence_dots_size",
            _op.get_attr("convergence_dots_size"), "dots_per_inch",
            _op.get_attr("dots_per_inch"), "eye_separation",
            _op.get_attr("eye_separation"), "mu", _op.get_attr("mu"),
            "normalize", _op.get_attr("normalize"), "normalize_max",
            _op.get_attr("normalize_max"), "normalize_min",
            _op.get_attr("normalize_min"), "border_level",
            _op.get_attr("border_level"), "number_colors",
            _op.get_attr("number_colors"), "output_image_shape",
            _op.get_attr("output_image_shape"), "output_data_window",
            _op.get_attr("output_data_window"))
  _execute.record_gradient(
      "SingleImageRandomDotStereograms", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def single_image_random_dot_stereograms_eager_fallback(depth_values, hidden_surface_removal=True, convergence_dots_size=8, dots_per_inch=72, eye_separation=2.5, mu=0.3333, normalize=True, normalize_max=-100, normalize_min=100, border_level=0, number_colors=256, output_image_shape=[1024, 768, 1], output_data_window=[1022, 757], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function single_image_random_dot_stereograms
  """
  _ctx = ctx if ctx else _context.context()
  if hidden_surface_removal is None:
    hidden_surface_removal = True
  hidden_surface_removal = _execute.make_bool(hidden_surface_removal, "hidden_surface_removal")
  if convergence_dots_size is None:
    convergence_dots_size = 8
  convergence_dots_size = _execute.make_int(convergence_dots_size, "convergence_dots_size")
  if dots_per_inch is None:
    dots_per_inch = 72
  dots_per_inch = _execute.make_int(dots_per_inch, "dots_per_inch")
  if eye_separation is None:
    eye_separation = 2.5
  eye_separation = _execute.make_float(eye_separation, "eye_separation")
  if mu is None:
    mu = 0.3333
  mu = _execute.make_float(mu, "mu")
  if normalize is None:
    normalize = True
  normalize = _execute.make_bool(normalize, "normalize")
  if normalize_max is None:
    normalize_max = -100
  normalize_max = _execute.make_float(normalize_max, "normalize_max")
  if normalize_min is None:
    normalize_min = 100
  normalize_min = _execute.make_float(normalize_min, "normalize_min")
  if border_level is None:
    border_level = 0
  border_level = _execute.make_float(border_level, "border_level")
  if number_colors is None:
    number_colors = 256
  number_colors = _execute.make_int(number_colors, "number_colors")
  if output_image_shape is None:
    output_image_shape = [1024, 768, 1]
  output_image_shape = _execute.make_shape(output_image_shape, "output_image_shape")
  if output_data_window is None:
    output_data_window = [1022, 757]
  output_data_window = _execute.make_shape(output_data_window, "output_data_window")
  _attr_T, (depth_values,) = _execute.args_to_matching_eager([depth_values], _ctx)
  _inputs_flat = [depth_values]
  _attrs = ("T", _attr_T, "hidden_surface_removal", hidden_surface_removal,
  "convergence_dots_size", convergence_dots_size, "dots_per_inch",
  dots_per_inch, "eye_separation", eye_separation, "mu", mu, "normalize",
  normalize, "normalize_max", normalize_max, "normalize_min", normalize_min,
  "border_level", border_level, "number_colors", number_colors,
  "output_image_shape", output_image_shape, "output_data_window",
  output_data_window)
  _result = _execute.execute(b"SingleImageRandomDotStereograms", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SingleImageRandomDotStereograms", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("SingleImageRandomDotStereograms")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "SingleImageRandomDotStereograms"
#   input_arg {
#     name: "depth_values"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "image"
#     type: DT_UINT8
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_DOUBLE
#         type: DT_FLOAT
#         type: DT_INT64
#         type: DT_INT32
#       }
#     }
#   }
#   attr {
#     name: "hidden_surface_removal"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "convergence_dots_size"
#     type: "int"
#     default_value {
#       i: 8
#     }
#   }
#   attr {
#     name: "dots_per_inch"
#     type: "int"
#     default_value {
#       i: 72
#     }
#   }
#   attr {
#     name: "eye_separation"
#     type: "float"
#     default_value {
#       f: 2.5
#     }
#   }
#   attr {
#     name: "mu"
#     type: "float"
#     default_value {
#       f: 0.3333
#     }
#   }
#   attr {
#     name: "normalize"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "normalize_max"
#     type: "float"
#     default_value {
#       f: -100
#     }
#   }
#   attr {
#     name: "normalize_min"
#     type: "float"
#     default_value {
#       f: 100
#     }
#   }
#   attr {
#     name: "border_level"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
#   attr {
#     name: "number_colors"
#     type: "int"
#     default_value {
#       i: 256
#     }
#   }
#   attr {
#     name: "output_image_shape"
#     type: "shape"
#     default_value {
#       shape {
#         dim {
#           size: 1024
#         }
#         dim {
#           size: 768
#         }
#         dim {
#           size: 1
#         }
#       }
#     }
#   }
#   attr {
#     name: "output_data_window"
#     type: "shape"
#     default_value {
#       shape {
#         dim {
#           size: 1022
#         }
#         dim {
#           size: 757
#         }
#       }
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\320\003\n\037SingleImageRandomDotStereograms\022\021\n\014depth_values\"\001T\032\t\n\005image\030\004\"\023\n\001T\022\004type:\010\n\0062\004\002\001\t\003\"\"\n\026hidden_surface_removal\022\004bool\032\002(\001\" \n\025convergence_dots_size\022\003int\032\002\030\010\"\030\n\rdots_per_inch\022\003int\032\002\030H\"\036\n\016eye_separation\022\005float\032\005%\000\000 @\"\022\n\002mu\022\005float\032\005%L\246\252>\"\025\n\tnormalize\022\004bool\032\002(\001\"\035\n\rnormalize_max\022\005float\032\005%\000\000\310\302\"\035\n\rnormalize_min\022\005float\032\005%\000\000\310B\"\034\n\014border_level\022\005float\032\005%\000\000\000\000\"\031\n\rnumber_colors\022\003int\032\003\030\200\002\"-\n\022output_image_shape\022\005shape\032\020:\016\022\003\010\200\010\022\003\010\200\006\022\002\010\001\")\n\022output_data_window\022\005shape\032\014:\n\022\003\010\376\007\022\003\010\365\005")
