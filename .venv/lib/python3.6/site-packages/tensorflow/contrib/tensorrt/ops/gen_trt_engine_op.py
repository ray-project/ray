"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: trt_engine_op.cc
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
@tf_export('trt_engine_op')
def trt_engine_op(in_tensor, serialized_segment, input_shapes, output_shapes, segment_funcdef_name, OutT, workspace_size_bytes, precision_mode, static_engine=True, fixed_input_size=True, cached_engine_batches=[], max_cached_engines_count=1, calibration_data="", use_calibration=True, name=None):
  r"""TODO: add doc.

  Args:
    in_tensor: A list of `Tensor` objects with types from: `int8`, `half`, `float32`.
    serialized_segment: A `string`.
    input_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
    segment_funcdef_name: A `string`.
    OutT: A list of `tf.DTypes` from: `tf.int8, tf.half, tf.float32` that has length `>= 1`.
    workspace_size_bytes: An `int`.
    precision_mode: A `string` from: `"FP32", "FP16", "INT8"`.
    static_engine: An optional `bool`. Defaults to `True`.
    fixed_input_size: An optional `bool`. Defaults to `True`.
    cached_engine_batches: An optional list of `ints`. Defaults to `[]`.
    max_cached_engines_count: An optional `int`. Defaults to `1`.
    calibration_data: An optional `string`. Defaults to `""`.
    use_calibration: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `OutT`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "TRTEngineOp",
        name, _ctx._post_execution_callbacks, in_tensor, "serialized_segment",
        serialized_segment, "input_shapes", input_shapes, "output_shapes",
        output_shapes, "segment_funcdef_name", segment_funcdef_name, "OutT",
        OutT, "static_engine", static_engine, "fixed_input_size",
        fixed_input_size, "cached_engine_batches", cached_engine_batches,
        "max_cached_engines_count", max_cached_engines_count,
        "workspace_size_bytes", workspace_size_bytes, "precision_mode",
        precision_mode, "calibration_data", calibration_data,
        "use_calibration", use_calibration)
      return _result
    except _core._FallbackException:
      try:
        return trt_engine_op_eager_fallback(
            in_tensor, serialized_segment=serialized_segment,
            input_shapes=input_shapes, output_shapes=output_shapes,
            segment_funcdef_name=segment_funcdef_name, OutT=OutT,
            static_engine=static_engine, fixed_input_size=fixed_input_size,
            cached_engine_batches=cached_engine_batches,
            max_cached_engines_count=max_cached_engines_count,
            workspace_size_bytes=workspace_size_bytes,
            precision_mode=precision_mode, calibration_data=calibration_data,
            use_calibration=use_calibration, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              trt_engine_op, in_tensor=in_tensor,
                             serialized_segment=serialized_segment,
                             input_shapes=input_shapes,
                             output_shapes=output_shapes,
                             segment_funcdef_name=segment_funcdef_name,
                             OutT=OutT,
                             workspace_size_bytes=workspace_size_bytes,
                             precision_mode=precision_mode,
                             static_engine=static_engine,
                             fixed_input_size=fixed_input_size,
                             cached_engine_batches=cached_engine_batches,
                             max_cached_engines_count=max_cached_engines_count,
                             calibration_data=calibration_data,
                             use_calibration=use_calibration, name=name)
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
  serialized_segment = _execute.make_str(serialized_segment, "serialized_segment")
  if not isinstance(input_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_shapes' argument to "
        "'trt_engine_op' Op, not %r." % input_shapes)
  input_shapes = [_execute.make_shape(_s, "input_shapes") for _s in input_shapes]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'trt_engine_op' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  segment_funcdef_name = _execute.make_str(segment_funcdef_name, "segment_funcdef_name")
  if not isinstance(OutT, (list, tuple)):
    raise TypeError(
        "Expected list for 'OutT' argument to "
        "'trt_engine_op' Op, not %r." % OutT)
  OutT = [_execute.make_type(_t, "OutT") for _t in OutT]
  workspace_size_bytes = _execute.make_int(workspace_size_bytes, "workspace_size_bytes")
  precision_mode = _execute.make_str(precision_mode, "precision_mode")
  if static_engine is None:
    static_engine = True
  static_engine = _execute.make_bool(static_engine, "static_engine")
  if fixed_input_size is None:
    fixed_input_size = True
  fixed_input_size = _execute.make_bool(fixed_input_size, "fixed_input_size")
  if cached_engine_batches is None:
    cached_engine_batches = []
  if not isinstance(cached_engine_batches, (list, tuple)):
    raise TypeError(
        "Expected list for 'cached_engine_batches' argument to "
        "'trt_engine_op' Op, not %r." % cached_engine_batches)
  cached_engine_batches = [_execute.make_int(_i, "cached_engine_batches") for _i in cached_engine_batches]
  if max_cached_engines_count is None:
    max_cached_engines_count = 1
  max_cached_engines_count = _execute.make_int(max_cached_engines_count, "max_cached_engines_count")
  if calibration_data is None:
    calibration_data = ""
  calibration_data = _execute.make_str(calibration_data, "calibration_data")
  if use_calibration is None:
    use_calibration = True
  use_calibration = _execute.make_bool(use_calibration, "use_calibration")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "TRTEngineOp", in_tensor=in_tensor,
                       serialized_segment=serialized_segment,
                       input_shapes=input_shapes, output_shapes=output_shapes,
                       segment_funcdef_name=segment_funcdef_name, OutT=OutT,
                       workspace_size_bytes=workspace_size_bytes,
                       precision_mode=precision_mode,
                       static_engine=static_engine,
                       fixed_input_size=fixed_input_size,
                       cached_engine_batches=cached_engine_batches,
                       max_cached_engines_count=max_cached_engines_count,
                       calibration_data=calibration_data,
                       use_calibration=use_calibration, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          trt_engine_op, in_tensor=in_tensor,
                         serialized_segment=serialized_segment,
                         input_shapes=input_shapes,
                         output_shapes=output_shapes,
                         segment_funcdef_name=segment_funcdef_name, OutT=OutT,
                         workspace_size_bytes=workspace_size_bytes,
                         precision_mode=precision_mode,
                         static_engine=static_engine,
                         fixed_input_size=fixed_input_size,
                         cached_engine_batches=cached_engine_batches,
                         max_cached_engines_count=max_cached_engines_count,
                         calibration_data=calibration_data,
                         use_calibration=use_calibration, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("serialized_segment", _op.get_attr("serialized_segment"),
            "input_shapes", _op.get_attr("input_shapes"), "output_shapes",
            _op.get_attr("output_shapes"), "segment_funcdef_name",
            _op.get_attr("segment_funcdef_name"), "InT", _op.get_attr("InT"),
            "OutT", _op.get_attr("OutT"), "static_engine",
            _op.get_attr("static_engine"), "fixed_input_size",
            _op.get_attr("fixed_input_size"), "cached_engine_batches",
            _op.get_attr("cached_engine_batches"), "max_cached_engines_count",
            _op.get_attr("max_cached_engines_count"), "workspace_size_bytes",
            _op.get_attr("workspace_size_bytes"), "precision_mode",
            _op.get_attr("precision_mode"), "calibration_data",
            _op.get_attr("calibration_data"), "use_calibration",
            _op.get_attr("use_calibration"))
  _execute.record_gradient(
      "TRTEngineOp", _inputs_flat, _attrs, _result, name)
  return _result



def trt_engine_op_eager_fallback(in_tensor, serialized_segment, input_shapes, output_shapes, segment_funcdef_name, OutT, workspace_size_bytes, precision_mode, static_engine=True, fixed_input_size=True, cached_engine_batches=[], max_cached_engines_count=1, calibration_data="", use_calibration=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function trt_engine_op
  """
  _ctx = ctx if ctx else _context.context()
  serialized_segment = _execute.make_str(serialized_segment, "serialized_segment")
  if not isinstance(input_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_shapes' argument to "
        "'trt_engine_op' Op, not %r." % input_shapes)
  input_shapes = [_execute.make_shape(_s, "input_shapes") for _s in input_shapes]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'trt_engine_op' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  segment_funcdef_name = _execute.make_str(segment_funcdef_name, "segment_funcdef_name")
  if not isinstance(OutT, (list, tuple)):
    raise TypeError(
        "Expected list for 'OutT' argument to "
        "'trt_engine_op' Op, not %r." % OutT)
  OutT = [_execute.make_type(_t, "OutT") for _t in OutT]
  workspace_size_bytes = _execute.make_int(workspace_size_bytes, "workspace_size_bytes")
  precision_mode = _execute.make_str(precision_mode, "precision_mode")
  if static_engine is None:
    static_engine = True
  static_engine = _execute.make_bool(static_engine, "static_engine")
  if fixed_input_size is None:
    fixed_input_size = True
  fixed_input_size = _execute.make_bool(fixed_input_size, "fixed_input_size")
  if cached_engine_batches is None:
    cached_engine_batches = []
  if not isinstance(cached_engine_batches, (list, tuple)):
    raise TypeError(
        "Expected list for 'cached_engine_batches' argument to "
        "'trt_engine_op' Op, not %r." % cached_engine_batches)
  cached_engine_batches = [_execute.make_int(_i, "cached_engine_batches") for _i in cached_engine_batches]
  if max_cached_engines_count is None:
    max_cached_engines_count = 1
  max_cached_engines_count = _execute.make_int(max_cached_engines_count, "max_cached_engines_count")
  if calibration_data is None:
    calibration_data = ""
  calibration_data = _execute.make_str(calibration_data, "calibration_data")
  if use_calibration is None:
    use_calibration = True
  use_calibration = _execute.make_bool(use_calibration, "use_calibration")
  _attr_InT, in_tensor = _execute.convert_to_mixed_eager_tensors(in_tensor, _ctx)
  _inputs_flat = list(in_tensor)
  _attrs = ("serialized_segment", serialized_segment, "input_shapes",
  input_shapes, "output_shapes", output_shapes, "segment_funcdef_name",
  segment_funcdef_name, "InT", _attr_InT, "OutT", OutT, "static_engine",
  static_engine, "fixed_input_size", fixed_input_size,
  "cached_engine_batches", cached_engine_batches, "max_cached_engines_count",
  max_cached_engines_count, "workspace_size_bytes", workspace_size_bytes,
  "precision_mode", precision_mode, "calibration_data", calibration_data,
  "use_calibration", use_calibration)
  _result = _execute.execute(b"TRTEngineOp", len(OutT), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TRTEngineOp", _inputs_flat, _attrs, _result, name)
  return _result

_ops.RegisterShape("TRTEngineOp")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "TRTEngineOp"
#   input_arg {
#     name: "in_tensor"
#     type_list_attr: "InT"
#   }
#   output_arg {
#     name: "out_tensor"
#     type_list_attr: "OutT"
#   }
#   attr {
#     name: "serialized_segment"
#     type: "string"
#   }
#   attr {
#     name: "input_shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#   }
#   attr {
#     name: "segment_funcdef_name"
#     type: "string"
#   }
#   attr {
#     name: "InT"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "OutT"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "static_engine"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "fixed_input_size"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "cached_engine_batches"
#     type: "list(int)"
#     default_value {
#       list {
#       }
#     }
#   }
#   attr {
#     name: "max_cached_engines_count"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "workspace_size_bytes"
#     type: "int"
#   }
#   attr {
#     name: "precision_mode"
#     type: "string"
#     allowed_values {
#       list {
#         s: "FP32"
#         s: "FP16"
#         s: "INT8"
#       }
#     }
#   }
#   attr {
#     name: "calibration_data"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "use_calibration"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\375\003\n\013TRTEngineOp\022\020\n\tin_tensor2\003InT\032\022\n\nout_tensor2\004OutT\"\034\n\022serialized_segment\022\006string\"\033\n\014input_shapes\022\013list(shape)\"\034\n\routput_shapes\022\013list(shape)\"\036\n\024segment_funcdef_name\022\006string\"\036\n\003InT\022\nlist(type)(\0010\001:\007\n\0052\003\006\023\001\"\037\n\004OutT\022\nlist(type)(\0010\001:\007\n\0052\003\006\023\001\"\031\n\rstatic_engine\022\004bool\032\002(\001\"\034\n\020fixed_input_size\022\004bool\032\002(\001\"&\n\025cached_engine_batches\022\tlist(int)\032\002\n\000\"#\n\030max_cached_engines_count\022\003int\032\002\030\001\"\033\n\024workspace_size_bytes\022\003int\".\n\016precision_mode\022\006string:\024\n\022\022\004FP32\022\004FP16\022\004INT8\"\036\n\020calibration_data\022\006string\032\002\022\000\"\033\n\017use_calibration\022\004bool\032\002(\001")
