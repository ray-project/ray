"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: image_ops.cc
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


_bipartite_match_outputs = ["row_to_col_match_indices",
                           "col_to_row_match_indices"]
_BipartiteMatchOutput = _collections.namedtuple(
    "BipartiteMatch", _bipartite_match_outputs)


@_dispatch.add_dispatch_list
@tf_export('bipartite_match')
def bipartite_match(distance_mat, num_valid_rows, top_k=-1, name=None):
  r"""Find bipartite matching based on a given distance matrix.

  A greedy bi-partite matching algorithm is used to obtain the matching with the
  (greedy) minimum distance.

  Args:
    distance_mat: A `Tensor` of type `float32`.
      A 2-D float tensor of shape `[num_rows, num_columns]`. It is a
      pair-wise distance matrix between the entities represented by each row and
      each column. It is an asymmetric matrix. The smaller the distance is, the more
      similar the pairs are. The bipartite matching is to minimize the distances.
    num_valid_rows: A `Tensor` of type `float32`.
      A scalar or a 1-D tensor with one element describing the
      number of valid rows of distance_mat to consider for the bipartite matching.
      If set to be negative, then all rows from `distance_mat` are used.
    top_k: An optional `int`. Defaults to `-1`.
      A scalar that specifies the number of top-k matches to retrieve.
      If set to be negative, then is set according to the maximum number of
      matches from `distance_mat`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (row_to_col_match_indices, col_to_row_match_indices).

    row_to_col_match_indices: A `Tensor` of type `int32`. A vector of length num_rows, which is the number of
      rows of the input `distance_matrix`.
      If `row_to_col_match_indices[i]` is not -1, row i is matched to column
      `row_to_col_match_indices[i]`.
    col_to_row_match_indices: A `Tensor` of type `int32`. A vector of length num_columns, which is the number
      of columns of the input distance matrix.
      If `col_to_row_match_indices[j]` is not -1, column j is matched to row
      `col_to_row_match_indices[j]`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "BipartiteMatch", name, _ctx._post_execution_callbacks, distance_mat,
        num_valid_rows, "top_k", top_k)
      _result = _BipartiteMatchOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return bipartite_match_eager_fallback(
            distance_mat, num_valid_rows, top_k=top_k, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              bipartite_match, distance_mat=distance_mat,
                               num_valid_rows=num_valid_rows, top_k=top_k,
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
  if top_k is None:
    top_k = -1
  top_k = _execute.make_int(top_k, "top_k")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BipartiteMatch", distance_mat=distance_mat,
                          num_valid_rows=num_valid_rows, top_k=top_k,
                          name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          bipartite_match, distance_mat=distance_mat,
                           num_valid_rows=num_valid_rows, top_k=top_k,
                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("top_k", _op.get_attr("top_k"))
  _execute.record_gradient(
      "BipartiteMatch", _inputs_flat, _attrs, _result, name)
  _result = _BipartiteMatchOutput._make(_result)
  return _result



def bipartite_match_eager_fallback(distance_mat, num_valid_rows, top_k=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function bipartite_match
  """
  _ctx = ctx if ctx else _context.context()
  if top_k is None:
    top_k = -1
  top_k = _execute.make_int(top_k, "top_k")
  distance_mat = _ops.convert_to_tensor(distance_mat, _dtypes.float32)
  num_valid_rows = _ops.convert_to_tensor(num_valid_rows, _dtypes.float32)
  _inputs_flat = [distance_mat, num_valid_rows]
  _attrs = ("top_k", top_k)
  _result = _execute.execute(b"BipartiteMatch", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BipartiteMatch", _inputs_flat, _attrs, _result, name)
  _result = _BipartiteMatchOutput._make(_result)
  return _result

_ops.RegisterShape("BipartiteMatch")(None)


@_dispatch.add_dispatch_list
@tf_export('image_connected_components')
def image_connected_components(image, name=None):
  r"""Find the connected components of image(s).

  For each image (along the 0th axis), all connected components of adjacent pixels
  with the same non-zero value are detected and given unique ids.

  The returned `components` tensor has 0s for the zero pixels of `images`, and
  arbitrary nonzero ids for the connected components of nonzero values. Ids are
  unique across all of the images, and are in row-major order by the first pixel
  in the component.

  Uses union-find with union by rank but not path compression, giving a runtime of
  `O(n log n)`. See:
      https://en.wikipedia.org/wiki/Disjoint-set_data_structure#Time_Complexity

  Args:
    image: A `Tensor`. Must be one of the following types: `int64`, `int32`, `uint16`, `int16`, `uint8`, `int8`, `half`, `float32`, `float64`, `bool`, `string`.
      Image(s) with shape (N, H, W).
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
    Component ids for each pixel in "image". Same shape as "image". Zero
    pixels all have an output of 0, and all components of adjacent pixels with
    the same value are given consecutive ids, starting from 1.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ImageConnectedComponents", name, _ctx._post_execution_callbacks,
        image)
      return _result
    except _core._FallbackException:
      try:
        return image_connected_components_eager_fallback(
            image, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              image_connected_components, image=image, name=name)
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
        "ImageConnectedComponents", image=image, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          image_connected_components, image=image, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"))
  _execute.record_gradient(
      "ImageConnectedComponents", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def image_connected_components_eager_fallback(image, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function image_connected_components
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtype, (image,) = _execute.args_to_matching_eager([image], _ctx)
  _inputs_flat = [image]
  _attrs = ("dtype", _attr_dtype)
  _result = _execute.execute(b"ImageConnectedComponents", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ImageConnectedComponents", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ImageConnectedComponents")(None)


@_dispatch.add_dispatch_list
@tf_export('image_projective_transform')
def image_projective_transform(images, transforms, interpolation, name=None):
  r"""Applies the given transform to each of the images.

  Input `image` is a `Tensor` in NHWC format (where the axes are image in batch,
  rows, columns, and channels. Input `transforms` is a num_images x 8 or 1 x 8
  matrix, where each row corresponds to a 3 x 3 projective transformation matrix,
  with the last entry assumed to be 1. If there is one row, the same
  transformation will be applied to all images.

  If one row of `transforms` is `[a0, a1, a2, b0, b1, b2, c0, c1]`, then it maps
  the *output* point `(x, y)` to a transformed *input* point
  `(x', y') = ((a0 x + a1 y + a2) / k, (b0 x + b1 y + b2) / k)`, where
  `k = c0 x + c1 y + 1`. If the transformed point lays outside of the input
  image, the output pixel is set to 0.

  Args:
    images: A `Tensor`. Must be one of the following types: `uint8`, `int32`, `int64`, `half`, `float32`, `float64`.
      4D `Tensor`, input image(s) in NHWC format.
    transforms: A `Tensor` of type `float32`.
      2D `Tensor`, projective transform(s) to apply to the image(s).
    interpolation: A `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
    4D `Tensor`, image(s) in NHWC format, generated by applying
    the `transforms` to the `images`. Satisfies the description above.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ImageProjectiveTransform", name, _ctx._post_execution_callbacks,
        images, transforms, "interpolation", interpolation)
      return _result
    except _core._FallbackException:
      try:
        return image_projective_transform_eager_fallback(
            images, transforms, interpolation=interpolation, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              image_projective_transform, images=images,
                                          transforms=transforms,
                                          interpolation=interpolation,
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
  interpolation = _execute.make_str(interpolation, "interpolation")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ImageProjectiveTransform", images=images, transforms=transforms,
                                    interpolation=interpolation, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          image_projective_transform, images=images, transforms=transforms,
                                      interpolation=interpolation, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "interpolation",
            _op.get_attr("interpolation"))
  _execute.record_gradient(
      "ImageProjectiveTransform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def image_projective_transform_eager_fallback(images, transforms, interpolation, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function image_projective_transform
  """
  _ctx = ctx if ctx else _context.context()
  interpolation = _execute.make_str(interpolation, "interpolation")
  _attr_dtype, (images,) = _execute.args_to_matching_eager([images], _ctx)
  transforms = _ops.convert_to_tensor(transforms, _dtypes.float32)
  _inputs_flat = [images, transforms]
  _attrs = ("dtype", _attr_dtype, "interpolation", interpolation)
  _result = _execute.execute(b"ImageProjectiveTransform", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ImageProjectiveTransform", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ImageProjectiveTransform")(None)


@_dispatch.add_dispatch_list
@tf_export('image_projective_transform_v2')
def image_projective_transform_v2(images, transforms, output_shape, interpolation, name=None):
  r"""Applies the given transform to each of the images.

  Input `image` is a `Tensor` in NHWC format (where the axes are image in batch,
  rows, columns, and channels. Input `transforms` is a num_images x 8 or 1 x 8
  matrix, where each row corresponds to a 3 x 3 projective transformation matrix,
  with the last entry assumed to be 1. If there is one row, the same
  transformation will be applied to all images.

  If one row of `transforms` is `[a0, a1, a2, b0, b1, b2, c0, c1]`, then it maps
  the *output* point `(x, y)` to a transformed *input* point
  `(x', y') = ((a0 x + a1 y + a2) / k, (b0 x + b1 y + b2) / k)`, where
  `k = c0 x + c1 y + 1`. If the transformed point lays outside of the input
  image, the output pixel is set to 0.

  Args:
    images: A `Tensor`. Must be one of the following types: `uint8`, `int32`, `int64`, `half`, `float32`, `float64`.
      4D `Tensor`, input image(s) in NHWC format.
    transforms: A `Tensor` of type `float32`.
      2D `Tensor`, projective transform(s) to apply to the image(s).
    output_shape: A `Tensor` of type `int32`.
    interpolation: A `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
    4D `Tensor`, image(s) in NHWC format, generated by applying
    the `transforms` to the `images`. Satisfies the description above.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ImageProjectiveTransformV2", name, _ctx._post_execution_callbacks,
        images, transforms, output_shape, "interpolation", interpolation)
      return _result
    except _core._FallbackException:
      try:
        return image_projective_transform_v2_eager_fallback(
            images, transforms, output_shape, interpolation=interpolation,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              image_projective_transform_v2, images=images,
                                             transforms=transforms,
                                             output_shape=output_shape,
                                             interpolation=interpolation,
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
  interpolation = _execute.make_str(interpolation, "interpolation")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ImageProjectiveTransformV2", images=images, transforms=transforms,
                                      output_shape=output_shape,
                                      interpolation=interpolation, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          image_projective_transform_v2, images=images, transforms=transforms,
                                         output_shape=output_shape,
                                         interpolation=interpolation,
                                         name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dtype", _op.get_attr("dtype"), "interpolation",
            _op.get_attr("interpolation"))
  _execute.record_gradient(
      "ImageProjectiveTransformV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def image_projective_transform_v2_eager_fallback(images, transforms, output_shape, interpolation, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function image_projective_transform_v2
  """
  _ctx = ctx if ctx else _context.context()
  interpolation = _execute.make_str(interpolation, "interpolation")
  _attr_dtype, (images,) = _execute.args_to_matching_eager([images], _ctx)
  transforms = _ops.convert_to_tensor(transforms, _dtypes.float32)
  output_shape = _ops.convert_to_tensor(output_shape, _dtypes.int32)
  _inputs_flat = [images, transforms, output_shape]
  _attrs = ("dtype", _attr_dtype, "interpolation", interpolation)
  _result = _execute.execute(b"ImageProjectiveTransformV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ImageProjectiveTransformV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("ImageProjectiveTransformV2")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BipartiteMatch"
#   input_arg {
#     name: "distance_mat"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "num_valid_rows"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "row_to_col_match_indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "col_to_row_match_indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "top_k"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ImageConnectedComponents"
#   input_arg {
#     name: "image"
#     type_attr: "dtype"
#   }
#   output_arg {
#     name: "components"
#     type: DT_INT64
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT64
#         type: DT_INT32
#         type: DT_UINT16
#         type: DT_INT16
#         type: DT_UINT8
#         type: DT_INT8
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_BOOL
#         type: DT_STRING
#       }
#     }
#   }
# }
# op {
#   name: "ImageProjectiveTransform"
#   input_arg {
#     name: "images"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "transforms"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "transformed_images"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "interpolation"
#     type: "string"
#   }
# }
# op {
#   name: "ImageProjectiveTransformV2"
#   input_arg {
#     name: "images"
#     type_attr: "dtype"
#   }
#   input_arg {
#     name: "transforms"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "output_shape"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "transformed_images"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "interpolation"
#     type: "string"
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\220\001\n\016BipartiteMatch\022\020\n\014distance_mat\030\001\022\022\n\016num_valid_rows\030\001\032\034\n\030row_to_col_match_indices\030\003\032\034\n\030col_to_row_match_indices\030\003\"\031\n\005top_k\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\nZ\n\030ImageConnectedComponents\022\016\n\005image\"\005dtype\032\016\n\ncomponents\030\t\"\036\n\005dtype\022\004type:\017\n\r2\013\t\003\021\005\004\006\023\001\002\n\007\n\214\001\n\030ImageProjectiveTransform\022\017\n\006images\"\005dtype\022\016\n\ntransforms\030\001\032\033\n\022transformed_images\"\005dtype\"\031\n\005dtype\022\004type:\n\n\0102\006\004\003\t\023\001\002\"\027\n\rinterpolation\022\006string\n\240\001\n\032ImageProjectiveTransformV2\022\017\n\006images\"\005dtype\022\016\n\ntransforms\030\001\022\020\n\014output_shape\030\003\032\033\n\022transformed_images\"\005dtype\"\031\n\005dtype\022\004type:\n\n\0102\006\004\003\t\023\001\002\"\027\n\rinterpolation\022\006string")
