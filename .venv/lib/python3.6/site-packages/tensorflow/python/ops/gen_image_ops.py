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


def adjust_contrast(images, contrast_factor, min_value, max_value, name=None):
  r"""Deprecated. Disallowed in GraphDef version >= 2.

  Args:
    images: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`.
    contrast_factor: A `Tensor` of type `float32`.
    min_value: A `Tensor` of type `float32`.
    max_value: A `Tensor` of type `float32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AdjustContrast", name, _ctx._post_execution_callbacks, images,
        contrast_factor, min_value, max_value)
      return _result
    except _core._FallbackException:
      try:
        return adjust_contrast_eager_fallback(
            images, contrast_factor, min_value, max_value, name=name,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "AdjustContrast", images=images, contrast_factor=contrast_factor,
                          min_value=min_value, max_value=max_value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "AdjustContrast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def adjust_contrast_eager_fallback(images, contrast_factor, min_value, max_value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function adjust_contrast
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  contrast_factor = _ops.convert_to_tensor(contrast_factor, _dtypes.float32)
  min_value = _ops.convert_to_tensor(min_value, _dtypes.float32)
  max_value = _ops.convert_to_tensor(max_value, _dtypes.float32)
  _inputs_flat = [images, contrast_factor, min_value, max_value]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"AdjustContrast", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AdjustContrast", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def adjust_contrastv2(images, contrast_factor, name=None):
  r"""Adjust the contrast of one or more images.

  `images` is a tensor of at least 3 dimensions.  The last 3 dimensions are
  interpreted as `[height, width, channels]`.  The other dimensions only
  represent a collection of images, such as `[batch, height, width, channels].`

  Contrast is adjusted independently for each channel of each image.

  For each channel, the Op first computes the mean of the image pixels in the
  channel and then adjusts each component of each pixel to
  `(x - mean) * contrast_factor + mean`.

  Args:
    images: A `Tensor` of type `float32`. Images to adjust.  At least 3-D.
    contrast_factor: A `Tensor` of type `float32`.
      A float multiplier for adjusting contrast.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AdjustContrastv2", name, _ctx._post_execution_callbacks, images,
        contrast_factor)
      return _result
    except _core._FallbackException:
      try:
        return adjust_contrastv2_eager_fallback(
            images, contrast_factor, name=name, ctx=_ctx)
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
        "AdjustContrastv2", images=images, contrast_factor=contrast_factor,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "AdjustContrastv2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def adjust_contrastv2_eager_fallback(images, contrast_factor, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function adjust_contrastv2
  """
  _ctx = ctx if ctx else _context.context()
  images = _ops.convert_to_tensor(images, _dtypes.float32)
  contrast_factor = _ops.convert_to_tensor(contrast_factor, _dtypes.float32)
  _inputs_flat = [images, contrast_factor]
  _attrs = None
  _result = _execute.execute(b"AdjustContrastv2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AdjustContrastv2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def adjust_hue(images, delta, name=None):
  r"""Adjust the hue of one or more images.

  `images` is a tensor of at least 3 dimensions.  The last dimension is
  interpretted as channels, and must be three.

  The input image is considered in the RGB colorspace. Conceptually, the RGB
  colors are first mapped into HSV. A delta is then applied all the hue values,
  and then remapped back to RGB colorspace.

  Args:
    images: A `Tensor` of type `float32`. Images to adjust.  At least 3-D.
    delta: A `Tensor` of type `float32`. A float delta to add to the hue.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "AdjustHue",
        name, _ctx._post_execution_callbacks, images, delta)
      return _result
    except _core._FallbackException:
      try:
        return adjust_hue_eager_fallback(
            images, delta, name=name, ctx=_ctx)
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
        "AdjustHue", images=images, delta=delta, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "AdjustHue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def adjust_hue_eager_fallback(images, delta, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function adjust_hue
  """
  _ctx = ctx if ctx else _context.context()
  images = _ops.convert_to_tensor(images, _dtypes.float32)
  delta = _ops.convert_to_tensor(delta, _dtypes.float32)
  _inputs_flat = [images, delta]
  _attrs = None
  _result = _execute.execute(b"AdjustHue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AdjustHue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def adjust_saturation(images, scale, name=None):
  r"""Adjust the saturation of one or more images.

  `images` is a tensor of at least 3 dimensions.  The last dimension is
  interpretted as channels, and must be three.

  The input image is considered in the RGB colorspace. Conceptually, the RGB
  colors are first mapped into HSV. A scale is then applied all the saturation
  values, and then remapped back to RGB colorspace.

  Args:
    images: A `Tensor` of type `float32`. Images to adjust.  At least 3-D.
    scale: A `Tensor` of type `float32`.
      A float scale to add to the saturation.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "AdjustSaturation", name, _ctx._post_execution_callbacks, images,
        scale)
      return _result
    except _core._FallbackException:
      try:
        return adjust_saturation_eager_fallback(
            images, scale, name=name, ctx=_ctx)
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
        "AdjustSaturation", images=images, scale=scale, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "AdjustSaturation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def adjust_saturation_eager_fallback(images, scale, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function adjust_saturation
  """
  _ctx = ctx if ctx else _context.context()
  images = _ops.convert_to_tensor(images, _dtypes.float32)
  scale = _ops.convert_to_tensor(scale, _dtypes.float32)
  _inputs_flat = [images, scale]
  _attrs = None
  _result = _execute.execute(b"AdjustSaturation", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AdjustSaturation", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def crop_and_resize(image, boxes, box_ind, crop_size, method="bilinear", extrapolation_value=0, name=None):
  r"""Extracts crops from the input image tensor and resizes them.

  Extracts crops from the input image tensor and resizes them using bilinear
  sampling or nearest neighbor sampling (possibly with aspect ratio change) to a
  common output size specified by `crop_size`. This is more general than the
  `crop_to_bounding_box` op which extracts a fixed size slice from the input image
  and does not allow resizing or aspect ratio change.

  Returns a tensor with `crops` from the input `image` at positions defined at the
  bounding box locations in `boxes`. The cropped boxes are all resized (with
  bilinear or nearest neighbor interpolation) to a fixed
  `size = [crop_height, crop_width]`. The result is a 4-D tensor
  `[num_boxes, crop_height, crop_width, depth]`. The resizing is corner aligned.
  In particular, if `boxes = [[0, 0, 1, 1]]`, the method will give identical
  results to using `tf.image.resize_bilinear()` or
  `tf.image.resize_nearest_neighbor()`(depends on the `method` argument) with
  `align_corners=True`.

  Args:
    image: A `Tensor`. Must be one of the following types: `uint8`, `uint16`, `int8`, `int16`, `int32`, `int64`, `half`, `float32`, `float64`.
      A 4-D tensor of shape `[batch, image_height, image_width, depth]`.
      Both `image_height` and `image_width` need to be positive.
    boxes: A `Tensor` of type `float32`.
      A 2-D tensor of shape `[num_boxes, 4]`. The `i`-th row of the tensor
      specifies the coordinates of a box in the `box_ind[i]` image and is specified
      in normalized coordinates `[y1, x1, y2, x2]`. A normalized coordinate value of
      `y` is mapped to the image coordinate at `y * (image_height - 1)`, so as the
      `[0, 1]` interval of normalized image height is mapped to
      `[0, image_height - 1]` in image height coordinates. We do allow `y1` > `y2`, in
      which case the sampled crop is an up-down flipped version of the original
      image. The width dimension is treated similarly. Normalized coordinates
      outside the `[0, 1]` range are allowed, in which case we use
      `extrapolation_value` to extrapolate the input image values.
    box_ind: A `Tensor` of type `int32`.
      A 1-D tensor of shape `[num_boxes]` with int32 values in `[0, batch)`.
      The value of `box_ind[i]` specifies the image that the `i`-th box refers to.
    crop_size: A `Tensor` of type `int32`.
      A 1-D tensor of 2 elements, `size = [crop_height, crop_width]`. All
      cropped image patches are resized to this size. The aspect ratio of the image
      content is not preserved. Both `crop_height` and `crop_width` need to be
      positive.
    method: An optional `string` from: `"bilinear", "nearest"`. Defaults to `"bilinear"`.
      A string specifying the sampling method for resizing. It can be either
      `"bilinear"` or `"nearest"` and default to `"bilinear"`. Currently two sampling
      methods are supported: Bilinear and Nearest Neighbor.
    extrapolation_value: An optional `float`. Defaults to `0`.
      Value used for extrapolation, when applicable.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CropAndResize", name, _ctx._post_execution_callbacks, image, boxes,
        box_ind, crop_size, "method", method, "extrapolation_value",
        extrapolation_value)
      return _result
    except _core._FallbackException:
      try:
        return crop_and_resize_eager_fallback(
            image, boxes, box_ind, crop_size, method=method,
            extrapolation_value=extrapolation_value, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  if extrapolation_value is None:
    extrapolation_value = 0
  extrapolation_value = _execute.make_float(extrapolation_value, "extrapolation_value")
  _, _, _op = _op_def_lib._apply_op_helper(
        "CropAndResize", image=image, boxes=boxes, box_ind=box_ind,
                         crop_size=crop_size, method=method,
                         extrapolation_value=extrapolation_value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "method", _op.get_attr("method"),
            "extrapolation_value", _op.get_attr("extrapolation_value"))
  _execute.record_gradient(
      "CropAndResize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def crop_and_resize_eager_fallback(image, boxes, box_ind, crop_size, method="bilinear", extrapolation_value=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function crop_and_resize
  """
  _ctx = ctx if ctx else _context.context()
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  if extrapolation_value is None:
    extrapolation_value = 0
  extrapolation_value = _execute.make_float(extrapolation_value, "extrapolation_value")
  _attr_T, (image,) = _execute.args_to_matching_eager([image], _ctx)
  boxes = _ops.convert_to_tensor(boxes, _dtypes.float32)
  box_ind = _ops.convert_to_tensor(box_ind, _dtypes.int32)
  crop_size = _ops.convert_to_tensor(crop_size, _dtypes.int32)
  _inputs_flat = [image, boxes, box_ind, crop_size]
  _attrs = ("T", _attr_T, "method", method, "extrapolation_value",
  extrapolation_value)
  _result = _execute.execute(b"CropAndResize", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CropAndResize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def crop_and_resize_grad_boxes(grads, image, boxes, box_ind, method="bilinear", name=None):
  r"""Computes the gradient of the crop_and_resize op wrt the input boxes tensor.

  Args:
    grads: A `Tensor` of type `float32`.
      A 4-D tensor of shape `[num_boxes, crop_height, crop_width, depth]`.
    image: A `Tensor`. Must be one of the following types: `uint8`, `uint16`, `int8`, `int16`, `int32`, `int64`, `half`, `float32`, `float64`.
      A 4-D tensor of shape `[batch, image_height, image_width, depth]`.
      Both `image_height` and `image_width` need to be positive.
    boxes: A `Tensor` of type `float32`.
      A 2-D tensor of shape `[num_boxes, 4]`. The `i`-th row of the tensor
      specifies the coordinates of a box in the `box_ind[i]` image and is specified
      in normalized coordinates `[y1, x1, y2, x2]`. A normalized coordinate value of
      `y` is mapped to the image coordinate at `y * (image_height - 1)`, so as the
      `[0, 1]` interval of normalized image height is mapped to
      `[0, image_height - 1] in image height coordinates. We do allow y1 > y2, in
      which case the sampled crop is an up-down flipped version of the original
      image. The width dimension is treated similarly. Normalized coordinates
      outside the `[0, 1]` range are allowed, in which case we use
      `extrapolation_value` to extrapolate the input image values.
    box_ind: A `Tensor` of type `int32`.
      A 1-D tensor of shape `[num_boxes]` with int32 values in `[0, batch)`.
      The value of `box_ind[i]` specifies the image that the `i`-th box refers to.
    method: An optional `string` from: `"bilinear"`. Defaults to `"bilinear"`.
      A string specifying the interpolation method. Only 'bilinear' is
      supported for now.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CropAndResizeGradBoxes", name, _ctx._post_execution_callbacks, grads,
        image, boxes, box_ind, "method", method)
      return _result
    except _core._FallbackException:
      try:
        return crop_and_resize_grad_boxes_eager_fallback(
            grads, image, boxes, box_ind, method=method, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  _, _, _op = _op_def_lib._apply_op_helper(
        "CropAndResizeGradBoxes", grads=grads, image=image, boxes=boxes,
                                  box_ind=box_ind, method=method, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "method", _op.get_attr("method"))
  _execute.record_gradient(
      "CropAndResizeGradBoxes", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def crop_and_resize_grad_boxes_eager_fallback(grads, image, boxes, box_ind, method="bilinear", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function crop_and_resize_grad_boxes
  """
  _ctx = ctx if ctx else _context.context()
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  _attr_T, (image,) = _execute.args_to_matching_eager([image], _ctx)
  grads = _ops.convert_to_tensor(grads, _dtypes.float32)
  boxes = _ops.convert_to_tensor(boxes, _dtypes.float32)
  box_ind = _ops.convert_to_tensor(box_ind, _dtypes.int32)
  _inputs_flat = [grads, image, boxes, box_ind]
  _attrs = ("T", _attr_T, "method", method)
  _result = _execute.execute(b"CropAndResizeGradBoxes", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "CropAndResizeGradBoxes", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def crop_and_resize_grad_image(grads, boxes, box_ind, image_size, T, method="bilinear", name=None):
  r"""Computes the gradient of the crop_and_resize op wrt the input image tensor.

  Args:
    grads: A `Tensor` of type `float32`.
      A 4-D tensor of shape `[num_boxes, crop_height, crop_width, depth]`.
    boxes: A `Tensor` of type `float32`.
      A 2-D tensor of shape `[num_boxes, 4]`. The `i`-th row of the tensor
      specifies the coordinates of a box in the `box_ind[i]` image and is specified
      in normalized coordinates `[y1, x1, y2, x2]`. A normalized coordinate value of
      `y` is mapped to the image coordinate at `y * (image_height - 1)`, so as the
      `[0, 1]` interval of normalized image height is mapped to
      `[0, image_height - 1] in image height coordinates. We do allow y1 > y2, in
      which case the sampled crop is an up-down flipped version of the original
      image. The width dimension is treated similarly. Normalized coordinates
      outside the `[0, 1]` range are allowed, in which case we use
      `extrapolation_value` to extrapolate the input image values.
    box_ind: A `Tensor` of type `int32`.
      A 1-D tensor of shape `[num_boxes]` with int32 values in `[0, batch)`.
      The value of `box_ind[i]` specifies the image that the `i`-th box refers to.
    image_size: A `Tensor` of type `int32`.
      A 1-D tensor with value `[batch, image_height, image_width, depth]`
      containing the original image size. Both `image_height` and `image_width` need
      to be positive.
    T: A `tf.DType` from: `tf.float32, tf.half, tf.float64`.
    method: An optional `string` from: `"bilinear", "nearest"`. Defaults to `"bilinear"`.
      A string specifying the interpolation method. Only 'bilinear' is
      supported for now.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `T`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "CropAndResizeGradImage", name, _ctx._post_execution_callbacks, grads,
        boxes, box_ind, image_size, "T", T, "method", method)
      return _result
    except _core._FallbackException:
      try:
        return crop_and_resize_grad_image_eager_fallback(
            grads, boxes, box_ind, image_size, T=T, method=method, name=name,
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
  T = _execute.make_type(T, "T")
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  _, _, _op = _op_def_lib._apply_op_helper(
        "CropAndResizeGradImage", grads=grads, boxes=boxes, box_ind=box_ind,
                                  image_size=image_size, T=T, method=method,
                                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "method", _op.get_attr("method"))
  _execute.record_gradient(
      "CropAndResizeGradImage", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def crop_and_resize_grad_image_eager_fallback(grads, boxes, box_ind, image_size, T, method="bilinear", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function crop_and_resize_grad_image
  """
  _ctx = ctx if ctx else _context.context()
  T = _execute.make_type(T, "T")
  if method is None:
    method = "bilinear"
  method = _execute.make_str(method, "method")
  grads = _ops.convert_to_tensor(grads, _dtypes.float32)
  boxes = _ops.convert_to_tensor(boxes, _dtypes.float32)
  box_ind = _ops.convert_to_tensor(box_ind, _dtypes.int32)
  image_size = _ops.convert_to_tensor(image_size, _dtypes.int32)
  _inputs_flat = [grads, boxes, box_ind, image_size]
  _attrs = ("T", T, "method", method)
  _result = _execute.execute(b"CropAndResizeGradImage", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "CropAndResizeGradImage", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def decode_and_crop_jpeg(contents, crop_window, channels=0, ratio=1, fancy_upscaling=True, try_recover_truncated=False, acceptable_fraction=1, dct_method="", name=None):
  r"""Decode and Crop a JPEG-encoded image to a uint8 tensor.

  The attr `channels` indicates the desired number of color channels for the
  decoded image.

  Accepted values are:

  *   0: Use the number of channels in the JPEG-encoded image.
  *   1: output a grayscale image.
  *   3: output an RGB image.

  If needed, the JPEG-encoded image is transformed to match the requested number
  of color channels.

  The attr `ratio` allows downscaling the image by an integer factor during
  decoding.  Allowed values are: 1, 2, 4, and 8.  This is much faster than
  downscaling the image later.


  It is equivalent to a combination of decode and crop, but much faster by only
  decoding partial jpeg image.

  Args:
    contents: A `Tensor` of type `string`. 0-D.  The JPEG-encoded image.
    crop_window: A `Tensor` of type `int32`.
      1-D.  The crop window: [crop_y, crop_x, crop_height, crop_width].
    channels: An optional `int`. Defaults to `0`.
      Number of color channels for the decoded image.
    ratio: An optional `int`. Defaults to `1`. Downscaling ratio.
    fancy_upscaling: An optional `bool`. Defaults to `True`.
      If true use a slower but nicer upscaling of the
      chroma planes (yuv420/422 only).
    try_recover_truncated: An optional `bool`. Defaults to `False`.
      If true try to recover an image from truncated input.
    acceptable_fraction: An optional `float`. Defaults to `1`.
      The minimum required fraction of lines before a truncated
      input is accepted.
    dct_method: An optional `string`. Defaults to `""`.
      string specifying a hint about the algorithm used for
      decompression.  Defaults to "" which maps to a system-specific
      default.  Currently valid values are ["INTEGER_FAST",
      "INTEGER_ACCURATE"].  The hint may be ignored (e.g., the internal
      jpeg library changes to a version that does not have that specific
      option.)
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DecodeAndCropJpeg", name, _ctx._post_execution_callbacks, contents,
        crop_window, "channels", channels, "ratio", ratio, "fancy_upscaling",
        fancy_upscaling, "try_recover_truncated", try_recover_truncated,
        "acceptable_fraction", acceptable_fraction, "dct_method", dct_method)
      return _result
    except _core._FallbackException:
      try:
        return decode_and_crop_jpeg_eager_fallback(
            contents, crop_window, channels=channels, ratio=ratio,
            fancy_upscaling=fancy_upscaling,
            try_recover_truncated=try_recover_truncated,
            acceptable_fraction=acceptable_fraction, dct_method=dct_method,
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
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if ratio is None:
    ratio = 1
  ratio = _execute.make_int(ratio, "ratio")
  if fancy_upscaling is None:
    fancy_upscaling = True
  fancy_upscaling = _execute.make_bool(fancy_upscaling, "fancy_upscaling")
  if try_recover_truncated is None:
    try_recover_truncated = False
  try_recover_truncated = _execute.make_bool(try_recover_truncated, "try_recover_truncated")
  if acceptable_fraction is None:
    acceptable_fraction = 1
  acceptable_fraction = _execute.make_float(acceptable_fraction, "acceptable_fraction")
  if dct_method is None:
    dct_method = ""
  dct_method = _execute.make_str(dct_method, "dct_method")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeAndCropJpeg", contents=contents, crop_window=crop_window,
                             channels=channels, ratio=ratio,
                             fancy_upscaling=fancy_upscaling,
                             try_recover_truncated=try_recover_truncated,
                             acceptable_fraction=acceptable_fraction,
                             dct_method=dct_method, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("channels", _op.get_attr("channels"), "ratio",
            _op.get_attr("ratio"), "fancy_upscaling",
            _op.get_attr("fancy_upscaling"), "try_recover_truncated",
            _op.get_attr("try_recover_truncated"), "acceptable_fraction",
            _op.get_attr("acceptable_fraction"), "dct_method",
            _op.get_attr("dct_method"))
  _execute.record_gradient(
      "DecodeAndCropJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_and_crop_jpeg_eager_fallback(contents, crop_window, channels=0, ratio=1, fancy_upscaling=True, try_recover_truncated=False, acceptable_fraction=1, dct_method="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_and_crop_jpeg
  """
  _ctx = ctx if ctx else _context.context()
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if ratio is None:
    ratio = 1
  ratio = _execute.make_int(ratio, "ratio")
  if fancy_upscaling is None:
    fancy_upscaling = True
  fancy_upscaling = _execute.make_bool(fancy_upscaling, "fancy_upscaling")
  if try_recover_truncated is None:
    try_recover_truncated = False
  try_recover_truncated = _execute.make_bool(try_recover_truncated, "try_recover_truncated")
  if acceptable_fraction is None:
    acceptable_fraction = 1
  acceptable_fraction = _execute.make_float(acceptable_fraction, "acceptable_fraction")
  if dct_method is None:
    dct_method = ""
  dct_method = _execute.make_str(dct_method, "dct_method")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  crop_window = _ops.convert_to_tensor(crop_window, _dtypes.int32)
  _inputs_flat = [contents, crop_window]
  _attrs = ("channels", channels, "ratio", ratio, "fancy_upscaling",
  fancy_upscaling, "try_recover_truncated", try_recover_truncated,
  "acceptable_fraction", acceptable_fraction, "dct_method", dct_method)
  _result = _execute.execute(b"DecodeAndCropJpeg", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeAndCropJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def decode_bmp(contents, channels=0, name=None):
  r"""Decode the first frame of a BMP-encoded image to a uint8 tensor.

  The attr `channels` indicates the desired number of color channels for the
  decoded image.

  Accepted values are:

  *   0: Use the number of channels in the BMP-encoded image.
  *   3: output an RGB image.
  *   4: output an RGBA image.

  Args:
    contents: A `Tensor` of type `string`. 0-D.  The BMP-encoded image.
    channels: An optional `int`. Defaults to `0`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeBmp",
        name, _ctx._post_execution_callbacks, contents, "channels", channels)
      return _result
    except _core._FallbackException:
      try:
        return decode_bmp_eager_fallback(
            contents, channels=channels, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeBmp", contents=contents, channels=channels, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("channels", _op.get_attr("channels"))
  _execute.record_gradient(
      "DecodeBmp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_bmp_eager_fallback(contents, channels=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_bmp
  """
  _ctx = ctx if ctx else _context.context()
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = ("channels", channels)
  _result = _execute.execute(b"DecodeBmp", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeBmp", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def decode_gif(contents, name=None):
  r"""Decode the first frame of a GIF-encoded image to a uint8 tensor.

  GIF with frame or transparency compression are not supported
  convert animated GIF from compressed to uncompressed by:

      convert $src.gif -coalesce $dst.gif

  This op also supports decoding JPEGs and PNGs, though it is cleaner to use
  `tf.image.decode_image`.

  Args:
    contents: A `Tensor` of type `string`. 0-D.  The GIF-encoded image.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeGif",
        name, _ctx._post_execution_callbacks, contents)
      return _result
    except _core._FallbackException:
      try:
        return decode_gif_eager_fallback(
            contents, name=name, ctx=_ctx)
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
        "DecodeGif", contents=contents, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "DecodeGif", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_gif_eager_fallback(contents, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_gif
  """
  _ctx = ctx if ctx else _context.context()
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = None
  _result = _execute.execute(b"DecodeGif", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeGif", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def decode_jpeg(contents, channels=0, ratio=1, fancy_upscaling=True, try_recover_truncated=False, acceptable_fraction=1, dct_method="", name=None):
  r"""Decode a JPEG-encoded image to a uint8 tensor.

  The attr `channels` indicates the desired number of color channels for the
  decoded image.

  Accepted values are:

  *   0: Use the number of channels in the JPEG-encoded image.
  *   1: output a grayscale image.
  *   3: output an RGB image.

  If needed, the JPEG-encoded image is transformed to match the requested number
  of color channels.

  The attr `ratio` allows downscaling the image by an integer factor during
  decoding.  Allowed values are: 1, 2, 4, and 8.  This is much faster than
  downscaling the image later.


  This op also supports decoding PNGs and non-animated GIFs since the interface is
  the same, though it is cleaner to use `tf.image.decode_image`.

  Args:
    contents: A `Tensor` of type `string`. 0-D.  The JPEG-encoded image.
    channels: An optional `int`. Defaults to `0`.
      Number of color channels for the decoded image.
    ratio: An optional `int`. Defaults to `1`. Downscaling ratio.
    fancy_upscaling: An optional `bool`. Defaults to `True`.
      If true use a slower but nicer upscaling of the
      chroma planes (yuv420/422 only).
    try_recover_truncated: An optional `bool`. Defaults to `False`.
      If true try to recover an image from truncated input.
    acceptable_fraction: An optional `float`. Defaults to `1`.
      The minimum required fraction of lines before a truncated
      input is accepted.
    dct_method: An optional `string`. Defaults to `""`.
      string specifying a hint about the algorithm used for
      decompression.  Defaults to "" which maps to a system-specific
      default.  Currently valid values are ["INTEGER_FAST",
      "INTEGER_ACCURATE"].  The hint may be ignored (e.g., the internal
      jpeg library changes to a version that does not have that specific
      option.)
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `uint8`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodeJpeg",
        name, _ctx._post_execution_callbacks, contents, "channels", channels,
        "ratio", ratio, "fancy_upscaling", fancy_upscaling,
        "try_recover_truncated", try_recover_truncated, "acceptable_fraction",
        acceptable_fraction, "dct_method", dct_method)
      return _result
    except _core._FallbackException:
      try:
        return decode_jpeg_eager_fallback(
            contents, channels=channels, ratio=ratio,
            fancy_upscaling=fancy_upscaling,
            try_recover_truncated=try_recover_truncated,
            acceptable_fraction=acceptable_fraction, dct_method=dct_method,
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
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if ratio is None:
    ratio = 1
  ratio = _execute.make_int(ratio, "ratio")
  if fancy_upscaling is None:
    fancy_upscaling = True
  fancy_upscaling = _execute.make_bool(fancy_upscaling, "fancy_upscaling")
  if try_recover_truncated is None:
    try_recover_truncated = False
  try_recover_truncated = _execute.make_bool(try_recover_truncated, "try_recover_truncated")
  if acceptable_fraction is None:
    acceptable_fraction = 1
  acceptable_fraction = _execute.make_float(acceptable_fraction, "acceptable_fraction")
  if dct_method is None:
    dct_method = ""
  dct_method = _execute.make_str(dct_method, "dct_method")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DecodeJpeg", contents=contents, channels=channels, ratio=ratio,
                      fancy_upscaling=fancy_upscaling,
                      try_recover_truncated=try_recover_truncated,
                      acceptable_fraction=acceptable_fraction,
                      dct_method=dct_method, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("channels", _op.get_attr("channels"), "ratio",
            _op.get_attr("ratio"), "fancy_upscaling",
            _op.get_attr("fancy_upscaling"), "try_recover_truncated",
            _op.get_attr("try_recover_truncated"), "acceptable_fraction",
            _op.get_attr("acceptable_fraction"), "dct_method",
            _op.get_attr("dct_method"))
  _execute.record_gradient(
      "DecodeJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_jpeg_eager_fallback(contents, channels=0, ratio=1, fancy_upscaling=True, try_recover_truncated=False, acceptable_fraction=1, dct_method="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_jpeg
  """
  _ctx = ctx if ctx else _context.context()
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if ratio is None:
    ratio = 1
  ratio = _execute.make_int(ratio, "ratio")
  if fancy_upscaling is None:
    fancy_upscaling = True
  fancy_upscaling = _execute.make_bool(fancy_upscaling, "fancy_upscaling")
  if try_recover_truncated is None:
    try_recover_truncated = False
  try_recover_truncated = _execute.make_bool(try_recover_truncated, "try_recover_truncated")
  if acceptable_fraction is None:
    acceptable_fraction = 1
  acceptable_fraction = _execute.make_float(acceptable_fraction, "acceptable_fraction")
  if dct_method is None:
    dct_method = ""
  dct_method = _execute.make_str(dct_method, "dct_method")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = ("channels", channels, "ratio", ratio, "fancy_upscaling",
  fancy_upscaling, "try_recover_truncated", try_recover_truncated,
  "acceptable_fraction", acceptable_fraction, "dct_method", dct_method)
  _result = _execute.execute(b"DecodeJpeg", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodeJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def decode_png(contents, channels=0, dtype=_dtypes.uint8, name=None):
  r"""Decode a PNG-encoded image to a uint8 or uint16 tensor.

  The attr `channels` indicates the desired number of color channels for the
  decoded image.

  Accepted values are:

  *   0: Use the number of channels in the PNG-encoded image.
  *   1: output a grayscale image.
  *   3: output an RGB image.
  *   4: output an RGBA image.

  If needed, the PNG-encoded image is transformed to match the requested number
  of color channels.

  This op also supports decoding JPEGs and non-animated GIFs since the interface
  is the same, though it is cleaner to use `tf.image.decode_image`.

  Args:
    contents: A `Tensor` of type `string`. 0-D.  The PNG-encoded image.
    channels: An optional `int`. Defaults to `0`.
      Number of color channels for the decoded image.
    dtype: An optional `tf.DType` from: `tf.uint8, tf.uint16`. Defaults to `tf.uint8`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dtype`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "DecodePng",
        name, _ctx._post_execution_callbacks, contents, "channels", channels,
        "dtype", dtype)
      return _result
    except _core._FallbackException:
      try:
        return decode_png_eager_fallback(
            contents, channels=channels, dtype=dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if dtype is None:
    dtype = _dtypes.uint8
  dtype = _execute.make_type(dtype, "dtype")
  _, _, _op = _op_def_lib._apply_op_helper(
        "DecodePng", contents=contents, channels=channels, dtype=dtype,
                     name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("channels", _op.get_attr("channels"), "dtype",
            _op.get_attr("dtype"))
  _execute.record_gradient(
      "DecodePng", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def decode_png_eager_fallback(contents, channels=0, dtype=_dtypes.uint8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function decode_png
  """
  _ctx = ctx if ctx else _context.context()
  if channels is None:
    channels = 0
  channels = _execute.make_int(channels, "channels")
  if dtype is None:
    dtype = _dtypes.uint8
  dtype = _execute.make_type(dtype, "dtype")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = ("channels", channels, "dtype", dtype)
  _result = _execute.execute(b"DecodePng", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DecodePng", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('image.draw_bounding_boxes')
def draw_bounding_boxes(images, boxes, name=None):
  r"""Draw bounding boxes on a batch of images.

  Outputs a copy of `images` but draws on top of the pixels zero or more bounding
  boxes specified by the locations in `boxes`. The coordinates of the each
  bounding box in `boxes` are encoded as `[y_min, x_min, y_max, x_max]`. The
  bounding box coordinates are floats in `[0.0, 1.0]` relative to the width and
  height of the underlying image.

  For example, if an image is 100 x 200 pixels (height x width) and the bounding
  box is `[0.1, 0.2, 0.5, 0.9]`, the upper-left and bottom-right coordinates of
  the bounding box will be `(40, 10)` to `(180, 50)` (in (x,y) coordinates).

  Parts of the bounding box may fall outside the image.

  Args:
    images: A `Tensor`. Must be one of the following types: `float32`, `half`.
      4-D with shape `[batch, height, width, depth]`. A batch of images.
    boxes: A `Tensor` of type `float32`.
      3-D with shape `[batch, num_bounding_boxes, 4]` containing bounding
      boxes.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DrawBoundingBoxes", name, _ctx._post_execution_callbacks, images,
        boxes)
      return _result
    except _core._FallbackException:
      try:
        return draw_bounding_boxes_eager_fallback(
            images, boxes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              draw_bounding_boxes, images=images, boxes=boxes, name=name)
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
        "DrawBoundingBoxes", images=images, boxes=boxes, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          draw_bounding_boxes, images=images, boxes=boxes, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "DrawBoundingBoxes", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def draw_bounding_boxes_eager_fallback(images, boxes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function draw_bounding_boxes
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx, _dtypes.float32)
  boxes = _ops.convert_to_tensor(boxes, _dtypes.float32)
  _inputs_flat = [images, boxes]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"DrawBoundingBoxes", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DrawBoundingBoxes", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def encode_jpeg(image, format="", quality=95, progressive=False, optimize_size=False, chroma_downsampling=True, density_unit="in", x_density=300, y_density=300, xmp_metadata="", name=None):
  r"""JPEG-encode an image.

  `image` is a 3-D uint8 Tensor of shape `[height, width, channels]`.

  The attr `format` can be used to override the color format of the encoded
  output.  Values can be:

  *   `''`: Use a default format based on the number of channels in the image.
  *   `grayscale`: Output a grayscale JPEG image.  The `channels` dimension
      of `image` must be 1.
  *   `rgb`: Output an RGB JPEG image. The `channels` dimension
      of `image` must be 3.

  If `format` is not specified or is the empty string, a default format is picked
  in function of the number of channels in `image`:

  *   1: Output a grayscale image.
  *   3: Output an RGB image.

  Args:
    image: A `Tensor` of type `uint8`.
      3-D with shape `[height, width, channels]`.
    format: An optional `string` from: `"", "grayscale", "rgb"`. Defaults to `""`.
      Per pixel image format.
    quality: An optional `int`. Defaults to `95`.
      Quality of the compression from 0 to 100 (higher is better and slower).
    progressive: An optional `bool`. Defaults to `False`.
      If True, create a JPEG that loads progressively (coarse to fine).
    optimize_size: An optional `bool`. Defaults to `False`.
      If True, spend CPU/RAM to reduce size with no quality change.
    chroma_downsampling: An optional `bool`. Defaults to `True`.
      See http://en.wikipedia.org/wiki/Chroma_subsampling.
    density_unit: An optional `string` from: `"in", "cm"`. Defaults to `"in"`.
      Unit used to specify `x_density` and `y_density`:
      pixels per inch (`'in'`) or centimeter (`'cm'`).
    x_density: An optional `int`. Defaults to `300`.
      Horizontal pixels per density unit.
    y_density: An optional `int`. Defaults to `300`.
      Vertical pixels per density unit.
    xmp_metadata: An optional `string`. Defaults to `""`.
      If not empty, embed this XMP metadata in the image header.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodeJpeg",
        name, _ctx._post_execution_callbacks, image, "format", format,
        "quality", quality, "progressive", progressive, "optimize_size",
        optimize_size, "chroma_downsampling", chroma_downsampling,
        "density_unit", density_unit, "x_density", x_density, "y_density",
        y_density, "xmp_metadata", xmp_metadata)
      return _result
    except _core._FallbackException:
      try:
        return encode_jpeg_eager_fallback(
            image, format=format, quality=quality, progressive=progressive,
            optimize_size=optimize_size,
            chroma_downsampling=chroma_downsampling,
            density_unit=density_unit, x_density=x_density,
            y_density=y_density, xmp_metadata=xmp_metadata, name=name,
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
  if format is None:
    format = ""
  format = _execute.make_str(format, "format")
  if quality is None:
    quality = 95
  quality = _execute.make_int(quality, "quality")
  if progressive is None:
    progressive = False
  progressive = _execute.make_bool(progressive, "progressive")
  if optimize_size is None:
    optimize_size = False
  optimize_size = _execute.make_bool(optimize_size, "optimize_size")
  if chroma_downsampling is None:
    chroma_downsampling = True
  chroma_downsampling = _execute.make_bool(chroma_downsampling, "chroma_downsampling")
  if density_unit is None:
    density_unit = "in"
  density_unit = _execute.make_str(density_unit, "density_unit")
  if x_density is None:
    x_density = 300
  x_density = _execute.make_int(x_density, "x_density")
  if y_density is None:
    y_density = 300
  y_density = _execute.make_int(y_density, "y_density")
  if xmp_metadata is None:
    xmp_metadata = ""
  xmp_metadata = _execute.make_str(xmp_metadata, "xmp_metadata")
  _, _, _op = _op_def_lib._apply_op_helper(
        "EncodeJpeg", image=image, format=format, quality=quality,
                      progressive=progressive, optimize_size=optimize_size,
                      chroma_downsampling=chroma_downsampling,
                      density_unit=density_unit, x_density=x_density,
                      y_density=y_density, xmp_metadata=xmp_metadata,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("format", _op.get_attr("format"), "quality",
            _op.get_attr("quality"), "progressive",
            _op.get_attr("progressive"), "optimize_size",
            _op.get_attr("optimize_size"), "chroma_downsampling",
            _op.get_attr("chroma_downsampling"), "density_unit",
            _op.get_attr("density_unit"), "x_density",
            _op.get_attr("x_density"), "y_density", _op.get_attr("y_density"),
            "xmp_metadata", _op.get_attr("xmp_metadata"))
  _execute.record_gradient(
      "EncodeJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_jpeg_eager_fallback(image, format="", quality=95, progressive=False, optimize_size=False, chroma_downsampling=True, density_unit="in", x_density=300, y_density=300, xmp_metadata="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_jpeg
  """
  _ctx = ctx if ctx else _context.context()
  if format is None:
    format = ""
  format = _execute.make_str(format, "format")
  if quality is None:
    quality = 95
  quality = _execute.make_int(quality, "quality")
  if progressive is None:
    progressive = False
  progressive = _execute.make_bool(progressive, "progressive")
  if optimize_size is None:
    optimize_size = False
  optimize_size = _execute.make_bool(optimize_size, "optimize_size")
  if chroma_downsampling is None:
    chroma_downsampling = True
  chroma_downsampling = _execute.make_bool(chroma_downsampling, "chroma_downsampling")
  if density_unit is None:
    density_unit = "in"
  density_unit = _execute.make_str(density_unit, "density_unit")
  if x_density is None:
    x_density = 300
  x_density = _execute.make_int(x_density, "x_density")
  if y_density is None:
    y_density = 300
  y_density = _execute.make_int(y_density, "y_density")
  if xmp_metadata is None:
    xmp_metadata = ""
  xmp_metadata = _execute.make_str(xmp_metadata, "xmp_metadata")
  image = _ops.convert_to_tensor(image, _dtypes.uint8)
  _inputs_flat = [image]
  _attrs = ("format", format, "quality", quality, "progressive", progressive,
  "optimize_size", optimize_size, "chroma_downsampling", chroma_downsampling,
  "density_unit", density_unit, "x_density", x_density, "y_density",
  y_density, "xmp_metadata", xmp_metadata)
  _result = _execute.execute(b"EncodeJpeg", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodeJpeg", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('image.encode_png')
def encode_png(image, compression=-1, name=None):
  r"""PNG-encode an image.

  `image` is a 3-D uint8 or uint16 Tensor of shape `[height, width, channels]`
  where `channels` is:

  *   1: for grayscale.
  *   2: for grayscale + alpha.
  *   3: for RGB.
  *   4: for RGBA.

  The ZLIB compression level, `compression`, can be -1 for the PNG-encoder
  default or a value from 0 to 9.  9 is the highest compression level, generating
  the smallest output, but is slower.

  Args:
    image: A `Tensor`. Must be one of the following types: `uint8`, `uint16`.
      3-D with shape `[height, width, channels]`.
    compression: An optional `int`. Defaults to `-1`. Compression level.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "EncodePng",
        name, _ctx._post_execution_callbacks, image, "compression",
        compression)
      return _result
    except _core._FallbackException:
      try:
        return encode_png_eager_fallback(
            image, compression=compression, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              encode_png, image=image, compression=compression, name=name)
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
  if compression is None:
    compression = -1
  compression = _execute.make_int(compression, "compression")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "EncodePng", image=image, compression=compression, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          encode_png, image=image, compression=compression, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("compression", _op.get_attr("compression"), "T",
            _op.get_attr("T"))
  _execute.record_gradient(
      "EncodePng", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def encode_png_eager_fallback(image, compression=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function encode_png
  """
  _ctx = ctx if ctx else _context.context()
  if compression is None:
    compression = -1
  compression = _execute.make_int(compression, "compression")
  _attr_T, (image,) = _execute.args_to_matching_eager([image], _ctx, _dtypes.uint8)
  _inputs_flat = [image]
  _attrs = ("compression", compression, "T", _attr_T)
  _result = _execute.execute(b"EncodePng", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "EncodePng", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('image.extract_glimpse')
def extract_glimpse(input, size, offsets, centered=True, normalized=True, uniform_noise=True, name=None):
  r"""Extracts a glimpse from the input tensor.

  Returns a set of windows called glimpses extracted at location
  `offsets` from the input tensor. If the windows only partially
  overlaps the inputs, the non overlapping areas will be filled with
  random noise.

  The result is a 4-D tensor of shape `[batch_size, glimpse_height,
  glimpse_width, channels]`. The channels and batch dimensions are the
  same as that of the input tensor. The height and width of the output
  windows are specified in the `size` parameter.

  The argument `normalized` and `centered` controls how the windows are built:

  * If the coordinates are normalized but not centered, 0.0 and 1.0
    correspond to the minimum and maximum of each height and width
    dimension.
  * If the coordinates are both normalized and centered, they range from
    -1.0 to 1.0. The coordinates (-1.0, -1.0) correspond to the upper
    left corner, the lower right corner is located at (1.0, 1.0) and the
    center is at (0, 0).
  * If the coordinates are not normalized they are interpreted as
    numbers of pixels.

  Args:
    input: A `Tensor` of type `float32`.
      A 4-D float tensor of shape `[batch_size, height, width, channels]`.
    size: A `Tensor` of type `int32`.
      A 1-D tensor of 2 elements containing the size of the glimpses
      to extract.  The glimpse height must be specified first, following
      by the glimpse width.
    offsets: A `Tensor` of type `float32`.
      A 2-D integer tensor of shape `[batch_size, 2]` containing
      the y, x locations of the center of each window.
    centered: An optional `bool`. Defaults to `True`.
      indicates if the offset coordinates are centered relative to
      the image, in which case the (0, 0) offset is relative to the center
      of the input images. If false, the (0,0) offset corresponds to the
      upper left corner of the input images.
    normalized: An optional `bool`. Defaults to `True`.
      indicates if the offset coordinates are normalized.
    uniform_noise: An optional `bool`. Defaults to `True`.
      indicates if the noise should be generated using a
      uniform distribution or a Gaussian distribution.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExtractGlimpse", name, _ctx._post_execution_callbacks, input, size,
        offsets, "centered", centered, "normalized", normalized,
        "uniform_noise", uniform_noise)
      return _result
    except _core._FallbackException:
      try:
        return extract_glimpse_eager_fallback(
            input, size, offsets, centered=centered, normalized=normalized,
            uniform_noise=uniform_noise, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              extract_glimpse, input=input, size=size, offsets=offsets,
                               centered=centered, normalized=normalized,
                               uniform_noise=uniform_noise, name=name)
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
  if centered is None:
    centered = True
  centered = _execute.make_bool(centered, "centered")
  if normalized is None:
    normalized = True
  normalized = _execute.make_bool(normalized, "normalized")
  if uniform_noise is None:
    uniform_noise = True
  uniform_noise = _execute.make_bool(uniform_noise, "uniform_noise")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "ExtractGlimpse", input=input, size=size, offsets=offsets,
                          centered=centered, normalized=normalized,
                          uniform_noise=uniform_noise, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          extract_glimpse, input=input, size=size, offsets=offsets,
                           centered=centered, normalized=normalized,
                           uniform_noise=uniform_noise, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("centered", _op.get_attr("centered"), "normalized",
            _op.get_attr("normalized"), "uniform_noise",
            _op.get_attr("uniform_noise"))
  _execute.record_gradient(
      "ExtractGlimpse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def extract_glimpse_eager_fallback(input, size, offsets, centered=True, normalized=True, uniform_noise=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function extract_glimpse
  """
  _ctx = ctx if ctx else _context.context()
  if centered is None:
    centered = True
  centered = _execute.make_bool(centered, "centered")
  if normalized is None:
    normalized = True
  normalized = _execute.make_bool(normalized, "normalized")
  if uniform_noise is None:
    uniform_noise = True
  uniform_noise = _execute.make_bool(uniform_noise, "uniform_noise")
  input = _ops.convert_to_tensor(input, _dtypes.float32)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  offsets = _ops.convert_to_tensor(offsets, _dtypes.float32)
  _inputs_flat = [input, size, offsets]
  _attrs = ("centered", centered, "normalized", normalized, "uniform_noise",
  uniform_noise)
  _result = _execute.execute(b"ExtractGlimpse", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExtractGlimpse", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def extract_jpeg_shape(contents, output_type=_dtypes.int32, name=None):
  r"""Extract the shape information of a JPEG-encoded image.

  This op only parses the image header, so it is much faster than DecodeJpeg.

  Args:
    contents: A `Tensor` of type `string`. 0-D. The JPEG-encoded image.
    output_type: An optional `tf.DType` from: `tf.int32, tf.int64`. Defaults to `tf.int32`.
      (Optional) The output type of the operation (int32 or int64).
      Defaults to int32.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `output_type`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExtractJpegShape", name, _ctx._post_execution_callbacks, contents,
        "output_type", output_type)
      return _result
    except _core._FallbackException:
      try:
        return extract_jpeg_shape_eager_fallback(
            contents, output_type=output_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if output_type is None:
    output_type = _dtypes.int32
  output_type = _execute.make_type(output_type, "output_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExtractJpegShape", contents=contents, output_type=output_type,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_type", _op.get_attr("output_type"))
  _execute.record_gradient(
      "ExtractJpegShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def extract_jpeg_shape_eager_fallback(contents, output_type=_dtypes.int32, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function extract_jpeg_shape
  """
  _ctx = ctx if ctx else _context.context()
  if output_type is None:
    output_type = _dtypes.int32
  output_type = _execute.make_type(output_type, "output_type")
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [contents]
  _attrs = ("output_type", output_type)
  _result = _execute.execute(b"ExtractJpegShape", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExtractJpegShape", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('image.hsv_to_rgb')
def hsv_to_rgb(images, name=None):
  r"""Convert one or more images from HSV to RGB.

  Outputs a tensor of the same shape as the `images` tensor, containing the RGB
  value of the pixels. The output is only well defined if the value in `images`
  are in `[0,1]`.

  See `rgb_to_hsv` for a description of the HSV encoding.

  Args:
    images: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      1-D or higher rank. HSV data to convert. Last dimension must be size 3.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "HSVToRGB",
        name, _ctx._post_execution_callbacks, images)
      return _result
    except _core._FallbackException:
      try:
        return hsv_to_rgb_eager_fallback(
            images, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              hsv_to_rgb, images=images, name=name)
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
        "HSVToRGB", images=images, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          hsv_to_rgb, images=images, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "HSVToRGB", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def hsv_to_rgb_eager_fallback(images, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function hsv_to_rgb
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx, _dtypes.float32)
  _inputs_flat = [images]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"HSVToRGB", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "HSVToRGB", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def non_max_suppression(boxes, scores, max_output_size, iou_threshold=0.5, name=None):
  r"""Greedily selects a subset of bounding boxes in descending order of score,

  pruning away boxes that have high intersection-over-union (IOU) overlap
  with previously selected boxes.  Bounding boxes are supplied as
  [y1, x1, y2, x2], where (y1, x1) and (y2, x2) are the coordinates of any
  diagonal pair of box corners and the coordinates can be provided as normalized
  (i.e., lying in the interval [0, 1]) or absolute.  Note that this algorithm
  is agnostic to where the origin is in the coordinate system.  Note that this
  algorithm is invariant to orthogonal transformations and translations
  of the coordinate system; thus translating or reflections of the coordinate
  system result in the same boxes being selected by the algorithm.
  The output of this operation is a set of integers indexing into the input
  collection of bounding boxes representing the selected boxes.  The bounding
  box coordinates corresponding to the selected indices can then be obtained
  using the `tf.gather operation`.  For example:
    selected_indices = tf.image.non_max_suppression(
        boxes, scores, max_output_size, iou_threshold)
    selected_boxes = tf.gather(boxes, selected_indices)

  Args:
    boxes: A `Tensor` of type `float32`.
      A 2-D float tensor of shape `[num_boxes, 4]`.
    scores: A `Tensor` of type `float32`.
      A 1-D float tensor of shape `[num_boxes]` representing a single
      score corresponding to each box (each row of boxes).
    max_output_size: A `Tensor` of type `int32`.
      A scalar integer tensor representing the maximum number of
      boxes to be selected by non max suppression.
    iou_threshold: An optional `float`. Defaults to `0.5`.
      A float representing the threshold for deciding whether boxes
      overlap too much with respect to IOU.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NonMaxSuppression", name, _ctx._post_execution_callbacks, boxes,
        scores, max_output_size, "iou_threshold", iou_threshold)
      return _result
    except _core._FallbackException:
      try:
        return non_max_suppression_eager_fallback(
            boxes, scores, max_output_size, iou_threshold=iou_threshold,
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
  if iou_threshold is None:
    iou_threshold = 0.5
  iou_threshold = _execute.make_float(iou_threshold, "iou_threshold")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NonMaxSuppression", boxes=boxes, scores=scores,
                             max_output_size=max_output_size,
                             iou_threshold=iou_threshold, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("iou_threshold", _op.get_attr("iou_threshold"))
  _execute.record_gradient(
      "NonMaxSuppression", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def non_max_suppression_eager_fallback(boxes, scores, max_output_size, iou_threshold=0.5, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function non_max_suppression
  """
  _ctx = ctx if ctx else _context.context()
  if iou_threshold is None:
    iou_threshold = 0.5
  iou_threshold = _execute.make_float(iou_threshold, "iou_threshold")
  boxes = _ops.convert_to_tensor(boxes, _dtypes.float32)
  scores = _ops.convert_to_tensor(scores, _dtypes.float32)
  max_output_size = _ops.convert_to_tensor(max_output_size, _dtypes.int32)
  _inputs_flat = [boxes, scores, max_output_size]
  _attrs = ("iou_threshold", iou_threshold)
  _result = _execute.execute(b"NonMaxSuppression", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NonMaxSuppression", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def non_max_suppression_v2(boxes, scores, max_output_size, iou_threshold, name=None):
  r"""Greedily selects a subset of bounding boxes in descending order of score,

  pruning away boxes that have high intersection-over-union (IOU) overlap
  with previously selected boxes.  Bounding boxes are supplied as
  [y1, x1, y2, x2], where (y1, x1) and (y2, x2) are the coordinates of any
  diagonal pair of box corners and the coordinates can be provided as normalized
  (i.e., lying in the interval [0, 1]) or absolute.  Note that this algorithm
  is agnostic to where the origin is in the coordinate system.  Note that this
  algorithm is invariant to orthogonal transformations and translations
  of the coordinate system; thus translating or reflections of the coordinate
  system result in the same boxes being selected by the algorithm.

  The output of this operation is a set of integers indexing into the input
  collection of bounding boxes representing the selected boxes.  The bounding
  box coordinates corresponding to the selected indices can then be obtained
  using the `tf.gather operation`.  For example:

    selected_indices = tf.image.non_max_suppression_v2(
        boxes, scores, max_output_size, iou_threshold)
    selected_boxes = tf.gather(boxes, selected_indices)

  Args:
    boxes: A `Tensor`. Must be one of the following types: `half`, `float32`.
      A 2-D float tensor of shape `[num_boxes, 4]`.
    scores: A `Tensor`. Must have the same type as `boxes`.
      A 1-D float tensor of shape `[num_boxes]` representing a single
      score corresponding to each box (each row of boxes).
    max_output_size: A `Tensor` of type `int32`.
      A scalar integer tensor representing the maximum number of
      boxes to be selected by non max suppression.
    iou_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding whether
      boxes overlap too much with respect to IOU.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NonMaxSuppressionV2", name, _ctx._post_execution_callbacks, boxes,
        scores, max_output_size, iou_threshold)
      return _result
    except _core._FallbackException:
      try:
        return non_max_suppression_v2_eager_fallback(
            boxes, scores, max_output_size, iou_threshold, name=name,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "NonMaxSuppressionV2", boxes=boxes, scores=scores,
                               max_output_size=max_output_size,
                               iou_threshold=iou_threshold, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "NonMaxSuppressionV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def non_max_suppression_v2_eager_fallback(boxes, scores, max_output_size, iou_threshold, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function non_max_suppression_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([boxes, scores], _ctx, _dtypes.float32)
  (boxes, scores) = _inputs_T
  max_output_size = _ops.convert_to_tensor(max_output_size, _dtypes.int32)
  iou_threshold = _ops.convert_to_tensor(iou_threshold, _dtypes.float32)
  _inputs_flat = [boxes, scores, max_output_size, iou_threshold]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"NonMaxSuppressionV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NonMaxSuppressionV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def non_max_suppression_v3(boxes, scores, max_output_size, iou_threshold, score_threshold, name=None):
  r"""Greedily selects a subset of bounding boxes in descending order of score,

  pruning away boxes that have high intersection-over-union (IOU) overlap
  with previously selected boxes.  Bounding boxes with score less than
  `score_threshold` are removed.  Bounding boxes are supplied as
  [y1, x1, y2, x2], where (y1, x1) and (y2, x2) are the coordinates of any
  diagonal pair of box corners and the coordinates can be provided as normalized
  (i.e., lying in the interval [0, 1]) or absolute.  Note that this algorithm
  is agnostic to where the origin is in the coordinate system and more
  generally is invariant to orthogonal transformations and translations
  of the coordinate system; thus translating or reflections of the coordinate
  system result in the same boxes being selected by the algorithm.
  The output of this operation is a set of integers indexing into the input
  collection of bounding boxes representing the selected boxes.  The bounding
  box coordinates corresponding to the selected indices can then be obtained
  using the `tf.gather operation`.  For example:
    selected_indices = tf.image.non_max_suppression_v2(
        boxes, scores, max_output_size, iou_threshold, score_threshold)
    selected_boxes = tf.gather(boxes, selected_indices)

  Args:
    boxes: A `Tensor`. Must be one of the following types: `half`, `float32`.
      A 2-D float tensor of shape `[num_boxes, 4]`.
    scores: A `Tensor`. Must have the same type as `boxes`.
      A 1-D float tensor of shape `[num_boxes]` representing a single
      score corresponding to each box (each row of boxes).
    max_output_size: A `Tensor` of type `int32`.
      A scalar integer tensor representing the maximum number of
      boxes to be selected by non max suppression.
    iou_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding whether
      boxes overlap too much with respect to IOU.
    score_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding when to remove
      boxes based on score.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NonMaxSuppressionV3", name, _ctx._post_execution_callbacks, boxes,
        scores, max_output_size, iou_threshold, score_threshold)
      return _result
    except _core._FallbackException:
      try:
        return non_max_suppression_v3_eager_fallback(
            boxes, scores, max_output_size, iou_threshold, score_threshold,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "NonMaxSuppressionV3", boxes=boxes, scores=scores,
                               max_output_size=max_output_size,
                               iou_threshold=iou_threshold,
                               score_threshold=score_threshold, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "NonMaxSuppressionV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def non_max_suppression_v3_eager_fallback(boxes, scores, max_output_size, iou_threshold, score_threshold, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function non_max_suppression_v3
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, _inputs_T = _execute.args_to_matching_eager([boxes, scores], _ctx, _dtypes.float32)
  (boxes, scores) = _inputs_T
  max_output_size = _ops.convert_to_tensor(max_output_size, _dtypes.int32)
  iou_threshold = _ops.convert_to_tensor(iou_threshold, _dtypes.float32)
  score_threshold = _ops.convert_to_tensor(score_threshold, _dtypes.float32)
  _inputs_flat = [boxes, scores, max_output_size, iou_threshold, score_threshold]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"NonMaxSuppressionV3", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NonMaxSuppressionV3", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_non_max_suppression_v4_outputs = ["selected_indices", "valid_outputs"]
_NonMaxSuppressionV4Output = _collections.namedtuple(
    "NonMaxSuppressionV4", _non_max_suppression_v4_outputs)


def non_max_suppression_v4(boxes, scores, max_output_size, iou_threshold, score_threshold, pad_to_max_output_size=False, name=None):
  r"""Greedily selects a subset of bounding boxes in descending order of score,

  pruning away boxes that have high intersection-over-union (IOU) overlap
  with previously selected boxes.  Bounding boxes with score less than
  `score_threshold` are removed.  Bounding boxes are supplied as
  [y1, x1, y2, x2], where (y1, x1) and (y2, x2) are the coordinates of any
  diagonal pair of box corners and the coordinates can be provided as normalized
  (i.e., lying in the interval [0, 1]) or absolute.  Note that this algorithm
  is agnostic to where the origin is in the coordinate system and more
  generally is invariant to orthogonal transformations and translations
  of the coordinate system; thus translating or reflections of the coordinate
  system result in the same boxes being selected by the algorithm.
  The output of this operation is a set of integers indexing into the input
  collection of bounding boxes representing the selected boxes.  The bounding
  box coordinates corresponding to the selected indices can then be obtained
  using the `tf.gather operation`.  For example:
    selected_indices = tf.image.non_max_suppression_v2(
        boxes, scores, max_output_size, iou_threshold, score_threshold)
    selected_boxes = tf.gather(boxes, selected_indices)

  Args:
    boxes: A `Tensor`. Must be one of the following types: `half`, `float32`.
      A 2-D float tensor of shape `[num_boxes, 4]`.
    scores: A `Tensor`. Must have the same type as `boxes`.
      A 1-D float tensor of shape `[num_boxes]` representing a single
      score corresponding to each box (each row of boxes).
    max_output_size: A `Tensor` of type `int32`.
      A scalar integer tensor representing the maximum number of
      boxes to be selected by non max suppression.
    iou_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding whether
      boxes overlap too much with respect to IOU.
    score_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding when to remove
      boxes based on score.
    pad_to_max_output_size: An optional `bool`. Defaults to `False`.
      If true, the output `selected_indices` is padded to be of length
      `max_output_size`. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (selected_indices, valid_outputs).

    selected_indices: A `Tensor` of type `int32`.
    valid_outputs: A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NonMaxSuppressionV4", name, _ctx._post_execution_callbacks, boxes,
        scores, max_output_size, iou_threshold, score_threshold,
        "pad_to_max_output_size", pad_to_max_output_size)
      _result = _NonMaxSuppressionV4Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return non_max_suppression_v4_eager_fallback(
            boxes, scores, max_output_size, iou_threshold, score_threshold,
            pad_to_max_output_size=pad_to_max_output_size, name=name,
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
  if pad_to_max_output_size is None:
    pad_to_max_output_size = False
  pad_to_max_output_size = _execute.make_bool(pad_to_max_output_size, "pad_to_max_output_size")
  _, _, _op = _op_def_lib._apply_op_helper(
        "NonMaxSuppressionV4", boxes=boxes, scores=scores,
                               max_output_size=max_output_size,
                               iou_threshold=iou_threshold,
                               score_threshold=score_threshold,
                               pad_to_max_output_size=pad_to_max_output_size,
                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "pad_to_max_output_size",
            _op.get_attr("pad_to_max_output_size"))
  _execute.record_gradient(
      "NonMaxSuppressionV4", _inputs_flat, _attrs, _result, name)
  _result = _NonMaxSuppressionV4Output._make(_result)
  return _result



def non_max_suppression_v4_eager_fallback(boxes, scores, max_output_size, iou_threshold, score_threshold, pad_to_max_output_size=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function non_max_suppression_v4
  """
  _ctx = ctx if ctx else _context.context()
  if pad_to_max_output_size is None:
    pad_to_max_output_size = False
  pad_to_max_output_size = _execute.make_bool(pad_to_max_output_size, "pad_to_max_output_size")
  _attr_T, _inputs_T = _execute.args_to_matching_eager([boxes, scores], _ctx, _dtypes.float32)
  (boxes, scores) = _inputs_T
  max_output_size = _ops.convert_to_tensor(max_output_size, _dtypes.int32)
  iou_threshold = _ops.convert_to_tensor(iou_threshold, _dtypes.float32)
  score_threshold = _ops.convert_to_tensor(score_threshold, _dtypes.float32)
  _inputs_flat = [boxes, scores, max_output_size, iou_threshold, score_threshold]
  _attrs = ("T", _attr_T, "pad_to_max_output_size", pad_to_max_output_size)
  _result = _execute.execute(b"NonMaxSuppressionV4", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NonMaxSuppressionV4", _inputs_flat, _attrs, _result, name)
  _result = _NonMaxSuppressionV4Output._make(_result)
  return _result


def non_max_suppression_with_overlaps(overlaps, scores, max_output_size, overlap_threshold, score_threshold, name=None):
  r"""Greedily selects a subset of bounding boxes in descending order of score,

  pruning away boxes that have high overlaps
  with previously selected boxes.  Bounding boxes with score less than
  `score_threshold` are removed. N-by-n overlap values are supplied as square matrix,
  which allows for defining a custom overlap criterium (eg. intersection over union,
  intersection over area, etc.).

  The output of this operation is a set of integers indexing into the input
  collection of bounding boxes representing the selected boxes.  The bounding
  box coordinates corresponding to the selected indices can then be obtained
  using the `tf.gather operation`.  For example:

    selected_indices = tf.image.non_max_suppression_with_overlaps(
        overlaps, scores, max_output_size, overlap_threshold, score_threshold)
    selected_boxes = tf.gather(boxes, selected_indices)

  Args:
    overlaps: A `Tensor` of type `float32`.
      A 2-D float tensor of shape `[num_boxes, num_boxes]` representing
      the n-by-n box overlap values.
    scores: A `Tensor` of type `float32`.
      A 1-D float tensor of shape `[num_boxes]` representing a single
      score corresponding to each box (each row of boxes).
    max_output_size: A `Tensor` of type `int32`.
      A scalar integer tensor representing the maximum number of
      boxes to be selected by non max suppression.
    overlap_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding whether
      boxes overlap too.
    score_threshold: A `Tensor` of type `float32`.
      A 0-D float tensor representing the threshold for deciding when to remove
      boxes based on score.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NonMaxSuppressionWithOverlaps", name, _ctx._post_execution_callbacks,
        overlaps, scores, max_output_size, overlap_threshold, score_threshold)
      return _result
    except _core._FallbackException:
      try:
        return non_max_suppression_with_overlaps_eager_fallback(
            overlaps, scores, max_output_size, overlap_threshold,
            score_threshold, name=name, ctx=_ctx)
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
        "NonMaxSuppressionWithOverlaps", overlaps=overlaps, scores=scores,
                                         max_output_size=max_output_size,
                                         overlap_threshold=overlap_threshold,
                                         score_threshold=score_threshold,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "NonMaxSuppressionWithOverlaps", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def non_max_suppression_with_overlaps_eager_fallback(overlaps, scores, max_output_size, overlap_threshold, score_threshold, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function non_max_suppression_with_overlaps
  """
  _ctx = ctx if ctx else _context.context()
  overlaps = _ops.convert_to_tensor(overlaps, _dtypes.float32)
  scores = _ops.convert_to_tensor(scores, _dtypes.float32)
  max_output_size = _ops.convert_to_tensor(max_output_size, _dtypes.int32)
  overlap_threshold = _ops.convert_to_tensor(overlap_threshold, _dtypes.float32)
  score_threshold = _ops.convert_to_tensor(score_threshold, _dtypes.float32)
  _inputs_flat = [overlaps, scores, max_output_size, overlap_threshold, score_threshold]
  _attrs = None
  _result = _execute.execute(b"NonMaxSuppressionWithOverlaps", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "NonMaxSuppressionWithOverlaps", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_quantized_resize_bilinear_outputs = ["resized_images", "out_min", "out_max"]
_QuantizedResizeBilinearOutput = _collections.namedtuple(
    "QuantizedResizeBilinear", _quantized_resize_bilinear_outputs)


def quantized_resize_bilinear(images, size, min, max, align_corners=False, name=None):
  r"""Resize quantized `images` to `size` using quantized bilinear interpolation.

  Input images and output images must be quantized types.

  Args:
    images: A `Tensor`. Must be one of the following types: `quint8`, `qint32`, `float32`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    min: A `Tensor` of type `float32`.
    max: A `Tensor` of type `float32`.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (resized_images, out_min, out_max).

    resized_images: A `Tensor`. Has the same type as `images`.
    out_min: A `Tensor` of type `float32`.
    out_max: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "QuantizedResizeBilinear", name, _ctx._post_execution_callbacks,
        images, size, min, max, "align_corners", align_corners)
      _result = _QuantizedResizeBilinearOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return quantized_resize_bilinear_eager_fallback(
            images, size, min, max, align_corners=align_corners, name=name,
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
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "QuantizedResizeBilinear", images=images, size=size, min=min, max=max,
                                   align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "QuantizedResizeBilinear", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedResizeBilinearOutput._make(_result)
  return _result



def quantized_resize_bilinear_eager_fallback(images, size, min, max, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function quantized_resize_bilinear
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  min = _ops.convert_to_tensor(min, _dtypes.float32)
  max = _ops.convert_to_tensor(max, _dtypes.float32)
  _inputs_flat = [images, size, min, max]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"QuantizedResizeBilinear", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "QuantizedResizeBilinear", _inputs_flat, _attrs, _result, name)
  _result = _QuantizedResizeBilinearOutput._make(_result)
  return _result


@_dispatch.add_dispatch_list
@tf_export('image.rgb_to_hsv')
def rgb_to_hsv(images, name=None):
  r"""Converts one or more images from RGB to HSV.

  Outputs a tensor of the same shape as the `images` tensor, containing the HSV
  value of the pixels. The output is only well defined if the value in `images`
  are in `[0,1]`.

  `output[..., 0]` contains hue, `output[..., 1]` contains saturation, and
  `output[..., 2]` contains value. All HSV values are in `[0,1]`. A hue of 0
  corresponds to pure red, hue 1/3 is pure green, and 2/3 is pure blue.

  Args:
    images: A `Tensor`. Must be one of the following types: `half`, `bfloat16`, `float32`, `float64`.
      1-D or higher rank. RGB data to convert. Last dimension must be size 3.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RGBToHSV",
        name, _ctx._post_execution_callbacks, images)
      return _result
    except _core._FallbackException:
      try:
        return rgb_to_hsv_eager_fallback(
            images, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              rgb_to_hsv, images=images, name=name)
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
        "RGBToHSV", images=images, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          rgb_to_hsv, images=images, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"))
  _execute.record_gradient(
      "RGBToHSV", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def rgb_to_hsv_eager_fallback(images, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function rgb_to_hsv
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx, _dtypes.float32)
  _inputs_flat = [images]
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"RGBToHSV", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RGBToHSV", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def random_crop(image, size, seed=0, seed2=0, name=None):
  r"""Randomly crop `image`.

  `size` is a 1-D int64 tensor with 2 elements representing the crop height and
  width.  The values must be non negative.

  This Op picks a random location in `image` and crops a `height` by `width`
  rectangle from that location.  The random location is picked so the cropped
  area will fit inside the original image.

  Args:
    image: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`.
      3-D of shape `[height, width, channels]`.
    size: A `Tensor` of type `int64`.
      1-D of length 2 containing: `crop_height`, `crop_width`..
    seed: An optional `int`. Defaults to `0`.
      If either seed or seed2 are set to be non-zero, the random number
      generator is seeded by the given seed.  Otherwise, it is seeded by a
      random seed.
    seed2: An optional `int`. Defaults to `0`.
      An second seed to avoid seed collision.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `image`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RandomCrop",
        name, _ctx._post_execution_callbacks, image, size, "seed", seed,
        "seed2", seed2)
      return _result
    except _core._FallbackException:
      try:
        return random_crop_eager_fallback(
            image, size, seed=seed, seed2=seed2, name=name, ctx=_ctx)
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
        "RandomCrop", image=image, size=size, seed=seed, seed2=seed2,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "seed", _op.get_attr("seed"), "seed2",
            _op.get_attr("seed2"))
  _execute.record_gradient(
      "RandomCrop", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def random_crop_eager_fallback(image, size, seed=0, seed2=0, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function random_crop
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  _attr_T, (image,) = _execute.args_to_matching_eager([image], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int64)
  _inputs_flat = [image, size]
  _attrs = ("T", _attr_T, "seed", seed, "seed2", seed2)
  _result = _execute.execute(b"RandomCrop", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RandomCrop", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_area(images, size, align_corners=False, name=None):
  r"""Resize `images` to `size` using area interpolation.

  Input images can be of different types but output images are always float.

  The range of pixel values for the output image might be slightly different
  from the range for the input image because of limited numerical precision.
  To guarantee an output range, for example `[0.0, 1.0]`, apply
  `tf.clip_by_value` to the output.

  Each output pixel is computed by first transforming the pixel's footprint into
  the input tensor and then averaging the pixels that intersect the footprint. An
  input pixel's contribution to the average is weighted by the fraction of its
  area that intersects the footprint.  This is the same as OpenCV's INTER_AREA.

  Args:
    images: A `Tensor`. Must be one of the following types: `int8`, `uint8`, `int16`, `uint16`, `int32`, `int64`, `half`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ResizeArea",
        name, _ctx._post_execution_callbacks, images, size, "align_corners",
        align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_area_eager_fallback(
            images, size, align_corners=align_corners, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeArea", images=images, size=size, align_corners=align_corners,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeArea", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_area_eager_fallback(images, size, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_area
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [images, size]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeArea", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeArea", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_bicubic(images, size, align_corners=False, name=None):
  r"""Resize `images` to `size` using bicubic interpolation.

  Input images can be of different types but output images are always float.

  Args:
    images: A `Tensor`. Must be one of the following types: `int8`, `uint8`, `int16`, `uint16`, `int32`, `int64`, `half`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeBicubic", name, _ctx._post_execution_callbacks, images, size,
        "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_bicubic_eager_fallback(
            images, size, align_corners=align_corners, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeBicubic", images=images, size=size,
                         align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeBicubic", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_bicubic_eager_fallback(images, size, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_bicubic
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [images, size]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeBicubic", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeBicubic", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_bicubic_grad(grads, original_image, align_corners=False, name=None):
  r"""Computes the gradient of bicubic interpolation.

  Args:
    grads: A `Tensor` of type `float32`.
      4-D with shape `[batch, height, width, channels]`.
    original_image: A `Tensor`. Must be one of the following types: `float32`, `float64`.
      4-D with shape `[batch, orig_height, orig_width, channels]`,
      The image tensor that was resized.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and grad tensors are
      aligned. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `original_image`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeBicubicGrad", name, _ctx._post_execution_callbacks, grads,
        original_image, "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_bicubic_grad_eager_fallback(
            grads, original_image, align_corners=align_corners, name=name,
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
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeBicubicGrad", grads=grads, original_image=original_image,
                             align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeBicubicGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_bicubic_grad_eager_fallback(grads, original_image, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_bicubic_grad
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (original_image,) = _execute.args_to_matching_eager([original_image], _ctx)
  grads = _ops.convert_to_tensor(grads, _dtypes.float32)
  _inputs_flat = [grads, original_image]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeBicubicGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeBicubicGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_bilinear(images, size, align_corners=False, name=None):
  r"""Resize `images` to `size` using bilinear interpolation.

  Input images can be of different types but output images are always float.

  Args:
    images: A `Tensor`. Must be one of the following types: `int8`, `uint8`, `int16`, `uint16`, `int32`, `int64`, `bfloat16`, `half`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeBilinear", name, _ctx._post_execution_callbacks, images, size,
        "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_bilinear_eager_fallback(
            images, size, align_corners=align_corners, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeBilinear", images=images, size=size,
                          align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeBilinear", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_bilinear_eager_fallback(images, size, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_bilinear
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [images, size]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeBilinear", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeBilinear", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_bilinear_grad(grads, original_image, align_corners=False, name=None):
  r"""Computes the gradient of bilinear interpolation.

  Args:
    grads: A `Tensor` of type `float32`.
      4-D with shape `[batch, height, width, channels]`.
    original_image: A `Tensor`. Must be one of the following types: `float32`, `bfloat16`, `half`, `float64`.
      4-D with shape `[batch, orig_height, orig_width, channels]`,
      The image tensor that was resized.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and grad tensors are
      aligned. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `original_image`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeBilinearGrad", name, _ctx._post_execution_callbacks, grads,
        original_image, "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_bilinear_grad_eager_fallback(
            grads, original_image, align_corners=align_corners, name=name,
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
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeBilinearGrad", grads=grads, original_image=original_image,
                              align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeBilinearGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_bilinear_grad_eager_fallback(grads, original_image, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_bilinear_grad
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (original_image,) = _execute.args_to_matching_eager([original_image], _ctx)
  grads = _ops.convert_to_tensor(grads, _dtypes.float32)
  _inputs_flat = [grads, original_image]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeBilinearGrad", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeBilinearGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_nearest_neighbor(images, size, align_corners=False, name=None):
  r"""Resize `images` to `size` using nearest neighbor interpolation.

  Args:
    images: A `Tensor`. Must be one of the following types: `int8`, `uint8`, `int16`, `uint16`, `int32`, `int64`, `half`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `new_height, new_width`.  The
      new size for the images.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and output tensors are
      aligned, preserving the values at the corner pixels. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `images`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeNearestNeighbor", name, _ctx._post_execution_callbacks, images,
        size, "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_nearest_neighbor_eager_fallback(
            images, size, align_corners=align_corners, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeNearestNeighbor", images=images, size=size,
                                 align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeNearestNeighbor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_nearest_neighbor_eager_fallback(images, size, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_nearest_neighbor
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (images,) = _execute.args_to_matching_eager([images], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [images, size]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeNearestNeighbor", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ResizeNearestNeighbor", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def resize_nearest_neighbor_grad(grads, size, align_corners=False, name=None):
  r"""Computes the gradient of nearest neighbor interpolation.

  Args:
    grads: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int32`, `half`, `float32`, `float64`.
      4-D with shape `[batch, height, width, channels]`.
    size:  A 1-D int32 Tensor of 2 elements: `orig_height, orig_width`. The
      original input size.
    align_corners: An optional `bool`. Defaults to `False`.
      If true, the centers of the 4 corner pixels of the input and grad tensors are
      aligned. Defaults to false.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `grads`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ResizeNearestNeighborGrad", name, _ctx._post_execution_callbacks,
        grads, size, "align_corners", align_corners)
      return _result
    except _core._FallbackException:
      try:
        return resize_nearest_neighbor_grad_eager_fallback(
            grads, size, align_corners=align_corners, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ResizeNearestNeighborGrad", grads=grads, size=size,
                                     align_corners=align_corners, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "align_corners",
            _op.get_attr("align_corners"))
  _execute.record_gradient(
      "ResizeNearestNeighborGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def resize_nearest_neighbor_grad_eager_fallback(grads, size, align_corners=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function resize_nearest_neighbor_grad
  """
  _ctx = ctx if ctx else _context.context()
  if align_corners is None:
    align_corners = False
  align_corners = _execute.make_bool(align_corners, "align_corners")
  _attr_T, (grads,) = _execute.args_to_matching_eager([grads], _ctx)
  size = _ops.convert_to_tensor(size, _dtypes.int32)
  _inputs_flat = [grads, size]
  _attrs = ("T", _attr_T, "align_corners", align_corners)
  _result = _execute.execute(b"ResizeNearestNeighborGrad", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ResizeNearestNeighborGrad", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_sample_distorted_bounding_box_outputs = ["begin", "size", "bboxes"]
_SampleDistortedBoundingBoxOutput = _collections.namedtuple(
    "SampleDistortedBoundingBox", _sample_distorted_bounding_box_outputs)


def sample_distorted_bounding_box(image_size, bounding_boxes, seed=0, seed2=0, min_object_covered=0.1, aspect_ratio_range=[0.75, 1.33], area_range=[0.05, 1], max_attempts=100, use_image_if_no_bounding_boxes=False, name=None):
  r"""Generate a single randomly distorted bounding box for an image.

  Bounding box annotations are often supplied in addition to ground-truth labels
  in image recognition or object localization tasks. A common technique for
  training such a system is to randomly distort an image while preserving
  its content, i.e. *data augmentation*. This Op outputs a randomly distorted
  localization of an object, i.e. bounding box, given an `image_size`,
  `bounding_boxes` and a series of constraints.

  The output of this Op is a single bounding box that may be used to crop the
  original image. The output is returned as 3 tensors: `begin`, `size` and
  `bboxes`. The first 2 tensors can be fed directly into `tf.slice` to crop the
  image. The latter may be supplied to `tf.image.draw_bounding_boxes` to visualize
  what the bounding box looks like.

  Bounding boxes are supplied and returned as `[y_min, x_min, y_max, x_max]`. The
  bounding box coordinates are floats in `[0.0, 1.0]` relative to the width and
  height of the underlying image.

  For example,

  ```python
      # Generate a single distorted bounding box.
      begin, size, bbox_for_draw = tf.image.sample_distorted_bounding_box(
          tf.shape(image),
          bounding_boxes=bounding_boxes)

      # Draw the bounding box in an image summary.
      image_with_box = tf.image.draw_bounding_boxes(tf.expand_dims(image, 0),
                                                    bbox_for_draw)
      tf.summary.image('images_with_box', image_with_box)

      # Employ the bounding box to distort the image.
      distorted_image = tf.slice(image, begin, size)
  ```

  Note that if no bounding box information is available, setting
  `use_image_if_no_bounding_boxes = true` will assume there is a single implicit
  bounding box covering the whole image. If `use_image_if_no_bounding_boxes` is
  false and no bounding boxes are supplied, an error is raised.

  Args:
    image_size: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int16`, `int32`, `int64`.
      1-D, containing `[height, width, channels]`.
    bounding_boxes: A `Tensor` of type `float32`.
      3-D with shape `[batch, N, 4]` describing the N bounding boxes
      associated with the image.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to non-zero, the random number
      generator is seeded by the given `seed`.  Otherwise, it is seeded by a random
      seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    min_object_covered: An optional `float`. Defaults to `0.1`.
      The cropped area of the image must contain at least this
      fraction of any bounding box supplied. The value of this parameter should be
      non-negative. In the case of 0, the cropped area does not need to overlap
      any of the bounding boxes supplied.
    aspect_ratio_range: An optional list of `floats`. Defaults to `[0.75, 1.33]`.
      The cropped area of the image must have an aspect ratio =
      width / height within this range.
    area_range: An optional list of `floats`. Defaults to `[0.05, 1]`.
      The cropped area of the image must contain a fraction of the
      supplied image within this range.
    max_attempts: An optional `int`. Defaults to `100`.
      Number of attempts at generating a cropped region of the image
      of the specified constraints. After `max_attempts` failures, return the entire
      image.
    use_image_if_no_bounding_boxes: An optional `bool`. Defaults to `False`.
      Controls behavior if no bounding boxes supplied.
      If true, assume an implicit bounding box covering the whole input. If false,
      raise an error.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (begin, size, bboxes).

    begin: A `Tensor`. Has the same type as `image_size`.
    size: A `Tensor`. Has the same type as `image_size`.
    bboxes: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SampleDistortedBoundingBox", name, _ctx._post_execution_callbacks,
        image_size, bounding_boxes, "seed", seed, "seed2", seed2,
        "min_object_covered", min_object_covered, "aspect_ratio_range",
        aspect_ratio_range, "area_range", area_range, "max_attempts",
        max_attempts, "use_image_if_no_bounding_boxes",
        use_image_if_no_bounding_boxes)
      _result = _SampleDistortedBoundingBoxOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sample_distorted_bounding_box_eager_fallback(
            image_size, bounding_boxes, seed=seed, seed2=seed2,
            min_object_covered=min_object_covered,
            aspect_ratio_range=aspect_ratio_range, area_range=area_range,
            max_attempts=max_attempts,
            use_image_if_no_bounding_boxes=use_image_if_no_bounding_boxes,
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
  if min_object_covered is None:
    min_object_covered = 0.1
  min_object_covered = _execute.make_float(min_object_covered, "min_object_covered")
  if aspect_ratio_range is None:
    aspect_ratio_range = [0.75, 1.33]
  if not isinstance(aspect_ratio_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'aspect_ratio_range' argument to "
        "'sample_distorted_bounding_box' Op, not %r." % aspect_ratio_range)
  aspect_ratio_range = [_execute.make_float(_f, "aspect_ratio_range") for _f in aspect_ratio_range]
  if area_range is None:
    area_range = [0.05, 1]
  if not isinstance(area_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'area_range' argument to "
        "'sample_distorted_bounding_box' Op, not %r." % area_range)
  area_range = [_execute.make_float(_f, "area_range") for _f in area_range]
  if max_attempts is None:
    max_attempts = 100
  max_attempts = _execute.make_int(max_attempts, "max_attempts")
  if use_image_if_no_bounding_boxes is None:
    use_image_if_no_bounding_boxes = False
  use_image_if_no_bounding_boxes = _execute.make_bool(use_image_if_no_bounding_boxes, "use_image_if_no_bounding_boxes")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SampleDistortedBoundingBox", image_size=image_size,
                                      bounding_boxes=bounding_boxes,
                                      seed=seed, seed2=seed2,
                                      min_object_covered=min_object_covered,
                                      aspect_ratio_range=aspect_ratio_range,
                                      area_range=area_range,
                                      max_attempts=max_attempts,
                                      use_image_if_no_bounding_boxes=use_image_if_no_bounding_boxes,
                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "seed", _op.get_attr("seed"), "seed2",
            _op.get_attr("seed2"), "min_object_covered",
            _op.get_attr("min_object_covered"), "aspect_ratio_range",
            _op.get_attr("aspect_ratio_range"), "area_range",
            _op.get_attr("area_range"), "max_attempts",
            _op.get_attr("max_attempts"), "use_image_if_no_bounding_boxes",
            _op.get_attr("use_image_if_no_bounding_boxes"))
  _execute.record_gradient(
      "SampleDistortedBoundingBox", _inputs_flat, _attrs, _result, name)
  _result = _SampleDistortedBoundingBoxOutput._make(_result)
  return _result



def sample_distorted_bounding_box_eager_fallback(image_size, bounding_boxes, seed=0, seed2=0, min_object_covered=0.1, aspect_ratio_range=[0.75, 1.33], area_range=[0.05, 1], max_attempts=100, use_image_if_no_bounding_boxes=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sample_distorted_bounding_box
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if min_object_covered is None:
    min_object_covered = 0.1
  min_object_covered = _execute.make_float(min_object_covered, "min_object_covered")
  if aspect_ratio_range is None:
    aspect_ratio_range = [0.75, 1.33]
  if not isinstance(aspect_ratio_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'aspect_ratio_range' argument to "
        "'sample_distorted_bounding_box' Op, not %r." % aspect_ratio_range)
  aspect_ratio_range = [_execute.make_float(_f, "aspect_ratio_range") for _f in aspect_ratio_range]
  if area_range is None:
    area_range = [0.05, 1]
  if not isinstance(area_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'area_range' argument to "
        "'sample_distorted_bounding_box' Op, not %r." % area_range)
  area_range = [_execute.make_float(_f, "area_range") for _f in area_range]
  if max_attempts is None:
    max_attempts = 100
  max_attempts = _execute.make_int(max_attempts, "max_attempts")
  if use_image_if_no_bounding_boxes is None:
    use_image_if_no_bounding_boxes = False
  use_image_if_no_bounding_boxes = _execute.make_bool(use_image_if_no_bounding_boxes, "use_image_if_no_bounding_boxes")
  _attr_T, (image_size,) = _execute.args_to_matching_eager([image_size], _ctx)
  bounding_boxes = _ops.convert_to_tensor(bounding_boxes, _dtypes.float32)
  _inputs_flat = [image_size, bounding_boxes]
  _attrs = ("T", _attr_T, "seed", seed, "seed2", seed2, "min_object_covered",
  min_object_covered, "aspect_ratio_range", aspect_ratio_range, "area_range",
  area_range, "max_attempts", max_attempts, "use_image_if_no_bounding_boxes",
  use_image_if_no_bounding_boxes)
  _result = _execute.execute(b"SampleDistortedBoundingBox", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SampleDistortedBoundingBox", _inputs_flat, _attrs, _result, name)
  _result = _SampleDistortedBoundingBoxOutput._make(_result)
  return _result


_sample_distorted_bounding_box_v2_outputs = ["begin", "size", "bboxes"]
_SampleDistortedBoundingBoxV2Output = _collections.namedtuple(
    "SampleDistortedBoundingBoxV2", _sample_distorted_bounding_box_v2_outputs)


def sample_distorted_bounding_box_v2(image_size, bounding_boxes, min_object_covered, seed=0, seed2=0, aspect_ratio_range=[0.75, 1.33], area_range=[0.05, 1], max_attempts=100, use_image_if_no_bounding_boxes=False, name=None):
  r"""Generate a single randomly distorted bounding box for an image.

  Bounding box annotations are often supplied in addition to ground-truth labels
  in image recognition or object localization tasks. A common technique for
  training such a system is to randomly distort an image while preserving
  its content, i.e. *data augmentation*. This Op outputs a randomly distorted
  localization of an object, i.e. bounding box, given an `image_size`,
  `bounding_boxes` and a series of constraints.

  The output of this Op is a single bounding box that may be used to crop the
  original image. The output is returned as 3 tensors: `begin`, `size` and
  `bboxes`. The first 2 tensors can be fed directly into `tf.slice` to crop the
  image. The latter may be supplied to `tf.image.draw_bounding_boxes` to visualize
  what the bounding box looks like.

  Bounding boxes are supplied and returned as `[y_min, x_min, y_max, x_max]`. The
  bounding box coordinates are floats in `[0.0, 1.0]` relative to the width and
  height of the underlying image.

  For example,

  ```python
      # Generate a single distorted bounding box.
      begin, size, bbox_for_draw = tf.image.sample_distorted_bounding_box(
          tf.shape(image),
          bounding_boxes=bounding_boxes)

      # Draw the bounding box in an image summary.
      image_with_box = tf.image.draw_bounding_boxes(tf.expand_dims(image, 0),
                                                    bbox_for_draw)
      tf.summary.image('images_with_box', image_with_box)

      # Employ the bounding box to distort the image.
      distorted_image = tf.slice(image, begin, size)
  ```

  Note that if no bounding box information is available, setting
  `use_image_if_no_bounding_boxes = true` will assume there is a single implicit
  bounding box covering the whole image. If `use_image_if_no_bounding_boxes` is
  false and no bounding boxes are supplied, an error is raised.

  Args:
    image_size: A `Tensor`. Must be one of the following types: `uint8`, `int8`, `int16`, `int32`, `int64`.
      1-D, containing `[height, width, channels]`.
    bounding_boxes: A `Tensor` of type `float32`.
      3-D with shape `[batch, N, 4]` describing the N bounding boxes
      associated with the image.
    min_object_covered: A `Tensor` of type `float32`.
      The cropped area of the image must contain at least this
      fraction of any bounding box supplied. The value of this parameter should be
      non-negative. In the case of 0, the cropped area does not need to overlap
      any of the bounding boxes supplied.
    seed: An optional `int`. Defaults to `0`.
      If either `seed` or `seed2` are set to non-zero, the random number
      generator is seeded by the given `seed`.  Otherwise, it is seeded by a random
      seed.
    seed2: An optional `int`. Defaults to `0`.
      A second seed to avoid seed collision.
    aspect_ratio_range: An optional list of `floats`. Defaults to `[0.75, 1.33]`.
      The cropped area of the image must have an aspect ratio =
      width / height within this range.
    area_range: An optional list of `floats`. Defaults to `[0.05, 1]`.
      The cropped area of the image must contain a fraction of the
      supplied image within this range.
    max_attempts: An optional `int`. Defaults to `100`.
      Number of attempts at generating a cropped region of the image
      of the specified constraints. After `max_attempts` failures, return the entire
      image.
    use_image_if_no_bounding_boxes: An optional `bool`. Defaults to `False`.
      Controls behavior if no bounding boxes supplied.
      If true, assume an implicit bounding box covering the whole input. If false,
      raise an error.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (begin, size, bboxes).

    begin: A `Tensor`. Has the same type as `image_size`.
    size: A `Tensor`. Has the same type as `image_size`.
    bboxes: A `Tensor` of type `float32`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SampleDistortedBoundingBoxV2", name, _ctx._post_execution_callbacks,
        image_size, bounding_boxes, min_object_covered, "seed", seed, "seed2",
        seed2, "aspect_ratio_range", aspect_ratio_range, "area_range",
        area_range, "max_attempts", max_attempts,
        "use_image_if_no_bounding_boxes", use_image_if_no_bounding_boxes)
      _result = _SampleDistortedBoundingBoxV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return sample_distorted_bounding_box_v2_eager_fallback(
            image_size, bounding_boxes, min_object_covered, seed=seed,
            seed2=seed2, aspect_ratio_range=aspect_ratio_range,
            area_range=area_range, max_attempts=max_attempts,
            use_image_if_no_bounding_boxes=use_image_if_no_bounding_boxes,
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
  if aspect_ratio_range is None:
    aspect_ratio_range = [0.75, 1.33]
  if not isinstance(aspect_ratio_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'aspect_ratio_range' argument to "
        "'sample_distorted_bounding_box_v2' Op, not %r." % aspect_ratio_range)
  aspect_ratio_range = [_execute.make_float(_f, "aspect_ratio_range") for _f in aspect_ratio_range]
  if area_range is None:
    area_range = [0.05, 1]
  if not isinstance(area_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'area_range' argument to "
        "'sample_distorted_bounding_box_v2' Op, not %r." % area_range)
  area_range = [_execute.make_float(_f, "area_range") for _f in area_range]
  if max_attempts is None:
    max_attempts = 100
  max_attempts = _execute.make_int(max_attempts, "max_attempts")
  if use_image_if_no_bounding_boxes is None:
    use_image_if_no_bounding_boxes = False
  use_image_if_no_bounding_boxes = _execute.make_bool(use_image_if_no_bounding_boxes, "use_image_if_no_bounding_boxes")
  _, _, _op = _op_def_lib._apply_op_helper(
        "SampleDistortedBoundingBoxV2", image_size=image_size,
                                        bounding_boxes=bounding_boxes,
                                        min_object_covered=min_object_covered,
                                        seed=seed, seed2=seed2,
                                        aspect_ratio_range=aspect_ratio_range,
                                        area_range=area_range,
                                        max_attempts=max_attempts,
                                        use_image_if_no_bounding_boxes=use_image_if_no_bounding_boxes,
                                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("T", _op.get_attr("T"), "seed", _op.get_attr("seed"), "seed2",
            _op.get_attr("seed2"), "aspect_ratio_range",
            _op.get_attr("aspect_ratio_range"), "area_range",
            _op.get_attr("area_range"), "max_attempts",
            _op.get_attr("max_attempts"), "use_image_if_no_bounding_boxes",
            _op.get_attr("use_image_if_no_bounding_boxes"))
  _execute.record_gradient(
      "SampleDistortedBoundingBoxV2", _inputs_flat, _attrs, _result, name)
  _result = _SampleDistortedBoundingBoxV2Output._make(_result)
  return _result



def sample_distorted_bounding_box_v2_eager_fallback(image_size, bounding_boxes, min_object_covered, seed=0, seed2=0, aspect_ratio_range=[0.75, 1.33], area_range=[0.05, 1], max_attempts=100, use_image_if_no_bounding_boxes=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sample_distorted_bounding_box_v2
  """
  _ctx = ctx if ctx else _context.context()
  if seed is None:
    seed = 0
  seed = _execute.make_int(seed, "seed")
  if seed2 is None:
    seed2 = 0
  seed2 = _execute.make_int(seed2, "seed2")
  if aspect_ratio_range is None:
    aspect_ratio_range = [0.75, 1.33]
  if not isinstance(aspect_ratio_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'aspect_ratio_range' argument to "
        "'sample_distorted_bounding_box_v2' Op, not %r." % aspect_ratio_range)
  aspect_ratio_range = [_execute.make_float(_f, "aspect_ratio_range") for _f in aspect_ratio_range]
  if area_range is None:
    area_range = [0.05, 1]
  if not isinstance(area_range, (list, tuple)):
    raise TypeError(
        "Expected list for 'area_range' argument to "
        "'sample_distorted_bounding_box_v2' Op, not %r." % area_range)
  area_range = [_execute.make_float(_f, "area_range") for _f in area_range]
  if max_attempts is None:
    max_attempts = 100
  max_attempts = _execute.make_int(max_attempts, "max_attempts")
  if use_image_if_no_bounding_boxes is None:
    use_image_if_no_bounding_boxes = False
  use_image_if_no_bounding_boxes = _execute.make_bool(use_image_if_no_bounding_boxes, "use_image_if_no_bounding_boxes")
  _attr_T, (image_size,) = _execute.args_to_matching_eager([image_size], _ctx)
  bounding_boxes = _ops.convert_to_tensor(bounding_boxes, _dtypes.float32)
  min_object_covered = _ops.convert_to_tensor(min_object_covered, _dtypes.float32)
  _inputs_flat = [image_size, bounding_boxes, min_object_covered]
  _attrs = ("T", _attr_T, "seed", seed, "seed2", seed2, "aspect_ratio_range",
  aspect_ratio_range, "area_range", area_range, "max_attempts", max_attempts,
  "use_image_if_no_bounding_boxes", use_image_if_no_bounding_boxes)
  _result = _execute.execute(b"SampleDistortedBoundingBoxV2", 3,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SampleDistortedBoundingBoxV2", _inputs_flat, _attrs, _result, name)
  _result = _SampleDistortedBoundingBoxV2Output._make(_result)
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "AdjustContrast"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "contrast_factor"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_value"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_value"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   deprecation {
#     version: 2
#     explanation: "Use AdjustContrastv2 instead"
#   }
# }
# op {
#   name: "AdjustContrastv2"
#   input_arg {
#     name: "images"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "contrast_factor"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "AdjustHue"
#   input_arg {
#     name: "images"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "delta"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "AdjustSaturation"
#   input_arg {
#     name: "images"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "scale"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "CropAndResize"
#   input_arg {
#     name: "image"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "boxes"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "box_ind"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "crop_size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "crops"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_UINT16
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
#   attr {
#     name: "method"
#     type: "string"
#     default_value {
#       s: "bilinear"
#     }
#     allowed_values {
#       list {
#         s: "bilinear"
#         s: "nearest"
#       }
#     }
#   }
#   attr {
#     name: "extrapolation_value"
#     type: "float"
#     default_value {
#       f: 0
#     }
#   }
# }
# op {
#   name: "CropAndResizeGradBoxes"
#   input_arg {
#     name: "grads"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "image"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "boxes"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "box_ind"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "output"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_UINT16
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
#   attr {
#     name: "method"
#     type: "string"
#     default_value {
#       s: "bilinear"
#     }
#     allowed_values {
#       list {
#         s: "bilinear"
#       }
#     }
#   }
# }
# op {
#   name: "CropAndResizeGradImage"
#   input_arg {
#     name: "grads"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "boxes"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "box_ind"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "image_size"
#     type: DT_INT32
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
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "method"
#     type: "string"
#     default_value {
#       s: "bilinear"
#     }
#     allowed_values {
#       list {
#         s: "bilinear"
#         s: "nearest"
#       }
#     }
#   }
# }
# op {
#   name: "DecodeAndCropJpeg"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "crop_window"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "image"
#     type: DT_UINT8
#   }
#   attr {
#     name: "channels"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ratio"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "fancy_upscaling"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "try_recover_truncated"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "acceptable_fraction"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "dct_method"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "DecodeBmp"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "image"
#     type: DT_UINT8
#   }
#   attr {
#     name: "channels"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
# }
# op {
#   name: "DecodeGif"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "image"
#     type: DT_UINT8
#   }
# }
# op {
#   name: "DecodeJpeg"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "image"
#     type: DT_UINT8
#   }
#   attr {
#     name: "channels"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "ratio"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "fancy_upscaling"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "try_recover_truncated"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "acceptable_fraction"
#     type: "float"
#     default_value {
#       f: 1
#     }
#   }
#   attr {
#     name: "dct_method"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "DecodePng"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "image"
#     type_attr: "dtype"
#   }
#   attr {
#     name: "channels"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "dtype"
#     type: "type"
#     default_value {
#       type: DT_UINT8
#     }
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_UINT16
#       }
#     }
#   }
# }
# op {
#   name: "DrawBoundingBoxes"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "boxes"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
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
#         type: DT_HALF
#       }
#     }
#   }
# }
# op {
#   name: "EncodeJpeg"
#   input_arg {
#     name: "image"
#     type: DT_UINT8
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   attr {
#     name: "format"
#     type: "string"
#     default_value {
#       s: ""
#     }
#     allowed_values {
#       list {
#         s: ""
#         s: "grayscale"
#         s: "rgb"
#       }
#     }
#   }
#   attr {
#     name: "quality"
#     type: "int"
#     default_value {
#       i: 95
#     }
#   }
#   attr {
#     name: "progressive"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "optimize_size"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "chroma_downsampling"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "density_unit"
#     type: "string"
#     default_value {
#       s: "in"
#     }
#     allowed_values {
#       list {
#         s: "in"
#         s: "cm"
#       }
#     }
#   }
#   attr {
#     name: "x_density"
#     type: "int"
#     default_value {
#       i: 300
#     }
#   }
#   attr {
#     name: "y_density"
#     type: "int"
#     default_value {
#       i: 300
#     }
#   }
#   attr {
#     name: "xmp_metadata"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
# op {
#   name: "EncodePng"
#   input_arg {
#     name: "image"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   attr {
#     name: "compression"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_UINT8
#     }
#     allowed_values {
#       list {
#         type: DT_UINT8
#         type: DT_UINT16
#       }
#     }
#   }
# }
# op {
#   name: "ExtractGlimpse"
#   input_arg {
#     name: "input"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "offsets"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "glimpse"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "centered"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "normalized"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "uniform_noise"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "ExtractJpegShape"
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "image_shape"
#     type_attr: "output_type"
#   }
#   attr {
#     name: "output_type"
#     type: "type"
#     default_value {
#       type: DT_INT32
#     }
#     allowed_values {
#       list {
#         type: DT_INT32
#         type: DT_INT64
#       }
#     }
#   }
# }
# op {
#   name: "HSVToRGB"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "NonMaxSuppression"
#   input_arg {
#     name: "boxes"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "scores"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_output_size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "selected_indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "iou_threshold"
#     type: "float"
#     default_value {
#       f: 0.5
#     }
#   }
# }
# op {
#   name: "NonMaxSuppressionV2"
#   input_arg {
#     name: "boxes"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scores"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "max_output_size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "iou_threshold"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "selected_indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "NonMaxSuppressionV3"
#   input_arg {
#     name: "boxes"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scores"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "max_output_size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "iou_threshold"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "score_threshold"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "selected_indices"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
# }
# op {
#   name: "NonMaxSuppressionV4"
#   input_arg {
#     name: "boxes"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "scores"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "max_output_size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "iou_threshold"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "score_threshold"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "selected_indices"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "valid_outputs"
#     type: DT_INT32
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "pad_to_max_output_size"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "NonMaxSuppressionWithOverlaps"
#   input_arg {
#     name: "overlaps"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "scores"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max_output_size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "overlap_threshold"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "score_threshold"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "selected_indices"
#     type: DT_INT32
#   }
# }
# op {
#   name: "QuantizedResizeBilinear"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "min"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "max"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "resized_images"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "out_min"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "out_max"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_QUINT8
#         type: DT_QINT32
#         type: DT_FLOAT
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "RGBToHSV"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "output"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     default_value {
#       type: DT_FLOAT
#     }
#     allowed_values {
#       list {
#         type: DT_HALF
#         type: DT_BFLOAT16
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
# }
# op {
#   name: "RandomCrop"
#   input_arg {
#     name: "image"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT64
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
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
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
#   deprecation {
#     version: 8
#     explanation: "Random crop is now pure Python"
#   }
#   is_stateful: true
# }
# op {
#   name: "ResizeArea"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "resized_images"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_UINT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeBicubic"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "resized_images"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_UINT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeBicubicGrad"
#   input_arg {
#     name: "grads"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "original_image"
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
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeBilinear"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "resized_images"
#     type: DT_FLOAT
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_UINT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeBilinearGrad"
#   input_arg {
#     name: "grads"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "original_image"
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
#         type: DT_BFLOAT16
#         type: DT_HALF
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeNearestNeighbor"
#   input_arg {
#     name: "images"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "resized_images"
#     type_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "type"
#     allowed_values {
#       list {
#         type: DT_INT8
#         type: DT_UINT8
#         type: DT_INT16
#         type: DT_UINT16
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ResizeNearestNeighborGrad"
#   input_arg {
#     name: "grads"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT32
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
#         type: DT_INT32
#         type: DT_HALF
#         type: DT_FLOAT
#         type: DT_DOUBLE
#       }
#     }
#   }
#   attr {
#     name: "align_corners"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "SampleDistortedBoundingBox"
#   input_arg {
#     name: "image_size"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "bounding_boxes"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "begin"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "size"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "bboxes"
#     type: DT_FLOAT
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
#       }
#     }
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
#     name: "min_object_covered"
#     type: "float"
#     default_value {
#       f: 0.1
#     }
#   }
#   attr {
#     name: "aspect_ratio_range"
#     type: "list(float)"
#     default_value {
#       list {
#         f: 0.75
#         f: 1.33
#       }
#     }
#   }
#   attr {
#     name: "area_range"
#     type: "list(float)"
#     default_value {
#       list {
#         f: 0.05
#         f: 1
#       }
#     }
#   }
#   attr {
#     name: "max_attempts"
#     type: "int"
#     default_value {
#       i: 100
#     }
#   }
#   attr {
#     name: "use_image_if_no_bounding_boxes"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "SampleDistortedBoundingBoxV2"
#   input_arg {
#     name: "image_size"
#     type_attr: "T"
#   }
#   input_arg {
#     name: "bounding_boxes"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "min_object_covered"
#     type: DT_FLOAT
#   }
#   output_arg {
#     name: "begin"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "size"
#     type_attr: "T"
#   }
#   output_arg {
#     name: "bboxes"
#     type: DT_FLOAT
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
#       }
#     }
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
#     name: "aspect_ratio_range"
#     type: "list(float)"
#     default_value {
#       list {
#         f: 0.75
#         f: 1.33
#       }
#     }
#   }
#   attr {
#     name: "area_range"
#     type: "list(float)"
#     default_value {
#       list {
#         f: 0.05
#         f: 1
#       }
#     }
#   }
#   attr {
#     name: "max_attempts"
#     type: "int"
#     default_value {
#       i: 100
#     }
#   }
#   attr {
#     name: "use_image_if_no_bounding_boxes"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\226\001\n\016AdjustContrast\022\013\n\006images\"\001T\022\023\n\017contrast_factor\030\001\022\r\n\tmin_value\030\001\022\r\n\tmax_value\030\001\032\n\n\006output\030\001\"\026\n\001T\022\004type:\013\n\t2\007\004\006\005\003\t\001\002B \010\002\022\034Use AdjustContrastv2 instead\n?\n\020AdjustContrastv2\022\n\n\006images\030\001\022\023\n\017contrast_factor\030\001\032\n\n\006output\030\001\n.\n\tAdjustHue\022\n\n\006images\030\001\022\t\n\005delta\030\001\032\n\n\006output\030\001\n5\n\020AdjustSaturation\022\n\n\006images\030\001\022\t\n\005scale\030\001\032\n\n\006output\030\001\n\301\001\n\rCropAndResize\022\n\n\005image\"\001T\022\t\n\005boxes\030\001\022\013\n\007box_ind\030\003\022\r\n\tcrop_size\030\003\032\t\n\005crops\030\001\"\030\n\001T\022\004type:\r\n\0132\t\004\021\006\005\003\t\023\001\002\"3\n\006method\022\006string\032\n\022\010bilinear:\025\n\023\022\010bilinear\022\007nearest\"#\n\023extrapolation_value\022\005float\032\005%\000\000\000\000\n\231\001\n\026CropAndResizeGradBoxes\022\t\n\005grads\030\001\022\n\n\005image\"\001T\022\t\n\005boxes\030\001\022\013\n\007box_ind\030\003\032\n\n\006output\030\001\"\030\n\001T\022\004type:\r\n\0132\t\004\021\006\005\003\t\023\001\002\"*\n\006method\022\006string\032\n\022\010bilinear:\014\n\n\022\010bilinear\n\241\001\n\026CropAndResizeGradImage\022\t\n\005grads\030\001\022\t\n\005boxes\030\001\022\013\n\007box_ind\030\003\022\016\n\nimage_size\030\003\032\013\n\006output\"\001T\"\022\n\001T\022\004type:\007\n\0052\003\001\023\002\"3\n\006method\022\006string\032\n\022\010bilinear:\025\n\023\022\010bilinear\022\007nearest\n\343\001\n\021DecodeAndCropJpeg\022\014\n\010contents\030\007\022\017\n\013crop_window\030\003\032\t\n\005image\030\004\"\023\n\010channels\022\003int\032\002\030\000\"\020\n\005ratio\022\003int\032\002\030\001\"\033\n\017fancy_upscaling\022\004bool\032\002(\001\"!\n\025try_recover_truncated\022\004bool\032\002(\000\"#\n\023acceptable_fraction\022\005float\032\005%\000\000\200?\"\030\n\ndct_method\022\006string\032\002\022\000\n9\n\tDecodeBmp\022\014\n\010contents\030\007\032\t\n\005image\030\004\"\023\n\010channels\022\003int\032\002\030\000\n$\n\tDecodeGif\022\014\n\010contents\030\007\032\t\n\005image\030\004\n\313\001\n\nDecodeJpeg\022\014\n\010contents\030\007\032\t\n\005image\030\004\"\023\n\010channels\022\003int\032\002\030\000\"\020\n\005ratio\022\003int\032\002\030\001\"\033\n\017fancy_upscaling\022\004bool\032\002(\001\"!\n\025try_recover_truncated\022\004bool\032\002(\000\"#\n\023acceptable_fraction\022\005float\032\005%\000\000\200?\"\030\n\ndct_method\022\006string\032\002\022\000\nY\n\tDecodePng\022\014\n\010contents\030\007\032\016\n\005image\"\005dtype\"\023\n\010channels\022\003int\032\002\030\000\"\031\n\005dtype\022\004type\032\0020\004:\006\n\0042\002\004\021\nO\n\021DrawBoundingBoxes\022\013\n\006images\"\001T\022\t\n\005boxes\030\001\032\013\n\006output\"\001T\"\025\n\001T\022\004type\032\0020\001:\006\n\0042\002\001\023\n\256\002\n\nEncodeJpeg\022\t\n\005image\030\004\032\014\n\010contents\030\007\"*\n\006format\022\006string\032\002\022\000:\024\n\022\022\000\022\tgrayscale\022\003rgb\"\022\n\007quality\022\003int\032\002\030_\"\027\n\013progressive\022\004bool\032\002(\000\"\031\n\roptimize_size\022\004bool\032\002(\000\"\037\n\023chroma_downsampling\022\004bool\032\002(\001\"(\n\014density_unit\022\006string\032\004\022\002in:\n\n\010\022\002in\022\002cm\"\025\n\tx_density\022\003int\032\003\030\254\002\"\025\n\ty_density\022\003int\032\003\030\254\002\"\032\n\014xmp_metadata\022\006string\032\002\022\000\n]\n\tEncodePng\022\n\n\005image\"\001T\032\014\n\010contents\030\007\"\037\n\013compression\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\"\025\n\001T\022\004type\032\0020\004:\006\n\0042\002\004\021\n\210\001\n\016ExtractGlimpse\022\t\n\005input\030\001\022\010\n\004size\030\003\022\013\n\007offsets\030\001\032\013\n\007glimpse\030\001\"\024\n\010centered\022\004bool\032\002(\001\"\026\n\nnormalized\022\004bool\032\002(\001\"\031\n\runiform_noise\022\004bool\032\002(\001\n]\n\020ExtractJpegShape\022\014\n\010contents\030\007\032\032\n\013image_shape\"\013output_type\"\037\n\013output_type\022\004type\032\0020\003:\006\n\0042\002\003\t\n=\n\010HSVToRGB\022\013\n\006images\"\001T\032\013\n\006output\"\001T\"\027\n\001T\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\nt\n\021NonMaxSuppression\022\t\n\005boxes\030\001\022\n\n\006scores\030\001\022\023\n\017max_output_size\030\003\032\024\n\020selected_indices\030\003\"\035\n\riou_threshold\022\005float\032\005%\000\000\000?\n\203\001\n\023NonMaxSuppressionV2\022\n\n\005boxes\"\001T\022\013\n\006scores\"\001T\022\023\n\017max_output_size\030\003\022\021\n\riou_threshold\030\001\032\024\n\020selected_indices\030\003\"\025\n\001T\022\004type\032\0020\001:\006\n\0042\002\023\001\n\230\001\n\023NonMaxSuppressionV3\022\n\n\005boxes\"\001T\022\013\n\006scores\"\001T\022\023\n\017max_output_size\030\003\022\021\n\riou_threshold\030\001\022\023\n\017score_threshold\030\001\032\024\n\020selected_indices\030\003\"\025\n\001T\022\004type\032\0020\001:\006\n\0042\002\023\001\n\317\001\n\023NonMaxSuppressionV4\022\n\n\005boxes\"\001T\022\013\n\006scores\"\001T\022\023\n\017max_output_size\030\003\022\021\n\riou_threshold\030\001\022\023\n\017score_threshold\030\001\032\024\n\020selected_indices\030\003\032\021\n\rvalid_outputs\030\003\"\025\n\001T\022\004type\032\0020\001:\006\n\0042\002\023\001\"\"\n\026pad_to_max_output_size\022\004bool\032\002(\000\n\220\001\n\035NonMaxSuppressionWithOverlaps\022\014\n\010overlaps\030\001\022\n\n\006scores\030\001\022\023\n\017max_output_size\030\003\022\025\n\021overlap_threshold\030\001\022\023\n\017score_threshold\030\001\032\024\n\020selected_indices\030\003\n\240\001\n\027QuantizedResizeBilinear\022\013\n\006images\"\001T\022\010\n\004size\030\003\022\007\n\003min\030\001\022\007\n\003max\030\001\032\023\n\016resized_images\"\001T\032\013\n\007out_min\030\001\032\013\n\007out_max\030\001\"\022\n\001T\022\004type:\007\n\0052\003\014\r\001\"\031\n\ralign_corners\022\004bool\032\002(\000\n=\n\010RGBToHSV\022\013\n\006images\"\001T\032\013\n\006output\"\001T\"\027\n\001T\022\004type\032\0020\001:\010\n\0062\004\023\016\001\002\n\221\001\n\nRandomCrop\022\n\n\005image\"\001T\022\010\n\004size\030\t\032\013\n\006output\"\001T\"\026\n\001T\022\004type:\013\n\t2\007\004\006\005\003\t\001\002\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000B\"\010\010\022\036Random crop is now pure Python\210\001\001\nl\n\nResizeArea\022\013\n\006images\"\001T\022\010\n\004size\030\003\032\022\n\016resized_images\030\001\"\030\n\001T\022\004type:\r\n\0132\t\006\004\005\021\003\t\023\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\no\n\rResizeBicubic\022\013\n\006images\"\001T\022\010\n\004size\030\003\032\022\n\016resized_images\030\001\"\030\n\001T\022\004type:\r\n\0132\t\006\004\005\021\003\t\023\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\nn\n\021ResizeBicubicGrad\022\t\n\005grads\030\001\022\023\n\016original_image\"\001T\032\013\n\006output\"\001T\"\021\n\001T\022\004type:\006\n\0042\002\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\nq\n\016ResizeBilinear\022\013\n\006images\"\001T\022\010\n\004size\030\003\032\022\n\016resized_images\030\001\"\031\n\001T\022\004type:\016\n\0142\n\006\004\005\021\003\t\016\023\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\nq\n\022ResizeBilinearGrad\022\t\n\005grads\030\001\022\023\n\016original_image\"\001T\032\013\n\006output\"\001T\"\023\n\001T\022\004type:\010\n\0062\004\001\016\023\002\"\031\n\ralign_corners\022\004bool\032\002(\000\nx\n\025ResizeNearestNeighbor\022\013\n\006images\"\001T\022\010\n\004size\030\003\032\023\n\016resized_images\"\001T\"\030\n\001T\022\004type:\r\n\0132\t\006\004\005\021\003\t\023\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\np\n\031ResizeNearestNeighborGrad\022\n\n\005grads\"\001T\022\010\n\004size\030\003\032\013\n\006output\"\001T\"\025\n\001T\022\004type:\n\n\0102\006\004\006\003\023\001\002\"\031\n\ralign_corners\022\004bool\032\002(\000\n\343\002\n\032SampleDistortedBoundingBox\022\017\n\nimage_size\"\001T\022\022\n\016bounding_boxes\030\001\032\n\n\005begin\"\001T\032\t\n\004size\"\001T\032\n\n\006bboxes\030\001\"\024\n\001T\022\004type:\t\n\0072\005\004\006\005\003\t\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"\"\n\022min_object_covered\022\005float\032\005%\315\314\314=\"/\n\022aspect_ratio_range\022\013list(float)\032\014\n\n\"\010\000\000@?q=\252?\"\'\n\narea_range\022\013list(float)\032\014\n\n\"\010\315\314L=\000\000\200?\"\027\n\014max_attempts\022\003int\032\002\030d\"*\n\036use_image_if_no_bounding_boxes\022\004bool\032\002(\000\210\001\001\n\331\002\n\034SampleDistortedBoundingBoxV2\022\017\n\nimage_size\"\001T\022\022\n\016bounding_boxes\030\001\022\026\n\022min_object_covered\030\001\032\n\n\005begin\"\001T\032\t\n\004size\"\001T\032\n\n\006bboxes\030\001\"\024\n\001T\022\004type:\t\n\0072\005\004\006\005\003\t\"\017\n\004seed\022\003int\032\002\030\000\"\020\n\005seed2\022\003int\032\002\030\000\"/\n\022aspect_ratio_range\022\013list(float)\032\014\n\n\"\010\000\000@?q=\252?\"\'\n\narea_range\022\013list(float)\032\014\n\n\"\010\315\314L=\000\000\200?\"\027\n\014max_attempts\022\003int\032\002\030d\"*\n\036use_image_if_no_bounding_boxes\022\004bool\032\002(\000\210\001\001")
