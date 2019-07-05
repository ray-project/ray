# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Helper methods for tensor data."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import binascii

import numpy as np
from tensorflow.python.debug.cli import command_parser

from tensorboard.util import encoder
from tensorboard.plugins.debugger import health_pill_calc


def numel(shape):
  """Obtain total number of elements from a tensor (ndarray) shape.

  Args:
    shape: A list or tuple represenitng a tensor (ndarray) shape.
  """
  output = 1
  for dim in shape:
    output *= dim
  return output


def parse_time_indices(s):
  """Parse a string as time indices.

  Args:
    s: A valid slicing string for time indices. E.g., '-1', '[:]', ':', '2:10'

  Returns:
    A slice object.

  Raises:
    ValueError: If `s` does not represent valid time indices.
  """
  if not s.startswith('['):
    s = '[' + s + ']'
  parsed = command_parser._parse_slices(s)
  if len(parsed) != 1:
    raise ValueError(
        'Invalid number of slicing objects in time indices (%d)' % len(parsed))
  else:
    return parsed[0]


def translate_dtype(dtype):
  """Translate numpy dtype into a string.

  The 'object' type is understood as a TensorFlow string and translated into
  'string'.

  Args:
    dtype: A numpy dtype object.

  Returns:
    A string representing the data type.
  """
  out = str(dtype)
  # String-type TensorFlow Tensors are represented as object-type arrays in
  # numpy. We map the type name back to 'string' for clarity.
  return 'string' if out == 'object' else out


def process_buffers_for_display(s, limit=40):
  """Process a buffer for human-readable display.

  This function performs the following operation on each of the buffers in `s`.
    1. Truncate input buffer if the length of the buffer is greater than
       `limit`, to prevent large strings from overloading the frontend.
    2. Apply `binascii.b2a_qp` on the truncated buffer to make the buffer
       printable and convertible to JSON.
    3. If truncation happened (in step 1), append a string at the end
       describing the original length and the truncation.

  Args:
    s: The buffer to be processed, either a single buffer or a nested array of
      them.
    limit: Length limit for each buffer, beyond which truncation will occur.

  Return:
    A single processed buffer or a nested array of processed buffers.
  """
  if isinstance(s, (list, tuple)):
    return [process_buffers_for_display(elem, limit=limit) for elem in s]
  else:
    length = len(s)
    if length > limit:
      return (binascii.b2a_qp(s[:limit]) +
              b' (length-%d truncated at %d bytes)' % (length, limit))
    else:
      return binascii.b2a_qp(s)


def array_view(array, slicing=None, mapping=None):
  """View a slice or the entirety of an ndarray.

  Args:
    array: The input array, as an numpy.ndarray.
    slicing: Optional slicing string, e.g., "[:, 1:3, :]".
    mapping: Optional mapping string. Supported mappings:
      `None` or case-insensitive `'None'`: Unmapped nested list.
      `'image/png'`: Image encoding of a 2D sliced array or 3D sliced array
        with 3 as the last dimension. If the sliced array is not 2D or 3D with
        3 as the last dimension, a `ValueError` will be thrown.
      `health-pill`: A succinct summary of the numeric values of a tensor.
        See documentation in [`health_pill_calc.py`] for more details.

  Returns:
    1. dtype as a `str`.
    2. shape of the sliced array, as a tuple of `int`s.
    3. the potentially sliced values, as a nested `list`.
  """

  dtype = translate_dtype(array.dtype)
  sliced_array = (array[command_parser._parse_slices(slicing)] if slicing
                  else array)

  if np.isscalar(sliced_array) and str(dtype) == 'string':
    # When a string Tensor (for which dtype is 'object') is sliced down to only
    # one element, it becomes a string, instead of an numpy array.
    # We preserve the dimensionality of original array in the returned shape
    # and slice.
    ndims = len(array.shape)
    slice_shape = []
    for _ in range(ndims):
      sliced_array = [sliced_array]
      slice_shape.append(1)
    return dtype, tuple(slice_shape), sliced_array
  else:
    shape = sliced_array.shape
    if mapping == "image/png":
      if len(sliced_array.shape) == 2:
        return dtype, shape, array_to_base64_png(sliced_array)
      elif len(sliced_array.shape) == 3:
        raise NotImplementedError(
            "image/png mapping for 3D array has not been implemented")
      else:
        raise ValueError("Invalid rank for image/png mapping: %d" %
                         len(sliced_array.shape))
    elif mapping == 'health-pill':
      health_pill = health_pill_calc.calc_health_pill(array)
      return dtype, shape, health_pill
    elif mapping is None or mapping == '' or  mapping.lower() == 'none':
      return dtype, shape, sliced_array.tolist()
    else:
      raise ValueError("Invalid mapping: %s" % mapping)


IMAGE_COLOR_CHANNELS = 3
POSITIVE_INFINITY_RGB = (0, 62, 212)  # +inf --> Blue.
NEGATIVE_INFINITY_RGB = (255, 127, 0)  # -inf --> Orange.
NAN_RGB = (221, 47, 45)  # nan -> Red.


def array_to_base64_png(array):
  """Convert an array into base64-enoded PNG image.

  Args:
    array: A 2D np.ndarray or nested list of items.

  Returns:
    A base64-encoded string the image. The image is grayscale if the array is
    2D. The image is RGB color if the image is 3D with lsat dimension equal to
    3.

  Raises:
    ValueError: If the input `array` is not rank-2, or if the rank-2 `array` is
      empty.
  """
  # TODO(cais): Deal with 3D case.
  # TODO(cais): If there are None values in here, replace them with all NaNs.
  array = np.array(array, dtype=np.float32)
  if len(array.shape) != 2:
    raise ValueError(
        "Expected rank-2 array; received rank-%d array." % len(array.shape))
  if not np.size(array):
    raise ValueError(
        "Cannot encode an empty array (size: %s) as image." % (array.shape,))

  is_infinity = np.isinf(array)
  is_positive = array > 0.0
  is_positive_infinity = np.logical_and(is_infinity, is_positive)
  is_negative_infinity = np.logical_and(is_infinity,
                                        np.logical_not(is_positive))
  is_nan = np.isnan(array)
  finite_indices = np.where(np.logical_and(np.logical_not(is_infinity),
                                           np.logical_not(is_nan)))
  if np.size(finite_indices):
    # Finite subset is not empty.
    minval = np.min(array[finite_indices])
    maxval = np.max(array[finite_indices])
    scaled = np.array((array - minval) / (maxval - minval) * 255,
                      dtype=np.uint8)
    rgb = np.repeat(np.expand_dims(scaled, -1), IMAGE_COLOR_CHANNELS, axis=-1)
  else:
    rgb = np.zeros(array.shape + (IMAGE_COLOR_CHANNELS,), dtype=np.uint8)

  # Color-code pixels that correspond to infinities and nans.
  rgb[is_positive_infinity] = POSITIVE_INFINITY_RGB
  rgb[is_negative_infinity] = NEGATIVE_INFINITY_RGB
  rgb[is_nan] = NAN_RGB

  image_encoded = base64.b64encode(encoder.encode_png(rgb))
  return image_encoded
