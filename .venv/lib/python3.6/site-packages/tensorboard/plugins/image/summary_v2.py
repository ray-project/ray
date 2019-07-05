# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Image summaries and TensorFlow operations to create them, V2 versions.

An image summary stores the width, height, and PNG-encoded data for zero
or more images in a rank-1 string array: `[w, h, png0, png1, ...]`.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorboard.compat import tf2 as tf
from tensorboard.plugins.image import metadata


def image(name,
          data,
          step,
          max_outputs=3,
          description=None):
  """Write an image summary.

  Arguments:
    name: A name for this summary. The summary tag used for TensorBoard will
      be this name prefixed by any active name scopes.
    data: A `Tensor` representing pixel data with shape `[k, h, w, c]`,
      where `k` is the number of images, `h` and `w` are the height and
      width of the images, and `c` is the number of channels, which
      should be 1, 2, 3, or 4 (grayscale, grayscale with alpha, RGB, RGBA).
      Any of the dimensions may be statically unknown (i.e., `None`).
      Floating point data will be clipped to the range [0,1).
    step: Required `int64`-castable monotonic step value.
    max_outputs: Optional `int` or rank-0 integer `Tensor`. At most this
      many images will be emitted at each step. When more than
      `max_outputs` many images are provided, the first `max_outputs` many
      images will be used and the rest silently discarded.
    description: Optional long-form description for this summary, as a
      constant `str`. Markdown is supported. Defaults to empty.

  Returns:
    True on success, or false if no summary was emitted because no default
    summary writer was available.
  """
  summary_metadata = metadata.create_summary_metadata(
      display_name=None, description=description)
  with tf.summary.summary_scope(
      name, 'image_summary', values=[data, max_outputs, step]) as (tag, _):
    tf.debugging.assert_rank(data, 4)
    tf.debugging.assert_non_negative(max_outputs)
    images = tf.image.convert_image_dtype(data, tf.uint8, saturate=True)
    limited_images = images[:max_outputs]
    encoded_images = tf.map_fn(tf.image.encode_png, limited_images,
                               dtype=tf.string,
                               name='encode_each_image')
    # Workaround for map_fn returning float dtype for an empty elems input.
    encoded_images = tf.cond(
        tf.shape(input=encoded_images)[0] > 0,
        lambda: encoded_images, lambda: tf.constant([], tf.string))
    image_shape = tf.shape(input=images)
    dimensions = tf.stack([tf.as_string(image_shape[2], name='width'),
                           tf.as_string(image_shape[1], name='height')],
                          name='dimensions')
    tensor = tf.concat([dimensions, encoded_images], axis=0)
    return tf.summary.write(
        tag=tag, tensor=tensor, step=step, metadata=summary_metadata)
