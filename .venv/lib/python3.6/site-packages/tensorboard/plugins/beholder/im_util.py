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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import numpy as np
import tensorflow as tf

from tensorboard.util import encoder
from tensorboard.util import op_evaluator
from tensorboard.plugins.beholder import colormaps


# pylint: disable=not-context-manager

def global_extrema(arrays):
  return min([x.min() for x in arrays]), max([x.max() for x in arrays])


def scale_sections(sections, scaling_scope):
  '''
  input: unscaled sections.
  returns: sections scaled to [0, 255]
  '''
  new_sections = []

  if scaling_scope == 'layer':
    for section in sections:
      new_sections.append(scale_image_for_display(section))

  elif scaling_scope == 'network':
    global_min, global_max = global_extrema(sections)

    for section in sections:
      new_sections.append(scale_image_for_display(section,
                                                  global_min,
                                                  global_max))
  return new_sections


def scale_image_for_display(image, minimum=None, maximum=None):
  image = image.astype(float)

  minimum = image.min() if minimum is None else minimum
  image -= minimum

  maximum = image.max() if maximum is None else maximum

  if maximum == 0:
    return image
  else:
    image *= 255 / maximum
    return image.astype(np.uint8)


def pad_to_shape(array, shape, constant=245):
  padding = []

  for actual_dim, target_dim in zip(array.shape, shape):
    start_padding = 0
    end_padding = target_dim - actual_dim

    padding.append((start_padding, end_padding))

  return np.pad(array, padding, mode='constant', constant_values=constant)


def apply_colormap(image, colormap='magma'):
  if colormap == 'grayscale':
    return image
  cm = getattr(colormaps, colormap)
  return image if cm is None else cm[image]


class PNGDecoder(op_evaluator.PersistentOpEvaluator):

  def __init__(self):
    super(PNGDecoder, self).__init__()
    self._image_placeholder = None
    self._decode_op = None


  def initialize_graph(self):
    self._image_placeholder = tf.compat.v1.placeholder(dtype=tf.string)
    self._decode_op = tf.image.decode_png(self._image_placeholder)


  # pylint: disable=arguments-differ
  def run(self, image):
    return self._decode_op.eval(feed_dict={
        self._image_placeholder: image,
    })


class Resizer(op_evaluator.PersistentOpEvaluator):

  def __init__(self):
    super(Resizer, self).__init__()
    self._image_placeholder = None
    self._size_placeholder = None
    self._resize_op = None


  def initialize_graph(self):
    self._image_placeholder = tf.compat.v1.placeholder(dtype=tf.float32)
    self._size_placeholder = tf.compat.v1.placeholder(dtype=tf.int32)
    self._resize_op = tf.image.resize(self._image_placeholder,
                                      self._size_placeholder,
                                      method=tf.image.ResizeMethod.NEAREST_NEIGHBOR)

  # pylint: disable=arguments-differ
  def run(self, image, height, width):
    if len(image.shape) == 2:
      image = image.reshape([image.shape[0], image.shape[1], 1])

    resized = np.squeeze(self._resize_op.eval(feed_dict={
        self._image_placeholder: [image],
        self._size_placeholder: [height, width]
    }))

    return resized


decode_png = PNGDecoder()
resize = Resizer()

def read_image(filename):
  with tf.io.gfile.GFile(filename, 'rb') as image_file:
    return np.array(decode_png(image_file.read()))


def write_image(array, filename):
  with tf.io.gfile.GFile(filename, 'w') as image_file:
    image_file.write(encoder.encode_png(array))


def get_image_relative_to_script(filename):
  script_directory = os.path.dirname(__file__)
  filename = os.path.join(script_directory, 'resources', filename)

  return read_image(filename)
