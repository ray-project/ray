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

from collections import deque
from math import floor, sqrt

import numpy as np
import tensorflow as tf

from tensorboard.plugins.beholder import im_util
from tensorboard.plugins.beholder.shared_config import SECTION_HEIGHT,\
  IMAGE_WIDTH, DEFAULT_CONFIG, SECTION_INFO_FILENAME
from tensorboard.plugins.beholder.file_system_tools import write_pickle

MIN_SQUARE_SIZE = 3


class Visualizer(object):

  def __init__(self, logdir):
    self.logdir = logdir
    self.sections_over_time = deque([], DEFAULT_CONFIG['window_size'])
    self.config = dict(DEFAULT_CONFIG)
    self.old_config = dict(DEFAULT_CONFIG)


  def _reshape_conv_array(self, array, section_height, image_width):
    '''Reshape a rank 4 array to be rank 2, where each column of block_width is
    a filter, and each row of block height is an input channel. For example:

    [[[[ 11,  21,  31,  41],
       [ 51,  61,  71,  81],
       [ 91, 101, 111, 121]],
      [[ 12,  22,  32,  42],
       [ 52,  62,  72,  82],
       [ 92, 102, 112, 122]],
      [[ 13,  23,  33,  43],
       [ 53,  63,  73,  83],
       [ 93, 103, 113, 123]]],
     [[[ 14,  24,  34,  44],
       [ 54,  64,  74,  84],
       [ 94, 104, 114, 124]],
      [[ 15,  25,  35,  45],
       [ 55,  65,  75,  85],
       [ 95, 105, 115, 125]],
      [[ 16,  26,  36,  46],
       [ 56,  66,  76,  86],
       [ 96, 106, 116, 126]]],
     [[[ 17,  27,  37,  47],
       [ 57,  67,  77,  87],
       [ 97, 107, 117, 127]],
      [[ 18,  28,  38,  48],
       [ 58,  68,  78,  88],
       [ 98, 108, 118, 128]],
      [[ 19,  29,  39,  49],
       [ 59,  69,  79,  89],
       [ 99, 109, 119, 129]]]]

       should be reshaped to:

       [[ 11,  12,  13,  21,  22,  23,  31,  32,  33,  41,  42,  43],
        [ 14,  15,  16,  24,  25,  26,  34,  35,  36,  44,  45,  46],
        [ 17,  18,  19,  27,  28,  29,  37,  38,  39,  47,  48,  49],
        [ 51,  52,  53,  61,  62,  63,  71,  72,  73,  81,  82,  83],
        [ 54,  55,  56,  64,  65,  66,  74,  75,  76,  84,  85,  86],
        [ 57,  58,  59,  67,  68,  69,  77,  78,  79,  87,  88,  89],
        [ 91,  92,  93, 101, 102, 103, 111, 112, 113, 121, 122, 123],
        [ 94,  95,  96, 104, 105, 106, 114, 115, 116, 124, 125, 126],
        [ 97,  98,  99, 107, 108, 109, 117, 118, 119, 127, 128, 129]]
    '''

    # E.g. [100, 24, 24, 10]: this shouldn't be reshaped like normal.
    if array.shape[1] == array.shape[2] and array.shape[0] != array.shape[1]:
      array = np.rollaxis(np.rollaxis(array, 2), 2)

    block_height, block_width, in_channels = array.shape[:3]
    rows = []

    max_element_count = section_height * int(image_width / MIN_SQUARE_SIZE)
    element_count = 0

    for i in range(in_channels):
      rows.append(array[:, :, i, :].reshape(block_height, -1, order='F'))

      # This line should be left in this position. Gives it one extra row.
      if element_count >= max_element_count and not self.config['show_all']:
        break

      element_count += block_height * in_channels * block_width

    return np.vstack(rows)


  def _reshape_irregular_array(self, array, section_height, image_width):
    '''Reshapes arrays of ranks not in {1, 2, 4}
    '''
    section_area = section_height * image_width
    flattened_array = np.ravel(array)

    if not self.config['show_all']:
      flattened_array = flattened_array[:int(section_area/MIN_SQUARE_SIZE)]

    cell_count = np.prod(flattened_array.shape)
    cell_area = section_area / cell_count

    cell_side_length = max(1, floor(sqrt(cell_area)))
    row_count = max(1, int(section_height / cell_side_length))
    col_count = int(cell_count / row_count)

    # Reshape the truncated array so that it has the same aspect ratio as
    # the section.

    # Truncate whatever remaining values there are that don't fit. Hopefully
    # it doesn't matter that the last few (< section count) aren't there.
    section = np.reshape(flattened_array[:row_count * col_count],
                         (row_count, col_count))

    return section


  def _determine_image_width(self, arrays, show_all):
    final_width = IMAGE_WIDTH

    if show_all:
      for array in arrays:
        rank = len(array.shape)

        if rank == 1:
          width = len(array)
        elif rank == 2:
          width = array.shape[1]
        elif rank == 4:
          width = array.shape[1] * array.shape[3]
        else:
          width = IMAGE_WIDTH

        if width > final_width:
          final_width = width

    return final_width


  def _determine_section_height(self, array, show_all):
    rank = len(array.shape)
    height = SECTION_HEIGHT

    if show_all:
      if rank == 1:
        height = SECTION_HEIGHT
      if rank == 2:
        height = max(SECTION_HEIGHT, array.shape[0])
      elif rank == 4:
        height = max(SECTION_HEIGHT, array.shape[0] * array.shape[2])
      else:
        height = max(SECTION_HEIGHT, np.prod(array.shape) // IMAGE_WIDTH)

    return height


  def _arrays_to_sections(self, arrays):
    '''
    input: unprocessed numpy arrays.
    returns: columns of the size that they will appear in the image, not scaled
             for display. That needs to wait until after variance is computed.
    '''
    sections = []
    sections_to_resize_later = {}
    show_all = self.config['show_all']
    image_width = self._determine_image_width(arrays, show_all)

    for array_number, array in enumerate(arrays):
      rank = len(array.shape)
      section_height = self._determine_section_height(array, show_all)

      if rank == 1:
        section = np.atleast_2d(array)
      elif rank == 2:
        section = array
      elif rank == 4:
        section = self._reshape_conv_array(array, section_height, image_width)
      else:
        section = self._reshape_irregular_array(array,
                                                section_height,
                                                image_width)
      # Only calculate variance for what we have to. In some cases (biases),
      # the section is larger than the array, so we don't want to calculate
      # variance for the same value over and over - better to resize later.
      # About a 6-7x speedup for a big network with a big variance window.
      section_size = section_height * image_width
      array_size = np.prod(array.shape)

      if section_size > array_size:
        sections.append(section)
        sections_to_resize_later[array_number] = section_height
      else:
        sections.append(im_util.resize(section, section_height, image_width))

    self.sections_over_time.append(sections)

    if self.config['mode'] == 'variance':
      sections = self._sections_to_variance_sections(self.sections_over_time)

    for array_number, height in sections_to_resize_later.items():
      sections[array_number] = im_util.resize(sections[array_number],
                                              height,
                                              image_width)
    return sections


  def _sections_to_variance_sections(self, sections_over_time):
    '''Computes the variance of corresponding sections over time.

    Returns:
      a list of np arrays.
    '''
    variance_sections = []

    for i in range(len(sections_over_time[0])):
      time_sections = [sections[i] for sections in sections_over_time]
      variance = np.var(time_sections, axis=0)
      variance_sections.append(variance)

    return variance_sections


  def _sections_to_image(self, sections):
    padding_size = 5

    sections = im_util.scale_sections(sections, self.config['scaling'])

    final_stack = [sections[0]]
    padding = np.zeros((padding_size, sections[0].shape[1]))

    for section in sections[1:]:
      final_stack.append(padding)
      final_stack.append(section)

    return np.vstack(final_stack).astype(np.uint8)


  def _maybe_clear_deque(self):
    '''Clears the deque if certain parts of the config have changed.'''

    for config_item in ['values', 'mode', 'show_all']:
      if self.config[config_item] != self.old_config[config_item]:
        self.sections_over_time.clear()
        break

    self.old_config = self.config

    window_size = self.config['window_size']
    if window_size != self.sections_over_time.maxlen:
      self.sections_over_time = deque(self.sections_over_time, window_size)


  def _save_section_info(self, arrays, sections):
    infos = []

    if self.config['values'] == 'trainable_variables':
      names = [x.name for x in tf.compat.v1.trainable_variables()]
    else:
      names = range(len(arrays))

    for array, section, name in zip(arrays, sections, names):
      info = {}

      info['name'] = name
      info['shape'] = str(array.shape)
      info['min'] = '{:.3e}'.format(section.min())
      info['mean'] = '{:.3e}'.format(section.mean())
      info['max'] = '{:.3e}'.format(section.max())
      info['range'] = '{:.3e}'.format(section.max() - section.min())
      info['height'] = section.shape[0]

      infos.append(info)

    write_pickle(infos, '{}/{}'.format(self.logdir, SECTION_INFO_FILENAME))


  def build_frame(self, arrays):
    self._maybe_clear_deque()

    arrays = arrays if isinstance(arrays, list) else [arrays]

    sections = self._arrays_to_sections(arrays)
    self._save_section_info(arrays, sections)
    final_image = self._sections_to_image(sections)
    final_image = im_util.apply_colormap(final_image, self.config['colormap'])

    return final_image

  def update(self, config):
    self.config = config
