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
import time

import numpy as np
import tensorflow as tf

from tensorboard.plugins.beholder import im_util
from tensorboard.plugins.beholder.file_system_tools import read_pickle,\
  write_pickle, write_file
from tensorboard.plugins.beholder.shared_config import PLUGIN_NAME, TAG_NAME,\
  SUMMARY_FILENAME, DEFAULT_CONFIG, CONFIG_FILENAME, SUMMARY_COLLECTION_KEY_NAME
from tensorboard.plugins.beholder import video_writing
from tensorboard.plugins.beholder.visualizer import Visualizer
from tensorboard.util import tb_logging


logger = tb_logging.get_logger()


class Beholder(object):

  def __init__(self, logdir):
    self.PLUGIN_LOGDIR = logdir + '/plugins/' + PLUGIN_NAME

    self.is_recording = False
    self.video_writer = video_writing.VideoWriter(
        self.PLUGIN_LOGDIR,
        outputs=[
            video_writing.FFmpegVideoOutput,
            video_writing.PNGVideoOutput])

    self.frame_placeholder = tf.compat.v1.placeholder(tf.uint8, [None, None, None])
    self.summary_op = tf.compat.v1.summary.tensor_summary(TAG_NAME,
                                                self.frame_placeholder,
                                                collections=[
                                                    SUMMARY_COLLECTION_KEY_NAME
                                                ])

    self.last_image_shape = []
    self.last_update_time = time.time()
    self.config_last_modified_time = -1
    self.previous_config = dict(DEFAULT_CONFIG)

    if not tf.io.gfile.exists(self.PLUGIN_LOGDIR + '/config.pkl'):
      tf.io.gfile.makedirs(self.PLUGIN_LOGDIR)
      write_pickle(DEFAULT_CONFIG, '{}/{}'.format(self.PLUGIN_LOGDIR,
                                                  CONFIG_FILENAME))

    self.visualizer = Visualizer(self.PLUGIN_LOGDIR)


  def _get_config(self):
    '''Reads the config file from disk or creates a new one.'''
    filename = '{}/{}'.format(self.PLUGIN_LOGDIR, CONFIG_FILENAME)
    modified_time = os.path.getmtime(filename)

    if modified_time != self.config_last_modified_time:
      config = read_pickle(filename, default=self.previous_config)
      self.previous_config = config
    else:
      config = self.previous_config

    self.config_last_modified_time = modified_time
    return config


  def _write_summary(self, session, frame):
    '''Writes the frame to disk as a tensor summary.'''
    summary = session.run(self.summary_op, feed_dict={
        self.frame_placeholder: frame
    })
    path = '{}/{}'.format(self.PLUGIN_LOGDIR, SUMMARY_FILENAME)
    write_file(summary, path)


  def _get_final_image(self, session, config, arrays=None, frame=None):
    if config['values'] == 'frames':
      if frame is None:
        final_image = im_util.get_image_relative_to_script('frame-missing.png')
      else:
        frame = frame() if callable(frame) else frame
        final_image = im_util.scale_image_for_display(frame)

    elif config['values'] == 'arrays':
      if arrays is None:
        final_image = im_util.get_image_relative_to_script('arrays-missing.png')
        # TODO: hack to clear the info. Should be cleaner.
        self.visualizer._save_section_info([], [])
      else:
        final_image = self.visualizer.build_frame(arrays)

    elif config['values'] == 'trainable_variables':
      arrays = [session.run(x) for x in tf.compat.v1.trainable_variables()]
      final_image = self.visualizer.build_frame(arrays)

    if len(final_image.shape) == 2:
      # Map grayscale images to 3D tensors.
      final_image = np.expand_dims(final_image, -1)

    return final_image


  def _enough_time_has_passed(self, FPS):
    '''For limiting how often frames are computed.'''
    if FPS == 0:
      return False
    else:
      earliest_time = self.last_update_time + (1.0 / FPS)
      return time.time() >= earliest_time


  def _update_frame(self, session, arrays, frame, config):
    final_image = self._get_final_image(session, config, arrays, frame)
    self._write_summary(session, final_image)
    self.last_image_shape = final_image.shape

    return final_image


  def _update_recording(self, frame, config):
    '''Adds a frame to the current video output.'''
    # pylint: disable=redefined-variable-type
    should_record = config['is_recording']

    if should_record:
      if not self.is_recording:
        self.is_recording = True
        logger.info(
            'Starting recording using %s',
            self.video_writer.current_output().name())
      self.video_writer.write_frame(frame)
    elif self.is_recording:
      self.is_recording = False
      self.video_writer.finish()
      logger.info('Finished recording')


  # TODO: blanket try and except for production? I don't someone's script to die
  #       after weeks of running because of a visualization.
  def update(self, session, arrays=None, frame=None):
    '''Creates a frame and writes it to disk.

    Args:
      arrays: a list of np arrays. Use the "custom" option in the client.
      frame: a 2D np array. This way the plugin can be used for video of any
             kind, not just the visualization that comes with the plugin.

             frame can also be a function, which only is evaluated when the
             "frame" option is selected by the client.
    '''
    new_config = self._get_config()

    if self._enough_time_has_passed(self.previous_config['FPS']):
      self.visualizer.update(new_config)
      self.last_update_time = time.time()
      final_image = self._update_frame(session, arrays, frame, new_config)
      self._update_recording(final_image, new_config)


  ##############################################################################

  @staticmethod
  def gradient_helper(optimizer, loss, var_list=None):
    '''A helper to get the gradients out at each step.

    Args:
      optimizer: the optimizer op.
      loss: the op that computes your loss value.

    Returns: the gradient tensors and the train_step op.
    '''
    if var_list is None:
      var_list = tf.compat.v1.trainable_variables()

    grads_and_vars = optimizer.compute_gradients(loss, var_list=var_list)
    grads = [pair[0] for pair in grads_and_vars]

    return grads, optimizer.apply_gradients(grads_and_vars)


class BeholderHook(tf.estimator.SessionRunHook):
  """SessionRunHook implementation that runs Beholder every step.

  Convenient when using tf.train.MonitoredSession:
  ```python
  beholder_hook = BeholderHook(LOG_DIRECTORY)
  with MonitoredSession(..., hooks=[beholder_hook]) as sess:
    sess.run(train_op)
  ```
  """
  def __init__(self, logdir):
    """Creates new Hook instance

    Args:
      logdir: Directory where Beholder should write data.
    """
    self._logdir = logdir
    self.beholder = None

  def begin(self):
    self.beholder = Beholder(self._logdir)

  def after_run(self, run_context, unused_run_values):
    self.beholder.update(run_context.session)
