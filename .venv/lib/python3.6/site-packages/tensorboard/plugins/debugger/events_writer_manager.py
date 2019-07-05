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
"""Manages writing debugger-related events to disk.

Creates new events files if previous files get too big.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import threading
import time

import tensorflow as tf
from tensorboard.util import tb_logging

logger = tb_logging.get_logger()

# Files containing debugger-related events should start with this string.
DEBUGGER_EVENTS_FILE_STARTING_TEXT = "events.debugger"

# A regex used to match the names of debugger-related events files.
_DEBUGGER_EVENTS_FILE_NAME_REGEX = re.compile(
    r"^" + re.escape(DEBUGGER_EVENTS_FILE_STARTING_TEXT) + r"\.\d+\.(\d+)")

# The default events file size cap (in bytes).
_DEFAULT_EVENTS_FILE_SIZE_CAP_BYTES = 500 * 1024 * 1024

# By default, we check whether the current events file has exceeded the file
# size cap every this many steps.
_DEFAULT_CHECK_EVENT_FILES_SIZE_CAP_EVERY = 1e5

# When the total size that all events on disk exceeds this value, the events
# writer manager deletes least recently created debugger-related events files.
_DEFAULT_TOTAL_SIZE_CAP_BYTES = 1e9


class EventsWriterManager(object):
  """Manages writing debugger-related events to disk.

  Creates new events writers if files get too big.
  """

  def __init__(self,
               events_directory,
               single_file_size_cap_bytes=_DEFAULT_EVENTS_FILE_SIZE_CAP_BYTES,
               check_this_often=_DEFAULT_CHECK_EVENT_FILES_SIZE_CAP_EVERY,
               total_file_size_cap_bytes=_DEFAULT_TOTAL_SIZE_CAP_BYTES,
               always_flush=False):
    """Constructs an EventsWriterManager.

    Args:
      events_directory: (`string`) The log directory in which debugger events
        reside.
      single_file_size_cap_bytes: (`int`) A number of bytes. During a check, if
        the manager determines that the events file being written to exceeds
        this size, it creates a new events file to write to. Note that events
        file may still exceed this size - the events writer manager just creates
        a new events file if it finds that the current file exceeds this size.
      check_this_often: (`int`) The manager performs a file size check every
        this many events. We want to avoid checking upon every event for
        performance reasons. If provided, must be greater than 1.
      total_file_size_cap_bytes: A cap on the total number of bytes occupied by
        all events. When a new events writer is created, the least recently
        created events file will be deleted if the total size occupied by
        debugger-related events on disk exceeds this cap. Note that the total
        size could now and then be larger than this cap because the events
        writer manager only checks when it creates a new events file.
      always_flush: (`bool`) Whether to flush to disk after every write. Useful
        for testing.
    """
    self._events_directory = events_directory
    self._single_file_size_cap_bytes = single_file_size_cap_bytes
    self.total_file_size_cap_bytes = total_file_size_cap_bytes
    self._check_this_often = check_this_often
    self._always_flush = always_flush

    # Each events file gets a unique file count within its file name. This value
    # increments every time a new events file is created.
    self._events_file_count = 0

    # If there are existing event files, assign the events file count to be
    # greater than the last existing one.
    events_file_names = self._fetch_events_files_on_disk()
    if events_file_names:
      self._events_file_count = self._obtain_file_index(
          events_file_names[-1]) + 1

    self._event_count = 0
    self._lock = threading.Lock()
    self._events_writer = self._create_events_writer(events_directory)

  def write_event(self, event):
    """Writes an event proto to disk.

    This method is threadsafe with respect to invocations of itself.

    Args:
      event: The event proto.

    Raises:
      IOError: If writing the event proto to disk fails.
    """
    self._lock.acquire()
    try:
      self._events_writer.WriteEvent(event)
      self._event_count += 1
      if self._always_flush:
        # We flush on every event within the integration test.
        self._events_writer.Flush()

      if self._event_count == self._check_this_often:
        # Every so often, we check whether the size of the file is too big.
        self._event_count = 0

        # Flush to get an accurate size check.
        self._events_writer.Flush()

        file_path = os.path.join(self._events_directory,
                                 self.get_current_file_name())
        if not tf.io.gfile.exists(file_path):
          # The events file does not exist. Perhaps the user had manually
          # deleted it after training began. Create a new one.
          self._events_writer.Close()
          self._events_writer = self._create_events_writer(
              self._events_directory)
        elif tf.io.gfile.stat(file_path).length > self._single_file_size_cap_bytes:
          # The current events file has gotten too big. Close the previous
          # events writer. Make a new one.
          self._events_writer.Close()
          self._events_writer = self._create_events_writer(
              self._events_directory)
    except IOError as err:
      logger.error(
          "Writing to %s failed: %s", self.get_current_file_name(), err)
    self._lock.release()

  def get_current_file_name(self):
    """Gets the name of the events file currently being written to.

    Returns:
      The name of the events file being written to.
    """
    return tf.compat.as_text(self._events_writer.FileName())

  def dispose(self):
    """Disposes of this events writer manager, making it no longer usable.

    Call this method when this object is done being used in order to clean up
    resources and handlers. This method should ever only be called once.
    """
    self._lock.acquire()
    self._events_writer.Close()
    self._events_writer = None
    self._lock.release()

  def _create_events_writer(self, directory):
    """Creates a new events writer.

    Args:
      directory: The directory in which to write files containing events.

    Returns:
      A new events writer, which corresponds to a new events file.
    """
    total_size = 0
    events_files = self._fetch_events_files_on_disk()
    for file_name in events_files:
      file_path = os.path.join(self._events_directory, file_name)
      total_size += tf.io.gfile.stat(file_path).length

    if total_size >= self.total_file_size_cap_bytes:
      # The total size written to disk is too big. Delete events files until
      # the size is below the cap.
      for file_name in events_files:
        if total_size < self.total_file_size_cap_bytes:
          break

        file_path = os.path.join(self._events_directory, file_name)
        file_size = tf.io.gfile.stat(file_path).length
        try:
          tf.io.gfile.remove(file_path)
          total_size -= file_size
          logger.info(
              "Deleted %s because events files take up over %d bytes",
              file_path, self.total_file_size_cap_bytes)
        except IOError as err:
          logger.error("Deleting %s failed: %s", file_path, err)

    # We increment this index because each events writer must differ in prefix.
    self._events_file_count += 1
    file_path = "%s.%d.%d" % (
        os.path.join(directory, DEBUGGER_EVENTS_FILE_STARTING_TEXT),
        time.time(), self._events_file_count)
    logger.info("Creating events file %s", file_path)
    return tf.compat.v1.pywrap_tensorflow.EventsWriter(tf.compat.as_bytes(file_path))

  def _fetch_events_files_on_disk(self):
    """Obtains the names of debugger-related events files within the directory.

    Returns:
      The names of the debugger-related events files written to disk. The names
      are sorted in increasing events file index.
    """
    all_files = tf.io.gfile.listdir(self._events_directory)
    relevant_files = [
        file_name for file_name in all_files
        if _DEBUGGER_EVENTS_FILE_NAME_REGEX.match(file_name)
    ]
    return sorted(relevant_files, key=self._obtain_file_index)

  def _obtain_file_index(self, file_name):
    """Obtains the file index associated with an events file.

    The index is stored within a file name and is incremented every time a new
    events file is created. Assumes that the file name is a valid debugger
    events file name.

    Args:
      file_name: The name of the debugger-related events file. The file index is
          stored within the file name.

    Returns:
      The integer events file index.
    """
    return int(_DEBUGGER_EVENTS_FILE_NAME_REGEX.match(file_name).group(1))
