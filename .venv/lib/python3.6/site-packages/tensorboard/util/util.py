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

"""TensorBoard helper routine module.

This module is a trove of succinct generic helper routines that don't
pull in any heavyweight dependencies.

Only pulls in TensorFlow as a dependency if requested per build rules.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import locale
import logging
import os
import re
import sys
import time

from absl import logging as absl_logging
import six

from tensorboard.compat import tf
from tensorboard.util import tb_logging

logger = tb_logging.get_logger()


# TODO(stephanwlee): Move this to program.py
def setup_logging():
  """Configures Python logging the way the TensorBoard team likes it.

  This should be called exactly once at the beginning of main().
  """
  # TODO(stephanwlee): Check the flag passed from CLI and set it to WARN only
  # it was not explicitly set
  absl_logging.set_verbosity(absl_logging.WARN)


def closeable(class_):
  """Makes a class with a close method able to be a context manager.

  This decorator is a great way to avoid having to choose between the
  boilerplate of __enter__ and __exit__ methods, versus the boilerplate
  of using contextlib.closing on every with statement.

  Args:
    class_: The class being decorated.

  Raises:
    ValueError: If class didn't have a close method, or already
        implements __enter__ or __exit__.
  """
  if 'close' not in class_.__dict__:
    # coffee is for closers
    raise ValueError('Class does not define a close() method: %s' % class_)
  if '__enter__' in class_.__dict__ or '__exit__' in class_.__dict__:
    raise ValueError('Class already defines __enter__ or __exit__: ' + class_)
  class_.__enter__ = lambda self: self
  class_.__exit__ = lambda self, t, v, b: self.close() and None
  return class_


def close_all(resources):
  """Safely closes multiple resources.

  The close method on all resources is guaranteed to be called. If
  multiple close methods throw exceptions, then the first will be
  raised and the rest will be logged.

  Args:
    resources: An iterable of object instances whose classes implement
        the close method.

  Raises:
    Exception: To rethrow the last exception raised by a close method.
  """
  exc_info = None
  for resource in resources:
    try:
      resource.close()
    except Exception as e:  # pylint: disable=broad-except
      if exc_info is not None:
        logger.error('Suppressing close(%s) failure: %s', resource, e,
                         exc_info=exc_info)
      exc_info = sys.exc_info()
  if exc_info is not None:
    six.reraise(*exc_info)


def guarded_by(field):
  """Indicates method should be called from within a lock.

  This decorator is purely for documentation purposes. It has the same
  semantics as Java's @GuardedBy annotation.

  Args:
    field: The string name of the lock field, e.g. "_lock".
  """
  del field
  return lambda method: method


class Retrier(object):
  """Helper class for retrying things with exponential back-off."""

  DELAY = 0.1

  def __init__(self, is_transient, max_attempts=8, sleep=time.sleep):
    """Creates new instance.

    :type is_transient: (Exception) -> bool
    :type max_attempts: int
    :type sleep: (float) -> None
    """
    self._is_transient = is_transient
    self._max_attempts = max_attempts
    self._sleep = sleep

  def run(self, callback):
    """Invokes callback, retrying on transient exceptions.

    After the first failure, we wait 100ms, and then double with each
    subsequent failed attempt. The default max attempts is 8 which
    equates to about thirty seconds of sleeping total.

    :type callback: () -> T
    :rtype: T
    """
    failures = 0
    while True:
      try:
        return callback()
      except Exception as e:  # pylint: disable=broad-except
        failures += 1
        if failures == self._max_attempts or not self._is_transient(e):
          raise
        logger.warn('Retrying on transient %s', e)
        self._sleep(2 ** (failures - 1) * Retrier.DELAY)


class LogFormatter(logging.Formatter):
  """Google style log formatter.

  The format is in essence the following:

      [DIWEF]mmdd hh:mm:ss.uuuuuu thread_name file:line] msg

  This class is meant to be used with LogHandler.
  """

  DATE_FORMAT = '%m%d %H:%M:%S'
  LOG_FORMAT = ('%(levelname)s%(asctime)s %(threadName)s '
                '%(filename)s:%(lineno)d] %(message)s')

  LEVEL_NAMES = {
      logging.FATAL: 'F',
      logging.ERROR: 'E',
      logging.WARN: 'W',
      logging.INFO: 'I',
      logging.DEBUG: 'D',
  }

  def __init__(self):
    """Creates new instance."""
    super(LogFormatter, self).__init__(LogFormatter.LOG_FORMAT,
                                       LogFormatter.DATE_FORMAT)

  def format(self, record):
    """Formats the log record.

    :type record: logging.LogRecord
    :rtype: str
    """
    record.levelname = LogFormatter.LEVEL_NAMES[record.levelno]
    return super(LogFormatter, self).format(record)

  def formatTime(self, record, datefmt=None):
    """Return creation time of the specified LogRecord as formatted text.

    This override adds microseconds.

    :type record: logging.LogRecord
    :rtype: str
    """
    return (super(LogFormatter, self).formatTime(record, datefmt) +
            '.%06d' % (record.created * 1e6 % 1e6))

# TODO(stephanwlee): Remove ANSI after removing loader.py
class Ansi(object):
  """ANSI terminal codes container."""

  ESCAPE = '\x1b['
  ESCAPE_PATTERN = re.compile(re.escape(ESCAPE) + r'\??(?:\d+)(?:;\d+)*[mlh]')
  RESET = ESCAPE + '0m'
  BOLD = ESCAPE + '1m'
  FLIP = ESCAPE + '7m'
  RED = ESCAPE + '31m'
  YELLOW = ESCAPE + '33m'
  MAGENTA = ESCAPE + '35m'
  CURSOR_HIDE = ESCAPE + '?25l'
  CURSOR_SHOW = ESCAPE + '?25h'


# TODO(stephanwlee): Remove LogHandler after removing loader.py
class LogHandler(logging.StreamHandler):
  """Log handler that supports ANSI colors and ephemeral records.

  Colors are applied on a line-by-line basis to non-INFO records. The
  goal is to help the user visually distinguish meaningful information,
  even when logging is verbose.

  This handler will also strip ANSI color codes from emitted log
  records automatically when the output stream is not a terminal.

  Ephemeral log records are only emitted to a teletype emulator, only
  display on the final row, and get overwritten as soon as another
  ephemeral record is outputted. Ephemeral records are also sticky. If
  a normal record is written then the previous ephemeral record is
  restored right beneath it. When an ephemeral record with an empty
  message is emitted, then the last ephemeral record turns into a
  normal record and is allowed to spool.

  This class is thread safe.
  """

  EPHEMERAL = '.ephemeral'  # Name suffix for ephemeral loggers.

  COLORS = {
      logging.FATAL: Ansi.BOLD + Ansi.RED,
      logging.ERROR: Ansi.RED,
      logging.WARN: Ansi.YELLOW,
      logging.INFO: '',
      logging.DEBUG: Ansi.MAGENTA,
  }

  def __init__(self, stream, type_='detect'):
    """Creates new instance.

    Args:
      stream: A file-like object.
      type_: If "detect", will call stream.isatty() and perform system
          checks to determine if it's safe to output ANSI terminal
          codes. If type is "ansi" then this forces the use of ANSI
          terminal codes.

    Raises:
      ValueError: If type is not "detect" or "ansi".
    """
    if type_ not in ('detect', 'ansi'):
      raise ValueError('type should be detect or ansi')
    super(LogHandler, self).__init__(stream)
    self._stream = stream
    self._disable_flush = False
    self._is_tty = (type_ == 'ansi' or
                    (hasattr(stream, 'isatty') and
                     stream.isatty() and
                     os.name != 'nt'))
    self._ephemeral = ''

  def emit(self, record):
    """Emits a log record.

    :type record: logging.LogRecord
    """
    self.acquire()
    try:
      is_ephemeral = record.name.endswith(LogHandler.EPHEMERAL)
      color = LogHandler.COLORS.get(record.levelno)
      if is_ephemeral:
        if self._is_tty:
          ephemeral = record.getMessage()
          if ephemeral:
            if color:
              ephemeral = color + ephemeral + Ansi.RESET
            self._clear_line()
            self._stream.write(ephemeral)
          else:
            if self._ephemeral:
              self._stream.write('\n')
          self._ephemeral = ephemeral
      else:
        self._clear_line()
        if self._is_tty and color:
          self._stream.write(color)
        self._disable_flush = True  # prevent double flush
        super(LogHandler, self).emit(record)
        self._disable_flush = False
        if self._is_tty and color:
          self._stream.write(Ansi.RESET)
        if self._ephemeral:
          self._stream.write(self._ephemeral)
      self.flush()
    finally:
      self._disable_flush = False
      self.release()

  def format(self, record):
    """Turns a log record into a string.

    :type record: logging.LogRecord
    :rtype: str
    """
    message = super(LogHandler, self).format(record)
    if not self._is_tty:
      message = Ansi.ESCAPE_PATTERN.sub('', message)
    return message

  def flush(self):
    """Flushes output stream."""
    self.acquire()
    try:
      if not self._disable_flush:
        super(LogHandler, self).flush()
    finally:
      self.release()

  def _clear_line(self):
    if self._is_tty and self._ephemeral:
      # We're counting columns in the terminal, not bytes. So we don't
      # want to take UTF-8 or color codes into consideration.
      text = Ansi.ESCAPE_PATTERN.sub('', tf.compat.as_text(self._ephemeral))
      self._stream.write('\r' + ' ' * len(text) + '\r')
