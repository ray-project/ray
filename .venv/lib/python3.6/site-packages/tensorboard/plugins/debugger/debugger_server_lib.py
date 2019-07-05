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
"""Receives data from a TensorFlow debugger. Writes event summaries.

This listener server writes debugging-related events into a logdir directory,
from which a TensorBoard instance can read.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import json
import os
import re
import threading
import time

import tensorflow as tf
from tensorflow.python.debug.lib import grpc_debug_server

from tensorboard.plugins.debugger import constants
# pylint: disable=line-too-long
from tensorboard.plugins.debugger import events_writer_manager as events_writer_manager_lib
# pylint: enable=line-too-long
from tensorboard.plugins.debugger import numerics_alert
from tensorboard.util import tb_logging
from tensorboard.util import tensor_util

logger = tb_logging.get_logger()


class DebuggerDataStreamHandler(
    grpc_debug_server.EventListenerBaseStreamHandler):
  """Implementation of stream handler for debugger data.

  Each instance of this class is created by a DebuggerDataServer upon a
  gRPC stream established between the debugged Session::Run() invocation in
  TensorFlow core runtime and the DebuggerDataServer instance.

  Each instance of this class does the following:
    1) receives a core metadata Event proto during its constructor call.
    2) receives GraphDef Event proto(s) through its on_graph_event method.
    3) receives tensor value Event proto(s) through its on_value_event method.
  """

  def __init__(self,
               events_writer_manager,
               numerics_alert_callback=None):
    """Constructor of DebuggerDataStreamHandler.

    Args:
      events_writer_manager: Manages writing events to disk.
      numerics_alert_callback: An optional callback run every time a health pill
        event with bad values (Nan, -Inf, or +Inf) is received. The callback
        takes the event as a parameter.
    """
    super(DebuggerDataStreamHandler, self).__init__()
    self._events_writer_manager = events_writer_manager
    self._numerics_alert_callback = numerics_alert_callback

    # We use session_run_index as the "step" value for debugger events because
    # it is unique across all runs. It is not specific to a set of feeds and
    # fetches.
    self._session_run_index = -1

  def on_core_metadata_event(self, event):
    """Implementation of the core metadata-carrying Event proto callback.

    Args:
      event: An Event proto that contains core metadata about the debugged
        Session::Run() in its log_message.message field, as a JSON string.
        See the doc string of debug_data.DebugDumpDir.core_metadata for details.
    """
    self._session_run_index = self._parse_session_run_index(event)

  def on_graph_def(self, graph_def, device_name, wall_time):
    """Implementation of the GraphDef-carrying Event proto callback.

    Args:
      graph_def: A GraphDef proto. N.B.: The GraphDef is from
        the core runtime of a debugged Session::Run() call, after graph
        partition. Therefore it may differ from the GraphDef available to
        the general TensorBoard. For example, the GraphDef in general
        TensorBoard may get partitioned for multiple devices (CPUs and GPUs),
        each of which will generate a GraphDef event proto sent to this
        method.
      device_name: Name of the device on which the graph was created.
      wall_time: An epoch timestamp (in microseconds) for the graph.
    """
    # For now, we do nothing with the graph def. However, we must define this
    # method to satisfy the handler's interface. Furthermore, we may use the
    # graph in the future (for instance to provide a graph if there is no graph
    # provided otherwise).
    del device_name
    del wall_time
    del graph_def

  def on_value_event(self, event):
    """Records the summary values based on an updated message from the debugger.

    Logs an error message if writing the event to disk fails.

    Args:
      event: The Event proto to be processed.
    """
    if not event.summary.value:
      logger.warn("The summary of the event lacks a value.")
      return

    # The node name property is actually a watch key, which is a concatenation
    # of several pieces of data.
    watch_key = event.summary.value[0].node_name
    if not watch_key.endswith(constants.DEBUG_NUMERIC_SUMMARY_SUFFIX):
      # Ignore events that lack a DebugNumericSummary.
      # NOTE(@chihuahua): We may later handle other types of debug ops.
      return

    # We remove the constants.DEBUG_NUMERIC_SUMMARY_SUFFIX from the end of the
    # watch name because it is not distinguishing: every health pill entry ends
    # with it.
    node_name_and_output_slot = watch_key[
        :-len(constants.DEBUG_NUMERIC_SUMMARY_SUFFIX)]

    shape = tensor_util.make_ndarray(event.summary.value[0].tensor).shape
    if (len(shape) != 1 or
        shape[0] < constants.MIN_DEBUG_NUMERIC_SUMMARY_TENSOR_LENGTH):
      logger.warn("Health-pill tensor either lacks a dimension or is "
                         "shaped incorrectly: %s" % shape)
      return

    match = re.match(r"^(.*):(\d+)$", node_name_and_output_slot)
    if not match:
      logger.warn(
          ("A event with a health pill has an invalid node name and output "
           "slot combination, (i.e., an unexpected debug op): %r"),
          node_name_and_output_slot)
      return

    if self._session_run_index >= 0:
      event.step = self._session_run_index
    else:
      # Data from parameter servers (or any graphs without a master) do not
      # contain core metadata. So the session run count is missing. Set its
      # value to a microsecond epoch timestamp.
      event.step = int(time.time() * 1e6)

    # Write this event to the events file designated for data from the
    # debugger.
    self._events_writer_manager.write_event(event)

    alert = numerics_alert.extract_numerics_alert(event)
    if self._numerics_alert_callback and alert:
      self._numerics_alert_callback(alert)

  def _parse_session_run_index(self, event):
    """Parses the session_run_index value from the event proto.

    Args:
      event: The event with metadata that contains the session_run_index.

    Returns:
      The int session_run_index value. Or
      constants.SENTINEL_FOR_UNDETERMINED_STEP if it could not be determined.
    """
    metadata_string = event.log_message.message
    try:
      metadata = json.loads(metadata_string)
    except ValueError as e:
      logger.error(
          "Could not decode metadata string '%s' for step value: %s",
          metadata_string, e)
      return constants.SENTINEL_FOR_UNDETERMINED_STEP

    try:
      return metadata["session_run_index"]
    except KeyError:
      logger.error(
          "The session_run_index is missing from the metadata: %s",
          metadata_string)
      return constants.SENTINEL_FOR_UNDETERMINED_STEP


class DebuggerDataServer(grpc_debug_server.EventListenerBaseServicer):
  """A service that receives and writes debugger data such as health pills.
  """

  def __init__(self,
               receive_port,
               logdir,
               always_flush=False):
    """Receives health pills from a debugger and writes them to disk.

    Args:
      receive_port: The port at which to receive health pills from the
        TensorFlow debugger.
      logdir: The directory in which to write events files that TensorBoard will
        read.
      always_flush: A boolean indicating whether the EventsWriter will be
        flushed after every write. Can be used for testing.
    """
    # We create a special directory within logdir to store debugger-related
    # events (if that directory does not already exist). This is necessary
    # because for each directory within logdir, TensorBoard only reads through
    # each events file once. There may be other non-debugger events files being
    # written to at the same time. Without this special directory, TensorBoard
    # may stop surfacing health pills after some arbitrary step value.
    debugger_directory = os.path.join(
        os.path.expanduser(logdir), constants.DEBUGGER_DATA_DIRECTORY_NAME)

    if not tf.io.gfile.exists(debugger_directory):
      try:
        tf.io.gfile.makedirs(debugger_directory)
        logger.info("Created directory for debugger data: %s",
                        debugger_directory)
      except tf.errors.OpError as e:
        logger.fatal(
            "Could not make directory for debugger data: %s. Error: %s",
            debugger_directory, e)

    self._events_writer_manager = events_writer_manager_lib.EventsWriterManager(
        events_directory=debugger_directory,
        always_flush=always_flush)

    # Write an event with a file version as the first event within the events
    # file. If the event version is 2, TensorBoard uses a path for purging
    # events that does not depend on step. This is important because debugger
    # events use a notion of step that differs from that of the rest of
    # TensorBoard.
    try:
      self._events_writer_manager.write_event(
          tf.Event(
              wall_time=0, step=0, file_version=constants.EVENTS_VERSION))
    except IOError as e:
      logger.error(
          "Writing to %s failed: %s",
          self._events_writer_manager.get_current_file_name(), e)

    # See if a backup file exists. If so, use it to initialize the registry.
    self._registry_backup_file_path = os.path.join(
        debugger_directory, constants.ALERT_REGISTRY_BACKUP_FILE_NAME)
    initial_data = None

    if tf.io.gfile.exists(self._registry_backup_file_path):
      # A backup file exists. Read its contents to use for initialization.
      with tf.io.gfile.GFile(self._registry_backup_file_path, "r") as backup_file:
        try:
          # Use the data to initialize the registry.
          initial_data = json.load(backup_file)
        except ValueError as err:
          # Could not parse the data. No backup data obtained.
          logger.error(
              "Could not parse contents of %s: %s",
              self._registry_backup_file_path, err)

    self._numerics_alert_registry = numerics_alert.NumericsAlertRegistry(
        initialization_list=initial_data)

    self._numerics_alert_lock = threading.Lock()
    curried_handler_constructor = functools.partial(
        DebuggerDataStreamHandler,
        self._events_writer_manager,
        self._numerics_alert_callback)
    grpc_debug_server.EventListenerBaseServicer.__init__(
        self, receive_port, curried_handler_constructor)

  def start_the_debugger_data_receiving_server(self):
    """Starts the HTTP server for receiving health pills at `receive_port`.

    After this method is called, health pills issued to host:receive_port
    will be stored by this object. Calling this method also creates a file
    within the log directory for storing health pill summary events.
    """
    self.run_server()

  def get_events_file_name(self):
    """Gets the name of the debugger events file currently being written to.

    Returns:
      The string name of the debugger events file currently being written to.
      This is just the name of that file, not the full path to that file.
    """
    return self._events_writer_manager.get_current_file_name()

  def _numerics_alert_callback(self, alert):
    """Handles the case in which we receive a bad value (NaN, -/+ Inf).

    Args:
      alert: The alert to be registered.
    """
    with self._numerics_alert_lock:
      self._numerics_alert_registry.register(alert)

  def numerics_alert_report(self):
    """Get a report of the numerics alerts that have occurred.

    Returns:
      A list of `numerics_alert.NumericsAlertReportRow`, sorted in ascending
        order of first_timestamp.
    """
    with self._numerics_alert_lock:
      return self._numerics_alert_registry.report()

  def dispose(self):
    """Disposes of this object. Call only after this is done being used."""
    self._events_writer_manager.dispose()
