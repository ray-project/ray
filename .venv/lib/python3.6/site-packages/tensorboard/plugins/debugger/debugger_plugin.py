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
"""The plugin for serving data from a TensorFlow debugger."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import glob
import json
import os
import re
import sys
import threading

import tensorflow as tf
from werkzeug import wrappers

from tensorboard.backend import http_util
from tensorboard.backend.event_processing import event_file_loader
from tensorboard.plugins import base_plugin
from tensorboard.plugins.debugger import constants
from tensorboard.plugins.debugger import debugger_server_lib
from tensorboard.util import tb_logging
from tensorboard.util import tensor_util

logger = tb_logging.get_logger()

# HTTP routes.
_HEALTH_PILLS_ROUTE = '/health_pills'
_NUMERICS_ALERT_REPORT_ROUTE = '/numerics_alert_report'

# The POST key of HEALTH_PILLS_ROUTE for a JSON list of node names.
_NODE_NAMES_POST_KEY = 'node_names'

# The POST key of HEALTH_PILLS_ROUTE for the run to retrieve health pills for.
_RUN_POST_KEY = 'run'

# The default run to retrieve health pills for.
_DEFAULT_RUN = '.'

# The POST key of HEALTH_PILLS_ROUTE for the specific step to retrieve health
# pills for.
_STEP_POST_KEY = 'step'

# A glob pattern for files containing debugger-related events.
_DEBUGGER_EVENTS_GLOB_PATTERN = 'events.debugger*'

# Encapsulates data for a single health pill.
HealthPillEvent = collections.namedtuple('HealthPillEvent', [
    'wall_time', 'step', 'device_name', 'output_slot', 'node_name', 'dtype',
    'shape', 'value'
])


class DebuggerPlugin(base_plugin.TBPlugin):
  """TensorFlow Debugger plugin. Receives requests for debugger-related data.

  That data could include health pills, which unveil the status of tensor
  values.
  """

  # This string field is used by TensorBoard to generate the paths for routes
  # provided by this plugin. It must thus be URL-friendly. This field is also
  # used to uniquely identify this plugin throughout TensorBoard. See BasePlugin
  # for details.
  plugin_name = constants.DEBUGGER_PLUGIN_NAME

  def __init__(self, context):
    """Constructs a debugger plugin for TensorBoard.

    This plugin adds handlers for retrieving debugger-related data. The plugin
    also starts a debugger data server once the log directory is passed to the
    plugin via the call to get_plugin_apps.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self._event_multiplexer = context.multiplexer
    self._logdir = context.logdir
    self._debugger_data_server = None
    self._grpc_port = None

  def listen(self, grpc_port):
    """Start listening on the given gRPC port.

    This method of an instance of DebuggerPlugin can be invoked at most once.
    This method is not thread safe.

    Args:
      grpc_port: port number to listen at.

    Raises:
      ValueError: If this instance is already listening at a gRPC port.
    """
    if self._grpc_port:
      raise ValueError(
          "This DebuggerPlugin instance is already listening at gRPC port %d" %
          self._grpc_port)
    self._grpc_port = grpc_port

    sys.stderr.write('Creating DebuggerDataServer at port %d and logdir %s\n' %
                     (self._grpc_port, self._logdir))
    sys.stderr.flush()
    self._debugger_data_server = debugger_server_lib.DebuggerDataServer(
        self._grpc_port, self._logdir)

    threading.Thread(target=self._debugger_data_server.
                     start_the_debugger_data_receiving_server).start()

  def get_plugin_apps(self):
    """Obtains a mapping between routes and handlers.

    This function also starts a debugger data server on separate thread if the
    plugin has not started one yet.

    Returns:
      A mapping between routes and handlers (functions that respond to
      requests).
    """
    return {
        _HEALTH_PILLS_ROUTE: self._serve_health_pills_handler,
        _NUMERICS_ALERT_REPORT_ROUTE: self._serve_numerics_alert_report_handler,
    }

  def is_active(self):
    """Determines whether this plugin is active.

    This plugin is active if any health pills information is present for any
    run.

    Returns:
      A boolean. Whether this plugin is active.
    """
    return bool(
        self._grpc_port is not None and
        self._event_multiplexer and
        self._event_multiplexer.PluginRunToTagToContent(
            constants.DEBUGGER_PLUGIN_NAME))

  @wrappers.Request.application
  def _serve_health_pills_handler(self, request):
    """A (wrapped) werkzeug handler for serving health pills.

    Accepts POST requests and responds with health pills. The request accepts
    several POST parameters:

      node_names: (required string) A JSON-ified list of node names for which
          the client would like to request health pills.
      run: (optional string) The run to retrieve health pills for. Defaults to
          '.'. This data is sent via POST (not GET) since URL length is limited.
      step: (optional integer): The session run step for which to
          retrieve health pills. If provided, the handler reads the health pills
          of that step from disk (which is slow) and produces a response with
          only health pills at that step. If not provided, the handler returns a
          response with health pills at all steps sampled by the event
          multiplexer (the fast path). The motivation here is that, sometimes,
          one desires to examine health pills at a specific step (to say find
          the first step that causes a model to blow up with NaNs).
          get_plugin_apps must be called before this slower feature is used
          because that method passes the logdir (directory path) to this plugin.

    This handler responds with a JSON-ified object mapping from node names to a
    list (of size 1) of health pill event objects, each of which has these
    properties.

    {
        'wall_time': float,
        'step': int,
        'node_name': string,
        'output_slot': int,
        # A list of 12 floats that summarizes the elements of the tensor.
        'value': float[],
    }

    Node names for which there are no health pills to be found are excluded from
    the mapping.

    Args:
      request: The request issued by the client for health pills.

    Returns:
      A werkzeug BaseResponse object.
    """
    if request.method != 'POST':
      return wrappers.Response(response=(
          '%s requests are forbidden by the debugger plugin.' %
          request.method), status=405)

    if _NODE_NAMES_POST_KEY not in request.form:
      return wrappers.Response(response=(
          'The %r POST key was not found in the request for health pills.' %
          _NODE_NAMES_POST_KEY), status=400)

    jsonified_node_names = request.form[_NODE_NAMES_POST_KEY]
    try:
      node_names = json.loads(tf.compat.as_text(jsonified_node_names))
    except Exception as e:  # pylint: disable=broad-except
      # Different JSON libs raise different exceptions, so we just do a
      # catch-all here. This problem is complicated by how Tensorboard might be
      # run in many different environments, as it is open-source.
      # TODO(@caisq, @chihuahua): Create platform-dependent adapter to catch
      # specific types of exceptions, instead of the broad catching here.
      logger.error('Could not decode node name JSON string %r: %s',
                       jsonified_node_names, e)
      return wrappers.Response(status=400)

    if not isinstance(node_names, list):
      logger.error('%r is not a JSON list of node names:',
                       jsonified_node_names)
      return wrappers.Response(status=400)

    run = request.form.get(_RUN_POST_KEY, _DEFAULT_RUN)
    step_string = request.form.get(_STEP_POST_KEY, None)
    if step_string is None:
      # Use all steps sampled by the event multiplexer (Relatively fast).
      mapping = self._obtain_sampled_health_pills(run, node_names)
    else:
      # Read disk to obtain the health pills for that step (Relatively slow).
      # Make sure that the directory for the run exists.
      # Determine the directory of events file to read.
      events_directory = self._logdir
      if run != _DEFAULT_RUN:
        # Use the directory for the specific run.
        events_directory = os.path.join(events_directory, run)

      step = int(step_string)
      try:
        mapping = self._obtain_health_pills_at_step(
            events_directory, node_names, step)
      except IOError as error:
        logger.error(
            'Error retrieving health pills for step %d: %s', step, error)
        return wrappers.Response(status=404)

    # Convert event_accumulator.HealthPillEvents to JSON-able dicts.
    jsonable_mapping = {}
    for node_name, events in mapping.items():
      jsonable_mapping[node_name] = [e._asdict() for e in events]
    return http_util.Respond(request, jsonable_mapping, 'application/json')

  def _obtain_sampled_health_pills(self, run, node_names):
    """Obtains the health pills for a run sampled by the event multiplexer.

    This is much faster than the alternative path of reading health pills from
    disk.

    Args:
      run: The run to fetch health pills for.
      node_names: A list of node names for which to retrieve health pills.

    Returns:
      A dictionary mapping from node name to a list of
      event_accumulator.HealthPillEvents.
    """
    runs_to_tags_to_content = self._event_multiplexer.PluginRunToTagToContent(
        constants.DEBUGGER_PLUGIN_NAME)

    if run not in runs_to_tags_to_content:
      # The run lacks health pills.
      return {}

    # This is also a mapping between node name and plugin content because this
    # plugin tags by node name.
    tags_to_content = runs_to_tags_to_content[run]

    mapping = {}
    for node_name in node_names:
      if node_name not in tags_to_content:
        # This node lacks health pill data.
        continue

      health_pills = []
      for tensor_event in self._event_multiplexer.Tensors(run, node_name):
        json_string = tags_to_content[node_name]
        try:
          content_object = json.loads(tf.compat.as_text(json_string))
          device_name = content_object['device']
          output_slot = content_object['outputSlot']
          health_pills.append(
              self._tensor_proto_to_health_pill(tensor_event, node_name,
                                                device_name, output_slot))
        except (KeyError, ValueError) as e:
          logger.error('Could not determine device from JSON string '
                           '%r: %r', json_string, e)

      mapping[node_name] = health_pills

    return mapping

  def _tensor_proto_to_health_pill(self, tensor_event, node_name, device,
                                   output_slot):
    """Converts an event_accumulator.TensorEvent to a HealthPillEvent.

    Args:
      tensor_event: The event_accumulator.TensorEvent to convert.
      node_name: The name of the node (without the output slot).
      device: The device.
      output_slot: The integer output slot this health pill is relevant to.

    Returns:
      A HealthPillEvent.
    """
    return self._process_health_pill_value(
        wall_time=tensor_event.wall_time,
        step=tensor_event.step,
        device_name=device,
        output_slot=output_slot,
        node_name=node_name,
        tensor_proto=tensor_event.tensor_proto)

  def _obtain_health_pills_at_step(self, events_directory, node_names, step):
    """Reads disk to obtain the health pills for a run at a specific step.

    This could be much slower than the alternative path of just returning all
    health pills sampled by the event multiplexer. It could take tens of minutes
    to complete this call for large graphs for big step values (in the
    thousands).

    Args:
      events_directory: The directory containing events for the desired run.
      node_names: A list of node names for which to retrieve health pills.
      step: The step to obtain health pills for.

    Returns:
      A dictionary mapping from node name to a list of health pill objects (see
      docs for _serve_health_pills_handler for properties of those objects).

    Raises:
      IOError: If no files with health pill events could be found.
    """
    # Obtain all files with debugger-related events.
    pattern = os.path.join(events_directory, _DEBUGGER_EVENTS_GLOB_PATTERN)
    file_paths = glob.glob(pattern)

    if not file_paths:
      raise IOError(
          'No events files found that matches the pattern %r.' % pattern)

    # Sort by name (and thus by timestamp).
    file_paths.sort()

    mapping = collections.defaultdict(list)
    node_name_set = frozenset(node_names)

    for file_path in file_paths:
      should_stop = self._process_health_pill_event(
          node_name_set, mapping, step, file_path)
      if should_stop:
        break

    return mapping

  def _process_health_pill_event(self, node_name_set, mapping, target_step,
                                 file_path):
    """Creates health pills out of data in an event.

    Creates health pills out of the event and adds them to the mapping.

    Args:
      node_name_set: A set of node names that are relevant.
      mapping: The mapping from node name to HealthPillEvents.
          This object may be destructively modified.
      target_step: The target step at which to obtain health pills.
      file_path: The path to the file with health pill events.

    Returns:
      Whether we should stop reading events because future events are no longer
      relevant.
    """
    events_loader = event_file_loader.EventFileLoader(file_path)
    for event in events_loader.Load():
      if not event.HasField('summary'):
        logger.warn(
            'An event in a debugger events file lacks a summary.')
        continue

      if event.step < target_step:
        # This event is not of the relevant step. We perform this check
        # first because the majority of events will be eliminated from
        # consideration by this check.
        continue

      if event.step > target_step:
        # We have passed the relevant step. No need to read more events.
        return True

      for value in event.summary.value:
        # Obtain the device name from the metadata.
        summary_metadata = value.metadata
        plugin_data = summary_metadata.plugin_data
        if plugin_data.plugin_name == constants.DEBUGGER_PLUGIN_NAME:
          try:
            content = json.loads(
                tf.compat.as_text(summary_metadata.plugin_data.content))
          except ValueError as err:
            logger.warn(
                'Could not parse the JSON string containing data for '
                'the debugger plugin: %r, %r', content, err)
            continue
          device_name = content['device']
          output_slot = content['outputSlot']
        else:
          logger.error(
              'No debugger plugin data found for event with tag %s and node '
              'name %s.', value.tag, value.node_name)
          continue

        if not value.HasField('tensor'):
          logger.warn(
              'An event in a debugger events file lacks a tensor value.')
          continue

        match = re.match(r'^(.*):(\d+):DebugNumericSummary$', value.node_name)
        if not match:
          logger.warn(
              ('A event with a health pill has an invalid watch, (i.e., an '
               'unexpected debug op): %r'), value.node_name)
          return None

        health_pill = self._process_health_pill_value(
            wall_time=event.wall_time,
            step=event.step,
            device_name=device_name,
            output_slot=output_slot,
            node_name=match.group(1),
            tensor_proto=value.tensor,
            node_name_set=node_name_set)
        if not health_pill:
          continue
        mapping[health_pill.node_name].append(health_pill)

    # Keep reading events.
    return False

  def _process_health_pill_value(self,
                                 wall_time,
                                 step,
                                 device_name,
                                 output_slot,
                                 node_name,
                                 tensor_proto,
                                 node_name_set=None):
    """Creates a HealthPillEvent containing various properties of a health pill.

    Args:
      wall_time: The wall time in seconds.
      step: The session run step of the event.
      device_name: The name of the node's device.
      output_slot: The numeric output slot.
      node_name: The name of the node (without the output slot).
      tensor_proto: A tensor proto of data.
      node_name_set: An optional set of node names that are relevant. If not
        provided, no filtering by relevance occurs.

    Returns:
      An event_accumulator.HealthPillEvent. Or None if one could not be created.
    """
    if node_name_set and node_name not in node_name_set:
      # This event is not relevant.
      return None

    # Since we seek health pills for a specific step, this function
    # returns 1 health pill per node per step. The wall time is the
    # seconds since the epoch.
    elements = list(tensor_util.make_ndarray(tensor_proto))
    return HealthPillEvent(
        wall_time=wall_time,
        step=step,
        device_name=device_name,
        output_slot=output_slot,
        node_name=node_name,
        dtype=repr(tf.as_dtype(elements[12])),
        shape=elements[14:],
        value=elements)

  @wrappers.Request.application
  def _serve_numerics_alert_report_handler(self, request):
    """A (wrapped) werkzeug handler for serving numerics alert report.

    Accepts GET requests and responds with an array of JSON-ified
    NumericsAlertReportRow.

    Each JSON-ified NumericsAlertReportRow object has the following format:
    {
        'device_name': string,
        'tensor_name': string,
        'first_timestamp': float,
        'nan_event_count': int,
        'neg_inf_event_count': int,
        'pos_inf_event_count': int
    }

    These objects are sorted by ascending order of first_timestamp in the
    response array.

    Args:
      request: The request, currently assumed to be empty.

    Returns:
      A werkzeug BaseResponse object.
    """
    if request.method != 'GET':
      logger.error(
          '%s requests are forbidden by the debugger plugin.', request.method)
      return wrappers.Response(status=405)

    report = self._debugger_data_server.numerics_alert_report()

    # Convert the named tuples to dictionaries so we JSON them into objects.
    response = [r._asdict() for r in report]  # pylint: disable=protected-access
    return http_util.Respond(request, response, 'application/json')
