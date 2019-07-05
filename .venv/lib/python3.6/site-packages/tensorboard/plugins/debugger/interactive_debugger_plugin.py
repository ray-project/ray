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
"""The plugin for the interactive Debugger Dashboard."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import platform
import signal
import sys
import threading

from six.moves import xrange  # pylint:disable=redefined-builtin
import tensorflow as tf
from werkzeug import wrappers

from tensorboard.backend import http_util
from tensorboard.plugins import base_plugin
from tensorboard.plugins.debugger import constants
from tensorboard.plugins.debugger import interactive_debugger_server_lib
from tensorboard.util import tb_logging

logger = tb_logging.get_logger()

# HTTP routes.
_ACK_ROUTE = '/ack'
_COMM_ROUTE = '/comm'
_DEBUGGER_GRAPH_ROUTE = '/debugger_graph'
_DEBUGGER_GRPC_HOST_PORT_ROUTE = '/debugger_grpc_host_port'
_GATED_GRPC_ROUTE = '/gated_grpc'
_TENSOR_DATA_ROUTE = '/tensor_data'
_SOURCE_CODE_ROUTE = '/source_code'


class InteractiveDebuggerPlugin(base_plugin.TBPlugin):
  """Interactive TensorFlow Debugger plugin.

  This underlies the interactive Debugger Dashboard.

  This is different from the non-interactive `DebuggerPlugin` in module
  `debugger_plugin`. The latter is for the "health pills" feature in the Graph
  Dashboard.
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
    del context  # Unused.
    self._debugger_data_server = None
    self._server_thread = None
    self._grpc_port = None

  def listen(self, grpc_port):
    """Start listening on the given gRPC port.

    This method of an instance of InteractiveDebuggerPlugin can be invoked at
    most once. This method is not thread safe.

    Args:
      grpc_port: port number to listen at.

    Raises:
      ValueError: If this instance is already listening at a gRPC port.
    """
    if self._grpc_port:
      raise ValueError(
          'This InteractiveDebuggerPlugin instance is already listening at '
          'gRPC port %d' % self._grpc_port)
    self._grpc_port = grpc_port

    sys.stderr.write('Creating InteractiveDebuggerPlugin at port %d\n' %
                     self._grpc_port)
    sys.stderr.flush()
    self._debugger_data_server = (
        interactive_debugger_server_lib.InteractiveDebuggerDataServer(
            self._grpc_port))

    self._server_thread = threading.Thread(
        target=self._debugger_data_server.run_server)
    self._server_thread.start()

    signal.signal(signal.SIGINT, self.signal_handler)
    # Note: this is required because of a wontfix issue in grpc/python 2.7:
    #   https://github.com/grpc/grpc/issues/3820

  def signal_handler(self, unused_signal, unused_frame):
    if self._debugger_data_server and self._server_thread:
      print('Stopping InteractiveDebuggerPlugin...')
      # Enqueue a number of messages to the incoming message queue to try to
      # let the debugged tensorflow runtime proceed past the current Session.run
      # in the C++ layer and return to the Python layer, so the SIGINT handler
      # registered there may be triggered.
      for _ in xrange(len(self._debugger_data_server.breakpoints) + 1):
        self._debugger_data_server.put_incoming_message(True)
      try:
        self._debugger_data_server.stop_server()
      except ValueError:
        # In case the server has already stopped running.
        pass
      self._server_thread.join()
      print('InteractiveDebuggerPlugin stopped.')
    sys.exit(0)

  def get_plugin_apps(self):
    """Obtains a mapping between routes and handlers.

    This function also starts a debugger data server on separate thread if the
    plugin has not started one yet.

    Returns:
      A mapping between routes and handlers (functions that respond to
      requests).
    """
    return {
        _ACK_ROUTE: self._serve_ack,
        _COMM_ROUTE: self._serve_comm,
        _DEBUGGER_GRPC_HOST_PORT_ROUTE: self._serve_debugger_grpc_host_port,
        _DEBUGGER_GRAPH_ROUTE: self._serve_debugger_graph,
        _GATED_GRPC_ROUTE: self._serve_gated_grpc,
        _TENSOR_DATA_ROUTE: self._serve_tensor_data,
        _SOURCE_CODE_ROUTE: self._serve_source_code,
    }

  def is_active(self):
    """Determines whether this plugin is active.

    This plugin is active if any health pills information is present for any
    run.

    Returns:
      A boolean. Whether this plugin is active.
    """
    return self._grpc_port is not None

  @wrappers.Request.application
  def _serve_ack(self, request):
    # Send client acknowledgement. `True` is just used as a dummy value.
    self._debugger_data_server.put_incoming_message(True)
    return http_util.Respond(request, {}, 'application/json')

  @wrappers.Request.application
  def _serve_comm(self, request):
    # comm_channel.get() blocks until an item is put into the queue (by
    # self._debugger_data_server). This is how the HTTP long polling ends.
    pos = int(request.args.get("pos"))
    comm_data = self._debugger_data_server.get_outgoing_message(pos)
    return http_util.Respond(request, comm_data, 'application/json')

  @wrappers.Request.application
  def _serve_debugger_graph(self, request):
    device_name = request.args.get('device_name')
    if not device_name or device_name == 'null':
      return http_util.Respond(request, str(None), 'text/x-protobuf')

    run_key = interactive_debugger_server_lib.RunKey(
        *json.loads(request.args.get('run_key')))
    graph_def = self._debugger_data_server.get_graph(run_key, device_name)
    logger.debug(
        '_serve_debugger_graph(): device_name = %s, run_key = %s, '
        'type(graph_def) = %s', device_name, run_key, type(graph_def))
    # TODO(cais): Sending text proto may be slow in Python. Investigate whether
    # there are ways to optimize it.
    return http_util.Respond(request, str(graph_def), 'text/x-protobuf')

  def _error_response(self, request, error_msg):
    logger.error(error_msg)
    return http_util.Respond(
        request, {'error': error_msg}, 'application/json', 400)

  @wrappers.Request.application
  def _serve_gated_grpc(self, request):
    mode = request.args.get('mode')
    if mode == 'retrieve_all' or mode == 'retrieve_device_names':
      # 'retrieve_all': Retrieve all gated-gRPC debug tensors and currently
      #   enabled breakpoints associated with the given run_key.
      # 'retrieve_device_names': Retrieve all device names associated with the
      #   given run key.
      run_key = interactive_debugger_server_lib.RunKey(
          *json.loads(request.args.get('run_key')))
      # debug_graph_defs is a map from device_name to GraphDef.
      debug_graph_defs = self._debugger_data_server.get_graphs(run_key,
                                                               debug=True)
      if mode == 'retrieve_device_names':
        return http_util.Respond(request, {
            'device_names': list(debug_graph_defs.keys()),
        }, 'application/json')

      gated = {}
      for device_name in debug_graph_defs:
        gated[device_name] = self._debugger_data_server.get_gated_grpc_tensors(
            run_key, device_name)

      # Both gated and self._debugger_data_server.breakpoints are lists whose
      # items are (node_name, output_slot, debug_op_name).
      return http_util.Respond(request, {
          'gated_grpc_tensors': gated,
          'breakpoints': self._debugger_data_server.breakpoints,
          'device_names': list(debug_graph_defs.keys()),
      }, 'application/json')
    elif mode == 'breakpoints':
      # Retrieve currently enabled breakpoints.
      return http_util.Respond(
          request, self._debugger_data_server.breakpoints, 'application/json')
    elif mode == 'set_state':
      # Set the state of gated-gRPC debug tensors, e.g., disable, enable
      # breakpoint.
      node_name = request.args.get('node_name')
      output_slot = int(request.args.get('output_slot'))
      debug_op = request.args.get('debug_op')
      state = request.args.get('state')
      logger.debug('Setting state of %s:%d:%s to: %s' %
                       (node_name, output_slot, debug_op, state))
      if state == 'disable':
        self._debugger_data_server.request_unwatch(
            node_name, output_slot, debug_op)
      elif state == 'watch':
        self._debugger_data_server.request_watch(
            node_name, output_slot, debug_op, breakpoint=False)
      elif state == 'break':
        self._debugger_data_server.request_watch(
            node_name, output_slot, debug_op, breakpoint=True)
      else:
        return self._error_response(
            request, 'Unrecognized new state for %s:%d:%s: %s' % (node_name,
                                                                  output_slot,
                                                                  debug_op,
                                                                  state))
      return http_util.Respond(
          request,
          {'node_name': node_name,
           'output_slot': output_slot,
           'debug_op': debug_op,
           'state': state},
          'application/json')
    else:
      return self._error_response(
          request, 'Unrecognized mode for the gated_grpc route: %s' % mode)

  @wrappers.Request.application
  def _serve_debugger_grpc_host_port(self, request):
    return http_util.Respond(
        request,
        {'host': platform.node(), 'port': self._grpc_port}, 'application/json')

  @wrappers.Request.application
  def _serve_tensor_data(self, request):
    response_encoding = 'application/json'
    watch_key = request.args.get('watch_key')
    time_indices = request.args.get('time_indices')
    mapping = request.args.get('mapping')
    slicing = request.args.get('slicing')

    try:
      sliced_tensor_data = self._debugger_data_server.query_tensor_store(
          watch_key, time_indices=time_indices, slicing=slicing,
          mapping=mapping)
      response = {
          'tensor_data': sliced_tensor_data,
          'error': None
      }
      status_code = 200
    except (IndexError, ValueError) as e:
      response = {
          'tensor_data': None,
          'error': {
              'type': type(e).__name__,
          },
      }
      # TODO(cais): Provide safe and succinct error messages for common error
      # conditions, such as index out of bound, or invalid mapping for given
      # tensor ranks.
      status_code = 500
    return http_util.Respond(request, response, response_encoding, status_code)

  @wrappers.Request.application
  def _serve_source_code(self, request):
    response_encoding = 'application/json'

    mode = request.args.get('mode')
    if mode == 'paths':
      # Retrieve all file paths.
      response = {'paths': self._debugger_data_server.query_source_file_paths()}
      return http_util.Respond(request, response, response_encoding)
    elif mode == 'content':
      # Retrieve the content of a source file.
      file_path = request.args.get('file_path')
      response = {
          'content': {
              file_path: self._debugger_data_server.query_source_file_content(
                  file_path)},
          'lineno_to_op_name_and_stack_pos':
              self._debugger_data_server.query_file_tracebacks(file_path)}
      return http_util.Respond(request, response, response_encoding)
    elif mode == 'op_traceback':
      # Retrieve the traceback of a graph op by name of the op.
      op_name = request.args.get('op_name')
      response = {
          'op_traceback': {
              op_name: self._debugger_data_server.query_op_traceback(op_name)
          }
      }
      return http_util.Respond(request, response, response_encoding)
    else:
      response = {'error': 'Invalid mode for source_code endpoint: %s' % mode}
      return http_util.Response(request, response, response_encoding, 500)
