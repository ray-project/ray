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

import collections
import functools
import json

from six.moves import queue

import tensorflow as tf
from tensorboard.plugins.debugger import comm_channel as comm_channel_lib
from tensorboard.plugins.debugger import debug_graphs_helper
from tensorboard.plugins.debugger import tensor_helper
from tensorboard.plugins.debugger import tensor_store as tensor_store_lib
from tensorboard.util import tb_logging
from tensorflow.core.debug import debug_service_pb2
from tensorflow.python import debug as tf_debug
from tensorflow.python.debug.lib import debug_data
from tensorflow.python.debug.lib import grpc_debug_server

logger = tb_logging.get_logger()

RunKey = collections.namedtuple(
    'RunKey', ['input_names', 'output_names', 'target_nodes'])


def _extract_device_name_from_event(event):
  """Extract device name from a tf.Event proto carrying tensor value."""
  plugin_data_content = json.loads(
      tf.compat.as_str(event.summary.value[0].metadata.plugin_data.content))
  return plugin_data_content['device']


def _comm_metadata(run_key, timestamp):
  return {
      'type': 'meta',
      'timestamp': timestamp,
      'data': {
          'run_key': run_key,
      }
  }


UNINITIALIZED_TAG = 'Uninitialized'
UNSUPPORTED_TAG = 'Unsupported'
NA_TAG = 'N/A'

STRING_ELEMENT_MAX_LEN = 40


def _comm_tensor_data(device_name,
                      node_name,
                      maybe_base_expanded_node_name,
                      output_slot,
                      debug_op,
                      tensor_value,
                      wall_time):
  """Create a dict() as the outgoing data in the tensor data comm route.

  Note: The tensor data in the comm route does not include the value of the
  tensor in its entirety in general. Only if a tensor satisfies the following
  conditions will its entire value be included in the return value of this
  method:
  1. Has a numeric data type (e.g., float32, int32) and has fewer than 5
     elements.
  2. Is a string tensor and has fewer than 5 elements. Each string element is
     up to 40 bytes.

  Args:
    device_name: Name of the device that the tensor is on.
    node_name: (Original) name of the node that produces the tensor.
    maybe_base_expanded_node_name: Possbily base-expanded node name.
    output_slot: Output slot number.
    debug_op: Name of the debug op.
    tensor_value: Value of the tensor, as a numpy.ndarray.
    wall_time: Wall timestamp for the tensor.

  Returns:
    A dict representing the tensor data.
  """
  output_slot = int(output_slot)
  logger.info(
      'Recording tensor value: %s, %d, %s', node_name, output_slot, debug_op)
  tensor_values = None
  if isinstance(tensor_value, debug_data.InconvertibleTensorProto):
    if not tensor_value.initialized:
      tensor_dtype = UNINITIALIZED_TAG
      tensor_shape = UNINITIALIZED_TAG
    else:
      tensor_dtype = UNSUPPORTED_TAG
      tensor_shape = UNSUPPORTED_TAG
    tensor_values = NA_TAG
  else:
    tensor_dtype = tensor_helper.translate_dtype(tensor_value.dtype)
    tensor_shape = tensor_value.shape

    # The /comm endpoint should respond with tensor values only if the tensor is
    # small enough. Otherwise, the detailed values sould be queried through a
    # dedicated tensor_data that supports slicing.
    if tensor_helper.numel(tensor_shape) < 5:
      _, _, tensor_values = tensor_helper.array_view(tensor_value)
      if tensor_dtype == 'string' and tensor_value is not None:
        tensor_values = tensor_helper.process_buffers_for_display(
            tensor_values, limit=STRING_ELEMENT_MAX_LEN)

  return {
      'type': 'tensor',
      'timestamp': wall_time,
      'data': {
          'device_name': device_name,
          'node_name': node_name,
          'maybe_base_expanded_node_name': maybe_base_expanded_node_name,
          'output_slot': output_slot,
          'debug_op': debug_op,
          'dtype': tensor_dtype,
          'shape': tensor_shape,
          'values': tensor_values,
      },
  }


class RunStates(object):
  """A class that keeps track of state of debugged Session.run() calls."""

  def __init__(self, breakpoints_func=None):
    """Constructor of RunStates.

    Args:
      breakpoint_func: A callable of the signatuer:
        def breakpoint_func():
        which returns all the currently activated breakpoints.
    """
    # Maps from run key to debug_graphs_helper.DebugGraphWrapper instance.
    self._run_key_to_original_graphs = dict()
    self._run_key_to_debug_graphs = dict()

    if breakpoints_func:
      assert callable(breakpoints_func)
      self._breakpoints_func = breakpoints_func

  def add_graph(self, run_key, device_name, graph_def, debug=False):
    """Add a GraphDef.

    Args:
      run_key: A key for the run, containing information about the feeds,
        fetches, and targets.
      device_name: The name of the device that the `GraphDef` is for.
      graph_def: An instance of the `GraphDef` proto.
      debug: Whether `graph_def` consists of the debug ops.
    """
    graph_dict = (self._run_key_to_debug_graphs if debug else
                  self._run_key_to_original_graphs)
    if not run_key in graph_dict:
      graph_dict[run_key] = dict()  # Mapping device_name to GraphDef.
    graph_dict[run_key][tf.compat.as_str(device_name)] = (
        debug_graphs_helper.DebugGraphWrapper(graph_def))

  def get_graphs(self, run_key, debug=False):
    """Get the runtime GraphDef protos associated with a run key.

    Args:
      run_key: A Session.run kay.
      debug: Whether the debugger-decoratedgraph is to be retrieved.

    Returns:
      A `dict` mapping device name to `GraphDef` protos.
    """
    graph_dict = (self._run_key_to_debug_graphs if debug else
                  self._run_key_to_original_graphs)
    graph_wrappers = graph_dict.get(run_key, {})
    graph_defs = dict()
    for device_name, wrapper in graph_wrappers.items():
      graph_defs[device_name] = wrapper.graph_def
    return graph_defs

  def get_graph(self, run_key, device_name, debug=False):
    """Get the runtime GraphDef proto associated with a run key and a device.

    Args:
      run_key: A Session.run kay.
      device_name: Name of the device in question.
      debug: Whether the debugger-decoratedgraph is to be retrieved.

    Returns:
      A `GraphDef` proto.
    """
    return self.get_graphs(run_key, debug=debug).get(device_name, None)

  def get_breakpoints(self):
    """Obtain all the currently activated breakpoints."""
    return self._breakpoints_func()

  def get_gated_grpc_tensors(self, run_key, device_name):
    return self._run_key_to_debug_graphs[
        run_key][device_name].get_gated_grpc_tensors()

  def get_maybe_base_expanded_node_name(self, node_name, run_key, device_name):
    """Obtain possibly base-expanded node name.

    Base-expansion is the transformation of a node name which happens to be the
    name scope of other nodes in the same graph. For example, if two nodes,
    called 'a/b' and 'a/b/read' in a graph, the name of the first node will
    be base-expanded to 'a/b/(b)'.

    This method uses caching to avoid unnecessary recomputation.

    Args:
      node_name: Name of the node.
      run_key: The run key to which the node belongs.
      graph_def: GraphDef to which the node belongs.

    Raises:
      ValueError: If `run_key` and/or `device_name` do not exist in the record.
    """
    device_name = tf.compat.as_str(device_name)
    if run_key not in self._run_key_to_original_graphs:
      raise ValueError('Unknown run_key: %s' % run_key)
    if device_name not in self._run_key_to_original_graphs[run_key]:
      raise ValueError(
          'Unknown device for run key "%s": %s' % (run_key, device_name))
    return self._run_key_to_original_graphs[
        run_key][device_name].maybe_base_expanded_node_name(node_name)


class InteractiveDebuggerDataStreamHandler(
    grpc_debug_server.EventListenerBaseStreamHandler):
  """Implementation of stream handler for debugger data.

  Each instance of this class is created by a InteractiveDebuggerDataServer
  upon a gRPC stream established between the debugged Session::Run() invocation
  in TensorFlow core runtime and the InteractiveDebuggerDataServer instance.

  Each instance of this class does the following:
    1) receives a core metadata Event proto during its constructor call.
    2) receives GraphDef Event proto(s) through its on_graph_def method.
    3) receives tensor value Event proto(s) through its on_value_event method.
  """

  def __init__(
      self, incoming_channel, outgoing_channel, run_states, tensor_store):
    """Constructor of InteractiveDebuggerDataStreamHandler.

    Args:
      incoming_channel: An instance of FIFO queue, which manages incoming data,
        e.g., ACK signals from the client side unblock breakpoints.
      outgoing_channel: An instance of `CommChannel`, which manages outgoing
        data, i.e., data regarding the starting of Session.runs and hitting of
        tensor breakpoint.s
      run_states: An instance of `RunStates`, which keeps track of the states
        (graphs and breakpoints) of debugged Session.run() calls.
      tensor_store: An instance of `TensorStore`, which stores Tensor values
        from debugged Session.run() calls.
    """
    super(InteractiveDebuggerDataStreamHandler, self).__init__()

    self._incoming_channel = incoming_channel
    self._outgoing_channel = outgoing_channel
    self._run_states = run_states
    self._tensor_store = tensor_store

    self._run_key = None
    self._graph_defs = dict()  # A dict mapping device name to GraphDef.
    self._graph_defs_arrive_first = True

  def on_core_metadata_event(self, event):
    """Implementation of the core metadata-carrying Event proto callback.

    Args:
      event: An Event proto that contains core metadata about the debugged
        Session::Run() in its log_message.message field, as a JSON string.
        See the doc string of debug_data.DebugDumpDir.core_metadata for details.
    """
    core_metadata = json.loads(event.log_message.message)
    input_names = ','.join(core_metadata['input_names'])
    output_names = ','.join(core_metadata['output_names'])
    target_nodes = ','.join(core_metadata['target_nodes'])

    self._run_key = RunKey(input_names, output_names, target_nodes)
    if not self._graph_defs:
      self._graph_defs_arrive_first = False
    else:
      for device_name in self._graph_defs:
        self._add_graph_def(device_name, self._graph_defs[device_name])

    self._outgoing_channel.put(_comm_metadata(self._run_key, event.wall_time))

    # Wait for acknowledgement from client. Blocks until an item is got.
    logger.info('on_core_metadata_event() waiting for client ack (meta)...')
    self._incoming_channel.get()
    logger.info('on_core_metadata_event() client ack received (meta).')

    # TODO(cais): If eager mode, this should return something to yield.

  def _add_graph_def(self, device_name, graph_def):
    self._run_states.add_graph(
        self._run_key, device_name,
        tf_debug.reconstruct_non_debug_graph_def(graph_def))
    self._run_states.add_graph(
        self._run_key, device_name, graph_def, debug=True)

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
    del wall_time
    self._graph_defs[device_name] = graph_def

    if not self._graph_defs_arrive_first:
      self._add_graph_def(device_name, graph_def)
      self._incoming_channel.get()

  def on_value_event(self, event):
    """Records the summary values based on an updated message from the debugger.

    Logs an error message if writing the event to disk fails.

    Args:
      event: The Event proto to be processed.
    """
    if not event.summary.value:
      logger.info('The summary of the event lacks a value.')
      return None

    # The node name property in the event proto is actually a watch key, which
    # is a concatenation of several pieces of data.
    watch_key = event.summary.value[0].node_name
    tensor_value = debug_data.load_tensor_from_event(event)
    device_name = _extract_device_name_from_event(event)
    node_name, output_slot, debug_op = (
        event.summary.value[0].node_name.split(':'))
    maybe_base_expanded_node_name = (
        self._run_states.get_maybe_base_expanded_node_name(node_name,
                                                           self._run_key,
                                                           device_name))
    self._tensor_store.add(watch_key, tensor_value)
    self._outgoing_channel.put(_comm_tensor_data(
        device_name, node_name, maybe_base_expanded_node_name, output_slot,
        debug_op, tensor_value, event.wall_time))

    logger.info('on_value_event(): waiting for client ack (tensors)...')
    self._incoming_channel.get()
    logger.info('on_value_event(): client ack received (tensor).')

    # Determine if the particular debug watch key is in the current list of
    # breakpoints. If it is, send an EventReply() to unblock the debug op.
    if self._is_debug_node_in_breakpoints(event.summary.value[0].node_name):
      logger.info('Sending empty EventReply for breakpoint: %s',
                      event.summary.value[0].node_name)
      # TODO(cais): Support receiving and sending tensor value from front-end.
      return debug_service_pb2.EventReply()
    return None

  def _is_debug_node_in_breakpoints(self, debug_node_key):
    node_name, output_slot, debug_op = debug_node_key.split(':')
    output_slot = int(output_slot)
    return (node_name, output_slot,
            debug_op) in self._run_states.get_breakpoints()


# TODO(cais): Consider moving to a seperate python module.
class SourceManager(object):
  """Manages source files and tracebacks involved in the debugged TF program.

  """

  def __init__(self):
    # A dict mapping file path to file content as a list of strings.
    self._source_file_content = dict()
    # A dict mapping file path to host name.
    self._source_file_host = dict()
    # A dict mapping file path to last modified timestamp.
    self._source_file_last_modified = dict()
    # A dict mapping file path to size in bytes.
    self._source_file_bytes = dict()
    # Keeps track f the traceback of the latest graph version.
    self._graph_traceback = None
    self._graph_version = -1

  def add_debugged_source_file(self, debugged_source_file):
    """Add a DebuggedSourceFile proto."""
    # TODO(cais): Should the key include a host name, for certain distributed
    #   cases?
    key = debugged_source_file.file_path
    self._source_file_host[key] = debugged_source_file.host
    self._source_file_last_modified[key] = debugged_source_file.last_modified
    self._source_file_bytes[key] = debugged_source_file.bytes
    self._source_file_content[key] = debugged_source_file.lines

  def add_graph_traceback(self, graph_version, graph_traceback):
    if graph_version > self._graph_version:
      self._graph_traceback = graph_traceback
      self._graph_version = graph_version

  def get_paths(self):
    """Get the paths to all available source files."""
    return list(self._source_file_content.keys())

  def get_content(self, file_path):
    """Get the content of a source file.

    # TODO(cais): Maybe support getting a range of lines by line number.

    Args:
      file_path: Path to the source file.
    """
    return self._source_file_content[file_path]

  def get_op_traceback(self, op_name):
    """Get the traceback of an op in the latest version of the TF graph.

    Args:
      op_name: Name of the op.

    Returns:
      Creation traceback of the op, in the form of a list of 2-tuples:
        (file_path, lineno)

    Raises:
      ValueError: If the op with the given name cannot be found in the latest
        version of the graph that this SourceManager instance has received, or
        if this SourceManager instance has not received any graph traceback yet.
    """
    if not self._graph_traceback:
      raise ValueError('No graph traceback has been received yet.')
    for op_log_entry in self._graph_traceback.log_entries:
      if op_log_entry.name == op_name:
        return self._code_def_to_traceback_list(op_log_entry.code_def)
    raise ValueError(
        'No op named "%s" can be found in the graph of the latest version '
        ' (%d).' % (op_name, self._graph_version))

  def get_file_tracebacks(self, file_path):
    """Get the lists of ops created at lines of a specified source file.

    Args:
      file_path: Path to the source file.

    Returns:
      A dict mapping line number to a list of 2-tuples,
        `(op_name, stack_position)`
      `op_name` is the name of the name of the op whose creation traceback
        includes the line.
      `stack_position` is the position of the line in the op's creation
        traceback, represented as a 0-based integer.

    Raises:
      ValueError: If `file_path` does not point to a source file that has been
        received by this instance of `SourceManager`.
    """
    if file_path not in self._source_file_content:
      raise ValueError(
          'Source file of path "%s" has not been received by this instance of '
          'SourceManager.' % file_path)

    lineno_to_op_names_and_stack_position = dict()
    for op_log_entry in self._graph_traceback.log_entries:
      for stack_pos, trace in enumerate(op_log_entry.code_def.traces):
        if self._graph_traceback.id_to_string[trace.file_id] == file_path:
          if trace.lineno not in lineno_to_op_names_and_stack_position:
            lineno_to_op_names_and_stack_position[trace.lineno] = []
          lineno_to_op_names_and_stack_position[trace.lineno].append(
              (op_log_entry.name, stack_pos))
    return lineno_to_op_names_and_stack_position

  def _code_def_to_traceback_list(self, code_def):
    return [
        (self._graph_traceback.id_to_string[trace.file_id], trace.lineno)
        for trace in code_def.traces]


class InteractiveDebuggerDataServer(
    grpc_debug_server.EventListenerBaseServicer):
  """A service that receives and writes debugger data such as health pills.
  """

  def __init__(self, receive_port):
    """Receives health pills from a debugger and writes them to disk.

    Args:
      receive_port: The port at which to receive health pills from the
        TensorFlow debugger.
      always_flush: A boolean indicating whether the EventsWriter will be
        flushed after every write. Can be used for testing.
    """
    super(InteractiveDebuggerDataServer, self).__init__(
        receive_port, InteractiveDebuggerDataStreamHandler)

    self._incoming_channel = queue.Queue()
    self._outgoing_channel = comm_channel_lib.CommChannel()
    self._run_states = RunStates(breakpoints_func=lambda: self.breakpoints)
    self._tensor_store = tensor_store_lib.TensorStore()
    self._source_manager = SourceManager()

    curried_handler_constructor = functools.partial(
        InteractiveDebuggerDataStreamHandler,
        self._incoming_channel, self._outgoing_channel, self._run_states,
        self._tensor_store)
    grpc_debug_server.EventListenerBaseServicer.__init__(
        self, receive_port, curried_handler_constructor)

  def SendTracebacks(self, request, context):
    self._source_manager.add_graph_traceback(request.graph_version,
                                             request.graph_traceback)
    return debug_service_pb2.EventReply()

  def SendSourceFiles(self, request, context):
    # TODO(cais): Handle case in which the size of the request is greater than
    #   the 4-MB gRPC limit.
    for source_file in request.source_files:
      self._source_manager.add_debugged_source_file(source_file)
    return debug_service_pb2.EventReply()

  def get_graphs(self, run_key, debug=False):
    return self._run_states.get_graphs(run_key, debug=debug)

  def get_graph(self, run_key, device_name, debug=False):
    return self._run_states.get_graph(run_key, device_name, debug=debug)

  def get_gated_grpc_tensors(self, run_key, device_name):
    return self._run_states.get_gated_grpc_tensors(run_key, device_name)

  def get_outgoing_message(self, pos):
    msg, _ = self._outgoing_channel.get(pos)
    return msg

  def put_incoming_message(self, message):
    return self._incoming_channel.put(message)

  def query_tensor_store(self,
                         watch_key,
                         time_indices=None,
                         slicing=None,
                         mapping=None):
    """Query tensor store for a given debugged tensor value.

    Args:
      watch_key: The watch key of the debugged tensor being sought. Format:
        <node_name>:<output_slot>:<debug_op>
        E.g., Dense_1/MatMul:0:DebugIdentity.
      time_indices: Optional time indices string By default, the lastest time
        index ('-1') is returned.
      slicing: Optional slicing string.
      mapping: Optional mapping string, e.g., 'image/png'.

    Returns:
      If mapping is `None`, the possibly sliced values as a nested list of
        values or its mapped format. A `list` of nested `list` of values,
      If mapping is not `None`, the format of the return value will depend on
        the mapping.
    """
    return self._tensor_store.query(watch_key,
                                    time_indices=time_indices,
                                    slicing=slicing,
                                    mapping=mapping)

  def query_source_file_paths(self):
    """Query the source files involved in the current debugged TF program.

    Returns:
      A `list` of file paths. The files that belong to the TensorFlow Python
        library itself are *not* included.
    """
    return self._source_manager.get_paths()

  def query_source_file_content(self, file_path):
    """Query the content of a given source file.

    # TODO(cais): Allow query only a range of the source lines.

    Returns:
      The source lines as a list of `str`.
    """
    return list(self._source_manager.get_content(file_path))

  def query_op_traceback(self, op_name):
    """Query the tracebacks of ops in a TensorFlow graph.

    Returns:
      TODO(cais):
    """
    return self._source_manager.get_op_traceback(op_name)

  def query_file_tracebacks(self, file_path):
    """Query the lists of ops created at lines of a given source file.

    Args:
      file_path: Path to the source file to get the tracebacks for.

    Returns:
      A `dict` mapping line number in the specified source file to a list of
        2-tuples:
          `(op_name, stack_position)`.
        `op_name` is the name of the name of the op whose creation traceback
          includes the line.
        `stack_position` is the position of the line in the op's creation
          traceback, represented as a 0-based integer.
    """
    return self._source_manager.get_file_tracebacks(file_path)

  def dispose(self):
    """Disposes of this object. Call only after this is done being used."""
    self._tensor_store.dispose()
