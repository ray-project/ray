# -*- coding: utf-8 -*-
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

"""The TensorBoard plugin for performance profiling."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from werkzeug import wrappers

import tensorflow as tf

from tensorboard.backend import http_util
from tensorboard.backend.event_processing import plugin_asset_util
from tensorboard.plugins import base_plugin
from tensorboard.plugins.profile import trace_events_json
from tensorboard.plugins.profile import trace_events_pb2
from tensorboard.util import tb_logging

logger = tb_logging.get_logger()

# The prefix of routes provided by this plugin.
PLUGIN_NAME = 'profile'

# HTTP routes
LOGDIR_ROUTE = '/logdir'
DATA_ROUTE = '/data'
TOOLS_ROUTE = '/tools'
HOSTS_ROUTE = '/hosts'

# Available profiling tools -> file name of the tool data.
_FILE_NAME = 'TOOL_FILE_NAME'
TOOLS = {
    'trace_viewer': 'trace',
    'trace_viewer@': 'tracetable',  #streaming traceviewer
    'op_profile': 'op_profile.json',
    'input_pipeline_analyzer': 'input_pipeline.json',
    'overview_page': 'overview_page.json',
    'memory_viewer': 'memory_viewer.json',
    'google_chart_demo': 'google_chart_demo.json',
}

# Tools that consume raw data.
_RAW_DATA_TOOLS = frozenset(['input_pipeline_analyzer',
                             'op_profile',
                             'overview_page',
                             'memory_viewer',
                             'google_chart_demo',])

def process_raw_trace(raw_trace):
  """Processes raw trace data and returns the UI data."""
  trace = trace_events_pb2.Trace()
  trace.ParseFromString(raw_trace)
  return ''.join(trace_events_json.TraceEventsJsonStream(trace))


class ProfilePluginLoader(base_plugin.TBLoader):
  """Loader for Profile Plugin."""

  def define_flags(self, parser):
    group = parser.add_argument_group('profile plugin')
    group.add_argument(
        '--master_tpu_unsecure_channel',
        metavar='ADDR',
        type=str,
        default='',
        help='''\
IP address of "master tpu", used for getting streaming trace data
through tpu profiler analysis grpc. The grpc channel is not secured.\
''')

  def load(self, context):
    return ProfilePlugin(context)


class ProfilePlugin(base_plugin.TBPlugin):
  """Profile Plugin for TensorBoard."""

  plugin_name = PLUGIN_NAME

  def __init__(self, context):
    """Constructs a profiler plugin for TensorBoard.

    This plugin adds handlers for performance-related frontends.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self.logdir = context.logdir
    self.plugin_logdir = plugin_asset_util.PluginDirectory(
        self.logdir, ProfilePlugin.plugin_name)
    self.stub = None
    self.master_tpu_unsecure_channel = context.flags.master_tpu_unsecure_channel

  @wrappers.Request.application
  def logdir_route(self, request):
    return http_util.Respond(request, {'logdir': self.plugin_logdir},
                             'application/json')

  def _run_dir(self, run):
    run_dir = os.path.join(self.plugin_logdir, run)
    return run_dir if tf.io.gfile.isdir(run_dir) else None

  def start_grpc_stub_if_necessary(self):
    # We will enable streaming trace viewer on two conditions:
    # 1. user specify the flags master_tpu_unsecure_channel to the ip address of
    #    as "master" TPU. grpc will be used to fetch streaming trace data.
    # 2. the logdir is on google cloud storage.
    if self.master_tpu_unsecure_channel and self.logdir.startswith('gs://'):
      if self.stub is None:
        import grpc
        from tensorflow.contrib.tpu.profiler import tpu_profiler_analysis_pb2_grpc # pylint: disable=line-too-long
        # Workaround the grpc's 4MB message limitation.
        gigabyte = 1024 * 1024 * 1024
        options = [('grpc.max_message_length', gigabyte),
                   ('grpc.max_send_message_length', gigabyte),
                   ('grpc.max_receive_message_length', gigabyte)]
        tpu_profiler_port = self.master_tpu_unsecure_channel + ':8466'
        channel = grpc.insecure_channel(tpu_profiler_port, options)
        self.stub = tpu_profiler_analysis_pb2_grpc.TPUProfileAnalysisStub(
            channel)

  def index_impl(self):
    """Returns available runs and available tool data in the log directory.

    In the plugin log directory, each directory contains profile data for a
    single run (identified by the directory name), and files in the run
    directory contains data for different tools. The file that contains profile
    for a specific tool "x" will have a suffix name TOOLS["x"].
    Example:
      log/
        run1/
          plugins/
            profile/
              host1.trace
              host2.trace
        run2/
          plugins/
            profile/
              host1.trace
              host2.trace

    Returns:
      A map from runs to tool names e.g.
        {"run1": ["trace_viewer"], "run2": ["trace_viewer"]} for the example.
    """
    # TODO(ioeric): use the following structure and use EventMultiplexer so that
    # the plugin still works when logdir is set to root_logdir/run1/
    #     root_logdir/
    #       run1/
    #         plugins/
    #           profile/
    #             host1.trace
    #       run2/
    #         plugins/
    #           profile/
    #             host2.trace
    run_to_tools = {}
    if not tf.io.gfile.isdir(self.plugin_logdir):
      return run_to_tools

    self.start_grpc_stub_if_necessary()
    for run in tf.io.gfile.listdir(self.plugin_logdir):
      run_dir = self._run_dir(run)
      if not run_dir:
        continue
      run_to_tools[run] = []
      for tool in TOOLS:
        tool_pattern = '*' + TOOLS[tool]
        path = os.path.join(run_dir, tool_pattern)
        try:
          files = tf.io.gfile.glob(path)
          if len(files) >= 1:
            run_to_tools[run].append(tool)
        except tf.errors.OpError as e:
          logger.warn("Cannot read asset directory: %s, OpError %s",
                          run_dir, e)
      if 'trace_viewer@' in run_to_tools[run]:
        # streaming trace viewer always override normal trace viewer.
        # the trailing '@' is to inform tf-profile-dashboard.html and
        # tf-trace-viewer.html that stream trace viewer should be used.
        removed_tool = 'trace_viewer@' if self.stub is None else 'trace_viewer'
        if removed_tool in run_to_tools[run]:
          run_to_tools[run].remove(removed_tool)
      run_to_tools[run].sort()
      op = 'overview_page'
      if op in run_to_tools[run]:
        # keep overview page at the top of the list
        run_to_tools[run].remove(op)
        run_to_tools[run].insert(0, op)
    return run_to_tools

  @wrappers.Request.application
  def tools_route(self, request):
    run_to_tools = self.index_impl()

    return http_util.Respond(request, run_to_tools, 'application/json')

  def host_impl(self, run, tool):
    """Returns available hosts for the run and tool in the log directory.

    In the plugin log directory, each directory contains profile data for a
    single run (identified by the directory name), and files in the run
    directory contains data for different tools and hosts. The file that
    contains profile for a specific tool "x" will have a prefix name TOOLS["x"].

    Example:
      log/
        run1/
          plugins/
            profile/
              host1.trace
              host2.trace
        run2/
          plugins/
            profile/
              host1.trace
              host2.trace

    Returns:
      A list of host names e.g.
        {"host1", "host2", "host3"} for the example.
    """
    hosts = {}
    if not tf.io.gfile.isdir(self.plugin_logdir):
      return hosts
    run_dir = self._run_dir(run)
    if not run_dir:
      logger.warn("Cannot find asset directory: %s", run_dir)
      return hosts
    tool_pattern = '*' + TOOLS[tool]
    try:
      files = tf.io.gfile.glob(os.path.join(run_dir, tool_pattern))
      hosts = [os.path.basename(f).replace(TOOLS[tool], '') for f in files]
    except tf.errors.OpError as e:
      logger.warn("Cannot read asset directory: %s, OpError %s",
                      run_dir, e)
    return hosts


  @wrappers.Request.application
  def hosts_route(self, request):
    run = request.args.get('run')
    tool = request.args.get('tag')
    hosts = self.host_impl(run, tool)
    return http_util.Respond(request, hosts, 'application/json')

  def data_impl(self, request):
    """Retrieves and processes the tool data for a run and a host.

    Args:
      request: XMLHttpRequest

    Returns:
      A string that can be served to the frontend tool or None if tool,
        run or host is invalid.
    """
    run = request.args.get('run')
    tool = request.args.get('tag')
    host = request.args.get('host')

    if tool not in TOOLS:
      return None

    self.start_grpc_stub_if_necessary()
    if tool == 'trace_viewer@' and self.stub is not None:
      from tensorflow.contrib.tpu.profiler import tpu_profiler_analysis_pb2
      grpc_request = tpu_profiler_analysis_pb2.ProfileSessionDataRequest()
      grpc_request.repository_root = self.plugin_logdir
      grpc_request.session_id = run[:-1]
      grpc_request.tool_name = 'trace_viewer'
      # Remove the trailing dot if present
      grpc_request.host_name = host.rstrip('.')

      grpc_request.parameters['resolution'] = request.args.get('resolution')
      if request.args.get('start_time_ms') is not None:
        grpc_request.parameters['start_time_ms'] = request.args.get(
            'start_time_ms')
      if request.args.get('end_time_ms') is not None:
        grpc_request.parameters['end_time_ms'] = request.args.get('end_time_ms')
      grpc_response = self.stub.GetSessionToolData(grpc_request)
      return grpc_response.output

    if tool not in TOOLS:
      return None
    tool_name = str(host) + TOOLS[tool]
    rel_data_path = os.path.join(run, tool_name)
    asset_path = os.path.join(self.plugin_logdir, rel_data_path)
    raw_data = None
    try:
      with tf.io.gfile.GFile(asset_path, 'rb') as f:
        raw_data = f.read()
    except tf.errors.NotFoundError:
      logger.warn('Asset path %s not found', asset_path)
    except tf.errors.OpError as e:
      logger.warn("Couldn't read asset path: %s, OpError %s", asset_path, e)

    if raw_data is None:
      return None
    if tool == 'trace_viewer':
      return process_raw_trace(raw_data)
    if tool in _RAW_DATA_TOOLS:
      return raw_data
    return None

  @wrappers.Request.application
  def data_route(self, request):
    # params
    #   request: XMLHTTPRequest.
    data = self.data_impl(request)
    if data is None:
      return http_util.Respond(request, '404 Not Found', 'text/plain', code=404)
    return http_util.Respond(request, data, 'application/json')

  def get_plugin_apps(self):
    return {
        LOGDIR_ROUTE: self.logdir_route,
        TOOLS_ROUTE: self.tools_route,
        HOSTS_ROUTE: self.hosts_route,
        DATA_ROUTE: self.data_route,
    }

  def is_active(self):
    """The plugin is active iff any run has at least one active tool/tag."""
    return any(self.index_impl().values())
