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
"""TensorBoard core plugin package."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import functools
import gzip
import math
import mimetypes
import os
import zipfile

import six
from werkzeug import utils
from werkzeug import wrappers

from tensorboard.backend import http_util
from tensorboard.plugins import base_plugin
from tensorboard.util import tb_logging

logger = tb_logging.get_logger()


# If no port is specified, try to bind to this port. See help for --port
# for more details.
DEFAULT_PORT = 6006


class CorePlugin(base_plugin.TBPlugin):
  """Core plugin for TensorBoard.

  This plugin serves runs, configuration data, and static assets. This plugin
  should always be present in a TensorBoard WSGI application.
  """

  plugin_name = 'core'

  def __init__(self, context):
    """Instantiates CorePlugin.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self._logdir = context.logdir
    self._db_uri = context.db_uri
    self._window_title = context.window_title
    self._multiplexer = context.multiplexer
    self._db_connection_provider = context.db_connection_provider
    self._assets_zip_provider = context.assets_zip_provider

  def is_active(self):
    return True

  def get_plugin_apps(self):
    apps = {
        '/___rPc_sWiTcH___': self._send_404_without_logging,
        '/audio': self._redirect_to_index,
        '/data/environment': self._serve_environment,
        '/data/logdir': self._serve_logdir,
        '/data/runs': self._serve_runs,
        '/data/experiments': self._serve_experiments,
        '/data/experiment_runs': self._serve_experiment_runs,
        '/data/window_properties': self._serve_window_properties,
        '/events': self._redirect_to_index,
        '/favicon.ico': self._send_404_without_logging,
        '/graphs': self._redirect_to_index,
        '/histograms': self._redirect_to_index,
        '/images': self._redirect_to_index,
    }
    if self._assets_zip_provider:
      with self._assets_zip_provider() as fp:
        with zipfile.ZipFile(fp) as zip_:
          for path in zip_.namelist():
            gzipped_asset_bytes = _gzip(zip_.read(path))
            apps['/' + path] = functools.partial(
                self._serve_asset, path, gzipped_asset_bytes)
      apps['/'] = apps['/index.html']
    return apps

  @wrappers.Request.application
  def _send_404_without_logging(self, request):
    return http_util.Respond(request, 'Not found', 'text/plain', code=404)

  @wrappers.Request.application
  def _redirect_to_index(self, unused_request):
    return utils.redirect('/')

  @wrappers.Request.application
  def _serve_asset(self, path, gzipped_asset_bytes, request):
    """Serves a pre-gzipped static asset from the zip file."""
    mimetype = mimetypes.guess_type(path)[0] or 'application/octet-stream'
    return http_util.Respond(
        request, gzipped_asset_bytes, mimetype, content_encoding='gzip')

  @wrappers.Request.application
  def _serve_environment(self, request):
    """Serve a JSON object containing some base properties used by the frontend.

    * data_location is either a path to a directory or an address to a
      database (depending on which mode TensorBoard is running in).
    * window_title is the title of the TensorBoard web page.
    """
    return http_util.Respond(
        request,
        {
            'data_location': self._logdir or self._db_uri,
            'mode': 'db' if self._db_uri else 'logdir',
            'window_title': self._window_title,
        },
        'application/json')

  @wrappers.Request.application
  def _serve_logdir(self, request):
    """Respond with a JSON object containing this TensorBoard's logdir."""
    # TODO(chihuahua): Remove this method once the frontend instead uses the
    # /data/environment route (and no deps throughout Google use the
    # /data/logdir route).
    return http_util.Respond(
        request, {'logdir': self._logdir}, 'application/json')

  @wrappers.Request.application
  def _serve_window_properties(self, request):
    """Serve a JSON object containing this TensorBoard's window properties."""
    # TODO(chihuahua): Remove this method once the frontend instead uses the
    # /data/environment route.
    return http_util.Respond(
        request, {'window_title': self._window_title}, 'application/json')

  @wrappers.Request.application
  def _serve_runs(self, request):
    """Serve a JSON array of run names, ordered by run started time.

    Sort order is by started time (aka first event time) with empty times sorted
    last, and then ties are broken by sorting on the run name.
    """
    if self._db_connection_provider:
      db = self._db_connection_provider()
      cursor = db.execute('''
        SELECT
          run_name,
          started_time IS NULL as started_time_nulls_last,
          started_time
        FROM Runs
        ORDER BY started_time_nulls_last, started_time, run_name
      ''')
      run_names = [row[0] for row in cursor]
    else:
      # Python's list.sort is stable, so to order by started time and
      # then by name, we can just do the sorts in the reverse order.
      run_names = sorted(self._multiplexer.Runs())
      def get_first_event_timestamp(run_name):
        try:
          return self._multiplexer.FirstEventTimestamp(run_name)
        except ValueError:
          logger.warn(
              'Unable to get first event timestamp for run %s', run_name)
          # Put runs without a timestamp at the end.
          return float('inf')
      run_names.sort(key=get_first_event_timestamp)
    return http_util.Respond(request, run_names, 'application/json')

  @wrappers.Request.application
  def _serve_experiments(self, request):
    """Serve a JSON array of experiments. Experiments are ordered by experiment
    started time (aka first event time) with empty times sorted last, and then
    ties are broken by sorting on the experiment name.
    """
    results = self.list_experiments_impl()
    return http_util.Respond(request, results, 'application/json')

  def list_experiments_impl(self):
    results = []
    if self._db_connection_provider:
      db = self._db_connection_provider()
      cursor = db.execute('''
        SELECT
          experiment_id,
          experiment_name,
          started_time,
          started_time IS NULL as started_time_nulls_last
        FROM Experiments
        ORDER BY started_time_nulls_last, started_time, experiment_name,
            experiment_id
      ''')
      results = [{
        "id": row[0],
        "name": row[1],
        "startTime": row[2],
      } for row in cursor]

    return results

  @wrappers.Request.application
  def _serve_experiment_runs(self, request):
    """Serve a JSON runs of an experiment, specified with query param
    `experiment`, with their nested data, tag, populated. Runs returned are
    ordered by started time (aka first event time) with empty times sorted last,
    and then ties are broken by sorting on the run name. Tags are sorted by
    its name, displayName, and lastly, inserted time.
    """
    results = []
    if self._db_connection_provider:
      exp_id = request.args.get('experiment')
      runs_dict = collections.OrderedDict()

      db = self._db_connection_provider()
      cursor = db.execute('''
        SELECT
          Runs.run_id,
          Runs.run_name,
          Runs.started_time,
          Runs.started_time IS NULL as started_time_nulls_last,
          Tags.tag_id,
          Tags.tag_name,
          Tags.display_name,
          Tags.plugin_name,
          Tags.inserted_time
        From Runs
        LEFT JOIN Tags ON Runs.run_id = Tags.run_id
        WHERE Runs.experiment_id = ?
        AND (Tags.tag_id IS NULL OR Tags.plugin_name IS NOT NULL)
        ORDER BY started_time_nulls_last,
          Runs.started_time,
          Runs.run_name,
          Runs.run_id,
          Tags.tag_name,
          Tags.display_name,
          Tags.inserted_time;
      ''', (exp_id,))
      for row in cursor:
        run_id = row[0]
        if not run_id in runs_dict:
          runs_dict[run_id] = {
            "id": run_id,
            "name": row[1],
            "startTime": math.floor(row[2]),
            "tags": [],
          }
        # tag can be missing.
        if row[4]:
          runs_dict[run_id].get("tags").append({
            "id": row[4],
            "displayName": row[6],
            "name": row[5],
            "pluginName": row[7],
          })
      results = list(runs_dict.values())
    return http_util.Respond(request, results, 'application/json')

class CorePluginLoader(base_plugin.TBLoader):
  """CorePlugin factory."""

  def define_flags(self, parser):
    """Adds standard TensorBoard CLI flags to parser."""
    parser.add_argument(
        '--logdir',
        metavar='PATH',
        type=str,
        default='',
        help='''\
Directory where TensorBoard will look to find TensorFlow event files
that it can display. TensorBoard will recursively walk the directory
structure rooted at logdir, looking for .*tfevents.* files.

You may also pass a comma separated list of log directories, and
TensorBoard will watch each directory. You can also assign names to
individual log directories by putting a colon between the name and the
path, as in:

`tensorboard --logdir=name1:/path/to/logs/1,name2:/path/to/logs/2`\
''')

    parser.add_argument(
        '--host',
        metavar='ADDR',
        type=str,
        default='',
        help='''\
What host to listen to. Defaults to serving on all interfaces. Other
commonly used values are 127.0.0.1 (localhost) and :: (for IPv6).\
''')

    parser.add_argument(
        '--port',
        metavar='PORT',
        type=lambda s: (None if s == "default" else int(s)),
        default="default",
        help='''\
Port to serve TensorBoard on. Pass 0 to request an unused port selected
by the operating system, or pass "default" to try to bind to the default
port (%s) but search for a nearby free port if the default port is
unavailable. (default: "default").\
''' % DEFAULT_PORT)

    parser.add_argument(
        '--purge_orphaned_data',
        metavar='BOOL',
        # Custom str-to-bool converter since regular bool() doesn't work.
        type=lambda v: {'true': True, 'false': False}.get(v.lower(), v),
        choices=[True, False],
        default=True,
        help='''\
Whether to purge data that may have been orphaned due to TensorBoard
restarts. Setting --purge_orphaned_data=False can be used to debug data
disappearance. (default: %(default)s)\
''')

    parser.add_argument(
        '--reload_interval',
        metavar='SECONDS',
        type=float,
        default=5.0,
        help='''\
How often the backend should load more data, in seconds. Set to 0 to
load just once at startup and a negative number to never reload at all.
Not relevant for DB read-only mode. (default: %(default)s)\
''')

    parser.add_argument(
        '--db',
        metavar='URI',
        type=str,
        default='',
        help='''\
[experimental] sets SQL database URI and enables DB backend mode, which is
read-only unless --db_import is also passed.\
''')

    parser.add_argument(
        '--db_import',
        action='store_true',
        help='''\
[experimental] enables DB read-and-import mode, which in combination with
--logdir imports event files into a DB backend on the fly. The backing DB is
temporary unless --db is also passed to specify a DB path to use.\
''')

    parser.add_argument(
        '--db_import_use_op',
        action='store_true',
        help='''\
[experimental] in combination with --db_import, if passed, use TensorFlow's
import_event() op for importing event data, otherwise use TensorBoard's own
sqlite ingestion logic.\
''')

    parser.add_argument(
        '--inspect',
        action='store_true',
        help='''\
Prints digests of event files to command line.

This is useful when no data is shown on TensorBoard, or the data shown
looks weird.

Must specify one of `logdir` or `event_file` flag.

Example usage:
  `tensorboard --inspect --logdir mylogdir --tag loss`

See tensorboard/backend/event_processing/event_file_inspector.py for more info.\
''')

    parser.add_argument(
        '--tag',
        metavar='TAG',
        type=str,
        default='',
        help='tag to query for; used with --inspect')

    parser.add_argument(
        '--event_file',
        metavar='PATH',
        type=str,
        default='',
        help='''\
The particular event file to query for. Only used if --inspect is
present and --logdir is not specified.\
''')

    parser.add_argument(
        '--path_prefix',
        metavar='PATH',
        type=str,
        default='',
        help='''\
An optional, relative prefix to the path, e.g. "/path/to/tensorboard".
resulting in the new base url being located at
localhost:6006/path/to/tensorboard under default settings. A leading
slash is required when specifying the path_prefix, however trailing
slashes can be omitted. The path_prefix can be leveraged for path based
routing of an elb when the website base_url is not available e.g.
"example.site.com/path/to/tensorboard/".\
''')

    parser.add_argument(
        '--window_title',
        metavar='TEXT',
        type=str,
        default='',
        help='changes title of browser window')

    parser.add_argument(
        '--max_reload_threads',
        metavar='COUNT',
        type=int,
        default=1,
        help='''\
The max number of threads that TensorBoard can use to reload runs. Not
relevant for db read-only mode. Each thread reloads one run at a time.
(default: %(default)s)\
''')

    parser.add_argument(
        '--reload_task',
        metavar='TYPE',
        type=str,
        default='auto',
        choices=['auto', 'thread', 'process', 'blocking'],
        help='''\
[experimental] The mechanism to use for the background data reload task.
The default "auto" option will conditionally use threads for legacy reloading
and a child process for DB import reloading. The "process" option is only
useful with DB import mode. The "blocking" option will block startup until
reload finishes, and requires --load_interval=0. (default: %(default)s)\
''')

    parser.add_argument(
        '--samples_per_plugin',
        type=str,
        default='',
        help='''\
An optional comma separated list of plugin_name=num_samples pairs to
explicitly specify how many samples to keep per tag for that plugin. For
unspecified plugins, TensorBoard randomly downsamples logged summaries
to reasonable values to prevent out-of-memory errors for long running
jobs. This flag allows fine control over that downsampling. Note that 0
means keep all samples of that type. For instance "scalars=500,images=0"
keeps 500 scalars and all images. Most users should not need to set this
flag.\
''')

  def fix_flags(self, flags):
    """Fixes standard TensorBoard CLI flags to parser."""
    if flags.inspect:
      if flags.logdir and flags.event_file:
        raise ValueError(
            'Must specify either --logdir or --event_file, but not both.')
      if not (flags.logdir or flags.event_file):
        raise ValueError('Must specify either --logdir or --event_file.')
    elif not flags.db and not flags.logdir:
      raise ValueError('A logdir or db must be specified. '
                       'For example `tensorboard --logdir mylogdir` '
                       'or `tensorboard --db sqlite:~/.tensorboard.db`. '
                       'Run `tensorboard --helpfull` for details and examples.')

    if flags.path_prefix.endswith('/'):
      flags.path_prefix = flags.path_prefix[:-1]

  def load(self, context):
    """Creates CorePlugin instance."""
    return CorePlugin(context)


def _gzip(bytestring):
  out = six.BytesIO()
  # Set mtime to zero for deterministic results across TensorBoard launches.
  with gzip.GzipFile(fileobj=out, mode='wb', compresslevel=3, mtime=0) as f:
    f.write(bytestring)
  return out.getvalue()
