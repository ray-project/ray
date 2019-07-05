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
"""The TensorBoard Text plugin."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import textwrap
import threading
import time

# pylint: disable=g-bad-import-order
# Necessary for an internal test with special behavior for numpy.
import numpy as np
# pylint: enable=g-bad-import-order

import six
from werkzeug import wrappers

from tensorboard import plugin_util
from tensorboard.backend import http_util
from tensorboard.compat import tf
from tensorboard.plugins import base_plugin
from tensorboard.plugins.text import metadata
from tensorboard.util import tb_logging
from tensorboard.util import tensor_util

logger = tb_logging.get_logger()

# HTTP routes
TAGS_ROUTE = '/tags'
TEXT_ROUTE = '/text'


WARNING_TEMPLATE = textwrap.dedent("""\
  **Warning:** This text summary contained data of dimensionality %d, but only \
  2d tables are supported. Showing a 2d slice of the data instead.""")


def make_table_row(contents, tag='td'):
  """Given an iterable of string contents, make a table row.

  Args:
    contents: An iterable yielding strings.
    tag: The tag to place contents in. Defaults to 'td', you might want 'th'.

  Returns:
    A string containing the content strings, organized into a table row.

  Example: make_table_row(['one', 'two', 'three']) == '''
  <tr>
  <td>one</td>
  <td>two</td>
  <td>three</td>
  </tr>'''
  """
  columns = ('<%s>%s</%s>\n' % (tag, s, tag) for s in contents)
  return '<tr>\n' + ''.join(columns) + '</tr>\n'


def make_table(contents, headers=None):
  """Given a numpy ndarray of strings, concatenate them into a html table.

  Args:
    contents: A np.ndarray of strings. May be 1d or 2d. In the 1d case, the
      table is laid out vertically (i.e. row-major).
    headers: A np.ndarray or list of string header names for the table.

  Returns:
    A string containing all of the content strings, organized into a table.

  Raises:
    ValueError: If contents is not a np.ndarray.
    ValueError: If contents is not 1d or 2d.
    ValueError: If contents is empty.
    ValueError: If headers is present and not a list, tuple, or ndarray.
    ValueError: If headers is not 1d.
    ValueError: If number of elements in headers does not correspond to number
      of columns in contents.
  """
  if not isinstance(contents, np.ndarray):
    raise ValueError('make_table contents must be a numpy ndarray')

  if contents.ndim not in [1, 2]:
    raise ValueError('make_table requires a 1d or 2d numpy array, was %dd' %
                     contents.ndim)

  if headers:
    if isinstance(headers, (list, tuple)):
      headers = np.array(headers)
    if not isinstance(headers, np.ndarray):
      raise ValueError('Could not convert headers %s into np.ndarray' % headers)
    if headers.ndim != 1:
      raise ValueError('Headers must be 1d, is %dd' % headers.ndim)
    expected_n_columns = contents.shape[1] if contents.ndim == 2 else 1
    if headers.shape[0] != expected_n_columns:
      raise ValueError('Number of headers %d must match number of columns %d' %
                       (headers.shape[0], expected_n_columns))
    header = '<thead>\n%s</thead>\n' % make_table_row(headers, tag='th')
  else:
    header = ''

  n_rows = contents.shape[0]
  if contents.ndim == 1:
    # If it's a vector, we need to wrap each element in a new list, otherwise
    # we would turn the string itself into a row (see test code)
    rows = (make_table_row([contents[i]]) for i in range(n_rows))
  else:
    rows = (make_table_row(contents[i, :]) for i in range(n_rows))

  return '<table>\n%s<tbody>\n%s</tbody>\n</table>' % (header, ''.join(rows))


def reduce_to_2d(arr):
  """Given a np.npdarray with nDims > 2, reduce it to 2d.

  It does this by selecting the zeroth coordinate for every dimension greater
  than two.

  Args:
    arr: a numpy ndarray of dimension at least 2.

  Returns:
    A two-dimensional subarray from the input array.

  Raises:
    ValueError: If the argument is not a numpy ndarray, or the dimensionality
      is too low.
  """
  if not isinstance(arr, np.ndarray):
    raise ValueError('reduce_to_2d requires a numpy.ndarray')

  ndims = len(arr.shape)
  if ndims < 2:
    raise ValueError('reduce_to_2d requires an array of dimensionality >=2')
  # slice(None) is equivalent to `:`, so we take arr[0,0,...0,:,:]
  slices = ([0] * (ndims - 2)) + [slice(None), slice(None)]
  return arr[slices]


def text_array_to_html(text_arr):
  """Take a numpy.ndarray containing strings, and convert it into html.

  If the ndarray contains a single scalar string, that string is converted to
  html via our sanitized markdown parser. If it contains an array of strings,
  the strings are individually converted to html and then composed into a table
  using make_table. If the array contains dimensionality greater than 2,
  all but two of the dimensions are removed, and a warning message is prefixed
  to the table.

  Args:
    text_arr: A numpy.ndarray containing strings.

  Returns:
    The array converted to html.
  """
  if not text_arr.shape:
    # It is a scalar. No need to put it in a table, just apply markdown
    return plugin_util.markdown_to_safe_html(np.asscalar(text_arr))
  warning = ''
  if len(text_arr.shape) > 2:
    warning = plugin_util.markdown_to_safe_html(WARNING_TEMPLATE
                                                % len(text_arr.shape))
    text_arr = reduce_to_2d(text_arr)

  html_arr = [plugin_util.markdown_to_safe_html(x)
              for x in text_arr.reshape(-1)]
  html_arr = np.array(html_arr).reshape(text_arr.shape)

  return warning + make_table(html_arr)


def process_string_tensor_event(event):
  """Convert a TensorEvent into a JSON-compatible response."""
  string_arr = tensor_util.make_ndarray(event.tensor_proto)
  html = text_array_to_html(string_arr)
  return {
      'wall_time': event.wall_time,
      'step': event.step,
      'text': html,
  }


class TextPlugin(base_plugin.TBPlugin):
  """Text Plugin for TensorBoard."""

  plugin_name = metadata.PLUGIN_NAME

  def __init__(self, context):
    """Instantiates TextPlugin via TensorBoard core.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self._multiplexer = context.multiplexer

    # Cache the last result of index_impl() so that methods that depend on it
    # can return without blocking (while kicking off a background thread to
    # recompute the current index).
    self._index_cached = None

    # Lock that ensures that only one thread attempts to compute index_impl()
    # at a given time, since it's expensive.
    self._index_impl_lock = threading.Lock()

    # Pointer to the current thread computing index_impl(), if any.  This is
    # stored on TextPlugin only to facilitate testing.
    self._index_impl_thread = None

  def is_active(self):
    """Determines whether this plugin is active.

    This plugin is only active if TensorBoard sampled any text summaries.

    Returns:
      Whether this plugin is active.
    """
    if not self._multiplexer:
      return False

    if self._index_cached is not None:
      # If we already have computed the index, use it to determine whether
      # the plugin should be active, and if so, return immediately.
      if any(self._index_cached.values()):
        return True

    if self._multiplexer.PluginRunToTagToContent(metadata.PLUGIN_NAME):
      # Text data is present in the multiplexer. No need to further check for
      # data stored via the outdated plugin assets method.
      return True

    # We haven't conclusively determined if the plugin should be active. Launch
    # a thread to compute index_impl() and return False to avoid blocking.
    self._maybe_launch_index_impl_thread()

    return False

  def _maybe_launch_index_impl_thread(self):
    """Attempts to launch a thread to compute index_impl().

    This may not launch a new thread if one is already running to compute
    index_impl(); in that case, this function is a no-op.
    """
    # Try to acquire the lock for computing index_impl(), without blocking.
    if self._index_impl_lock.acquire(False):
      # We got the lock. Start the thread, which will unlock the lock when done.
      self._index_impl_thread = threading.Thread(
          target=self._async_index_impl,
          name='TextPluginIndexImplThread')
      self._index_impl_thread.start()

  def _async_index_impl(self):
    """Computes index_impl() asynchronously on a separate thread."""
    start = time.time()
    logger.info('TextPlugin computing index_impl() in a new thread')
    self._index_cached = self.index_impl()
    self._index_impl_thread = None
    self._index_impl_lock.release()
    elapsed = time.time() - start
    logger.info(
        'TextPlugin index_impl() thread ending after %0.3f sec', elapsed)

  def index_impl(self):
    run_to_series = self._fetch_run_to_series_from_multiplexer()

    # A previous system of collecting and serving text summaries involved
    # storing the tags of text summaries within tensors.json files. See if we
    # are currently using that system. We do not want to drop support for that
    # use case.
    name = 'tensorboard_text'
    run_to_assets = self._multiplexer.PluginAssets(name)
    for run, assets in run_to_assets.items():
      if run in run_to_series:
        # When runs conflict, the summaries created via the new method override.
        continue

      if 'tensors.json' in assets:
        tensors_json = self._multiplexer.RetrievePluginAsset(
            run, name, 'tensors.json')
        tensors = json.loads(tensors_json)
        run_to_series[run] = tensors
      else:
        # The mapping should contain all runs among its keys.
        run_to_series[run] = []

    return run_to_series

  def _fetch_run_to_series_from_multiplexer(self):
    # TensorBoard is obtaining summaries related to the text plugin based on
    # SummaryMetadata stored within Value protos.
    mapping = self._multiplexer.PluginRunToTagToContent(
        metadata.PLUGIN_NAME)
    return {
        run: list(tag_to_content.keys())
        for (run, tag_to_content)
        in six.iteritems(mapping)
    }

  def tags_impl(self):
    # Recompute the index on demand whenever tags are requested, but do it
    # in a separate thread to avoid blocking.
    self._maybe_launch_index_impl_thread()

    # Use the cached index if present. If it's not, just return the result based
    # on data from the multiplexer, requiring no disk read.
    if self._index_cached:
      return self._index_cached
    else:
      return self._fetch_run_to_series_from_multiplexer()

  @wrappers.Request.application
  def tags_route(self, request):
    response = self.tags_impl()
    return http_util.Respond(request, response, 'application/json')

  def text_impl(self, run, tag):
    try:
      text_events = self._multiplexer.Tensors(run, tag)
    except KeyError:
      text_events = []
    responses = [process_string_tensor_event(ev) for ev in text_events]
    return responses

  @wrappers.Request.application
  def text_route(self, request):
    run = request.args.get('run')
    tag = request.args.get('tag')
    response = self.text_impl(run, tag)
    return http_util.Respond(request, response, 'application/json')

  def get_plugin_apps(self):
    return {
        TAGS_ROUTE: self.tags_route,
        TEXT_ROUTE: self.text_route,
    }
