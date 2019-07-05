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
"""The TensorBoard Images plugin."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import imghdr

import six
from six.moves import urllib
from werkzeug import wrappers

from tensorboard import plugin_util
from tensorboard.backend import http_util
from tensorboard.plugins import base_plugin
from tensorboard.plugins.image import metadata
from tensorboard.compat import tf


_IMGHDR_TO_MIMETYPE = {
    'bmp': 'image/bmp',
    'gif': 'image/gif',
    'jpeg': 'image/jpeg',
    'png': 'image/png',
    'svg': 'image/svg+xml'
}

_DEFAULT_IMAGE_MIMETYPE = 'application/octet-stream'


# Extend imghdr.tests to include svg.
def detect_svg(data, f):
  del f  # Unused.
  # Assume XML documents attached to image tag to be SVG.
  if data.startswith(b'<?xml ') or data.startswith(b'<svg '):
    return 'svg'

imghdr.tests.append(detect_svg)


class ImagesPlugin(base_plugin.TBPlugin):
  """Images Plugin for TensorBoard."""

  plugin_name = metadata.PLUGIN_NAME

  def __init__(self, context):
    """Instantiates ImagesPlugin via TensorBoard core.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self._multiplexer = context.multiplexer
    self._db_connection_provider = context.db_connection_provider

  def get_plugin_apps(self):
    return {
        '/images': self._serve_image_metadata,
        '/individualImage': self._serve_individual_image,
        '/tags': self._serve_tags,
    }

  def is_active(self):
    """The images plugin is active iff any run has at least one relevant tag."""
    if self._db_connection_provider:
      # The plugin is active if one relevant tag can be found in the database.
      db = self._db_connection_provider()
      cursor = db.execute(
          '''
          SELECT 1
          FROM Tags
          WHERE Tags.plugin_name = ?
          LIMIT 1
          ''',
          (metadata.PLUGIN_NAME,))
      return bool(list(cursor))
    if not self._multiplexer:
      return False
    return bool(self._multiplexer.PluginRunToTagToContent(metadata.PLUGIN_NAME))

  def _index_impl(self):
    if self._db_connection_provider:
      db = self._db_connection_provider()
      cursor = db.execute(
          '''
          SELECT
            Runs.run_name,
            Tags.tag_name,
            Tags.display_name,
            Descriptions.description,
            /* Subtract 2 for leading width and height elements. */
            MAX(CAST (Tensors.shape AS INT)) - 2 AS samples
          FROM Tags
          JOIN Runs USING (run_id)
          JOIN Tensors ON Tags.tag_id = Tensors.series
          LEFT JOIN Descriptions ON Tags.tag_id = Descriptions.id
          WHERE Tags.plugin_name = :plugin
            /* Shape should correspond to a rank-1 tensor. */
            AND NOT INSTR(Tensors.shape, ',')
            /* Required to use TensorSeriesStepIndex. */
            AND Tensors.step IS NOT NULL
          GROUP BY Tags.tag_id
          HAVING samples >= 1
          ''',
          {'plugin': metadata.PLUGIN_NAME})
      result = collections.defaultdict(dict)
      for row in cursor:
        run_name, tag_name, display_name, description, samples = row
        description = description or ''  # Handle missing descriptions.
        result[run_name][tag_name] = {
            'displayName': display_name,
            'description': plugin_util.markdown_to_safe_html(description),
            'samples': samples
        }
      return result

    runs = self._multiplexer.Runs()
    result = {run: {} for run in runs}
    mapping = self._multiplexer.PluginRunToTagToContent(metadata.PLUGIN_NAME)
    for (run, tag_to_content) in six.iteritems(mapping):
      for tag in tag_to_content:
        summary_metadata = self._multiplexer.SummaryMetadata(run, tag)
        tensor_events = self._multiplexer.Tensors(run, tag)
        samples = max([len(event.tensor_proto.string_val[2:])  # width, height
                       for event in tensor_events] + [0])
        result[run][tag] = {'displayName': summary_metadata.display_name,
                            'description': plugin_util.markdown_to_safe_html(
                                summary_metadata.summary_description),
                            'samples': samples}
    return result

  @wrappers.Request.application
  def _serve_image_metadata(self, request):
    """Given a tag and list of runs, serve a list of metadata for images.

    Note that the images themselves are not sent; instead, we respond with URLs
    to the images. The frontend should treat these URLs as opaque and should not
    try to parse information about them or generate them itself, as the format
    may change.

    Args:
      request: A werkzeug.wrappers.Request object.

    Returns:
      A werkzeug.Response application.
    """
    tag = request.args.get('tag')
    run = request.args.get('run')
    sample = int(request.args.get('sample', 0))
    response = self._image_response_for_run(run, tag, sample)
    return http_util.Respond(request, response, 'application/json')

  def _image_response_for_run(self, run, tag, sample):
    """Builds a JSON-serializable object with information about images.

    Args:
      run: The name of the run.
      tag: The name of the tag the images all belong to.
      sample: The zero-indexed sample of the image for which to retrieve
        information. For instance, setting `sample` to `2` will fetch
        information about only the third image of each batch. Steps with
        fewer than three images will be omitted from the results.

    Returns:
      A list of dictionaries containing the wall time, step, URL, width, and
      height for each image.
    """
    if self._db_connection_provider:
      db = self._db_connection_provider()
      cursor = db.execute(
          '''
          SELECT
            computed_time,
            step,
            CAST (T0.data AS INT) AS width,
            CAST (T1.data AS INT) AS height
          FROM Tensors
          JOIN TensorStrings AS T0
            ON Tensors.rowid = T0.tensor_rowid
          JOIN TensorStrings AS T1
            ON Tensors.rowid = T1.tensor_rowid
          WHERE
            series = (
              SELECT tag_id
              FROM Runs
              CROSS JOIN Tags USING (run_id)
              WHERE Runs.run_name = :run AND Tags.tag_name = :tag)
            AND step IS NOT NULL
            AND dtype = :dtype
            /* Should be n-vector, n >= 3: [width, height, samples...] */
            AND (NOT INSTR(shape, ',') AND CAST (shape AS INT) >= 3)
            AND T0.idx = 0
            AND T1.idx = 1
          ORDER BY step
          ''',
          {'run': run, 'tag': tag, 'dtype': tf.string.as_datatype_enum})
      return [{
          'wall_time': computed_time,
          'step': step,
          'width': width,
          'height': height,
          'query': self._query_for_individual_image(run, tag, sample, index)
      } for index, (computed_time, step, width, height) in enumerate(cursor)]
    response = []
    index = 0
    tensor_events = self._multiplexer.Tensors(run, tag)
    filtered_events = self._filter_by_sample(tensor_events, sample)
    for (index, tensor_event) in enumerate(filtered_events):
      (width, height) = tensor_event.tensor_proto.string_val[:2]
      response.append({
          'wall_time': tensor_event.wall_time,
          'step': tensor_event.step,
          # We include the size so that the frontend can add that to the <img>
          # tag so that the page layout doesn't change when the image loads.
          'width': int(width),
          'height': int(height),
          'query': self._query_for_individual_image(run, tag, sample, index)
      })
    return response

  def _filter_by_sample(self, tensor_events, sample):
    return [tensor_event for tensor_event in tensor_events
            if (len(tensor_event.tensor_proto.string_val) - 2  # width, height
                > sample)]

  def _query_for_individual_image(self, run, tag, sample, index):
    """Builds a URL for accessing the specified image.

    This should be kept in sync with _serve_image_metadata. Note that the URL is
    *not* guaranteed to always return the same image, since images may be
    unloaded from the reservoir as new images come in.

    Args:
      run: The name of the run.
      tag: The tag.
      sample: The relevant sample index, zero-indexed. See documentation
        on `_image_response_for_run` for more details.
      index: The index of the image. Negative values are OK.

    Returns:
      A string representation of a URL that will load the index-th sampled image
      in the given run with the given tag.
    """
    query_string = urllib.parse.urlencode({
        'run': run,
        'tag': tag,
        'sample': sample,
        'index': index,
    })
    return query_string

  def _get_individual_image(self, run, tag, index, sample):
    """
    Returns the actual image bytes for a given image.

    Args:
      run: The name of the run the image belongs to.
      tag: The name of the tag the images belongs to.
      index: The index of the image in the current reservoir.
      sample: The zero-indexed sample of the image to retrieve (for example,
        setting `sample` to `2` will fetch the third image sample at `step`).

    Returns:
      A bytestring of the raw image bytes.
    """
    if self._db_connection_provider:
      db = self._db_connection_provider()
      cursor = db.execute(
          '''
          SELECT data
          FROM TensorStrings
          WHERE
            /* Skip first 2 elements which are width and height. */
            idx = 2 + :sample
            AND tensor_rowid = (
              SELECT rowid
              FROM Tensors
              WHERE
                series = (
                   SELECT tag_id
                   FROM Runs
                   CROSS JOIN Tags USING (run_id)
                   WHERE
                     Runs.run_name = :run
                     AND Tags.tag_name = :tag)
                AND step IS NOT NULL
                AND dtype = :dtype
                /* Should be n-vector, n >= 3: [width, height, samples...] */
                AND (NOT INSTR(shape, ',') AND CAST (shape AS INT) >= 3)
              ORDER BY step
              LIMIT 1
              OFFSET :index)
          ''',
          {'run': run,
           'tag': tag,
           'sample': sample,
           'index': index,
           'dtype': tf.string.as_datatype_enum})
      (data,) = cursor.fetchone()
      return six.binary_type(data)

    events = self._filter_by_sample(self._multiplexer.Tensors(run, tag), sample)
    images = events[index].tensor_proto.string_val[2:]  # skip width, height
    return images[sample]

  @wrappers.Request.application
  def _serve_individual_image(self, request):
    """Serves an individual image."""
    run = request.args.get('run')
    tag = request.args.get('tag')
    index = int(request.args.get('index'))
    sample = int(request.args.get('sample', 0))
    data = self._get_individual_image(run, tag, index, sample)
    image_type = imghdr.what(None, data)
    content_type = _IMGHDR_TO_MIMETYPE.get(image_type, _DEFAULT_IMAGE_MIMETYPE)
    return http_util.Respond(request, data, content_type)

  @wrappers.Request.application
  def _serve_tags(self, request):
    index = self._index_impl()
    return http_util.Respond(request, index, 'application/json')
