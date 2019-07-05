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
"""The TensorBoard Audio plugin."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six
from six.moves import urllib
from werkzeug import wrappers

from tensorboard import plugin_util
from tensorboard.backend import http_util
from tensorboard.compat import tf
from tensorboard.plugins import base_plugin
from tensorboard.plugins.audio import metadata
from tensorboard.util import tensor_util


_DEFAULT_MIME_TYPE = 'application/octet-stream'
_MIME_TYPES = {
    metadata.Encoding.Value('WAV'): 'audio/wav',
}


class AudioPlugin(base_plugin.TBPlugin):
  """Audio Plugin for TensorBoard."""

  plugin_name = metadata.PLUGIN_NAME

  def __init__(self, context):
    """Instantiates AudioPlugin via TensorBoard core.

    Args:
      context: A base_plugin.TBContext instance.
    """
    self._multiplexer = context.multiplexer

  def get_plugin_apps(self):
    return {
        '/audio': self._serve_audio_metadata,
        '/individualAudio': self._serve_individual_audio,
        '/tags': self._serve_tags,
    }

  def is_active(self):
    """The audio plugin is active iff any run has at least one relevant tag."""
    if not self._multiplexer:
      return False
    return bool(self._multiplexer.PluginRunToTagToContent(metadata.PLUGIN_NAME))

  def _index_impl(self):
    """Return information about the tags in each run.

    Result is a dictionary of the form

        {
          "runName1": {
            "tagName1": {
              "displayName": "The first tag",
              "description": "<p>Long ago there was just one tag...</p>",
              "samples": 3
            },
            "tagName2": ...,
            ...
          },
          "runName2": ...,
          ...
        }

    For each tag, `samples` is the greatest number of audio clips that
    appear at any particular step. (It's not related to "samples of a
    waveform.") For example, if for tag `minibatch_input` there are
    five audio clips at step 0 and ten audio clips at step 1, then the
    dictionary for `"minibatch_input"` will contain `"samples": 10`.
    """
    runs = self._multiplexer.Runs()
    result = {run: {} for run in runs}

    mapping = self._multiplexer.PluginRunToTagToContent(metadata.PLUGIN_NAME)
    for (run, tag_to_content) in six.iteritems(mapping):
      for tag in tag_to_content:
        summary_metadata = self._multiplexer.SummaryMetadata(run, tag)
        tensor_events = self._multiplexer.Tensors(run, tag)
        samples = max([self._number_of_samples(event.tensor_proto)
                       for event in tensor_events] + [0])
        result[run][tag] = {'displayName': summary_metadata.display_name,
                            'description': plugin_util.markdown_to_safe_html(
                                summary_metadata.summary_description),
                            'samples': samples}

    return result

  def _number_of_samples(self, tensor_proto):
    """Count the number of samples of an audio TensorProto."""
    # We directly inspect the `tensor_shape` of the proto instead of
    # using the preferred `tensor_util.make_ndarray(...).shape`, because
    # these protos can contain a large amount of encoded audio data,
    # and we don't want to have to convert them all to numpy arrays
    # just to look at their shape.
    return tensor_proto.tensor_shape.dim[0].size

  def _filter_by_sample(self, tensor_events, sample):
    return [tensor_event for tensor_event in tensor_events
            if self._number_of_samples(tensor_event.tensor_proto) > sample]

  @wrappers.Request.application
  def _serve_audio_metadata(self, request):
    """Given a tag and list of runs, serve a list of metadata for audio.

    Note that the actual audio data are not sent; instead, we respond
    with URLs to the audio. The frontend should treat these URLs as
    opaque and should not try to parse information about them or
    generate them itself, as the format may change.

    Args:
      request: A werkzeug.wrappers.Request object.

    Returns:
      A werkzeug.Response application.
    """
    tag = request.args.get('tag')
    run = request.args.get('run')
    sample = int(request.args.get('sample', 0))

    events = self._multiplexer.Tensors(run, tag)
    response = self._audio_response_for_run(events, run, tag, sample)
    return http_util.Respond(request, response, 'application/json')

  def _audio_response_for_run(self, tensor_events, run, tag, sample):
    """Builds a JSON-serializable object with information about audio.

    Args:
      tensor_events: A list of image event_accumulator.TensorEvent objects.
      run: The name of the run.
      tag: The name of the tag the audio entries all belong to.
      sample: The zero-indexed sample of the audio sample for which to
      retrieve information. For instance, setting `sample` to `2` will
        fetch information about only the third audio clip of each batch,
        and steps with fewer than three audio clips will be omitted from
        the results.

    Returns:
      A list of dictionaries containing the wall time, step, URL, width, and
      height for each audio entry.
    """
    response = []
    index = 0
    filtered_events = self._filter_by_sample(tensor_events, sample)
    content_type = self._get_mime_type(run, tag)
    for (index, tensor_event) in enumerate(filtered_events):
      data = tensor_util.make_ndarray(tensor_event.tensor_proto)
      label = data[sample, 1]
      response.append({
          'wall_time': tensor_event.wall_time,
          'step': tensor_event.step,
          'label': plugin_util.markdown_to_safe_html(label),
          'contentType': content_type,
          'query': self._query_for_individual_audio(run, tag, sample, index)
      })
    return response

  def _query_for_individual_audio(self, run, tag, sample, index):
    """Builds a URL for accessing the specified audio.

    This should be kept in sync with _serve_audio_metadata. Note that the URL is
    *not* guaranteed to always return the same audio, since audio may be
    unloaded from the reservoir as new audio entries come in.

    Args:
      run: The name of the run.
      tag: The tag.
      index: The index of the audio entry. Negative values are OK.

    Returns:
      A string representation of a URL that will load the index-th sampled audio
      in the given run with the given tag.
    """
    query_string = urllib.parse.urlencode({
        'run': run,
        'tag': tag,
        'sample': sample,
        'index': index,
    })
    return query_string

  def _get_mime_type(self, run, tag):
    content = self._multiplexer.SummaryMetadata(run, tag).plugin_data.content
    parsed = metadata.parse_plugin_metadata(content)
    return _MIME_TYPES.get(parsed.encoding, _DEFAULT_MIME_TYPE)

  @wrappers.Request.application
  def _serve_individual_audio(self, request):
    """Serve encoded audio data."""
    tag = request.args.get('tag')
    run = request.args.get('run')
    index = int(request.args.get('index'))
    sample = int(request.args.get('sample', 0))
    events = self._filter_by_sample(self._multiplexer.Tensors(run, tag), sample)
    data = tensor_util.make_ndarray(events[index].tensor_proto)[sample, 0]
    mime_type = self._get_mime_type(run, tag)
    return http_util.Respond(request, data, mime_type)

  @wrappers.Request.application
  def _serve_tags(self, request):
    index = self._index_impl()
    return http_util.Respond(request, index, 'application/json')
