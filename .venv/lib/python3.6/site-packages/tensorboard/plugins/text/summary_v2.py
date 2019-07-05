# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Text summaries and TensorFlow operations to create them, V2 versions."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from tensorboard.compat import tf2 as tf
from tensorboard.compat.proto import summary_pb2
from tensorboard.plugins.text import metadata
from tensorboard.util import tensor_util


def text(name, data, step, description=None):
  """Write a text summary.

  Arguments:
    name: A name for this summary. The summary tag used for TensorBoard will
      be this name prefixed by any active name scopes.
    data: A UTF-8 string tensor value.
    step: Required `int64`-castable monotonic step value.
    description: Optional long-form description for this summary, as a
      constant `str`. Markdown is supported. Defaults to empty.

  Returns:
    True on success, or false if no summary was emitted because no default
    summary writer was available.
  """
  summary_metadata = metadata.create_summary_metadata(
      display_name=None, description=description)
  with tf.summary.summary_scope(
      name, 'text_summary', values=[data, step]) as (tag, _):
    tf.debugging.assert_type(data, tf.string)
    return tf.summary.write(
        tag=tag, tensor=data, step=step, metadata=summary_metadata)


def text_pb(tag, data, description=None):
  """Create a text tf.Summary protobuf.

  Arguments:
    tag: String tag for the summary.
    data: A Python bytestring (of type bytes), a Unicode string, or a numpy data
      array of those types.
    description: Optional long-form description for this summary, as a `str`.
      Markdown is supported. Defaults to empty.

  Raises:
    TypeError: If the type of the data is unsupported.

  Returns:
    A `tf.Summary` protobuf object.
  """
  try:
    tensor = tensor_util.make_tensor_proto(data, dtype=np.object)
  except TypeError as e:
    raise TypeError('tensor must be of type string', e)
  summary_metadata = metadata.create_summary_metadata(
      display_name=None, description=description)
  summary = summary_pb2.Summary()
  summary.value.add(tag=tag,
                    metadata=summary_metadata,
                    tensor=tensor)
  return summary
