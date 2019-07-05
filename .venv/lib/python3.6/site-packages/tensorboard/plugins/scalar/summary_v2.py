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
"""Scalar summaries and TensorFlow operations to create them, V2 versions.

A scalar summary stores a single floating-point value, as a rank-0 tensor.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from tensorboard.compat import tf2 as tf
from tensorboard.compat.proto import summary_pb2
from tensorboard.plugins.scalar import metadata
from tensorboard.util import tensor_util


def scalar(name, data, step, description=None):
  """Write a scalar summary.

  Arguments:
    name: A name for this summary. The summary tag used for TensorBoard will
      be this name prefixed by any active name scopes.
    data: A real numeric scalar value, convertible to a `float32` Tensor.
    step: Required `int64`-castable monotonic step value.
    description: Optional long-form description for this summary, as a
      constant `str`. Markdown is supported. Defaults to empty.

  Returns:
    True on success, or false if no summary was written because no default
    summary writer was available.
  """
  summary_metadata = metadata.create_summary_metadata(
      display_name=None, description=description)
  with tf.summary.summary_scope(
      name, 'scalar_summary', values=[data, step]) as (tag, _):
    tf.debugging.assert_scalar(data)
    return tf.summary.write(tag=tag,
                            tensor=tf.cast(data, tf.float32),
                            step=step,
                            metadata=summary_metadata)


def scalar_pb(tag, data, description=None):
  """Create a scalar summary_pb2.Summary protobuf.

  Arguments:
    tag: String tag for the summary.
    data: A 0-dimensional `np.array` or a compatible python number type.
    description: Optional long-form description for this summary, as a
      `str`. Markdown is supported. Defaults to empty.

  Raises:
    ValueError: If the type or shape of the data is unsupported.

  Returns:
    A `summary_pb2.Summary` protobuf object.
  """
  arr = np.array(data)
  if arr.shape != ():
    raise ValueError('Expected scalar shape for tensor, got shape: %s.'
                     % arr.shape)
  if arr.dtype.kind not in ('b', 'i', 'u', 'f'):  # bool, int, uint, float
    raise ValueError('Cast %s to float is not supported' % arr.dtype.name)
  tensor_proto = tensor_util.make_tensor_proto(arr.astype(np.float32))
  summary_metadata = metadata.create_summary_metadata(
      display_name=None, description=description)
  summary = summary_pb2.Summary()
  summary.value.add(tag=tag,
                    metadata=summary_metadata,
                    tensor=tensor_proto)
  return summary
