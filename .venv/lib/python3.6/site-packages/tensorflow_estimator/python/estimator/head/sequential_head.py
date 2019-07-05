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
"""Abstraction for the head of a sequential model."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc

from tensorflow_estimator.python.estimator.head import base_head


class _SequentialHead(base_head.Head):
  """Interface for the head of a sequential model.

  A sequential head handles input sequences of different lengths to compute the
  output of a model. It requires a sequence mask tensor, to indicate which steps
  of the sequences are padded and ensure proper aggregation for loss and metrics
  computation. It has a `input_sequence_mask_key` property that specifies which
  tensor of the feature dictionary to use as the sequence mask tensor.

  Such a head can for instance be used with `RNNEstimator` for sequential
  predictions.

  Example of usage:
    ```python
    def _my_model_fn(features, labels, mode, params, config=None):
      feature_layer = tf.feature_column.SequenceFeatureLayer(columns)
      input_layer, sequence_length = feature_layer(features)
      sequence_length_mask = tf.sequence_mask(sequence_length)
      rnn_layer = tf.keras.layers.RNN(cell=tf.keras.layers.SimpleRNNCell(units),
                                      return_sequences=True)
      logits = rnn_layer(input_layer, mask=sequence_length_mask)
      features[sequential_head.input_sequence_mask_key] = sequence_length_mask
      return sequential_head.create_estimator_spec(
          features=features,
          labels=labels,
          mode=mode,
          logits=logits,
          optimizer=optimizer)
    ```
  """
  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def input_sequence_mask_key(self):
    """Key of the sequence mask tensor in the feature dictionary.

    Returns:
      A string.
    """
    raise NotImplementedError('Calling an abstract method.')
