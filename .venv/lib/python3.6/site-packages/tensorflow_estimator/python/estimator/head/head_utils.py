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
"""Utilities for heads and unit tests."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.core.framework import summary_pb2
from tensorflow.python.saved_model import signature_constants
from tensorflow_estimator.python.estimator.canned import head as head_v1
from tensorflow_estimator.python.estimator.head import multi_class_head

_DEFAULT_SERVING_KEY = signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY


def binary_or_multi_class_head(n_classes, weight_column, label_vocabulary,
                               loss_reduction):
  """Creates either binary or multi-class head.

  Args:
    n_classes: Number of label classes.
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example. If it is a string, it is
      used as a key to fetch weight tensor from the `features`. If it is a
      `NumericColumn`, raw tensor is fetched by key `weight_column.key`,
      then weight_column.normalizer_fn is applied on it to get weight tensor.
    label_vocabulary: A list of strings represents possible label values. If
      given, labels must be string type and have any value in
      `label_vocabulary`. If it is not given, that means labels are
      already encoded as integer or float within [0, 1] for `n_classes=2` and
      encoded as integer values in {0, 1,..., n_classes-1} for `n_classes`>2 .
      Also there will be errors if vocabulary is not provided and labels are
      string.
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Defines how
      to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.

  Returns:
    A `Head` instance.
  """
  if n_classes == 2:
    # TODO(b/117517419): Update binary_class_head when it's fully implemented.
    head = head_v1._binary_logistic_head_with_sigmoid_cross_entropy_loss(  # pylint: disable=protected-access
        weight_column=weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=loss_reduction)
  else:
    head = multi_class_head.MultiClassHead(
        n_classes, weight_column=weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=loss_reduction)
  return head


def _initialize_variables(test_case, scaffold):
  scaffold.finalize()
  test_case.assertIsNone(scaffold.init_feed_dict)
  test_case.assertIsNone(scaffold.init_fn)
  scaffold.init_op.run()
  scaffold.ready_for_local_init_op.eval()
  scaffold.local_init_op.run()
  scaffold.ready_op.eval()
  test_case.assertIsNotNone(scaffold.saver)


def _assert_simple_summaries(test_case, expected_summaries, summary_str,
                             tol=1e-6):
  """Assert summary the specified simple values.

  Args:
    test_case: test case.
    expected_summaries: Dict of expected tags and simple values.
    summary_str: Serialized `summary_pb2.Summary`.
    tol: Tolerance for relative and absolute.
  """
  summary = summary_pb2.Summary()
  summary.ParseFromString(summary_str)
  test_case.assertAllClose(expected_summaries, {
      v.tag: v.simple_value for v in summary.value
  }, rtol=tol, atol=tol)


def _assert_no_hooks(test_case, spec):
  test_case.assertAllEqual([], spec.training_chief_hooks)
  test_case.assertAllEqual([], spec.training_hooks)
