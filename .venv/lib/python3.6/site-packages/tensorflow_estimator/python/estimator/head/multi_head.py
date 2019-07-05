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
"""Multi head class."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from tensorflow.python.eager import context
from tensorflow.python.framework import ops
from tensorflow.python.keras import metrics
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import control_flow_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.training import training_util
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.export import export_output
from tensorflow_estimator.python.estimator.head import base_head


def _no_op_train_fn(loss):
  del loss
  return control_flow_ops.no_op()


def _default_export_output(export_outputs, head_name):
  """Extracts the default export output from the given export_outputs dict."""
  if len(export_outputs) == 1:
    return next(six.itervalues(export_outputs))
  try:
    return export_outputs[base_head.DEFAULT_SERVING_KEY]
  except KeyError:
    raise ValueError(
        '{} did not specify default export_outputs. '
        'Given: {} '
        'Suggested fix: Use one of the heads in tf.estimator, or include '
        'key {} in export_outputs.'.format(
            head_name, export_outputs, base_head.DEFAULT_SERVING_KEY))


class MultiHead(base_head.Head):
  """Creates a `Head` for multi-objective learning.

  This class merges the output of multiple `Head` objects.
  Specifically:
  * For training, sums losses of each head, calls `train_op_fn` with this
    final loss.
  * For eval, merges metrics by adding `head.name` suffix to the keys in eval
    metrics, such as `precision/head1.name`, `precision/head2.name`.
  * For prediction, merges predictions and updates keys in prediction dict to a
    2-tuple, `(head.name, prediction_key)`. Merges `export_outputs` such that
    by default the first head is served.

  Usage:

  ```python
  # In `input_fn`, specify labels as a dict keyed by head name:
  def input_fn():
    features = ...
    labels1 = ...
    labels2 = ...
    return features, {'head1.name': labels1, 'head2.name': labels2}

  # In `model_fn`, specify logits as a dict keyed by head name:
  def model_fn(features, labels, mode):
    # Create simple heads and specify head name.
    head1 = tf.estimator.MultiClassHead(n_classes=3, name='head1')
    head2 = tf.estimator.BinaryClassHead(name='head2')
    # Create MultiHead from two simple heads.
    head = tf.estimator.MultiHead([head1, head2])
    # Create logits for each head, and combine them into a dict.
    logits1, logits2 = logit_fn()
    logits = {'head1.name': logits1, 'head2.name': logits2}
    # Return the merged EstimatorSpec
    return head.create_estimator_spec(..., logits=logits, ...)

  # Create an estimator with this model_fn.
  estimator = tf.estimator.Estimator(model_fn=model_fn)
  estimator.train(input_fn=input_fn)
  ```

  Also supports `logits` as a `Tensor` of shape
  `[D0, D1, ... DN, logits_dimension]`. It will split the `Tensor` along the
  last dimension and distribute it appropriately among the heads. E.g.:
  # Input logits.
  # logits = np.array([[-1., 1., 2., -2., 2.], [-1.5, 1., -3., 2., -2.]],
                      dtype=np.float32)
  # Suppose head1.logits_dimension = 2 and head2.logits_dimension = 3. After
  # splitting, the result is:
  # logits_dict = {'head1_name': [[-1., 1.], [-1.5, 1.]],
                   'head2_name':  [[2., -2., 2.], [-3., 2., -2.]]}
  Usage:

  ```python
  def model_fn(features, labels, mode):
    # Create simple heads and specify head name.
    head1 = tf.estimator.MultiClassHead(n_classes=3, name='head1')
    head2 = tf.estimator.BinaryClassHead(name='head2')
    # Create multi-head from two simple heads.
    head = tf.estimator.MultiHead([head1, head2])
    # Create logits for the multihead. The result of logits is a `Tensor`.
    logits = logit_fn(logits_dimension=head.logits_dimension)
    # Return the merged EstimatorSpec
    return head.create_estimator_spec(..., logits=logits, ...)
  ```

  Args:
    heads: List or tuple of `Head` instances. All heads must have `name`
      specified. The first head in the list is the default used at serving time.
    head_weights: Optional list of weights, same length as `heads`. Used when
      merging losses to calculate the weighted sum of losses from each head. If
      `None`, all losses are weighted equally.
  """

  def __init__(self, heads, head_weights=None):
    if not heads:
      raise ValueError('Must specify heads. Given: {}'.format(heads))
    if head_weights:
      if len(head_weights) != len(heads):
        raise ValueError(
            'heads and head_weights must have the same size. '
            'Given len(heads): {}. Given len(head_weights): {}.'.format(
                len(heads), len(head_weights)))
    self._logits_dimension = 0
    for head in heads:
      if head.name is None:
        raise ValueError(
            'All given heads must have name specified. Given: {}'.format(head))
      self._logits_dimension += head.logits_dimension
    self._heads = tuple(heads)
    self._head_weights = tuple(head_weights) if head_weights else tuple()
    # Metric keys.
    keys = metric_keys.MetricKeys
    self._loss_regularization_key = self._summary_key(keys.LOSS_REGULARIZATION)
    loss_keys = []
    for head in self._heads:
      loss_keys.append('{}/{}'.format(keys.LOSS, head.name))
    self._loss_keys = tuple(loss_keys)

  @property
  def name(self):
    """See `base_head.Head` for details."""
    return '_'.join([h.name for h in self._heads])

  @property
  def logits_dimension(self):
    """See `base_head.Head` for details."""
    return self._logits_dimension

  @property
  def loss_reduction(self):
    """See `base_head.Head` for details."""
    return None

  def _split_logits(self, logits):
    """Splits logits along the last dimension and returns a dict.

    If the input logits is not a dict, splitting is applied based on the logits
    dimension of each head.
    For example:

    ```python
    # head1.logits_dimension = 2
    # head2.logits_dimension = 3
    head1 = tf.estimator.MultiLabelHead(n_classes=2, name='head1_name')
    head2 = tf.estimator.MultiClassHead(n_classes=3, name='head2_name')
    multi_head = tf.estimator.MultiHead([head1, head2])
    # Input logits
    logits = np.array([[-1., 1., 2., -2., 2.], [-1.5, 1., -3., 2., -2.]],
                      dtype=np.float32)
    # As logits is not a dict, _split_logits is applied and returns the
    # logits_dict as
    logits_dict = {'head1_name': [[-1., 1.], [-1.5, 1.]],
                   'head2_name':  [[2., -2., 2.], [-3., 2., -2.]]}
    ```
    Args:
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.

    Returns:
      logits_dict: A dict of logits for each head.
    """
    logits_dict = {}
    with ops.name_scope('split_logits', values=[logits]):
      logits = ops.convert_to_tensor(logits)
      # TODO(b/119617064): unify eager and graph implementations
      if context.executing_eagerly():
        logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
        batch_shape = logits_shape[:-1]
      else:
        batch_shape = array_ops.shape(logits)[:-1]
      zeros_like_batch_shape = array_ops.zeros_like(batch_shape)
      minus_ones_like_batch_shape = -1 * array_ops.ones_like(batch_shape)
      begin_idx = 0
      for head in self._heads:
        begin_tensor = array_ops.concat(
            [zeros_like_batch_shape, [begin_idx]], axis=0)
        size_tensor = array_ops.concat(
            [minus_ones_like_batch_shape, [head.logits_dimension]], axis=0)
        logits_dict[head.name] = array_ops.slice(
            logits, begin=begin_tensor, size=size_tensor)
        begin_idx += head.logits_dimension
    return logits_dict

  def _check_logits_and_labels(self, logits, labels=None):
    """Validates the keys of logits and labels."""
    head_names = []
    for head in self._heads:
      head_names.append(head.name)
    # Checks logits keys and splits it if it's not a dict
    if isinstance(logits, dict):
      logits_missing_names = list(set(head_names) - set(list(logits)))
      if logits_missing_names:
        raise ValueError('logits has missing values for head(s): {}'.format(
            logits_missing_names))
      logits_dict = logits
    else:
      logits_dict = self._split_logits(logits)
    # Checks labels type and its keys
    if labels is not None:
      if not isinstance(labels, dict):
        raise ValueError('labels must be a dict. Given: {}'.format(labels))
      labels_missing_names = list(set(head_names) - set(list(labels)))
      if labels_missing_names:
        raise ValueError('labels has missing values for head(s): {}'.format(
            labels_missing_names))
    return logits_dict

  def loss(self, logits, labels, features=None, mode=None,
           regularization_losses=None):
    """Returns regularized training loss. See `base_head.Head` for details."""
    logits_dict = self._check_logits_and_labels(logits, labels)
    training_losses = []
    for head in self._heads:
      training_loss = head.loss(
          logits=logits_dict[head.name], labels=labels[head.name],
          features=features, mode=mode)
      training_losses.append(training_loss)

    training_losses = tuple(training_losses)
    with ops.name_scope(
        'merge_losses',
        values=training_losses + (self._head_weights or tuple())):
      if self._head_weights:
        head_weighted_training_losses = []
        for training_loss, head_weight in zip(
            training_losses, self._head_weights):
          head_weighted_training_losses.append(
              math_ops.multiply(training_loss, head_weight))
        training_losses = head_weighted_training_losses
      merged_training_loss = math_ops.add_n(training_losses)
      regularization_loss = math_ops.add_n(
          regularization_losses) if regularization_losses is not None else None
      regularized_training_loss = (merged_training_loss + regularization_loss
                                   if regularization_loss is not None
                                   else merged_training_loss)
    return regularized_training_loss

  def predictions(self, logits, keys=None):
    """Create predictions. See `base_head.Head` for details."""
    logits_dict = self._check_logits_and_labels(logits)
    predictions = {}
    with ops.name_scope('merge_pred'):
      for head in self._heads:
        head_preds = head.predictions(logits=logits_dict[head.name])
        for k, v in six.iteritems(head_preds):
          predictions[(head.name, k)] = v
    return predictions

  def metrics(self, regularization_losses=None):
    """Creates metrics. See `base_head.Head` for details."""
    eval_metrics = {}
    keys = metric_keys.MetricKeys
    # Add regularization loss metric for multi_head.
    if regularization_losses is not None:
      eval_metrics[self._loss_regularization_key] = metrics.Mean(
          name=keys.LOSS_REGULARIZATION)
    with ops.name_scope('merge_eval'):
      # Loss metric is not added by default in each head.
      for loss_key in self._loss_keys:
        eval_metrics[loss_key] = metrics.Mean(name=loss_key)
    return eval_metrics

  def update_metrics(self, eval_metrics, features, logits, labels,
                     regularization_losses=None):
    """Updates eval metrics. See `base_head.Head` for details."""
    logits_dict = self._check_logits_and_labels(logits, labels)
    # Update regularization loss metric
    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      eval_metrics[self._loss_regularization_key].update_state(
          values=regularization_loss)
    # Update metrics for each head
    for i, head in enumerate(self._heads):
      head_logits = logits_dict[head.name]
      head_labels = labels[head.name]
      # Update loss metrics
      training_loss = head.loss(
          logits=head_logits, labels=head_labels, features=features)
      eval_metrics[self._loss_keys[i]].update_state(values=training_loss)
      # Update existing metrics in each head
      head_metrics = head.metrics()
      updated_metrics = head.update_metrics(
          head_metrics, features, head_logits, head_labels)
      eval_metrics.update(updated_metrics or {})
    return eval_metrics

  def create_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns a `model_fn.EstimatorSpec`.

    Args:
      features: Input `dict` of `Tensor` or `SparseTensor` objects.
      mode: Estimator's `ModeKeys`.
      logits: Input `dict` keyed by head name, or logits `Tensor` with shape
        `[D0, D1, ... DN, logits_dimension]`. For many applications, the
        `Tensor` shape is `[batch_size, logits_dimension]`. If logits is a
        `Tensor`, it  will split the `Tensor` along the last dimension and
        distribute it appropriately among the heads. Check `MultiHead` for
        examples.
      labels: Input `dict` keyed by head name. For each head, the label value
        can be integer or string `Tensor` with shape matching its corresponding
        `logits`.`labels` is a required argument when `mode` equals `TRAIN` or
        `EVAL`.
      optimizer: `Optimizer` instance to optimize the loss in TRAIN mode.
        Namely, sets `train_op = optimizer.minimize(loss, global_step)`, which
        updates variables and increments `global_step`.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns
        `train_op`. Used if `optimizer` is `None`.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses. These losses are
        usually expressed as a batch average, so for best results, in each head,
        users need to use the default `loss_reduction=SUM_OVER_BATCH_SIZE` or
        set `loss_reduction=SUM_OVER_NONZERO_WEIGHTS` to avoid scaling errors.
        Compared to the regularization losses for each head, this loss is to
        regularize the merged loss of all heads in multi head, and will be added
        to the overall training loss of multi head.

    Returns:
      A `model_fn.EstimatorSpec` instance.

    Raises:
      ValueError: If both `train_op_fn` and `optimizer` are `None` in TRAIN
      mode, or if both are set.
      If `mode` is not in Estimator's `ModeKeys`.
    """
    with ops.name_scope(self.name, 'multi_head'):
      logits_dict = self._check_logits_and_labels(logits, labels)
      # Get all estimator spec.
      all_estimator_spec = []
      for head in self._heads:
        all_estimator_spec.append(
            head.create_estimator_spec(
                features=features,
                mode=mode,
                logits=logits_dict[head.name],
                labels=labels[head.name] if labels else None,
                train_op_fn=_no_op_train_fn))
      # Predict.
      predictions = self.predictions(logits)
      if mode == model_fn.ModeKeys.PREDICT:
        export_outputs = self._merge_predict_export_outputs(all_estimator_spec)
        return model_fn.EstimatorSpec(
            mode=model_fn.ModeKeys.PREDICT,
            predictions=predictions,
            export_outputs=export_outputs)
      loss = self.loss(logits, labels, features, mode, regularization_losses)
      # Eval.
      if mode == model_fn.ModeKeys.EVAL:
        eval_metrics = self.metrics(regularization_losses=regularization_losses)
        updated_metrics = self.update_metrics(
            eval_metrics, features, logits, labels,
            regularization_losses=regularization_losses)
        return model_fn.EstimatorSpec(
            mode=model_fn.ModeKeys.EVAL,
            predictions=predictions,
            loss=loss,
            eval_metric_ops=updated_metrics)
      # Train.
      if mode == model_fn.ModeKeys.TRAIN:
        # train_op.
        if optimizer is not None:
          if train_op_fn is not None:
            raise ValueError('train_op_fn and optimizer cannot both be set.')
          train_op = optimizer.minimize(
              loss, global_step=training_util.get_global_step())
        elif train_op_fn is not None:
          train_op = train_op_fn(loss)
        else:
          raise ValueError('train_op_fn and optimizer cannot both be None.')
        # Create summary.
        base_head.create_estimator_spec_summary(loss, regularization_losses)
        # eval_metrics.
        eval_metrics = {}
        for spec in all_estimator_spec:
          eval_metrics.update(spec.eval_metric_ops or {})
        # predictions can be used to access the logits in `TRAIN` mode
        return model_fn.EstimatorSpec(
            mode=model_fn.ModeKeys.TRAIN,
            loss=loss,
            train_op=train_op,
            predictions=predictions,
            eval_metric_ops=eval_metrics)
      raise ValueError('mode={} unrecognized'.format(mode))

  def _merge_predict_export_outputs(self, all_estimator_spec):
    """Merges list of `EstimatorSpec` export_outputs for PREDICT.

    For each individual head, its DEFAULT_SERVING_KEY and PREDICT_SERVING_KEY
    are extracted and merged for `export_outputs` in PREDICT mode of
    `EstimatorSpec`. By default, the first head is served.

    Args:
      all_estimator_spec: list of `EstimatorSpec` for the individual heads.

    Returns:
      A dict of merged export_outputs from all heads for PREDICT.
    """
    # The first head is used for serving by default.
    export_outputs = {
        base_head.DEFAULT_SERVING_KEY: _default_export_output(
            all_estimator_spec[0].export_outputs,
            self._heads[0].name),
    }
    merged_predict_outputs = {}
    for head, spec in zip(self._heads, all_estimator_spec):
      for k, v in six.iteritems(spec.export_outputs):
        # Collect default serving key for export_outputs
        key = (head.name if k == base_head.DEFAULT_SERVING_KEY
               else '{}/{}'.format(head.name, k))
        export_outputs[key] = v
        # Collect predict serving key for merged_predict_outputs
        if (k == base_head.PREDICT_SERVING_KEY and
            isinstance(v, export_output.PredictOutput)):
          for kp, vp in six.iteritems(v.outputs):
            merged_predict_outputs['{}/{}'.format(head.name, kp)] = vp
    export_outputs[base_head.PREDICT_SERVING_KEY] = (
        export_output.PredictOutput(merged_predict_outputs))
    return export_outputs
