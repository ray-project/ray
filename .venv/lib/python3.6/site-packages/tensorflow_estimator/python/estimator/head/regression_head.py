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
"""Regression head."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.python.framework import ops
from tensorflow.python.keras import metrics
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import nn
from tensorflow.python.ops.losses import losses
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.canned import prediction_keys
from tensorflow_estimator.python.estimator.export import export_output
from tensorflow_estimator.python.estimator.head import base_head


class RegressionHead(base_head.Head):
  """Creates a `Head` for regression using the `mean_squared_error` loss.

  The loss is the weighted sum over all input dimensions. Namely, if the input
  labels have shape `[batch_size, label_dimension]`, the loss is the weighted
  sum over both `batch_size` and `label_dimension`.

  The head expects `logits` with shape `[D0, D1, ... DN, label_dimension]`.
  In many applications, the shape is `[batch_size, label_dimension]`.

  The `labels` shape must match `logits`, namely
  `[D0, D1, ... DN, label_dimension]`. If `label_dimension=1`, shape
  `[D0, D1, ... DN]` is also supported.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]`, `[D0, D1, ... DN, 1]` or
  `[D0, D1, ... DN, label_dimension]`.

  Supports custom `loss_fn`. `loss_fn` takes `(labels, logits)` or
  `(labels, logits, features, loss_reduction)` as arguments and returns
  unreduced loss with shape `[D0, D1, ... DN, label_dimension]`.

  Also supports custom `inverse_link_fn`, also known as 'mean function'.
  `inverse_link_fn` is only used in `PREDICT` mode. It takes `logits` as
  argument and returns predicted values. This function is the inverse of the
  link function defined in
  https://en.wikipedia.org/wiki/Generalized_linear_model#Link_function
  Namely, for poisson regression, set `inverse_link_fn=tf.exp`.

  The head can be used with a canned estimator. Example:

  ```python
  my_head = tf.estimator.RegressionHead()
  my_estimator = tf.estimator.DNNEstimator(
      head=my_head,
      hidden_units=...,
      feature_columns=...)
  ```

  It can also be used with a custom `model_fn`. Example:

  ```python
  def _my_model_fn(features, labels, mode):
    my_head = tf.estimator.RegressionHead()
    logits = tf.keras.Model(...)(features)

    return my_head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        optimizer=tf.AdagradOptimizer(learning_rate=0.1),
        logits=logits)

  my_estimator = tf.estimator.Estimator(model_fn=_my_model_fn)
  ```

  Args:
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.
    label_dimension: Number of regression labels per example. This is the size
      of the last dimension of the labels `Tensor` (typically, this has shape
      `[batch_size, label_dimension]`).
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch and label dimension. Defaults to
      `SUM_OVER_BATCH_SIZE`, namely weighted sum of losses divided by
      `batch size * label_dimension`.
    loss_fn: Optional loss function. Defaults to `mean_squared_error`.
    inverse_link_fn: Optional inverse link function, also known as 'mean
      function'. Defaults to identity.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def __init__(
      self,
      label_dimension=1,
      weight_column=None,
      loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
      loss_fn=None,
      inverse_link_fn=None,
      name=None):
    if label_dimension < 1:
      raise ValueError('Invalid label_dimension {}.'.format(label_dimension))
    if (loss_reduction not in losses.Reduction.all() or
        loss_reduction == losses.Reduction.NONE):
      raise ValueError('Invalid loss_reduction: {}'.format(loss_reduction))
    if loss_fn:
      base_head.validate_loss_fn_args(loss_fn)
    self._logits_dimension = label_dimension
    self._weight_column = weight_column
    self._loss_reduction = loss_reduction
    self._loss_fn = loss_fn
    self._inverse_link_fn = inverse_link_fn
    self._name = name
    # Metric keys.
    keys = metric_keys.MetricKeys
    self._loss_mean_key = self._summary_key(keys.LOSS_MEAN)
    self._prediction_mean_key = self._summary_key(keys.PREDICTION_MEAN)
    self._label_mean_key = self._summary_key(keys.LABEL_MEAN)
    self._loss_regularization_key = self._summary_key(keys.LOSS_REGULARIZATION)

  @property
  def name(self):
    """See `base_head.Head` for details."""
    return self._name

  @property
  def logits_dimension(self):
    """See `base_head.Head` for details."""
    return self._logits_dimension

  @property
  def loss_reduction(self):
    """See `base_head.Head` for details."""
    return self._loss_reduction

  def _processed_labels(self, logits, labels):
    labels = base_head.check_dense_labels_match_logits_and_reshape(
        labels=labels, logits=logits,
        expected_labels_dimension=self._logits_dimension)
    labels = math_ops.to_float(labels)
    return labels

  def _unweighted_loss_and_weights(self, logits, labels, features):
    """Computes unweighted loss and weights."""
    if self._loss_fn:
      unweighted_loss = base_head.call_loss_fn(
          loss_fn=self._loss_fn, labels=labels, logits=logits,
          features=features, expected_loss_dim=self._logits_dimension)
    else:
      unweighted_loss = losses.mean_squared_error(
          labels=labels, predictions=logits, reduction=losses.Reduction.NONE)
    weights = base_head.get_weights_and_check_match_logits(
        features=features, weight_column=self._weight_column, logits=logits,
        allow_per_logit_weights=True)
    return unweighted_loss, weights

  def loss(self, logits, labels, features=None, mode=None,
           regularization_losses=None):
    """Return predictions based on keys. See `base_head.Head` for details."""
    del mode  # Unused for this head.
    with ops.name_scope('losses', values=(logits, labels, regularization_losses,
                                          features)):
      logits = base_head.check_logits_final_dim(logits, self._logits_dimension)
      labels = self._processed_labels(logits, labels)
      unweighted_loss, weights = self._unweighted_loss_and_weights(
          logits, labels, features)
      training_loss = losses.compute_weighted_loss(
          unweighted_loss, weights=weights, reduction=self._loss_reduction)
      regularization_loss = math_ops.add_n(
          regularization_losses) if regularization_losses is not None else None
      regularized_training_loss = (
          training_loss + regularization_loss if regularization_loss is not None
          else training_loss)
    return regularized_training_loss

  def predictions(self, logits):
    """Return predictions based on keys.  See `base_head.Head` for details.

    Args:
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.

    Returns:
      A dict of predictions.
    """
    logits = base_head.check_logits_final_dim(logits, self._logits_dimension)
    pred_keys = prediction_keys.PredictionKeys
    with ops.name_scope('predictions', values=(logits,)):
      if self._inverse_link_fn:
        predicted_value = self._inverse_link_fn(logits)
        predictions = {
            pred_keys.PREDICTIONS: predicted_value,
            pred_keys.LOGITS: logits,
        }
      else:
        predicted_value = logits
        predictions = {
            pred_keys.PREDICTIONS: predicted_value}
    return predictions

  def metrics(self, regularization_losses=None):
    """Creates metrics. See `base_head.Head` for details."""
    with ops.name_scope('metrics', values=(regularization_losses,)):
      keys = metric_keys.MetricKeys
      eval_metrics = {}
      eval_metrics[self._loss_mean_key] = metrics.Mean(name=keys.LOSS_MEAN)
      eval_metrics[self._prediction_mean_key] = metrics.Mean(
          name=keys.PREDICTION_MEAN)
      eval_metrics[self._label_mean_key] = metrics.Mean(name=keys.LABEL_MEAN)

      if regularization_losses is not None:
        eval_metrics[self._loss_regularization_key] = metrics.Mean(
            name=keys.LOSS_REGULARIZATION)
    return eval_metrics

  def update_metrics(self, eval_metrics, features, logits, labels,
                     regularization_losses=None):
    """Updates eval metrics. See `base_head.Head` for details."""
    # Compute predictions.
    predictions = self.predictions(logits)
    predicted_value = predictions[prediction_keys.PredictionKeys.PREDICTIONS]
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    label_ids = self._processed_labels(logits, labels)
    unweighted_loss, weights = self._unweighted_loss_and_weights(
        logits, label_ids, features)

    # Update metrics.
    eval_metrics[self._loss_mean_key].update_state(
        values=unweighted_loss, sample_weight=weights)
    eval_metrics[self._label_mean_key].update_state(
        values=labels, sample_weight=weights)
    base_head.update_metric_with_broadcast_weights(
        eval_metrics[self._prediction_mean_key], predicted_value, weights)
    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      eval_metrics[self._loss_regularization_key].update_state(
          values=regularization_loss)
    return eval_metrics

  def _create_tpu_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns an `EstimatorSpec`.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`.
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.
      labels: Labels `Tensor` with shape matching `logits`, namely
        `[D0, D1, ... DN, logits_dimension]`. When `logits_dimension=1`, shape
        `[D0, D1, ... DN]` is also supported. `labels` is a required argument
        when `mode` equals `TRAIN` or `EVAL`.
      optimizer: `Optimizer` instance to optimize the loss in TRAIN mode.
        Namely, sets `train_op = optimizer.minimize(loss, global_step)`, which
        updates variables and increments `global_step`.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns
        `train_op`. Used if `optimizer` is `None`.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses. These losses are
        usually expressed as a batch average, so for best results users need to
        set `loss_reduction=SUM_OVER_BATCH_SIZE` or
        `loss_reduction=SUM_OVER_NONZERO_WEIGHTS` when creating the head to
        avoid scaling errors.

    Returns:
      A `model_fn._TPUEstimatorSpec` instance.

    Raises:
      ValueError: If both `train_op_fn` and `optimizer` are `None` in TRAIN
        mode, or if both are set.
    """
    with ops.name_scope(self._name, 'head'):
      # Predict.
      predictions = self.predictions(logits)
      if mode == model_fn.ModeKeys.PREDICT:
        keys = prediction_keys.PredictionKeys
        regression_output = export_output.RegressionOutput(
            value=predictions[keys.PREDICTIONS])
        return model_fn._TPUEstimatorSpec(  # pylint: disable=protected-access
            mode=model_fn.ModeKeys.PREDICT,
            predictions=predictions,
            export_outputs={
                base_head.DEFAULT_SERVING_KEY: regression_output,
                base_head.REGRESS_SERVING_KEY: regression_output,
                base_head.PREDICT_SERVING_KEY: export_output.PredictOutput(
                    predictions)
            })
      regularized_training_loss = self.loss(
          logits=logits, labels=labels, features=features, mode=mode,
          regularization_losses=regularization_losses)
      # Eval.
      if mode == model_fn.ModeKeys.EVAL:
        eval_metrics = self.metrics(regularization_losses=regularization_losses)
        return model_fn._TPUEstimatorSpec(  # pylint: disable=protected-access
            mode=model_fn.ModeKeys.EVAL,
            predictions=predictions,
            loss=regularized_training_loss,
            eval_metrics=base_head.create_eval_metrics_tuple(
                self.update_metrics, {
                    'eval_metrics': eval_metrics,
                    'features': features,
                    'logits': logits,
                    'labels': labels,
                    'regularization_losses': regularization_losses
                }))
      # Train.
      train_op = base_head.create_estimator_spec_train_op(
          self._name, optimizer, train_op_fn, regularized_training_loss)
    # Create summary.
    base_head.create_estimator_spec_summary(
        regularized_training_loss, regularization_losses, self._summary_key)
    return model_fn._TPUEstimatorSpec(  # pylint: disable=protected-access
        mode=model_fn.ModeKeys.TRAIN,
        predictions=predictions,
        loss=regularized_training_loss,
        train_op=train_op)


class PoissonRegressionHead(RegressionHead):
  """Creates a `Head` for poisson regression using `tf.nn.log_poisson_loss`.

  The loss is the weighted sum over all input dimensions. Namely, if the input
  labels have shape `[batch_size, label_dimension]`, the loss is the weighted
  sum over both `batch_size` and `label_dimension`.

  The head expects `logits` with shape `[D0, D1, ... DN, label_dimension]`.
  In many applications, the shape is `[batch_size, label_dimension]`.

  The `labels` shape must match `logits`, namely
  `[D0, D1, ... DN, label_dimension]`. If `label_dimension=1`, shape
  `[D0, D1, ... DN]` is also supported.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]`, `[D0, D1, ... DN, 1]` or
  `[D0, D1, ... DN, label_dimension]`.

  This is implemented as a generalized linear model, see
  https://en.wikipedia.org/wiki/Generalized_linear_model.

  The head can be used with a canned estimator. Example:

  ```python
  my_head = tf.estimator.PoissonRegressionHead()
  my_estimator = tf.estimator.DNNEstimator(
      head=my_head,
      hidden_units=...,
      feature_columns=...)
  ```

  It can also be used with a custom `model_fn`. Example:

  ```python
  def _my_model_fn(features, labels, mode):
    my_head = tf.estimator.PoissonRegressionHead()
    logits = tf.keras.Model(...)(features)

    return my_head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        optimizer=tf.AdagradOptimizer(learning_rate=0.1),
        logits=logits)

  my_estimator = tf.estimator.Estimator(model_fn=_my_model_fn)
  ```

  Args:
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.
    label_dimension: Number of regression labels per example. This is the size
      of the last dimension of the labels `Tensor` (typically, this has shape
      `[batch_size, label_dimension]`).
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch and label dimension. Defaults to
      `SUM_OVER_BATCH_SIZE`, namely weighted sum of losses divided by
      `batch size * label_dimension`.
    compute_full_loss: Whether to include the constant `log(z!)` term in
      computing the poisson loss. See `tf.nn.log_poisson_loss` for the full
      documentation.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def __init__(self,
               label_dimension=1,
               weight_column=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               compute_full_loss=True,
               name=None):
    self._compute_full_loss = compute_full_loss
    super(PoissonRegressionHead, self).__init__(
        label_dimension=label_dimension,
        weight_column=weight_column,
        loss_reduction=loss_reduction,
        loss_fn=self._poisson_loss,
        inverse_link_fn=math_ops.exp,
        name=name)

  def _poisson_loss(self, labels, logits):
    return nn.log_poisson_loss(
        targets=labels, log_input=logits,
        compute_full_loss=self._compute_full_loss)


class LogisticRegressionHead(RegressionHead):
  """Creates a `Head` for logistic regression.

  Uses `sigmoid_cross_entropy_with_logits` loss, which is the same as
  `BinaryClassHead`. The differences compared to `BinaryClassHead` are:

  * Does not support `label_vocabulary`. Instead, labels must be float in the
    range [0, 1].
  * Does not calculate some metrics that do not make sense, such as AUC.
  * In `PREDICT` mode, only returns logits and predictions
    (`=tf.sigmoid(logits)`), whereas `BinaryClassHead` also returns
    probabilities, classes, and class_ids.
  * Export output defaults to `RegressionOutput`, whereas `BinaryClassHead`
    defaults to `PredictOutput`.

  The head expects `logits` with shape `[D0, D1, ... DN, 1]`.
  In many applications, the shape is `[batch_size, 1]`.

  The `labels` shape must match `logits`, namely
  `[D0, D1, ... DN]` or `[D0, D1, ... DN, 1]`.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]` or `[D0, D1, ... DN, 1]`.

  This is implemented as a generalized linear model, see
  https://en.wikipedia.org/wiki/Generalized_linear_model.

  The head can be used with a canned estimator. Example:

  ```python
  my_head = tf.estimator.LogisticRegressionHead()
  my_estimator = tf.estimator.DNNEstimator(
      head=my_head,
      hidden_units=...,
      feature_columns=...)
  ```

  It can also be used with a custom `model_fn`. Example:

  ```python
  def _my_model_fn(features, labels, mode):
    my_head = tf.estimator.LogisticRegressionHead()
    logits = tf.keras.Model(...)(features)

    return my_head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        optimizer=tf.AdagradOptimizer(learning_rate=0.1),
        logits=logits)

  my_estimator = tf.estimator.Estimator(model_fn=_my_model_fn)
  ```

  Args:
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch and label dimension. Defaults to
      `SUM_OVER_BATCH_SIZE`, namely weighted sum of losses divided by
      `batch size * label_dimension`.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def _logistic_loss(self, labels, logits):
    labels = base_head.check_label_range(
        labels, n_classes=2, message='Labels must be in range [0, 1]')
    return nn.sigmoid_cross_entropy_with_logits(
        labels=labels, logits=logits)

  def __init__(self,
               weight_column=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               name=None):
    super(LogisticRegressionHead, self).__init__(
        label_dimension=1,
        weight_column=weight_column,
        loss_reduction=loss_reduction,
        loss_fn=self._logistic_loss,
        inverse_link_fn=math_ops.sigmoid,
        name=name)
