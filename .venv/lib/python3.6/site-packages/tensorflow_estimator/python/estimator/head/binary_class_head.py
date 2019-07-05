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
"""Binary class head."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.python.eager import context
from tensorflow.python.framework import ops
from tensorflow.python.keras import metrics
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import control_flow_ops
from tensorflow.python.ops import lookup_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import nn
from tensorflow.python.ops import string_ops
from tensorflow.python.ops.losses import losses
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.canned import prediction_keys
from tensorflow_estimator.python.estimator.export import export_output
from tensorflow_estimator.python.estimator.head import base_head


class BinaryClassHead(base_head.Head):
  """Creates a `Head` for single label binary classification.

  Uses `sigmoid_cross_entropy_with_logits` loss.

  The head expects `logits` with shape `[D0, D1, ... DN, 1]`.
  In many applications, the shape is `[batch_size, 1]`.

  `labels` must be a dense `Tensor` with shape matching `logits`, namely
  `[D0, D1, ... DN, 1]`. If `label_vocabulary` given, `labels` must be a string
  `Tensor` with values from the vocabulary. If `label_vocabulary` is not given,
  `labels` must be float `Tensor` with values in the interval `[0, 1]`.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]`, or `[D0, D1, ... DN, 1]`.

  The loss is the weighted sum over the input dimensions. Namely, if the input
  labels have shape `[batch_size, 1]`, the loss is the weighted sum over
  `batch_size`.

  Also supports custom `loss_fn`. `loss_fn` takes `(labels, logits)` or
  `(labels, logits, features, loss_reduction)` as arguments and returns loss
  with shape `[D0, D1, ... DN, 1]`. `loss_fn` must support float `labels` with
  shape `[D0, D1, ... DN, 1]`. Namely, the head applies `label_vocabulary` to
  the input labels before passing them to `loss_fn`.

  Args:
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.
    thresholds: Iterable of floats in the range `(0, 1)`. For binary
      classification metrics such as precision and recall, an eval metric is
      generated for each threshold value. This threshold is applied to the
      logistic values to determine the binary classification (i.e., above the
      threshold is `true`, below is `false`.
    label_vocabulary: A list or tuple of strings representing possible label
      values. If it is not given, that means labels are already encoded within
      [0, 1]. If given, labels must be string type and have any value in
      `label_vocabulary`. Note that errors will be raised if `label_vocabulary`
      is not provided but labels are strings.
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`, namely
      weighted sum of losses divided by `batch size * label_dimension`.
    loss_fn: Optional loss function.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def __init__(self,
               weight_column=None,
               thresholds=None,
               label_vocabulary=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               loss_fn=None,
               name=None):
    if label_vocabulary is not None and not isinstance(label_vocabulary,
                                                       (list, tuple)):
      raise ValueError(
          'label_vocabulary should be a list or a tuple. Given type: {}'.format(
              type(label_vocabulary)))
    thresholds = tuple(thresholds) if thresholds else tuple()
    for threshold in thresholds:
      if (threshold <= 0.0) or (threshold >= 1.0):
        raise ValueError('thresholds not in (0, 1): {}.'.format((thresholds,)))
    if (loss_reduction not in losses.Reduction.all() or
        loss_reduction == losses.Reduction.NONE):
      raise ValueError(
          'Invalid loss_reduction: {}. See `tf.losses.Reduction` for valid '
          'options.'.format(loss_reduction))
    if loss_fn:
      base_head.validate_loss_fn_args(loss_fn)

    self._weight_column = weight_column
    self._thresholds = thresholds
    self._label_vocabulary = label_vocabulary
    self._loss_reduction = loss_reduction
    self._loss_fn = loss_fn
    self._name = name
    # Metric keys.
    keys = metric_keys.MetricKeys
    self._loss_mean_key = self._summary_key(keys.LOSS_MEAN)
    self._accuracy_key = self._summary_key(keys.ACCURACY)
    self._precision_key = self._summary_key(keys.PRECISION)
    self._recall_key = self._summary_key(keys.RECALL)
    self._prediction_mean_key = self._summary_key(keys.PREDICTION_MEAN)
    self._label_mean_key = self._summary_key(keys.LABEL_MEAN)
    self._accuracy_baseline_key = self._summary_key(keys.ACCURACY_BASELINE)
    self._auc_key = self._summary_key(keys.AUC)
    self._auc_pr_key = self._summary_key(keys.AUC_PR)
    self._loss_regularization_key = self._summary_key(keys.LOSS_REGULARIZATION)
    accuracy_keys = []
    precision_keys = []
    recall_keys = []
    for threshold in self._thresholds:
      accuracy_keys.append(
          self._summary_key(keys.ACCURACY_AT_THRESHOLD % threshold))
      precision_keys.append(
          self._summary_key(keys.PRECISION_AT_THRESHOLD % threshold))
      recall_keys.append(
          self._summary_key(keys.RECALL_AT_THRESHOLD % threshold))
    self._accuracy_keys = tuple(accuracy_keys)
    self._precision_keys = tuple(precision_keys)
    self._recall_keys = tuple(recall_keys)

  @property
  def name(self):
    """See `base_head.Head` for details."""
    return self._name

  @property
  def logits_dimension(self):
    """See `base_head.Head` for details."""
    return 1

  @property
  def loss_reduction(self):
    """See `base_head.Head` for details."""
    return self._loss_reduction

  # Attributes for lookup tables in Eager execution. Note that for Graph
  # execution, the lookup tables are created on demand to make sure the lookup
  # table is in the same graph as its input tensors for `train` and `eval` of
  # Estimator (as Estimator recreates graphes for `train`, `eval` and
  # `predict`).
  _cached_class_id_table = None
  _cached_class_string_table = None

  @property
  def _class_id_table(self):
    """Creates a lookup table for class_id.

    In eager execution, this lookup table will be lazily created on the first
    call of `self._class_id_table`, and cached for later use; In graph
    execution, it will be created on demand.

    Returns:
      A hash table for lookup.
    """
    if self._cached_class_id_table is None or not context.executing_eagerly():
      self._cached_class_id_table = lookup_ops.index_table_from_tensor(
          vocabulary_list=tuple(self._label_vocabulary), name='class_id_lookup')
    return self._cached_class_id_table

  @property
  def _class_string_table(self):
    """Creates a lookup table for class_string.

    In eager execution, this lookup table will be lazily created on the first
    call of `self._class_string_table` and cached for later use; In graph
    execution, it will be created on demand.

    Returns:
      A hash table for lookup.
    """
    if (self._cached_class_string_table is None
        or not context.executing_eagerly()):
      self._cached_class_string_table = (
          lookup_ops.index_to_string_table_from_tensor(
              vocabulary_list=self._label_vocabulary, name='class_string_lookup'
              ))
    return self._cached_class_string_table

  def _processed_labels(self, logits, labels):
    """Converts labels to integer id space."""
    labels = base_head.check_dense_labels_match_logits_and_reshape(
        labels=labels, logits=logits, expected_labels_dimension=1)
    if self._label_vocabulary is not None:
      labels = self._class_id_table.lookup(labels)
    labels = math_ops.to_float(labels)
    return base_head.check_label_range(labels, n_classes=2)

  def _unweighted_loss_and_weights(self, logits, labels, features):
    """Computes unweighted loss and weights."""
    if self._loss_fn:
      unweighted_loss = base_head.call_loss_fn(
          loss_fn=self._loss_fn, labels=labels, logits=logits,
          features=features, expected_loss_dim=1)
    else:
      unweighted_loss = nn.sigmoid_cross_entropy_with_logits(
          labels=labels, logits=logits)
    weights = base_head.get_weights_and_check_match_logits(
        features=features, weight_column=self._weight_column, logits=logits)
    return unweighted_loss, weights

  def loss(self, logits, labels, features=None, mode=None,
           regularization_losses=None):
    """Returns regularized training loss. See `base_head.Head` for details."""
    del mode  # Unused for this head.
    with ops.name_scope('losses', values=(logits, labels, regularization_losses,
                                          features)):
      logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
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

  def predictions(self, logits, keys=None):
    """Return predictions based on keys. See `base_head.Head` for details.

    Args:
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.
      keys: a list or tuple of prediction keys. Each key can be either the class
        variable of prediction_keys.PredictionKeys or its string value, such as:
        prediction_keys.PredictionKeys.CLASSES or 'classes'. If not specified,
        it will return the predictions for all valid keys.

    Returns:
      A dict of predictions.
    """
    pred_keys = prediction_keys.PredictionKeys
    valid_keys = [pred_keys.LOGITS, pred_keys.LOGISTIC, pred_keys.PROBABILITIES,
                  pred_keys.CLASS_IDS, pred_keys.CLASSES]
    if keys:
      base_head.check_prediction_keys(keys, valid_keys)
    else:
      keys = valid_keys
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    predictions = {}
    with ops.name_scope('predictions', values=(logits,)):
      if pred_keys.LOGITS in keys:
        predictions[pred_keys.LOGITS] = logits
      if pred_keys.LOGISTIC in keys:
        logistic = math_ops.sigmoid(logits, name=pred_keys.LOGISTIC)
        predictions[pred_keys.LOGISTIC] = logistic
      two_class_logits = array_ops.concat(
          (array_ops.zeros_like(logits), logits),
          axis=-1, name='two_class_logits')
      if pred_keys.PROBABILITIES in keys:
        probabilities = nn.softmax(
            two_class_logits, name=pred_keys.PROBABILITIES)
        predictions[pred_keys.PROBABILITIES] = probabilities
      if pred_keys.CLASS_IDS in keys or pred_keys.CLASSES in keys:
        class_ids = math_ops.argmax(
            two_class_logits, axis=-1, name=pred_keys.CLASS_IDS)
        class_ids = array_ops.expand_dims(class_ids, axis=-1)
        if pred_keys.CLASS_IDS in keys:
          predictions[pred_keys.CLASS_IDS] = class_ids
        if pred_keys.CLASSES in keys:
          if self._label_vocabulary is not None:
            classes = self._class_string_table.lookup(class_ids)
          else:
            classes = string_ops.as_string(class_ids, name='str_classes')
          predictions[pred_keys.CLASSES] = classes
      return predictions

  def metrics(self, regularization_losses=None):
    """Creates metrics. See `base_head.Head` for details."""
    keys = metric_keys.MetricKeys
    with ops.name_scope('metrics', values=(regularization_losses,)):
      # Mean metric.
      eval_metrics = {}
      eval_metrics[self._loss_mean_key] = metrics.Mean(name=keys.LOSS_MEAN)
      eval_metrics[self._accuracy_key] = metrics.Accuracy(name=keys.ACCURACY)
      eval_metrics[self._precision_key] = metrics.Precision(name=keys.PRECISION)
      eval_metrics[self._recall_key] = metrics.Recall(name=keys.RECALL)
      eval_metrics[self._prediction_mean_key] = metrics.Mean(
          name=keys.PREDICTION_MEAN)
      eval_metrics[self._label_mean_key] = metrics.Mean(name=keys.LABEL_MEAN)
      eval_metrics[self._accuracy_baseline_key] = (
          metrics.Mean(name=keys.ACCURACY_BASELINE))
      # TODO(b/118843532): create Keras metrics
      # eval_metrics[self._auc_key] = metrics.Precision(name=keys.AUC)
      # eval_metrics[self._auc_pr_key] = metrics.Precision(name=keys.AUC_PR)
      if regularization_losses is not None:
        eval_metrics[self._loss_regularization_key] = metrics.Mean(
            name=keys.LOSS_REGULARIZATION)
      for i, threshold in enumerate(self._thresholds):
        eval_metrics[self._accuracy_keys[i]] = metrics.BinaryAccuracy(
            name=self._accuracy_keys[i], threshold=threshold)
        eval_metrics[self._precision_keys[i]] = metrics.Precision(
            name=self._precision_keys[i], thresholds=threshold)
        eval_metrics[self._recall_keys[i]] = metrics.Recall(
            name=self._recall_keys[i], thresholds=threshold)
    return eval_metrics

  def _update_accuracy_baseline(self, eval_metrics):
    """Update accuracy baseline metric based on labels mean metric.

    This is the best the model could do by always predicting one class.

    For example, suppose the labels = [0, 1, 0, 1, 1]. So the
    label_mean.total = 3, label_mean.count = 5, and
    label_mean = label_mean.total / label_mean.count = 3 / 5 = 0.6
    By always predicting one class, there are two cases:
    (1) predicted_labels_0 = [0, 0, 0, 0, 0], accuracy_0 = 2 / 5 = 0.4
    (2) predicted_labels_1 = [1, 1, 1, 1, 1], accuracy_1 = 3 / 5 = 0.6
    So the accuracy_baseline = max(accuracy_0, accuracy_1) = 0.6,
                             = max(label_mean, 1 - label_mean)

    To update the total and count of accuracy_baseline,
    accuracy_baseline = max(label_mean, 1 - label_mean)
                      = max(label_mean.total / label_mean.count,
                            1 - label_mean.total / label_mean.count)
                      = max(label_mean.total / label_mean.count,
                      (label_mean.count - label_mean.total) / label_mean.count)
    So accuracy_baseline.total = max(label_mean.total,
                                    (label_mean.count - label_mean.total))
    accuracy_baseline.count = label_mean.count

    Args:
      eval_metrics: A `dict` of metrics to be updated.
    """
    label_mean_metric = eval_metrics[self._label_mean_key]
    accuracy_baseline_metric = eval_metrics[self._accuracy_baseline_key]
    # Mimic the call of accuracy_baseline_metric.update_state()
    accuracy_baseline_metric._updates = [control_flow_ops.no_op()]  # pylint: disable=protected-access
    accuracy_baseline_metric.total = math_ops.maximum(
        label_mean_metric.total,
        label_mean_metric.count - label_mean_metric.total)
    accuracy_baseline_metric.count = label_mean_metric.count

  def update_metrics(self, eval_metrics, features, logits, labels,
                     regularization_losses=None):
    """Updates eval metrics. See `base_head.Head` for details."""
    preds = self.predictions(logits)
    class_ids = preds[prediction_keys.PredictionKeys.CLASS_IDS]
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    labels = self._processed_labels(logits, labels)
    unweighted_loss, weights = self._unweighted_loss_and_weights(
        logits, labels, features)
    # Update metrics.
    eval_metrics[self._loss_mean_key].update_state(
        values=unweighted_loss, sample_weight=weights)
    eval_metrics[self._accuracy_key].update_state(
        y_true=labels, y_pred=class_ids, sample_weight=weights)
    eval_metrics[self._precision_key].update_state(
        y_true=labels, y_pred=class_ids, sample_weight=weights)
    eval_metrics[self._recall_key].update_state(
        y_true=labels, y_pred=class_ids, sample_weight=weights)
    logistic_key = prediction_keys.PredictionKeys.LOGISTIC
    predictions = self.predictions(logits, [logistic_key])
    logistic = predictions[logistic_key]
    base_head.update_metric_with_broadcast_weights(
        eval_metrics[self._prediction_mean_key], logistic, weights)
    base_head.update_metric_with_broadcast_weights(
        eval_metrics[self._label_mean_key], labels, weights)
    self._update_accuracy_baseline(eval_metrics)
    # TODO(b/118843532): update Keras metrics
    # eval_metrics[self._auc_key].update_state(...)
    # eval_metrics[self._auc_pr_key].update_state(...)
    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      eval_metrics[self._loss_regularization_key].update_state(
          values=regularization_loss)
    for i in range(len(self._thresholds)):
      eval_metrics[self._accuracy_keys[i]].update_state(
          y_true=labels, y_pred=logistic, sample_weight=weights)
      eval_metrics[self._precision_keys[i]].update_state(
          y_true=labels, y_pred=logistic, sample_weight=weights)
      eval_metrics[self._recall_keys[i]].update_state(
          y_true=labels, y_pred=logistic, sample_weight=weights)
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
      logits: logits `Tensor` with shape `[D0, D1, ... DN, 1]`. For many
        applications, the shape is `[batch_size, 1]`.
      labels: Labels integer or string `Tensor` with shape matching `logits`,
        namely `[D0, D1, ... DN, 1]` or `[D0, D1, ... DN]`. `labels` is required
        argument when `mode` equals `TRAIN` or `EVAL`.
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
      `EstimatorSpec`.

    Raises:
      ValueError: If both `train_op_fn` and `optimizer` are `None` in TRAIN
        mode, or if both are set.
    """
    with ops.name_scope(self._name, 'head'):
      # Predict.
      pred_keys = prediction_keys.PredictionKeys
      predictions = self.predictions(logits)
      if mode == model_fn.ModeKeys.PREDICT:
        probabilities = predictions[pred_keys.PROBABILITIES]
        logistic = predictions[pred_keys.LOGISTIC]
        classifier_output = base_head.classification_output(
            scores=probabilities, n_classes=2,
            label_vocabulary=self._label_vocabulary)
        return model_fn._TPUEstimatorSpec(  # pylint: disable=protected-access
            mode=model_fn.ModeKeys.PREDICT,
            predictions=predictions,
            export_outputs={
                base_head.DEFAULT_SERVING_KEY: classifier_output,
                base_head.CLASSIFY_SERVING_KEY: classifier_output,
                base_head.REGRESS_SERVING_KEY:
                    export_output.RegressionOutput(value=logistic),
                base_head.PREDICT_SERVING_KEY:
                    export_output.PredictOutput(predictions)
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
