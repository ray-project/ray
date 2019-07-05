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
"""Multi label head."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six

from tensorflow.python.eager import context
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.framework import sparse_tensor
from tensorflow.python.keras import metrics
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import lookup_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import sparse_ops
from tensorflow.python.ops.losses import losses
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.canned import prediction_keys
from tensorflow_estimator.python.estimator.export import export_output
from tensorflow_estimator.python.estimator.head import base_head


class MultiLabelHead(base_head.Head):
  """Creates a `Head` for multi-label classification.

  Multi-label classification handles the case where each example may have zero
  or more associated labels, from a discrete set. This is distinct from
  `MultiClassHead` which has exactly one label per example.

  Uses `sigmoid_cross_entropy` loss average over classes and weighted sum over
  the batch. Namely, if the input logits have shape `[batch_size, n_classes]`,
  the loss is the average over `n_classes` and the weighted sum over
  `batch_size`.

  The head expects `logits` with shape `[D0, D1, ... DN, n_classes]`. In many
  applications, the shape is `[batch_size, n_classes]`.

  Labels can be:

  * A multi-hot tensor of shape `[D0, D1, ... DN, n_classes]`
  * An integer `SparseTensor` of class indices. The `dense_shape` must be
    `[D0, D1, ... DN, ?]` and the values within `[0, n_classes)`.
  * If `label_vocabulary` is given, a string `SparseTensor`. The `dense_shape`
    must be `[D0, D1, ... DN, ?]` and the values within `label_vocabulary` or a
    multi-hot tensor of shape `[D0, D1, ... DN, n_classes]`.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]`, or `[D0, D1, ... DN, 1]`.

  Also supports custom `loss_fn`. `loss_fn` takes `(labels, logits)` or
  `(labels, logits, features)` as arguments and returns unreduced loss with
  shape `[D0, D1, ... DN, 1]`. `loss_fn` must support indicator `labels` with
  shape `[D0, D1, ... DN, n_classes]`. Namely, the head applies
  `label_vocabulary` to the input labels before passing them to `loss_fn`.

  The head can be used with a canned estimator. Example:

  ```python
  my_head = tf.estimator.MultiLabelHead(n_classes=3)
  my_estimator = tf.estimator.DNNEstimator(
      head=my_head,
      hidden_units=...,
      feature_columns=...)
  ```

  It can also be used with a custom `model_fn`. Example:

  ```python
  def _my_model_fn(features, labels, mode):
    my_head = tf.estimator.MultiLabelHead(n_classes=3)
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
    n_classes: Number of classes, must be greater than 1 (for 1 class, use
      `BinaryClassHead`).
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.  Per-class weighting is
      not supported.
    thresholds: Iterable of floats in the range `(0, 1)`. Accuracy, precision
      and recall metrics are evaluated for each threshold value. The threshold
      is applied to the predicted probabilities, i.e. above the threshold is
      `true`, below is `false`.
    label_vocabulary: A list of strings represents possible label values. If it
      is not given, that means labels are already encoded as integer within
      [0, n_classes) or multi-hot Tensor. If given, labels must be SparseTensor
      `string` type and have any value in `label_vocabulary`. Also there will be
      errors if vocabulary is not provided and labels are string.
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`, namely
      weighted sum of losses divided by batch size.
    loss_fn: Optional loss function.
    classes_for_class_based_metrics: List of integer class IDs or string class
      names for which per-class metrics are evaluated. If integers, all must be
      in the range `[0, n_classes - 1]`. If strings, all must be in
      `label_vocabulary`.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def __init__(self,
               n_classes,
               weight_column=None,
               thresholds=None,
               label_vocabulary=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               loss_fn=None,
               classes_for_class_based_metrics=None,
               name=None):
    if n_classes is None or n_classes < 2:
      raise ValueError(
          'n_classes must be > 1 for multi-label classification. '
          'Given: {}'.format(n_classes))
    thresholds = tuple(thresholds) if thresholds else tuple()
    for threshold in thresholds:
      if (threshold <= 0.0) or (threshold >= 1.0):
        raise ValueError(
            'thresholds must be in (0, 1) range. Given: {}'.format(threshold))
    if label_vocabulary is not None:
      if not isinstance(label_vocabulary, (list, tuple)):
        raise ValueError(
            'label_vocabulary must be a list or tuple. '
            'Given type: {}'.format(type(label_vocabulary)))
      if len(label_vocabulary) != n_classes:
        raise ValueError(
            'Length of label_vocabulary must be n_classes ({}). '
            'Given: {}'.format(n_classes, len(label_vocabulary)))

    if loss_fn:
      base_head.validate_loss_fn_args(loss_fn)
    if (loss_reduction not in losses.Reduction.all() or
        loss_reduction == losses.Reduction.NONE):
      raise ValueError('Invalid loss_reduction: {}'.format(loss_reduction))
    if classes_for_class_based_metrics:
      classes_for_class_based_metrics = tuple(classes_for_class_based_metrics)
      if isinstance(classes_for_class_based_metrics[0], six.string_types):
        if not label_vocabulary:
          raise ValueError(
              'label_vocabulary must be provided when '
              'classes_for_class_based_metrics are sting.')
        class_ids = []
        for class_string in classes_for_class_based_metrics:
          class_ids.append(label_vocabulary.index(class_string))
        classes_for_class_based_metrics = tuple(class_ids)
      else:
        for class_id in classes_for_class_based_metrics:
          if (class_id < 0) or (class_id >= n_classes):
            raise ValueError(
                'All classes_for_class_based_metrics must be in range [0, {}]. '
                'Given: {}'.format(n_classes - 1, class_id))
    else:
      classes_for_class_based_metrics = tuple()
    self._n_classes = n_classes
    self._weight_column = weight_column
    self._thresholds = thresholds
    self._label_vocabulary = label_vocabulary
    self._loss_reduction = loss_reduction
    self._loss_fn = loss_fn
    self._classes_for_class_based_metrics = classes_for_class_based_metrics
    self._name = name
    # Metric keys.
    keys = metric_keys.MetricKeys
    self._loss_mean_key = self._summary_key(keys.LOSS_MEAN)
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
    prob_keys = []
    auc_keys = []
    auc_pr_keys = []
    for class_id in self._classes_for_class_based_metrics:
      if self._label_vocabulary is None:
        prob_key = keys.PROBABILITY_MEAN_AT_CLASS % class_id
        auc_key = keys.AUC_AT_CLASS % class_id
        auc_pr_key = keys.AUC_PR_AT_CLASS % class_id
      else:
        prob_key = (
            keys.PROBABILITY_MEAN_AT_NAME % self._label_vocabulary[class_id])
        auc_key = keys.AUC_AT_NAME % self._label_vocabulary[class_id]
        auc_pr_key = keys.AUC_PR_AT_NAME % self._label_vocabulary[class_id]
      prob_keys.append(self._summary_key(prob_key))
      auc_keys.append(self._summary_key(auc_key))
      auc_pr_keys.append(self._summary_key(auc_pr_key))
    self._prob_keys = tuple(prob_keys)
    self._auc_keys = tuple(auc_keys)
    self._auc_pr_keys = tuple(auc_pr_keys)

  @property
  def name(self):
    """See `base_head.Head` for details."""
    return self._name

  @property
  def logits_dimension(self):
    """See `base_head.Head` for details."""
    return self._n_classes

  @property
  def loss_reduction(self):
    """See `base_head.Head` for details."""
    return self._loss_reduction

  # An attribute for lookup table. Note that for Graph execution, the lookup
  # table is created on demand to make sure the lookup table is in the same
  # graph as its input tensors for `train` and `eval` of Estimator (as Estimator
  # re-creates graphes for `train`, `eval` and `predict`).
  _cached_class_id_table = None

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

  def _processed_labels(self, logits, labels):
    """Converts labels to integer id space."""
    if labels is None:
      raise ValueError(base_head._LABEL_NONE_ERR_MSG)  # pylint:disable=protected-access
    if isinstance(labels, sparse_tensor.SparseTensor):
      label_values = labels.values
      if labels.dtype == dtypes.string:
        label_ids_values = self._class_id_table.lookup(label_values)
        label_ids = sparse_tensor.SparseTensor(
            indices=labels.indices,
            values=label_ids_values,
            dense_shape=labels.dense_shape)
        processed_labels = sparse_ops.sparse_to_indicator(
            label_ids, self._n_classes)
      else:
        if not label_values.dtype.is_integer:
          raise ValueError('Labels dtype should be integer. Instead got {}.'.
                           format(label_values.dtype))
        err_msg = (
            r'labels must be an integer SparseTensor with values in '
            r'[0, {})'.format(self._n_classes))
        label_values = base_head.check_label_range(
            labels.values, self._n_classes, message=err_msg)
        if context.executing_eagerly():
          processed_labels = sparse_ops.sparse_to_indicator(
              labels, self._n_classes)
        else:
          with ops.control_dependencies([label_values]):
            processed_labels = sparse_ops.sparse_to_indicator(
                labels, self._n_classes)
      processed_labels = math_ops.to_int64(processed_labels)
    else:
      err_msg = (
          r'labels must be an integer indicator Tensor with values in [0, 1]')
      processed_labels = base_head.check_label_range(labels, 2, message=err_msg)

    return base_head.check_dense_labels_match_logits_and_reshape(
        labels=processed_labels, logits=logits,
        expected_labels_dimension=self.logits_dimension)

  def _unweighted_loss_and_weights(self, logits, processed_labels, features):
    """Computes loss spec."""
    if self._loss_fn:
      unweighted_loss = base_head.call_loss_fn(
          loss_fn=self._loss_fn,
          labels=processed_labels,
          logits=logits,
          features=features,
          expected_loss_dim=1)
    else:
      unweighted_loss = losses.sigmoid_cross_entropy(
          multi_class_labels=processed_labels, logits=logits,
          reduction=losses.Reduction.NONE)
      # Averages loss over classes.
      unweighted_loss = math_ops.reduce_mean(
          unweighted_loss, axis=-1, keepdims=True)
    weights = base_head.get_weights_and_check_match_logits(
        features=features,
        weight_column=self._weight_column,
        logits=logits)
    return unweighted_loss, weights

  def loss(self, logits, labels, features=None, mode=None,
           regularization_losses=None):
    """Returns regularized training loss. See `base_head.Head` for details."""
    del mode  # Unused for this head.
    with ops.name_scope('losses', values=(logits, labels, regularization_losses,
                                          features)):
      logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
      processed_labels = self._processed_labels(logits, labels)
      unweighted_loss, weights = self._unweighted_loss_and_weights(
          logits, processed_labels, features)
      training_loss = losses.compute_weighted_loss(
          unweighted_loss, weights=weights, reduction=self._loss_reduction)
      regularization_loss = math_ops.add_n(
          regularization_losses) if regularization_losses is not None else None
      regularized_training_loss = (
          training_loss + regularization_loss if regularization_loss is not None
          else training_loss)
    return regularized_training_loss

  def predictions(self, logits, keys=None):
    """Return predictions based on keys.  See `base_head.Head` for details.

    Args:
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.
      keys: a list of prediction keys. Key can be either the class variable
        of prediction_keys.PredictionKeys or its string value, such as:
        prediction_keys.PredictionKeys.LOGITS or 'logits'.

    Returns:
      A dict of predictions.
    """
    pred_keys = prediction_keys.PredictionKeys
    valid_keys = [pred_keys.LOGITS, pred_keys.PROBABILITIES]
    if keys:
      base_head.check_prediction_keys(keys, valid_keys)
    else:
      keys = valid_keys
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    predictions = {}
    with ops.name_scope('predictions', values=(logits,)):
      if pred_keys.LOGITS in keys:
        predictions[pred_keys.LOGITS] = logits
      if pred_keys.PROBABILITIES in keys:
        probabilities = math_ops.sigmoid(logits, name=pred_keys.PROBABILITIES)
        predictions[pred_keys.PROBABILITIES] = probabilities
      return predictions

  def metrics(self, regularization_losses=None):
    """Creates metrics. See `base_head.Head` for details."""
    keys = metric_keys.MetricKeys
    with ops.name_scope(None, 'metrics', (regularization_losses,)):
      # Mean metric.
      eval_metrics = {}
      eval_metrics[self._loss_mean_key] = metrics.Mean(name=keys.LOSS_MEAN)
      # TODO(b/118843532): create Keras metrics
      # eval_metrics[self._auc] = metrics.Precision(name=keys.AUC)
      # eval_metrics[self._auc_pr_key] = metrics.Precision(name=keys.AUC_PR)
      if regularization_losses is not None:
        eval_metrics[self._loss_regularization_key] = metrics.Mean(
            name=keys.LOSS_REGULARIZATION)
      for i, threshold in enumerate(self._thresholds):
        eval_metrics[self._accuracy_keys[i]] = metrics.BinaryAccuracy(
            name=self._accuracy_keys[i], threshold=threshold)
        eval_metrics[self._precision_keys[i]] = (
            metrics.Precision(name=self._precision_keys[i],
                              thresholds=threshold))
        eval_metrics[self._recall_keys[i]] = metrics.Recall(
            name=self._recall_keys[i], thresholds=threshold)
      for i in range(len(self._classes_for_class_based_metrics)):
        # TODO(b/118843532): create Keras metrics
        eval_metrics[self._prob_keys[i]] = metrics.Mean(name=self._prob_keys[i])
        # eval_metrics[self._auc_keys[i]] = metrics.AUC(name=self._auc_keys[i])
        # eval_metrics[self._auc_pr_keys[i]] = metrics.AUC_PR(
        #     name=self._auc_pr_keys[i])

    return eval_metrics

  def update_metrics(self, eval_metrics, features, logits, labels,
                     regularization_losses=None):
    """Updates eval metrics. See `base_head.Head` for details."""
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    processed_labels = self._processed_labels(logits, labels)
    unweighted_loss, weights = self._unweighted_loss_and_weights(
        logits, processed_labels, features)
    prob_key = prediction_keys.PredictionKeys.PROBABILITIES
    predictions = self.predictions(logits, [prob_key])
    probabilities = predictions[prob_key]

    # Update metrics.
    eval_metrics[self._loss_mean_key].update_state(
        values=unweighted_loss, sample_weight=weights)
    # TODO(b/118843532): update Keras metrics
    # eval_metrics[self._auc_key].update_state(...)
    # eval_metrics[self._auc_pr_key].update_state(...)
    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      eval_metrics[self._loss_regularization_key].update_state(
          values=regularization_loss)
    for i in range(len(self._thresholds)):
      eval_metrics[self._accuracy_keys[i]].update_state(
          y_true=labels, y_pred=probabilities, sample_weight=weights)
      eval_metrics[self._precision_keys[i]].update_state(
          y_true=labels, y_pred=probabilities, sample_weight=weights)
      eval_metrics[self._recall_keys[i]].update_state(
          y_true=labels, y_pred=probabilities, sample_weight=weights)
    for i, class_id in enumerate(self._classes_for_class_based_metrics):
      batch_rank = array_ops.rank(probabilities) - 1
      begin = array_ops.concat(
          [array_ops.zeros([batch_rank], dtype=dtypes.int32), [class_id]],
          axis=0)
      size = array_ops.concat(
          [-1 * array_ops.ones([batch_rank], dtype=dtypes.int32), [1]],
          axis=0)
      class_probabilities = array_ops.slice(
          probabilities, begin=begin, size=size)
      # class_labels = array_ops.slice(labels, begin=begin, size=size)
      # TODO(b/118843532): update Keras metrics
      base_head.update_metric_with_broadcast_weights(
          eval_metrics[self._prob_keys[i]], class_probabilities, weights)
      # eval_metrics[self._auc_keys[i]].update_state(...)
      # eval_metrics[self._auc_pr_key[i]].update_state(...)
    return eval_metrics

  def _create_tpu_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns an `model_fn._TPUEstimatorSpec`.

    Args:
      features: Input `dict` of `Tensor` or `SparseTensor` objects.
      mode: Estimator's `ModeKeys`.
      logits: logits `Tensor` with shape `[D0, D1, ... DN, n_classes]`.
        For many applications, the shape is `[batch_size, n_classes]`.
      labels: Labels with shape matching `logits`. Can be multi-hot `Tensor`
        with shape `[D0, D1, ... DN, n_classes]` or `SparseTensor` with
        `dense_shape` `[D0, D1, ... DN, ?]`. `labels` is required argument when
        `mode` equals `TRAIN` or `EVAL`.
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
      `model_fn._TPUEstimatorSpec`.
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
        classifier_output = base_head.classification_output(
            scores=probabilities, n_classes=self._n_classes,
            label_vocabulary=self._label_vocabulary)
        return model_fn._TPUEstimatorSpec(  # pylint:disable=protected-access
            mode=model_fn.ModeKeys.PREDICT,
            predictions=predictions,
            export_outputs={
                base_head.DEFAULT_SERVING_KEY: classifier_output,
                base_head.CLASSIFY_SERVING_KEY: classifier_output,
                base_head.PREDICT_SERVING_KEY: (
                    export_output.PredictOutput(predictions))
            })

      regularized_training_loss = self.loss(
          logits=logits, labels=labels, features=features, mode=mode,
          regularization_losses=regularization_losses)
      # Eval.
      if mode == model_fn.ModeKeys.EVAL:
        eval_metrics = self.metrics(regularization_losses=regularization_losses)
        return model_fn._TPUEstimatorSpec(  # pylint:disable=protected-access
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
