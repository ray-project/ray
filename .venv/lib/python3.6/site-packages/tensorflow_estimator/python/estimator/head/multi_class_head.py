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
"""Multi class head."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.python.eager import context
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.keras import metrics
from tensorflow.python.ops import array_ops
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


class MultiClassHead(base_head.Head):
  """Creates a `Head` for multi class classification.

  Uses `sparse_softmax_cross_entropy` loss.

  The head expects `logits` with shape `[D0, D1, ... DN, n_classes]`.
  In many applications, the shape is `[batch_size, n_classes]`.

  `labels` must be a dense `Tensor` with shape matching `logits`, namely
  `[D0, D1, ... DN, 1]`. If `label_vocabulary` given, `labels` must be a string
  `Tensor` with values from the vocabulary. If `label_vocabulary` is not given,
  `labels` must be an integer `Tensor` with values specifying the class index.

  If `weight_column` is specified, weights must be of shape
  `[D0, D1, ... DN]`, or `[D0, D1, ... DN, 1]`.

  The loss is the weighted sum over the input dimensions. Namely, if the input
  labels have shape `[batch_size, 1]`, the loss is the weighted sum over
  `batch_size`.

  Also supports custom `loss_fn`. `loss_fn` takes `(labels, logits)` or
  `(labels, logits, features, loss_reduction)` as arguments and returns
  unreduced loss with shape `[D0, D1, ... DN, 1]`. `loss_fn` must support
  integer `labels` with shape `[D0, D1, ... DN, 1]`. Namely, the head applies
  `label_vocabulary` to the input labels before passing them to `loss_fn`.

  The head can be used with a canned estimator. Example:

  ```python
  my_head = tf.estimator.MultiClassHead(n_classes=3)
  my_estimator = tf.estimator.DNNEstimator(
      head=my_head,
      hidden_units=...,
      feature_columns=...)
  ```

  It can also be used with a custom `model_fn`. Example:

  ```python
  def _my_model_fn(features, labels, mode):
    my_head = tf.estimator.MultiClassHead(n_classes=3)
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
    n_classes: Number of classes, must be greater than 2 (for 2 classes, use
      `BinaryClassHead`).
    weight_column: A string or a `NumericColumn` created by
      `tf.feature_column.numeric_column` defining feature column representing
      weights. It is used to down weight or boost examples during training. It
      will be multiplied by the loss of the example.
    label_vocabulary: A list or tuple of strings representing possible label
      values. If it is not given, that means labels are already encoded as an
      integer within [0, n_classes). If given, labels must be of string type and
      have any value in `label_vocabulary`. Note that errors will be raised if
      `label_vocabulary` is not provided but labels are strings.
    loss_reduction: One of `tf.losses.Reduction` except `NONE`. Decides how to
      reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`, namely
      weighted sum of losses divided by `batch size * label_dimension`.
    loss_fn: Optional loss function.
    name: name of the head. If provided, summary and metrics keys will be
      suffixed by `"/" + name`. Also used as `name_scope` when creating ops.
  """

  def __init__(self,
               n_classes,
               weight_column=None,
               label_vocabulary=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               loss_fn=None,
               name=None):
    if (n_classes is None) or (n_classes <= 2):
      raise ValueError('n_classes must be > 2: {}.'.format(n_classes))
    if label_vocabulary is not None and not isinstance(label_vocabulary,
                                                       (list, tuple)):
      raise ValueError(
          'label_vocabulary should be a list or a tuple. Given type: {}'.format(
              type(label_vocabulary)))
    if (loss_reduction not in losses.Reduction.all() or
        loss_reduction == losses.Reduction.NONE):
      raise ValueError('Invalid loss_reduction: {}'.format(loss_reduction))
    if loss_fn:
      base_head.validate_loss_fn_args(loss_fn)
    self._n_classes = n_classes
    self._weight_column = weight_column
    self._label_vocabulary = label_vocabulary
    self._loss_reduction = loss_reduction
    self._loss_fn = loss_fn
    self._name = name
    # Metric keys.
    keys = metric_keys.MetricKeys
    self._loss_mean_key = self._summary_key(keys.LOSS_MEAN)
    self._accuracy_key = self._summary_key(keys.ACCURACY)
    self._loss_regularization_key = self._summary_key(keys.LOSS_REGULARIZATION)

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

  # Attributes for lookup tables in Eager execution. Note that for Graph
  # execution, the lookup tables are created on demanded to make sure the
  # lookup table is in the same graph as its iput tensors for `train` and `eval`
  # of Estimator (as Estimator recreates graphes for `train`, `eval` and
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
        labels=labels,
        logits=logits,
        expected_labels_dimension=1)
    if self._label_vocabulary is None:
      if not labels.dtype.is_integer:
        raise ValueError('Labels dtype should be integer. Instead got {}.'.
                         format(labels.dtype))
      label_ids = labels
    else:
      if labels.dtype != dtypes.string:
        raise ValueError('Labels dtype should be string if there is a '
                         'vocabulary. Instead got {}'.format(labels.dtype))
      label_ids = self._class_id_table.lookup(labels)
    return base_head.check_label_range(label_ids, self._n_classes)

  def _unweighted_loss_and_weights(self, logits, label_ids, features):
    """Computes loss spec."""
    if self._loss_fn:
      unweighted_loss = base_head.call_loss_fn(
          loss_fn=self._loss_fn,
          labels=label_ids,
          logits=logits,
          features=features,
          expected_loss_dim=1)
    else:
      unweighted_loss = losses.sparse_softmax_cross_entropy(
          labels=label_ids, logits=logits, reduction=losses.Reduction.NONE)
      # Restore the squeezed dim, so unweighted_loss matches the weights shape.
      unweighted_loss = array_ops.expand_dims(unweighted_loss, axis=-1)
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
      label_ids = self._processed_labels(logits, labels)
      unweighted_loss, weights = self._unweighted_loss_and_weights(
          logits, label_ids, features)
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
    valid_keys = [pred_keys.LOGITS, pred_keys.PROBABILITIES,
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
      if pred_keys.PROBABILITIES in keys:
        probabilities = nn.softmax(logits, name=pred_keys.PROBABILITIES)
        predictions[pred_keys.PROBABILITIES] = probabilities
      if pred_keys.CLASS_IDS in keys or pred_keys.CLASSES in keys:
        # class_ids's shape is [D0, D1, ... DN].
        class_ids = math_ops.argmax(logits, axis=-1, name=pred_keys.CLASS_IDS)
        # Expand to [batch_size, 1].
        class_ids = array_ops.expand_dims(class_ids, axis=-1)
        if pred_keys.CLASS_IDS in keys:
          predictions[pred_keys.CLASS_IDS] = class_ids
        if pred_keys.CLASSES in keys:
          if self._label_vocabulary:
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
      if regularization_losses is not None:
        eval_metrics[self._loss_regularization_key] = metrics.Mean(
            name=keys.LOSS_REGULARIZATION)
      # Accuracy metric.
      eval_metrics[self._accuracy_key] = metrics.Accuracy(name=keys.ACCURACY)
    return eval_metrics

  def update_metrics(self, eval_metrics, features, logits, labels,
                     regularization_losses=None):
    """Updates eval metrics. See `base_head.Head` for details."""
    preds = self.predictions(logits)
    class_ids = preds[prediction_keys.PredictionKeys.CLASS_IDS]
    logits = base_head.check_logits_final_dim(logits, self.logits_dimension)
    label_ids = self._processed_labels(logits, labels)
    unweighted_loss, weights = self._unweighted_loss_and_weights(
        logits, label_ids, features)

    # Update metrics.
    eval_metrics[self._loss_mean_key].update_state(
        values=unweighted_loss, sample_weight=weights)
    eval_metrics[self._accuracy_key].update_state(
        y_true=label_ids, y_pred=class_ids, sample_weight=weights)

    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      eval_metrics[self._loss_regularization_key].update_state(
          values=regularization_loss)
    return eval_metrics

  def _create_tpu_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns a `model_fn._TPUEstimatorSpec`.

    Args:
      features: Input `dict` of `Tensor` or `SparseTensor` objects.
      mode: Estimator's `ModeKeys`.
      logits: logits `Tensor` with shape `[D0, D1, ... DN, logits_dimension]`.
        For many applications, the shape is `[batch_size, logits_dimension]`.
      labels: Labels integer or string `Tensor` with shape matching `logits`,
        namely `[D0, D1, ... DN, 1]` or `[D0, D1, ... DN]`. `labels` is
        required argument when `mode` equals `TRAIN` or `EVAL`.
      optimizer: `Optimizer` instance to optimize the loss in TRAIN mode.
        Namely, sets `train_op = optimizer.minimize(loss, global_step)`, which
        updates variables and increments `global_step`.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns
        `train_op`. Used if `optimizer` is `None`.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses. These losses are
        usually expressed as a batch average, so for best results users need to
        use the default `loss_reduction=SUM_OVER_BATCH_SIZE` or set
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
      pred_keys = prediction_keys.PredictionKeys
      predictions = self.predictions(logits)
      if mode == model_fn.ModeKeys.PREDICT:
        probabilities = predictions[pred_keys.PROBABILITIES]
        classifier_output = base_head.classification_output(
            scores=probabilities,
            n_classes=self._n_classes,
            label_vocabulary=self._label_vocabulary)
        return model_fn._TPUEstimatorSpec(  # pylint: disable=protected-access
            mode=model_fn.ModeKeys.PREDICT,
            predictions=predictions,
            export_outputs={
                base_head.DEFAULT_SERVING_KEY:
                    classifier_output,
                base_head.CLASSIFY_SERVING_KEY:
                    classifier_output,
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
