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
"""Abstractions for the base head class."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc

import six

from tensorflow.python.eager import context
from tensorflow.python.feature_column import feature_column_lib
from tensorflow.python.feature_column.feature_column import _LazyBuilder
from tensorflow.python.feature_column.feature_column import _NumericColumn
from tensorflow.python.framework import ops
from tensorflow.python.framework import sparse_tensor
from tensorflow.python.framework import tensor_util
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import check_ops
from tensorflow.python.ops import control_flow_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import string_ops
from tensorflow.python.ops import weights_broadcast_ops
from tensorflow.python.saved_model import signature_constants
from tensorflow.python.summary import summary
from tensorflow.python.training import training_util
from tensorflow.python.util import function_utils
from tensorflow_estimator.python.estimator.canned import head as head_v1
from tensorflow_estimator.python.estimator.canned import metric_keys
from tensorflow_estimator.python.estimator.export import export_output

DEFAULT_SERVING_KEY = signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY

# The above default is defined by TF Serving, but these next three are just
# a local convention without any special meaning.
CLASSIFY_SERVING_KEY = 'classification'
REGRESS_SERVING_KEY = 'regression'
PREDICT_SERVING_KEY = 'predict'


class Head(object):
  """Interface for the head/top of a model.

  Head sits on top of the model network and handles computing the outputs of
  the network. Given logits (or output of a hidden layer), a Head knows how to
  compute predictions, loss, train_op, metrics and export outputs. It is meant
  to:

  1. Simplify writing model_fn and to make model_fn more configurable for
     Estimator.
  2. Simpilfy creating loss and metrics for the train and test loop in Eager
     execution.
  3. Support wide range of machine learning models. Since most heads can work
     with logits, they can support DNN, RNN, Wide, Wide&Deep,
     Global objectives, Gradient boosted trees and many other types
     of machine learning models.

  Common usage:
  Here is simplified model_fn to build a DNN regression model.
    ```python
    def _my_dnn_model_fn(features, labels, mode, params, config=None):
      # Optionally your callers can pass head to model_fn as a param.
      head = tf.estimator.RegressionHead(...)

      # TODO(b/117839674): update feature_column
      inputs = tf.feature_column.input_layer(features, ...)

      # Compute logits with tf.keras.layers API
      hidden_layer0 = tf.keras.layers.Dense(
          units=1000, activation="relu")(inputs)
      hidden_layer1 = tf.keras.layers.Dense(
          units=500, activation="relu")(hidden_layer0)
      logits = tf.keras.layers.Dense(
          units=head.logits_dimension, activation=None)(hidden_layer1)

      # Or use Keras model for logits computation
      model = tf.keras.Sequential()
      model.add(tf.keras.layers.Dense(units=1000, activation="relu"))
      model.add(tf.keras.layers.Dense(units=500, activation="relu"))
      model.add(tf.keras.layers.Dense(
         units=head.logits_dimension, activation=None))
      logits = model(inputs)

      return head.create_estimator_spec(
          features=features,
          labels=labels,
          mode=mode,
          logits=logits,
          optimizer=optimizer)
    ```

  There are cases where computing and applying gradients can not be meaningfully
  captured with `Optimizer`s or `train_op_fn`s that Tensorflow supports (for
  example, with sync optimizer). In such case, you can take the responsibility
  to define your own way to manipulate gradients and update the `train_op` in
  `model_fn`. Here is a common use case:
    ```python
    def _my_model_fn(features, labels, mode, params, config=None):
      head = tf.estimator.MultiClassHead(n_classes=3)
      estimator_spec = head.create_estimator_spec(
          features=features,
          labels=labels,
          mode=mode,
          logits=logits,
          train_op_fn=lambda _: tf.no_op())
      if mode == model_fn.ModeKeys.TRAIN:
        optimizer = ...
        sync_opt = tf.train.SyncReplicasOptimizer(opt=optimizer, ...)
        update_op = sync_opt.minimize(
            estimator_spec.loss, global_step=tf.get_global_step())
        hooks = [sync.make_session_run_hook(is_chief)]
        # update train_op and hooks in EstimatorSpec
        estimator_spec.train_op = update_op
        estimator_spec.training_hooks = hooks
        return estimator_spec
    ```
  """
  __metaclass__ = abc.ABCMeta

  @abc.abstractproperty
  def name(self):
    """The name of this head.

    Returns:
      A string.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractproperty
  def logits_dimension(self):
    """Size of the last dimension of the logits `Tensor`.

    Often is the number of classes, labels, or real values to be predicted.
    Typically, logits is of shape `[batch_size, logits_dimension]`.

    Returns:
      The expected size of the `logits` tensor.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractproperty
  def loss_reduction(self):
    """One of `tf.losses.Reduction`.

    Describes how to reduce training loss over batch, such as mean or sum.

    Returns:
      The type of loss reduction used in the head.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def loss(self, logits, labels, features=None, mode=None,
           regularization_losses=None):
    """Returns a loss `Tensor` from provided arguments.

    Note that, the args of `features` and `mode` are most likely not used, but
    some Head implementations may require them.

    Args:
      logits: Logits `Tensor` to be used for loss construction.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`. To be used in case loss calculation is
        different in Train and Eval mode.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      A scalar `Tensor` representing regularized training loss used in train and
      eval.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def predictions(self, logits, keys=None):
    """Returns a `dict` of predictions from provided logits.

    Args:
      logits: Logits `Tensor` to be used for prediction construction.
      keys: A list of `string` for prediction keys. Defaults to `None`, meaning
        if not specified, predictions will be created for all the pre-defined
        valid keys in the head.

    Returns:
      A `dict` of predicted `Tensor` keyed by prediction name.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def metrics(self, regularization_losses=None):
    """Returns a `dict` of metric objects.

    Args:
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
       A `dict` of metrics keyed by string name. The value is an instance of
       `Metric` class.
    """
    raise NotImplementedError('Calling an abstract method.')

  @abc.abstractmethod
  def update_metrics(self, eval_metrics, features, logits, labels,
                     mode=None, regularization_losses=None):
    """Updates metric objects and returns a `dict` of the updated metrics.

    Args:
      eval_metrics: A `dict` of metrics to be updated.
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      logits: logits `Tensor` to be used for metrics update.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      mode: Estimator's `ModeKeys`.
      regularization_losses: A list of additional scalar losses to be added to
        the training and evaluation loss, such as regularization losses.

    Returns:
       A `dict` of updated metrics keyed by name. The value is an instance of
       `Metric` class.
    """
    raise NotImplementedError('Calling an abstract method.')

  def _summary_key(self, key):
    return '{}/{}'.format(key, self.name) if self.name else key

  def create_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns `EstimatorSpec` that a model_fn can return.

    It is recommended to pass all args via name.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`.
      logits: Logits `Tensor` to be used by the head.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      optimizer: `Optimizer` instance to optimize the loss in TRAIN mode.
        Namely, sets `train_op = optimizer.minimize(loss, global_step)`, which
        updates variables and increments `global_step`.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns an op
        to optimize the model with the loss in TRAIN mode. Used if `optimizer`
        is `None`. Exactly one of `train_op_fn` and `optimizer` must be set in
        TRAIN mode. By default, it is `None` in other modes. If you want to
        optimize loss yourself, you can pass `lambda _: tf.no_op()` and then use
        `EstimatorSpec.loss` to compute and apply gradients.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      `EstimatorSpec`.
    """
    # Not all subclasses of Head will have implemented
    # _create_tpu_estimator_spec. If it is implemented, we can convert it to
    # the normal `EstimatorSpec` by calling the method of
    # `_TPUEstimatorSpec.as_estimator_spec()`.
    try:
      tpu_estimator_spec = (
          self._create_tpu_estimator_spec(
              features, mode, logits, labels, optimizer, train_op_fn,
              regularization_losses))
      return tpu_estimator_spec.as_estimator_spec()
    except NotImplementedError:
      raise NotImplementedError(
          'Subclasses of Head must implement `create_estimator_spec()` or '
          '_create_tpu_estimator_spec().')

  def _create_tpu_estimator_spec(
      self, features, mode, logits, labels=None, optimizer=None,
      train_op_fn=None, regularization_losses=None):
    """Returns `model_fn._TPUEstimatorSpec` that a model_fn can return.

    Args:
      features: Input `dict` mapping string feature names to `Tensor` or
        `SparseTensor` objects containing the values for that feature in a
        minibatch. Often to be used to fetch example-weight tensor.
      mode: Estimator's `ModeKeys`.
      logits: logits `Tensor` to be used by the head.
      labels: Labels `Tensor`, or `dict` mapping string label names to `Tensor`
        objects of the label values.
      optimizer: `Optimizer` instance to optimize the loss in TRAIN mode.
        Namely, sets `train_op = optimizer.minimize(loss, global_step)`, which
        updates variables and increments `global_step`.
      train_op_fn: Function that takes a scalar loss `Tensor` and returns an op
        to optimize the model with the loss in TRAIN mode. Used if `optimizer`
        is `None`. Exactly one of `train_op_fn` and `optimizer` must be set in
        TRAIN mode. None is allowed in other modes. If you want to optimize loss
        yourself you can pass `lambda _: tf.no_op()` and then use
        `EstimatorSpec.loss` to compute and apply gradients.
      regularization_losses: A list of additional scalar losses to be added to
        the training loss, such as regularization losses.

    Returns:
      A `model_fn._TPUEstimatorSpec' instance.
    """
    raise NotImplementedError(
        'TPUEstimatorSpec not available for this model head.')

# TODO(b/119617064): unify eager and graph implementations
# Note that, tensor shape checking is slow in Eager mode. To amend it, the
# tensor static shape is used for checking. The duplication of shape checking
# for eager mode in the following helper functions can be safely removed
# if there's some way to get around it in the future.

# Label shape error messages.
_LABEL_NONE_ERR_MSG = (
    'You must provide a labels Tensor. Given: None. '
    'Suggested troubleshooting steps: Check that your data contains your label '
    'feature. Check that your input_fn properly parses and returns labels.')

_SPARSE_LABEL_ERR_MSG = (
    'SparseTensor labels are not supported. Labels must be a Tensor of shape '
    '[D0, D1, ..., DN, {}], e.g. [batch_size, {}].Suggested Fix (1): Check the'
    ' label feature in your data. Each example must contain {} value(s). If '
    'not, your choice of label was probably incorrect. Suggested Fix (2): In '
    'your input_fn, use tf.sparse_tensor_to_dense() to turn labels into a '
    'Tensor.')

_MISMATCHED_LABEL_DIM_ERR_MSG = (
    'Mismatched label shape. Expected labels dimension={}.  Received {}. '
    'Suggested Fix: If your classifier expects one-hot encoding label, check '
    'your n_classes argument to the estimator and/or the shape of your label. '
    'Otherwise, check the shape of your label.')

_LABEL_SHAPE_ERR_MSG = (
    'labels shape must be [D0, D1, ... DN, {}]. Suggested Fix: check your '
    'n_classes argument to the head and/or the shape of your label.')


def check_dense_labels_match_logits_and_reshape(labels, logits,
                                                expected_labels_dimension):
  """Checks labels shape matches logits, and reshapes if needed.

  Consider logits of shape [D0, D1, ... DN, logits_dimension]. Then labels
  shape must be [D0, D1, ... DN, expected_labels_dimension].
  If expected_labels_dimension=1, labels could be [D0, D1, ... DN] and this
  method reshapes them to [D0, D1, ... DN, 1].

  Args:
    labels: labels Tensor.
    logits: logits Tensor.
    expected_labels_dimension: Integer.

  Returns:
    Validated and reshaped labels Tensor.

  Raises:
    ValueError: If labels is a SparseTensor.
    ValueError: If labels shape is statically defined and fails validation.
    OpError: If labels shape is not statically defined and fails validation.
  """
  if labels is None:
    raise ValueError(_LABEL_NONE_ERR_MSG)
  with ops.name_scope('labels', values=(labels, logits)) as scope:
    labels = sparse_tensor.convert_to_tensor_or_sparse_tensor(labels)
    if isinstance(labels, sparse_tensor.SparseTensor):
      raise ValueError(_SPARSE_LABEL_ERR_MSG.format(
          expected_labels_dimension, expected_labels_dimension,
          expected_labels_dimension))
    # Eager mode.
    if context.executing_eagerly():
      labels_rank = labels._rank()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if (labels_rank is not None and logits_rank is not None
          and labels_rank == logits_rank - 1):
        labels = array_ops.expand_dims(labels, -1)
        labels_rank += 1
      labels_shape = labels._shape_tuple()  # pylint: disable=protected-access
      if labels_rank < 2:
        raise ValueError('labels must have rank at least 2.  Received rank {}, '
                         'shape {}'.format(labels_rank, labels_shape))
      if labels_shape[-1] != expected_labels_dimension:
        raise ValueError(_MISMATCHED_LABEL_DIM_ERR_MSG.format(
            expected_labels_dimension, labels_shape[-1]))
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      expected_labels_shape = logits_shape[:-1] + (expected_labels_dimension,)
      if expected_labels_shape != labels_shape:
        raise ValueError(
            '{}, expected_labels_shape: {}. labels_shape: {}.'
            .format(_LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension),
                    expected_labels_shape, labels_shape))
      return labels

    # Graph mode.
    if (labels.shape.ndims is not None and logits.shape.ndims is not None and
        labels.shape.ndims == logits.shape.ndims - 1):
      labels = array_ops.expand_dims(labels, -1)
    assert_rank = check_ops.assert_rank_at_least(
        labels, 2,
        message=_LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension))
    with ops.control_dependencies([assert_rank]):
      static_shape = labels.shape
      if static_shape.ndims is not None:
        final_dim = static_shape[-1]
        if (final_dim is not None) and (final_dim != expected_labels_dimension):
          raise ValueError(_MISMATCHED_LABEL_DIM_ERR_MSG.format(
              expected_labels_dimension, final_dim))
      logits_shape = array_ops.shape(logits)
      expected_labels_shape = array_ops.concat(
          [logits_shape[:-1], [expected_labels_dimension]], axis=0)
      labels_shape = array_ops.shape(labels)
      assert_dimension = check_ops.assert_equal(
          expected_labels_shape, labels_shape,
          message=_LABEL_SHAPE_ERR_MSG.format(expected_labels_dimension),
          data=['expected_labels_shape: ', expected_labels_shape,
                'labels_shape: ', labels_shape])
      with ops.control_dependencies([assert_dimension]):
        return array_ops.identity(labels, name=scope)


def get_weights_and_check_match_logits(
    features, weight_column, logits, allow_per_logit_weights=False):
  """Fetches weights from features and checks that the shape matches logits.

  Consider logits of shape [D0, D1, ... DN, logits_dimension]. Weights shape
  can be either:
  * [D0, D1, ... DN, logits_dimension] if `allow_per_logit_weights=True`.
  * [D0, D1, ... DN, 1]
  * [D0, D1, ... DN]: In this case, weights is reshaped into
    [D0, D1, ... DN, 1] to work with weight broadcasting rules.

  Args:
    features: The features dict that contains weights.
    weight_column: The weight column. If not given, this method returns 1.
    logits: logits Tensor.
    allow_per_logit_weights: Boolean. Whether we allow weights along the logits
      dimension, namely shape `[D0, D1, ... DN, logits_dimension]`.

  Returns:
    Validated and reshaped weights Tensor.

  Raises:
    ValueError: If the weights `Tensor` cannot be cast into float.
  """
  if allow_per_logit_weights:
    err_msg = (
        'weights shape must be [D0, D1, ... DN], [D0, D1, ... DN, 1] or '
        '[D0, D1, ... DN, logits_dimension]')
  else:
    err_msg = (
        'weights shape must be [D0, D1, ... DN] or [D0, D1, ... DN, 1]')
  with ops.name_scope(
      'weights', values=tuple(six.itervalues(features)) + (logits,)) as scope:
    # Fetch the weights.
    if weight_column is None:
      return 1.
    # TODO(b/117839674): update feature_column
    if isinstance(weight_column, six.string_types):
      weight_column = feature_column_lib.numeric_column(
          key=weight_column, shape=(1,))
    if not isinstance(weight_column,
                      (feature_column_lib.NumericColumn, _NumericColumn)):
      raise TypeError('Weight column must be either a string or NumericColumn.'
                      ' Given type: {}.'.format(type(weight_column)))
    weights = weight_column._get_dense_tensor(  # pylint: disable=protected-access
        _LazyBuilder(features))
    if not (weights.dtype.is_floating or weights.dtype.is_integer):
      raise ValueError('Weight column should be castable to float. '
                       'Given dtype: {}'.format(weights.dtype))
    weights = math_ops.to_float(weights, name='weights')
    # Validate the weights shape.
    # Eager mode.
    if context.executing_eagerly():
      weights_shape = weights._shape_tuple()  # pylint: disable=protected-access
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      weights_rank = weights._rank()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if (weights_rank is not None and logits_rank is not None
          and weights_rank == logits_rank - 1):
        if logits_shape[:-1] != weights_shape:
          raise ValueError(
              '{}, logits_shape: {}. weights_shape: {}.'
              .format(err_msg, logits_shape, weights_shape))
        return array_ops.expand_dims(weights, -1, name=scope)
      supported_weights_shape = logits_shape[:-1] + (1,)
      if allow_per_logit_weights:
        if (logits_shape != weights_shape
            and supported_weights_shape != weights_shape):
          raise ValueError(
              '{}, logits_shape: {}. weights_shape: {}.'
              .format(err_msg, logits_shape, weights_shape))
      else:
        if supported_weights_shape != weights_shape:
          raise ValueError(
              '{}, logits_shape: {}. weights_shape: {}.'
              .format(err_msg, logits_shape, weights_shape))
      return weights

    # Graph mode.
    weights_shape = array_ops.shape(weights, name='weights_shape')
    logits_shape = array_ops.shape(logits, name='logits_shape')
    if (weights.shape.ndims is not None and logits.shape.ndims is not None and
        weights.shape.ndims == logits.shape.ndims - 1):
      assert_dimension = check_ops.assert_equal(
          logits_shape[:-1], weights_shape, message=err_msg,
          data=['logits_shape: ', logits_shape,
                'weights_shape: ', weights_shape])
      with ops.control_dependencies([assert_dimension]):
        return array_ops.expand_dims(weights, -1, name=scope)
    supported_weights_shape = array_ops.concat([logits_shape[:-1], [1]], axis=0)
    if allow_per_logit_weights:
      condition = math_ops.reduce_any(
          [math_ops.reduce_all(math_ops.equal(logits_shape, weights_shape)),
           math_ops.reduce_all(math_ops.equal(
               supported_weights_shape, weights_shape))])
      assert_dimension = control_flow_ops.Assert(
          condition=condition,
          data=[err_msg, 'logits_shape: ', logits_shape,
                'weights_shape: ', weights_shape])
    else:
      assert_dimension = check_ops.assert_equal(
          supported_weights_shape, weights_shape, message=err_msg,
          data=['logits_shape: ', logits_shape,
                'weights_shape: ', weights_shape])
    with ops.control_dependencies([assert_dimension]):
      return array_ops.identity(weights, name=scope)


def check_logits_final_dim(logits, expected_logits_dimension):
  """Checks that logits shape is [D0, D1, ... DN, logits_dimension]."""
  with ops.name_scope('logits', values=(logits,)) as scope:
    logits = math_ops.to_float(logits)
    # Eager mode
    if context.executing_eagerly():
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      logits_rank = logits._rank()  # pylint: disable=protected-access
      if logits_rank < 2:
        raise ValueError('logits must have rank at least 2.  Received rank {}, '
                         'shape {}'.format(logits_rank, logits_shape))
      if logits_shape[-1] != expected_logits_dimension:
        raise ValueError(
            'logits shape must be [D0, D1, ... DN, logits_dimension], '
            'got {}.'.format(logits_shape))
      return logits
    # Graph mode
    logits_shape = array_ops.shape(logits)
    assert_rank = check_ops.assert_rank_at_least(
        logits, 2, data=[logits_shape],
        message='logits shape must be [D0, D1, ... DN, logits_dimension]')
    with ops.control_dependencies([assert_rank]):
      static_shape = logits.shape
      if static_shape.ndims is not None and static_shape[-1] is not None:
        if static_shape[-1] != expected_logits_dimension:
          raise ValueError(
              'logits shape must be [D0, D1, ... DN, logits_dimension], '
              'got {}.'.format(static_shape))
        return logits
      assert_dimension = check_ops.assert_equal(
          expected_logits_dimension, logits_shape[-1], data=[logits_shape],
          message='logits shape must be [D0, D1, ... DN, logits_dimension]')
      with ops.control_dependencies([assert_dimension]):
        return array_ops.identity(logits, name=scope)


def validate_loss_fn_args(loss_fn):
  """Validates loss_fn arguments.

  Required arguments: labels, logits.
  Optional arguments: features, loss_reduction.

  Args:
    loss_fn: The loss function.

  Raises:
    ValueError: If the signature is unexpected.
  """
  loss_fn_args = function_utils.fn_args(loss_fn)
  for required_arg in ['labels', 'logits']:
    if required_arg not in loss_fn_args:
      raise ValueError(
          'loss_fn must contain argument: {}. '
          'Given arguments: {}'.format(required_arg, loss_fn_args))
  invalid_args = list(set(loss_fn_args) - set(
      ['labels', 'logits', 'features', 'loss_reduction']))
  if invalid_args:
    raise ValueError('loss_fn has unexpected args: {}'.format(invalid_args))


def call_loss_fn(loss_fn, labels, logits, features, expected_loss_dim=1):
  """Calls loss_fn and checks the returned shape.

  For shape checking, eager uses the static dimension to improve performance.

  Args:
    loss_fn: The loss function.
    labels: Processed labels Tensor.
    logits: Logits Tensor of shape [D0, D1, ... DN, logits_dimension].
    features: Features dict.
    expected_loss_dim: The expected last dimension of loss Tensor.

  Returns:
    Loss Tensor with shape [D0, D1, ... DN, expected_loss_dim].

  Raises:
    ValueError: If the loss tensor shape is unexpected.
  """
  loss_fn_args = function_utils.fn_args(loss_fn)
  kwargs = {}
  if 'features' in loss_fn_args:
    kwargs['features'] = features
  with ops.name_scope(
      'call_loss_fn',
      values=[labels, logits] + list(six.itervalues(features))):
    unweighted_loss = loss_fn(labels=labels, logits=logits, **kwargs)
    # Eager mode.
    if context.executing_eagerly():
      loss_shape = unweighted_loss._shape_tuple()  # pylint: disable=protected-access
      logits_shape = logits._shape_tuple()  # pylint: disable=protected-access
      expected_loss_shape = logits_shape[:-1] + (expected_loss_dim,)
      if loss_shape != expected_loss_shape:
        raise ValueError('loss_fn must return Tensor of shape '
                         '[D0, D1, ... DN, {}]. '.format(expected_loss_dim),
                         'logits_shape: ', logits_shape,
                         'loss_shape: ', loss_shape)
      return unweighted_loss
    # Graph mode.
    logits_shape = array_ops.shape(logits, name='logits_shape')
    expected_loss_shape = array_ops.concat(
        [logits_shape[:-1], [expected_loss_dim]], axis=0,
        name='expected_loss_shape')
    loss_shape = array_ops.shape(unweighted_loss, name='loss_shape')
    check_loss_shape_op = control_flow_ops.Assert(
        math_ops.reduce_all(math_ops.equal(loss_shape, expected_loss_shape)),
        data=[
            'loss_fn must return Tensor of shape '
            '[D0, D1, ... DN, {}]. '.format(expected_loss_dim),
            'logits_shape: ', logits_shape, 'loss_shape: ', loss_shape],
        name='check_loss_shape')
    with ops.control_dependencies([check_loss_shape_op]):
      return array_ops.identity(unweighted_loss)


def check_prediction_keys(pred_keys, valid_keys):
  for key in pred_keys:
    if key not in valid_keys:
      raise ValueError(
          'Prediction key must be in PredictionKeys, given: {}.'
          'Valid prediction keys include {}.'.format(key, valid_keys))


def classification_output(scores, n_classes, label_vocabulary=None):
  batch_size = array_ops.shape(scores)[0]
  if label_vocabulary:
    export_class_list = label_vocabulary
  else:
    export_class_list = string_ops.as_string(math_ops.range(n_classes))
  export_output_classes = array_ops.tile(
      input=array_ops.expand_dims(input=export_class_list, axis=0),
      multiples=[batch_size, 1])
  return export_output.ClassificationOutput(
      scores=scores,
      # `ClassificationOutput` requires string classes.
      classes=export_output_classes)


def check_label_range(labels, n_classes, message=None):
  """Check if labels are in the range of [0, n_classes)."""
  with ops.name_scope('check_label_range', values=(labels,)):
    # Eager mode
    if context.executing_eagerly():
      assert_less = math_ops.reduce_all(
          math_ops.less_equal(labels, n_classes - 1))
      if not assert_less:
        raise ValueError(
            message or 'Labels must be <= {} - 1'.format(n_classes))
      assert_greater = math_ops.reduce_all(math_ops.greater_equal(labels, 0))
      if not assert_greater:
        raise ValueError(message or 'Labels must be >= 0')
      return labels
    # Graph mode
    assert_less = check_ops.assert_less_equal(
        labels,
        ops.convert_to_tensor(n_classes - 1, dtype=labels.dtype),
        message=message or 'Labels must be <= n_classes - 1')
    assert_greater = check_ops.assert_non_negative(
        labels, message=message or 'Labels must be >= 0')
    with ops.control_dependencies((assert_less, assert_greater)):
      return array_ops.identity(labels)


def update_metric_with_broadcast_weights(eval_metric, values, weights):
  values = math_ops.to_float(values)
  if weights is not None:
    weights = weights_broadcast_ops.broadcast_weights(
        weights, values)
  eval_metric.update_state(
      values=values, sample_weight=weights)


def create_eval_metrics_tuple(fn, kwargs):
  """Creates TPU eval metrics tuple.

  Helper function to make eval_metric tuple (eval_metric_fn, fn_kwargs) used
  by `TPUEstimator`. TPUEstimator requires that `eval_metric_fn` take
  exclusively Tensor arguments. This helper can help create such a function from
  a more generic function that can take both Tensor and non-Tensor arguments.

  Args:
    fn: A eval_metric_fn that takes both Tensor and non-Tensor arguments. This
      function must return a dict of form
        {'metric name': (metric_tensor, eval_op)}
    kwargs: Dict of arguments for `fn`.

  Returns:
    `eval_metric` tuple that can be passed to a `model_fn._TPUEstimatorSpec`.
  """
  tensor_kwargs = {}
  nontensor_kwargs = {}
  for k, v in six.iteritems(kwargs):
    if tensor_util.is_tensor(v):
      tensor_kwargs[k] = v
    else:
      nontensor_kwargs[k] = v

  def _fn(**tensors):
    return fn(**dict(nontensor_kwargs, **tensors))

  return (_fn, tensor_kwargs)


def create_estimator_spec_train_op(head_name, optimizer, train_op_fn,
                                   regularized_training_loss):
  """Create train_op for estimator_spec."""
  with ops.name_scope(head_name, 'head'):
    if optimizer is not None:
      if train_op_fn is not None:
        raise ValueError('train_op_fn and optimizer cannot both be set.')
      train_op = optimizer.minimize(
          regularized_training_loss,
          global_step=training_util.get_global_step())
    elif train_op_fn is not None:
      train_op = train_op_fn(regularized_training_loss)
    else:
      raise ValueError('train_op_fn and optimizer cannot both be None.')
    train_op = head_v1._append_update_ops(train_op)  # pylint: disable=protected-access
    return train_op


def create_estimator_spec_summary(
    regularized_training_loss, regularization_losses, summary_key_fn=None):
  """Create summary for estimator_spec."""
  with ops.name_scope(''):
    keys = metric_keys.MetricKeys
    loss_key = summary_key_fn(keys.LOSS) if summary_key_fn else keys.LOSS
    summary.scalar(loss_key, regularized_training_loss)
    if regularization_losses is not None:
      regularization_loss = math_ops.add_n(regularization_losses)
      regularization_loss_key = (
          summary_key_fn(keys.LOSS_REGULARIZATION) if summary_key_fn
          else keys.LOSS_REGULARIZATION)
      summary.scalar(regularization_loss_key, regularization_loss)
