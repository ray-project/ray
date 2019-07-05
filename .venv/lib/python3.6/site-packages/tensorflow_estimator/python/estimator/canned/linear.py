# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
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
"""Linear Estimators."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import math

import six

from tensorflow.python.feature_column import feature_column
from tensorflow.python.feature_column import feature_column_lib
from tensorflow.python.framework import constant_op
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import control_flow_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import nn
from tensorflow.python.ops import partitioned_variables
from tensorflow.python.ops import resource_variable_ops
from tensorflow.python.ops import variable_scope
from tensorflow.python.ops import variables as variable_ops
from tensorflow.python.ops.losses import losses
from tensorflow.python.summary import summary
from tensorflow.python.training import ftrl
from tensorflow.python.training import session_run_hook
from tensorflow.python.training import training
from tensorflow.python.util import nest
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator import estimator
from tensorflow_estimator.python.estimator import model_fn
from tensorflow_estimator.python.estimator.canned import head as head_lib
from tensorflow_estimator.python.estimator.canned import optimizers
from tensorflow_estimator.python.estimator.canned.linear_optimizer.python.utils import sdca_ops
from tensorflow_estimator.python.estimator.head import head_utils
from tensorflow_estimator.python.estimator.head import regression_head

# The default learning rate of 0.2 is a historical artifact of the initial
# implementation, but seems a reasonable choice.
_LEARNING_RATE = 0.2


@estimator_export('estimator.experimental.LinearSDCA')
class LinearSDCA(object):
  """Stochastic Dual Coordinate Ascent helper for linear estimators.

  Objects of this class are intended to be provided as the optimizer argument
  (though LinearSDCA objects do not implement the tf.train.Optimizer interface)
  when creating tf.estimator.LinearClassifier or tf.estimator.LinearRegressor.

  SDCA can only be used with LinearClassifier and LinearRegressor under the
  following conditions:
    - Feature columns are of type V2.
    - Multivalent categorical columns are not normalized. In other words the
      `sparse_combiner` argument in the estimator constructor should be "sum".
    - For classification: binary label.
    - For regression: one-dimensional label.

  Example usage:

  ```python
  real_feature_column = numeric_column(...)
  sparse_feature_column = categorical_column_with_hash_bucket(...)
  linear_sdca = tf.estimator.experimental.LinearSDCA(
      example_id_column='example_id',
      num_loss_partitions=1,
      num_table_shards=1,
      symmetric_l2_regularization=2.0)
  classifier = tf.estimator.LinearClassifier(
      feature_columns=[real_feature_column, sparse_feature_column],
      weight_column=...,
      optimizer=linear_sdca)
  classifier.train(input_fn_train, steps=50)
  classifier.evaluate(input_fn=input_fn_eval)
  ```

  Here the expectation is that the `input_fn_*` functions passed to train and
  evaluate return a pair (dict, label_tensor) where dict has `example_id_column`
  as `key` whose value is a `Tensor` of shape [batch_size] and dtype string.
  num_loss_partitions defines sigma' in eq (11) of [3]. Convergence of (global)
  loss is guaranteed if `num_loss_partitions` is larger or equal to the product
  `(#concurrent train ops/per worker) x (#workers)`. Larger values for
  `num_loss_partitions` lead to slower convergence. The recommended value for
  `num_loss_partitions` in `tf.estimator` (where currently there is one process
  per worker) is the number of workers running the train steps. It defaults to 1
  (single machine).
  `num_table_shards` defines the number of shards for the internal state
  table, typically set to match the number of parameter servers for large
  data sets.

  The SDCA algorithm was originally introduced in [1] and it was followed by
  the L1 proximal step [2], a distributed version [3] and adaptive sampling [4].
  [1] www.jmlr.org/papers/volume14/shalev-shwartz13a/shalev-shwartz13a.pdf
  [2] https://arxiv.org/pdf/1309.2375.pdf
  [3] https://arxiv.org/pdf/1502.03508.pdf
  [4] https://arxiv.org/pdf/1502.08053.pdf
  Details specific to this implementation are provided in:
  https://github.com/tensorflow/estimator/tree/master/tensorflow_estimator/python/estimator/canned/linear_optimizer/doc/sdca.ipynb
  """

  def __init__(self,
               example_id_column,
               num_loss_partitions=1,
               num_table_shards=None,
               symmetric_l1_regularization=0.0,
               symmetric_l2_regularization=1.0,
               adaptive=False):
    """Construct a new SDCA optimizer for linear estimators.

    Args:
      example_id_column: The column name contraining the example ids.
      num_loss_partitions: Number of workers.
      num_table_shards: Number of shards of the internal state table, typically
        set to match the number of parameter servers.
      symmetric_l1_regularization: A float value, must be greater than or
        equal to zero.
      symmetric_l2_regularization: A float value, must be greater than zero and
        should typically be greater than 1.
      adaptive: A boolean indicating whether to use adaptive sampling.
    """

    self._example_id_column = example_id_column
    self._num_loss_partitions = num_loss_partitions
    self._num_table_shards = num_table_shards
    self._symmetric_l1_regularization = symmetric_l1_regularization
    self._symmetric_l2_regularization = symmetric_l2_regularization
    self._adaptive = adaptive

  def _prune_and_unique_sparse_ids(self, id_weight_pair):
    """Remove duplicate and negative ids in a sparse tendor."""

    id_tensor = id_weight_pair.id_tensor
    if id_weight_pair.weight_tensor:
      weight_tensor = id_weight_pair.weight_tensor.values
    else:
      weight_tensor = array_ops.ones(
          [array_ops.shape(id_tensor.indices)[0]], dtypes.float32)

    example_ids = array_ops.reshape(id_tensor.indices[:, 0], [-1])
    flat_ids = math_ops.to_int64(array_ops.reshape(id_tensor.values, [-1]))
    # Prune invalid IDs (< 0) from the flat_ids, example_ids, and
    # weight_tensor.  These can come from looking up an OOV entry in the
    # vocabulary (default value being -1).
    is_id_valid = math_ops.greater_equal(flat_ids, 0)
    flat_ids = array_ops.boolean_mask(flat_ids, is_id_valid)
    example_ids = array_ops.boolean_mask(example_ids, is_id_valid)
    weight_tensor = array_ops.boolean_mask(weight_tensor, is_id_valid)

    projection_length = math_ops.reduce_max(flat_ids) + 1
    # project ids based on example ids so that we can dedup ids that
    # occur multiple times for a single example.
    projected_ids = projection_length * example_ids + flat_ids

    # Remove any redundant ids.
    ids, idx = array_ops.unique(projected_ids)
    # Keep only one example id per duplicated ids.
    example_ids_filtered = math_ops.unsorted_segment_min(
        example_ids, idx,
        array_ops.shape(ids)[0])

    # reproject ids back feature id space.
    reproject_ids = (ids - projection_length * example_ids_filtered)

    weights = array_ops.reshape(
        math_ops.unsorted_segment_sum(weight_tensor, idx,
                                      array_ops.shape(ids)[0]), [-1])
    return sdca_ops._SparseFeatureColumn(  # pylint: disable=protected-access
        example_ids_filtered, reproject_ids, weights)

  def get_train_step(self, state_manager, weight_column_name, loss_type,
                     feature_columns, features, targets, bias_var, global_step):
    """Returns the training operation of an SdcaModel optimizer."""

    batch_size = array_ops.shape(targets)[0]
    cache = feature_column_lib.FeatureTransformationCache(features)

    # Iterate over all feature columns and create appropriate lists for dense
    # and sparse features as well as dense and sparse weights (variables) for
    # SDCA.
    dense_features, dense_feature_weights = [], []
    sparse_feature_with_values, sparse_feature_with_values_weights = [], []
    for column in sorted(feature_columns, key=lambda x: x.name):
      if isinstance(column, feature_column_lib.CategoricalColumn):
        id_weight_pair = column.get_sparse_tensors(cache, state_manager)
        sparse_feature_with_values.append(
            self._prune_and_unique_sparse_ids(id_weight_pair))
        # If a partitioner was used during variable creation, we will have a
        # list of Variables here larger than 1.
        sparse_feature_with_values_weights.append(
            state_manager.get_variable(column, 'weights'))
      elif isinstance(column, feature_column_lib.DenseColumn):
        if column.variable_shape.ndims != 1:
          raise ValueError('Column %s has rank %d, larger than 1.' % (
              type(column).__name__, column.variable_shape.ndims))
        dense_features.append(column.get_dense_tensor(cache, state_manager))
        # For real valued columns, the variables list contains exactly one
        # element.
        dense_feature_weights.append(
            state_manager.get_variable(column, 'weights'))
      else:
        raise ValueError('LinearSDCA does not support column type %s.' %
                         type(column).__name__)

    # Add the bias column
    dense_features.append(array_ops.ones([batch_size, 1]))
    dense_feature_weights.append(bias_var)

    example_weights = array_ops.reshape(
        features[weight_column_name],
        shape=[-1]) if weight_column_name else array_ops.ones([batch_size])
    example_ids = features[self._example_id_column]
    training_examples = dict(
        sparse_features=sparse_feature_with_values,
        dense_features=dense_features,
        example_labels=math_ops.to_float(
            array_ops.reshape(targets, shape=[-1])),
        example_weights=example_weights,
        example_ids=example_ids)
    training_variables = dict(
        sparse_features_weights=sparse_feature_with_values_weights,
        dense_features_weights=dense_feature_weights)
    sdca_model = sdca_ops._SDCAModel(  # pylint: disable=protected-access
        examples=training_examples,
        variables=training_variables,
        options=dict(
            symmetric_l1_regularization=self._symmetric_l1_regularization,
            symmetric_l2_regularization=self._symmetric_l2_regularization,
            adaptive=self._adaptive,
            num_loss_partitions=self._num_loss_partitions,
            num_table_shards=self._num_table_shards,
            loss_type=loss_type))
    train_op = sdca_model.minimize(global_step=global_step)
    return sdca_model, train_op


def _get_default_optimizer(feature_columns):
  learning_rate = min(_LEARNING_RATE, 1.0 / math.sqrt(len(feature_columns)))
  return ftrl.FtrlOptimizer(learning_rate=learning_rate)


def _get_expanded_variable_list(var_list):
  """Given an iterable of variables, expands them if they are partitioned.

  Args:
    var_list: An iterable of variables.

  Returns:
    A list of variables where each partitioned variable is expanded to its
    components.
  """
  returned_list = []
  for variable in var_list:
    if (isinstance(variable, variable_ops.Variable) or
        resource_variable_ops.is_resource_variable(variable)):
      returned_list.append(variable)  # Single variable case.
    else:  # Must be a PartitionedVariable, so convert into a list.
      returned_list.extend(list(variable))
  return returned_list


# TODO(rohanj): Consider making this a public utility method.
def _compute_fraction_of_zero(variables):
  """Given a linear variables list, compute the fraction of zero weights.

  Args:
    variables: A list or list of list of variables

  Returns:
    The fraction of zeros (sparsity) in the linear model.
  """
  with ops.name_scope('zero_fraction'):
    variables = nest.flatten(variables)

    with ops.name_scope('total_size'):
      sizes = [array_ops.size(x, out_type=dtypes.int64) for x in variables]
      total_size_int64 = math_ops.add_n(sizes)
    with ops.name_scope('total_zero'):
      total_zero_float32 = math_ops.add_n([
          control_flow_ops.cond(
              math_ops.equal(size, constant_op.constant(0, dtype=dtypes.int64)),
              true_fn=lambda: constant_op.constant(0, dtype=dtypes.float32),
              false_fn=lambda: nn.zero_fraction(x) * math_ops.to_float(size),
              name='zero_count')
          for x, size in zip(variables, sizes)
      ])

    with ops.name_scope('compute'):
      total_size_float32 = math_ops.cast(
          total_size_int64, dtype=dtypes.float32, name='float32_size')
      zero_fraction_or_nan = total_zero_float32 / total_size_float32

    zero_fraction_or_nan = array_ops.identity(
        zero_fraction_or_nan, name='zero_fraction_or_nan')
    return zero_fraction_or_nan


@estimator_export('estimator.experimental.linear_logit_fn_builder')
def linear_logit_fn_builder(units, feature_columns, sparse_combiner='sum'):
  """Function builder for a linear logit_fn.

  Args:
    units: An int indicating the dimension of the logit layer.
    feature_columns: An iterable containing all the feature columns used by
      the model.
    sparse_combiner: A string specifying how to reduce if a categorical column
      is multivalent.  One of "mean", "sqrtn", and "sum".

  Returns:
    A logit_fn (see below).

  """

  def linear_logit_fn(features):
    """Linear model logit_fn.

    Args:
      features: This is the first item returned from the `input_fn`
                passed to `train`, `evaluate`, and `predict`. This should be a
                single `Tensor` or `dict` of same.

    Returns:
      A `Tensor` representing the logits.
    """
    if feature_column_lib.is_feature_column_v2(feature_columns):
      linear_model = feature_column_lib.LinearModel(
          feature_columns=feature_columns,
          units=units,
          sparse_combiner=sparse_combiner,
          name='linear_model')
      logits = linear_model(features)
      bias = linear_model.bias

      # We'd like to get all the non-bias variables associated with this
      # LinearModel.
      # TODO(rohanj): Figure out how to get shared embedding weights variable
      # here.
      variables = linear_model.variables
      variables.remove(bias)

      # Expand (potential) Partitioned variables
      bias = _get_expanded_variable_list([bias])
    else:
      linear_model = feature_column._LinearModel(  # pylint: disable=protected-access
          feature_columns=feature_columns,
          units=units,
          sparse_combiner=sparse_combiner,
          name='linear_model')
      logits = linear_model(features)
      cols_to_vars = linear_model.cols_to_vars()
      bias = cols_to_vars.pop('bias')
      variables = cols_to_vars.values()
    variables = _get_expanded_variable_list(variables)

    if units > 1:
      summary.histogram('bias', bias)
    else:
      # If units == 1, the bias value is a length-1 list of a scalar Tensor,
      # so we should provide a scalar summary.
      summary.scalar('bias', bias[0][0])
    summary.scalar('fraction_of_zero_weights',
                   _compute_fraction_of_zero(variables))
    return logits

  return linear_logit_fn


def _sdca_model_fn(features, labels, mode, head, feature_columns, optimizer):
  """A model_fn for linear models that use the SDCA optimizer.

  Args:
    features: dict of `Tensor`.
    labels: `Tensor` of shape `[batch_size]`.
    mode: Defines whether this is training, evaluation or prediction.
      See `ModeKeys`.
    head: A `Head` instance.
    feature_columns: An iterable containing all the feature columns used by
      the model.
    optimizer: a `LinearSDCA` instance.

  Returns:
    An `EstimatorSpec` instance.

  Raises:
    ValueError: mode or params are invalid, or features has the wrong type.
  """
  assert feature_column_lib.is_feature_column_v2(feature_columns)
  if isinstance(head, head_lib._BinaryLogisticHeadWithSigmoidCrossEntropyLoss):  # pylint: disable=protected-access
    loss_type = 'logistic_loss'
  elif isinstance(head, regression_head.RegressionHead):  # pylint: disable=protected-access
    assert head.logits_dimension == 1
    loss_type = 'squared_loss'
  else:
    raise ValueError('Unsupported head type: {}'.format(head))

  linear_model = feature_column_lib.LinearModel(
      feature_columns=feature_columns, units=1, sparse_combiner='sum')
  logits = linear_model(features)

  bias = linear_model.bias

  # We'd like to get all the non-bias variables associated with this
  # LinearModel.
  # TODO(rohanj): Figure out how to get shared embedding weights variable
  # here.
  variables = linear_model.variables
  variables.remove(bias)

  # Expand (potential) Partitioned variables
  bias = _get_expanded_variable_list([bias])
  variables = _get_expanded_variable_list(variables)
  summary.scalar('bias', bias[0][0])
  summary.scalar('fraction_of_zero_weights',
                 _compute_fraction_of_zero(variables))

  if mode == model_fn.ModeKeys.TRAIN:
    sdca_model, train_op = optimizer.get_train_step(
        linear_model.layer._state_manager,  # pylint: disable=protected-access
        head._weight_column,  # pylint: disable=protected-access
        loss_type,
        feature_columns,
        features,
        labels,
        linear_model.bias,
        training.get_global_step())

    update_weights_hook = _SDCAUpdateWeightsHook(sdca_model, train_op)

    model_fn_ops = head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        train_op_fn=lambda unused_loss_fn: train_op,
        logits=logits)
    return model_fn_ops._replace(training_chief_hooks=(
        model_fn_ops.training_chief_hooks + (update_weights_hook,)))
  else:
    return head.create_estimator_spec(
        features=features,
        mode=mode,
        labels=labels,
        logits=logits)


class _SDCAUpdateWeightsHook(session_run_hook.SessionRunHook):
  """SessionRunHook to update and shrink SDCA model weights."""

  def __init__(self, sdca_model, train_op):
    self._sdca_model = sdca_model
    self._train_op = train_op

  def begin(self):
    """Construct the update_weights op.

    The op is implicitly added to the default graph.
    """
    self._update_op = self._sdca_model.update_weights(self._train_op)

  def before_run(self, run_context):
    """Return the update_weights op so that it is executed during this run."""
    return session_run_hook.SessionRunArgs(self._update_op)


def _linear_model_fn(features, labels, mode, head, feature_columns, optimizer,
                     partitioner, config, sparse_combiner='sum'):
  """A model_fn for linear models that use a gradient-based optimizer.

  Args:
    features: dict of `Tensor`.
    labels: `Tensor` of shape `[batch_size, logits_dimension]`.
    mode: Defines whether this is training, evaluation or prediction.
      See `ModeKeys`.
    head: A `Head` instance.
    feature_columns: An iterable containing all the feature columns used by
      the model.
    optimizer: string, `Optimizer` object, or callable that defines the
      optimizer to use for training. If `None`, will use a FTRL optimizer.
    partitioner: Partitioner for variables.
    config: `RunConfig` object to configure the runtime settings.
    sparse_combiner: A string specifying how to reduce if a categorical column
      is multivalent.  One of "mean", "sqrtn", and "sum".

  Returns:
    An `EstimatorSpec` instance.

  Raises:
    ValueError: mode or params are invalid, or features has the wrong type.
  """
  if not isinstance(features, dict):
    raise ValueError('features should be a dictionary of `Tensor`s. '
                     'Given type: {}'.format(type(features)))

  num_ps_replicas = config.num_ps_replicas if config else 0

  partitioner = partitioner or (
      partitioned_variables.min_max_variable_partitioner(
          max_partitions=num_ps_replicas,
          min_slice_size=64 << 20))

  with variable_scope.variable_scope(
      'linear',
      values=tuple(six.itervalues(features)),
      partitioner=partitioner):

    if isinstance(optimizer, LinearSDCA):
      assert sparse_combiner == 'sum'
      return _sdca_model_fn(
          features, labels, mode, head, feature_columns, optimizer)

    else:
      logit_fn = linear_logit_fn_builder(
          units=head.logits_dimension, feature_columns=feature_columns,
          sparse_combiner=sparse_combiner,
          )
      logits = logit_fn(features=features)

      optimizer = optimizers.get_optimizer_instance(
          optimizer or _get_default_optimizer(feature_columns),
          learning_rate=_LEARNING_RATE)

      return head.create_estimator_spec(
          features=features,
          mode=mode,
          labels=labels,
          optimizer=optimizer,
          logits=logits)


def _validate_linear_sdca_optimizer_for_linear_classifier(
    feature_columns,
    n_classes,
    optimizer,
    sparse_combiner):
  """Helper function for the initialization of LinearClassifier."""
  if isinstance(optimizer, LinearSDCA):
    if sparse_combiner != 'sum':
      raise ValueError('sparse_combiner must be "sum" when optimizer '
                       'is a LinearSDCA object.')
    if not feature_column_lib.is_feature_column_v2(feature_columns):
      raise ValueError('V2 feature columns required when optimizer '
                       'is a LinearSDCA object.')
    if n_classes > 2:
      raise ValueError('LinearSDCA cannot be used in a multi-class setting.')


@estimator_export('estimator.LinearClassifier', v1=[])
class LinearClassifierV2(estimator.EstimatorV2):
  """Linear classifier model.

  Train a linear model to classify instances into one of multiple possible
  classes. When number of possible classes is 2, this is binary classification.

  Example:

  ```python
  categorical_column_a = categorical_column_with_hash_bucket(...)
  categorical_column_b = categorical_column_with_hash_bucket(...)

  categorical_feature_a_x_categorical_feature_b = crossed_column(...)

  # Estimator using the default optimizer.
  estimator = LinearClassifier(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b])

  # Or estimator using the FTRL optimizer with regularization.
  estimator = LinearClassifier(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      optimizer=tf.train.FtrlOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
      ))

  # Or estimator using an optimizer with a learning rate decay.
  estimator = LinearClassifier(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      optimizer=lambda: tf.train.FtrlOptimizer(
          learning_rate=tf.exponential_decay(
              learning_rate=0.1,
              global_step=tf.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator with warm-starting from a previous checkpoint.
  estimator = LinearClassifier(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      warm_start_from="/path/to/checkpoint/dir")


  # Input builders
  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train)
  metrics = estimator.evaluate(input_fn=input_fn_eval)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
    otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `SparseColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedSparseColumn`, two features: the first with
      `key` the id column name, the second with `key` the weight column name.
      Both features' `value` must be a `SparseTensor`.
    - if `column` is a `RealValuedColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss is calculated by using softmax cross entropy.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               feature_columns,
               model_dir=None,
               n_classes=2,
               weight_column=None,
               label_vocabulary=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               warm_start_from=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               sparse_combiner='sum'):
    """Construct a `LinearClassifier` estimator object.

    Args:
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator
        to continue training a previously saved model.
      n_classes: number of label classes. Default is binary classification.
        Note that class labels are integers representing the class index (i.e.
        values from 0 to n_classes-1). For arbitrary label values (e.g. string
        labels), convert to class indices first.
      weight_column: A string or a `_NumericColumn` created by
        `tf.feature_column.numeric_column` defining feature column representing
        weights. It is used to down weight or boost examples during training. It
        will be multiplied by the loss of the example. If it is a string, it is
        used as a key to fetch weight tensor from the `features`. If it is a
        `_NumericColumn`, raw tensor is fetched by key `weight_column.key`,
        then weight_column.normalizer_fn is applied on it to get weight tensor.
      label_vocabulary: A list of strings represents possible label values. If
        given, labels must be string type and have any value in
        `label_vocabulary`. If it is not given, that means labels are
        already encoded as integer or float within [0, 1] for `n_classes=2` and
        encoded as integer values in {0, 1,..., n_classes-1} for `n_classes`>2 .
        Also there will be errors if vocabulary is not provided and labels are
        string.
      optimizer: An instance of `tf.Optimizer` or
        `tf.estimator.experimental.LinearSDCA` used to train the model. Can
        also be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp', 'SGD'),
        or callable. Defaults to FTRL optimizer.
      config: `RunConfig` object to configure the runtime settings.
      partitioner: Optional. Partitioner for input layer.
      warm_start_from: A string filepath to a checkpoint to warm-start from, or
        a `WarmStartSettings` object to fully configure warm-starting.  If the
        string filepath is provided instead of a `WarmStartSettings`, then all
        weights and biases are warm-started, and it is assumed that vocabularies
        and Tensor names are unchanged.
      loss_reduction: One of `tf.losses.Reduction` except `NONE`. Describes how
        to reduce training loss over batch. Defaults to `SUM_OVER_BATCH_SIZE`.
      sparse_combiner: A string specifying how to reduce if a categorical column
        is multivalent.  One of "mean", "sqrtn", and "sum" -- these are
        effectively different ways to do example-level normalization, which can
        be useful for bag-of-words features. for more details, see
        `tf.feature_column.linear_model`.

    Returns:
      A `LinearClassifier` estimator.

    Raises:
      ValueError: if n_classes < 2.
    """
    _validate_linear_sdca_optimizer_for_linear_classifier(
        feature_columns=feature_columns,
        n_classes=n_classes,
        optimizer=optimizer,
        sparse_combiner=sparse_combiner)

    head = head_utils.binary_or_multi_class_head(
        n_classes, weight_column=weight_column,
        label_vocabulary=label_vocabulary,
        loss_reduction=loss_reduction)

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _linear_model_fn."""
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)

    super(LinearClassifierV2, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export(v1=['estimator.LinearClassifier'])  # pylint: disable=missing-docstring
class LinearClassifier(estimator.Estimator):
  __doc__ = LinearClassifierV2.__doc__.replace('SUM_OVER_BATCH_SIZE', 'SUM')

  def __init__(self,
               feature_columns,
               model_dir=None,
               n_classes=2,
               weight_column=None,
               label_vocabulary=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               warm_start_from=None,
               loss_reduction=losses.Reduction.SUM,
               sparse_combiner='sum'):
    _validate_linear_sdca_optimizer_for_linear_classifier(
        feature_columns=feature_columns,
        n_classes=n_classes,
        optimizer=optimizer,
        sparse_combiner=sparse_combiner)

    head = head_lib._binary_logistic_or_multi_class_head(  # pylint: disable=protected-access
        n_classes, weight_column, label_vocabulary, loss_reduction)

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _linear_model_fn."""
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)

    super(LinearClassifier, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


# TODO(b/117517419): Update these contrib references once head moves to core.
# Also references to the "_Head" class need to be replaced with "Head".
@estimator_export('estimator.LinearEstimator', v1=[])
class LinearEstimatorV2(estimator.EstimatorV2):
  """An estimator for TensorFlow linear models with user-specified head.

  Example:

  ```python
  categorical_column_a = categorical_column_with_hash_bucket(...)
  categorical_column_b = categorical_column_with_hash_bucket(...)

  categorical_feature_a_x_categorical_feature_b = crossed_column(...)

  # Estimator using the default optimizer.
  estimator = LinearEstimator(
      head=tf.contrib.estimator.multi_label_head(n_classes=3),
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b])

  # Or estimator using an optimizer with a learning rate decay.
  estimator = LinearEstimator(
      head=tf.contrib.estimator.multi_label_head(n_classes=3),
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      optimizer=lambda: tf.train.FtrlOptimizer(
          learning_rate=tf.exponential_decay(
              learning_rate=0.1,
              global_step=tf.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator using the FTRL optimizer with regularization.
  estimator = LinearEstimator(
      head=tf.contrib.estimator.multi_label_head(n_classes=3),
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b])
      optimizer=tf.train.FtrlOptimizer(
          learning_rate=0.1,
          l1_regularization_strength=0.001
      ))

  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train, steps=100)
  metrics = estimator.evaluate(input_fn=input_fn_eval, steps=10)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
  otherwise there will be a `KeyError`:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `_CategoricalColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `_WeightedCategoricalColumn`, two features: the first
      with `key` the id column name, the second with `key` the weight column
      name. Both features' `value` must be a `SparseTensor`.
    - if `column` is a `_DenseColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss and predicted output are determined by the specified head.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               head,
               feature_columns,
               model_dir=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               sparse_combiner='sum'):
    """Initializes a `LinearEstimator` instance.

    Args:
      head: A `_Head` instance constructed with a method such as
        `tf.contrib.estimator.multi_label_head`.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator
        to continue training a previously saved model.
      optimizer: An instance of `tf.Optimizer` used to train the model. Can also
        be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp', 'SGD'), or
        callable. Defaults to FTRL optimizer.
      config: `RunConfig` object to configure the runtime settings.
      partitioner: Optional. Partitioner for input layer.
      sparse_combiner: A string specifying how to reduce if a categorical column
        is multivalent.  One of "mean", "sqrtn", and "sum" -- these are
        effectively different ways to do example-level normalization, which can
        be useful for bag-of-words features. for more details, see
        `tf.feature_column.linear_model`.
    """
    def _model_fn(features, labels, mode, config):
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)
    super(LinearEstimatorV2, self).__init__(
        model_fn=_model_fn, model_dir=model_dir, config=config)


@estimator_export(v1=['estimator.LinearEstimator'])  # pylint: disable=missing-docstring
class LinearEstimator(estimator.Estimator):
  __doc__ = LinearEstimatorV2.__doc__

  def __init__(self,
               head,
               feature_columns,
               model_dir=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               sparse_combiner='sum'):
    """Initializes a `LinearEstimator` instance.

    Args:
      head: A `_Head` instance constructed with a method such as
        `tf.contrib.estimator.multi_label_head`.
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator
        to continue training a previously saved model.
      optimizer: An instance of `tf.Optimizer` used to train the model. Can also
        be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp', 'SGD'), or
        callable. Defaults to FTRL optimizer.
      config: `RunConfig` object to configure the runtime settings.
      partitioner: Optional. Partitioner for input layer.
      sparse_combiner: A string specifying how to reduce if a categorical column
        is multivalent.  One of "mean", "sqrtn", and "sum" -- these are
        effectively different ways to do example-level normalization, which can
        be useful for bag-of-words features. for more details, see
        `tf.feature_column.linear_model`.
    """
    def _model_fn(features, labels, mode, config):
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)
    super(LinearEstimator, self).__init__(
        model_fn=_model_fn, model_dir=model_dir, config=config)


def _validate_linear_sdca_optimizer_for_linear_regressor(
    feature_columns,
    label_dimension,
    optimizer,
    sparse_combiner):
  """Helper function for the initialization of LinearRegressor."""
  if isinstance(optimizer, LinearSDCA):
    if sparse_combiner != 'sum':
      raise ValueError('sparse_combiner must be "sum" when optimizer '
                       'is a LinearSDCA object.')
    if not feature_column_lib.is_feature_column_v2(feature_columns):
      raise ValueError('V2 feature columns required when optimizer '
                       'is a LinearSDCA object.')
    if label_dimension > 1:
      raise ValueError('LinearSDCA can only be used with one-dimensional '
                       'label.')


@estimator_export('estimator.LinearRegressor', v1=[])
class LinearRegressorV2(estimator.EstimatorV2):
  """An estimator for TensorFlow Linear regression problems.

  Train a linear regression model to predict label value given observation of
  feature values.

  Example:

  ```python
  categorical_column_a = categorical_column_with_hash_bucket(...)
  categorical_column_b = categorical_column_with_hash_bucket(...)

  categorical_feature_a_x_categorical_feature_b = crossed_column(...)

  # Estimator using the default optimizer.
  estimator = LinearRegressor(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b])

  # Or estimator using the FTRL optimizer with regularization.
  estimator = LinearRegressor(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      optimizer=tf.train.FtrlOptimizer(
        learning_rate=0.1,
        l1_regularization_strength=0.001
      ))

  # Or estimator using an optimizer with a learning rate decay.
  estimator = LinearRegressor(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      optimizer=lambda: tf.train.FtrlOptimizer(
          learning_rate=tf.exponential_decay(
              learning_rate=0.1,
              global_step=tf.get_global_step(),
              decay_steps=10000,
              decay_rate=0.96))

  # Or estimator with warm-starting from a previous checkpoint.
  estimator = LinearRegressor(
      feature_columns=[categorical_column_a,
                       categorical_feature_a_x_categorical_feature_b],
      warm_start_from="/path/to/checkpoint/dir")


  # Input builders
  def input_fn_train:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_eval:
    # Returns tf.data.Dataset of (x, y) tuple where y represents label's class
    # index.
    pass
  def input_fn_predict:
    # Returns tf.data.Dataset of (x, None) tuple.
    pass
  estimator.train(input_fn=input_fn_train)
  metrics = estimator.evaluate(input_fn=input_fn_eval)
  predictions = estimator.predict(input_fn=input_fn_predict)
  ```

  Input of `train` and `evaluate` should have following features,
    otherwise there will be a KeyError:

  * if `weight_column` is not `None`, a feature with `key=weight_column` whose
    value is a `Tensor`.
  * for each `column` in `feature_columns`:
    - if `column` is a `SparseColumn`, a feature with `key=column.name`
      whose `value` is a `SparseTensor`.
    - if `column` is a `WeightedSparseColumn`, two features: the first with
      `key` the id column name, the second with `key` the weight column name.
      Both features' `value` must be a `SparseTensor`.
    - if `column` is a `RealValuedColumn`, a feature with `key=column.name`
      whose `value` is a `Tensor`.

  Loss is calculated by using mean squared error.

  @compatibility(eager)
  Estimators can be used while eager execution is enabled. Note that `input_fn`
  and all hooks are executed inside a graph context, so they have to be written
  to be compatible with graph mode. Note that `input_fn` code using `tf.data`
  generally works in both graph and eager modes.
  @end_compatibility
  """

  def __init__(self,
               feature_columns,
               model_dir=None,
               label_dimension=1,
               weight_column=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               warm_start_from=None,
               loss_reduction=losses.Reduction.SUM_OVER_BATCH_SIZE,
               sparse_combiner='sum'):
    """Initializes a `LinearRegressor` instance.

    Args:
      feature_columns: An iterable containing all the feature columns used by
        the model. All items in the set should be instances of classes derived
        from `FeatureColumn`.
      model_dir: Directory to save model parameters, graph and etc. This can
        also be used to load checkpoints from the directory into a estimator to
        continue training a previously saved model.
      label_dimension: Number of regression targets per example. This is the
        size of the last dimension of the labels and logits `Tensor` objects
        (typically, these have shape `[batch_size, label_dimension]`).
      weight_column: A string or a `_NumericColumn` created by
        `tf.feature_column.numeric_column` defining feature column representing
        weights. It is used to down weight or boost examples during training. It
        will be multiplied by the loss of the example. If it is a string, it is
        used as a key to fetch weight tensor from the `features`. If it is a
        `_NumericColumn`, raw tensor is fetched by key `weight_column.key`, then
        weight_column.normalizer_fn is applied on it to get weight tensor.
      optimizer: An instance of `tf.Optimizer` or
        `tf.estimator.experimental.LinearSDCA` used to train the model. Can also
        be a string (one of 'Adagrad', 'Adam', 'Ftrl', 'RMSProp', 'SGD'), or
        callable. Defaults to FTRL optimizer.
      config: `RunConfig` object to configure the runtime settings.
      partitioner: Optional. Partitioner for input layer.
      warm_start_from: A string filepath to a checkpoint to warm-start from, or
        a `WarmStartSettings` object to fully configure warm-starting.  If the
        string filepath is provided instead of a `WarmStartSettings`, then all
        weights and biases are warm-started, and it is assumed that vocabularies
        and Tensor names are unchanged.
      loss_reduction: One of `tf.losses.Reduction` except `NONE`. Describes how
        to reduce training loss over batch. Defaults to `SUM`.
      sparse_combiner: A string specifying how to reduce if a categorical column
        is multivalent.  One of "mean", "sqrtn", and "sum" -- these are
        effectively different ways to do example-level normalization, which can
        be useful for bag-of-words features. for more details, see
        `tf.feature_column.linear_model`.
    """
    _validate_linear_sdca_optimizer_for_linear_regressor(
        feature_columns=feature_columns,
        label_dimension=label_dimension,
        optimizer=optimizer,
        sparse_combiner=sparse_combiner)

    head = regression_head.RegressionHead(
        label_dimension=label_dimension,
        weight_column=weight_column,
        loss_reduction=loss_reduction)

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _linear_model_fn."""
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)

    super(LinearRegressorV2, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)


@estimator_export(v1=['estimator.LinearRegressor'])  # pylint: disable=missing-docstring
class LinearRegressor(estimator.Estimator):
  __doc__ = LinearRegressorV2.__doc__.replace('SUM_OVER_BATCH_SIZE', 'SUM')

  def __init__(self,
               feature_columns,
               model_dir=None,
               label_dimension=1,
               weight_column=None,
               optimizer='Ftrl',
               config=None,
               partitioner=None,
               warm_start_from=None,
               loss_reduction=losses.Reduction.SUM,
               sparse_combiner='sum'):
    _validate_linear_sdca_optimizer_for_linear_regressor(
        feature_columns=feature_columns,
        label_dimension=label_dimension,
        optimizer=optimizer,
        sparse_combiner=sparse_combiner)

    head = head_lib._regression_head(  # pylint: disable=protected-access
        label_dimension=label_dimension,
        weight_column=weight_column,
        loss_reduction=loss_reduction)

    def _model_fn(features, labels, mode, config):
      """Call the defined shared _linear_model_fn."""
      return _linear_model_fn(
          features=features,
          labels=labels,
          mode=mode,
          head=head,
          feature_columns=tuple(feature_columns or []),
          optimizer=optimizer,
          partitioner=partitioner,
          config=config,
          sparse_combiner=sparse_combiner)

    super(LinearRegressor, self).__init__(
        model_fn=_model_fn,
        model_dir=model_dir,
        config=config,
        warm_start_from=warm_start_from)
