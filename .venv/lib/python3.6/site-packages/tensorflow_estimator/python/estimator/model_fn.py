# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
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

"""Classes and methods related to model_fn."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

import six

from tensorflow.python.framework import ops
from tensorflow.python.framework import tensor_shape
from tensorflow.python.keras.metrics import Metric
from tensorflow.python.ops import array_ops
from tensorflow.python.saved_model import signature_constants
from tensorflow.python.saved_model import tag_constants
from tensorflow.python.training import monitored_session
from tensorflow.python.training import session_run_hook
from tensorflow.python.util import function_utils
from tensorflow.python.util import nest
from tensorflow.python.util.tf_export import estimator_export
from tensorflow_estimator.python.estimator.export import export_output as export_output_lib


@estimator_export('estimator.ModeKeys')
class ModeKeys(object):
  """Standard names for model modes.

  The following standard keys are defined:

  * `TRAIN`: training mode.
  * `EVAL`: evaluation mode.
  * `PREDICT`: inference mode.
  """

  TRAIN = 'train'
  EVAL = 'eval'
  PREDICT = 'infer'


LOSS_METRIC_KEY = 'loss'
AVERAGE_LOSS_METRIC_KEY = 'average_loss'

# Mapping of the modes to appropriate tag_constants that are used for saving.
EXPORT_TAG_MAP = {
    ModeKeys.PREDICT: [tag_constants.SERVING],
    ModeKeys.TRAIN: [tag_constants.TRAINING],
    ModeKeys.EVAL: [tag_constants.EVAL],
}


@estimator_export('estimator.EstimatorSpec')
class EstimatorSpec(
    collections.namedtuple('EstimatorSpec', [
        'mode', 'predictions', 'loss', 'train_op', 'eval_metric_ops',
        'export_outputs', 'training_chief_hooks', 'training_hooks', 'scaffold',
        'evaluation_hooks', 'prediction_hooks'
    ])):
  """Ops and objects returned from a `model_fn` and passed to an `Estimator`.

  `EstimatorSpec` fully defines the model to be run by an `Estimator`.
  """

  def __new__(cls,
              mode,
              predictions=None,
              loss=None,
              train_op=None,
              eval_metric_ops=None,
              export_outputs=None,
              training_chief_hooks=None,
              training_hooks=None,
              scaffold=None,
              evaluation_hooks=None,
              prediction_hooks=None):
    """Creates a validated `EstimatorSpec` instance.

    Depending on the value of `mode`, different arguments are required. Namely

    * For `mode == ModeKeys.TRAIN`: required fields are `loss` and `train_op`.
    * For `mode == ModeKeys.EVAL`: required field is `loss`.
    * For `mode == ModeKeys.PREDICT`: required fields are `predictions`.

    model_fn can populate all arguments independent of mode. In this case, some
    arguments will be ignored by an `Estimator`. E.g. `train_op` will be
    ignored in eval and infer modes. Example:

    ```python
    def my_model_fn(features, labels, mode):
      predictions = ...
      loss = ...
      train_op = ...
      return tf.estimator.EstimatorSpec(
          mode=mode,
          predictions=predictions,
          loss=loss,
          train_op=train_op)
    ```

    Alternatively, model_fn can just populate the arguments appropriate to the
    given mode. Example:

    ```python
    def my_model_fn(features, labels, mode):
      if (mode == tf.estimator.ModeKeys.TRAIN or
          mode == tf.estimator.ModeKeys.EVAL):
        loss = ...
      else:
        loss = None
      if mode == tf.estimator.ModeKeys.TRAIN:
        train_op = ...
      else:
        train_op = None
      if mode == tf.estimator.ModeKeys.PREDICT:
        predictions = ...
      else:
        predictions = None

      return tf.estimator.EstimatorSpec(
          mode=mode,
          predictions=predictions,
          loss=loss,
          train_op=train_op)
    ```

    Args:
      mode: A `ModeKeys`. Specifies if this is training, evaluation or
        prediction.
      predictions: Predictions `Tensor` or dict of `Tensor`.
      loss: Training loss `Tensor`. Must be either scalar, or with shape `[1]`.
      train_op: Op for the training step.
      eval_metric_ops: Dict of metric results keyed by name.
        The values of the dict can be one of the following:
        (1) instance of `Metric` class.
        (2) Results of calling a metric function, namely a
        `(metric_tensor, update_op)` tuple. `metric_tensor` should be
        evaluated without any impact on state (typically is a pure computation
        results based on variables.). For example, it should not trigger the
        `update_op` or requires any input fetching.
      export_outputs: Describes the output signatures to be exported to
        `SavedModel` and used during serving.
        A dict `{name: output}` where:
        * name: An arbitrary name for this output.
        * output: an `ExportOutput` object such as `ClassificationOutput`,
            `RegressionOutput`, or `PredictOutput`.
        Single-headed models only need to specify one entry in this dictionary.
        Multi-headed models should specify one entry for each head, one of
        which must be named using
        signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY.
        If no entry is provided, a default `PredictOutput` mapping to
        `predictions` will be created.
      training_chief_hooks: Iterable of `tf.train.SessionRunHook` objects to
        run on the chief worker during training.
      training_hooks: Iterable of `tf.train.SessionRunHook` objects to run
        on all workers during training.
      scaffold: A `tf.train.Scaffold` object that can be used to set
        initialization, saver, and more to be used in training.
      evaluation_hooks: Iterable of `tf.train.SessionRunHook` objects to
        run during evaluation.
      prediction_hooks: Iterable of `tf.train.SessionRunHook` objects to
        run during predictions.

    Returns:
      A validated `EstimatorSpec` object.

    Raises:
      ValueError: If validation fails.
      TypeError: If any of the arguments is not the expected type.
    """
    train_op = _validate_estimator_spec_train_op(train_op, mode)
    loss = _validate_estimator_spec_loss(loss, mode)
    predictions = _validate_estimator_spec_predictions(predictions, mode)
    export_outputs = _validate_estimator_spec_export_outputs(
        export_outputs, predictions, mode)
    training_hooks = _validate_estimator_spec_hooks(training_hooks)
    evaluation_hooks = _validate_estimator_spec_hooks(evaluation_hooks)
    prediction_hooks = _validate_estimator_spec_hooks(prediction_hooks)
    training_chief_hooks = _validate_estimator_spec_hooks(training_chief_hooks)
    eval_metric_ops = _validate_eval_metric_ops(eval_metric_ops)
    scaffold = _validate_scaffold(scaffold)

    return super(EstimatorSpec, cls).__new__(
        cls,
        mode=mode,
        predictions=predictions,
        loss=loss,
        train_op=train_op,
        eval_metric_ops=eval_metric_ops,
        export_outputs=export_outputs,
        training_chief_hooks=training_chief_hooks,
        training_hooks=training_hooks,
        scaffold=scaffold,
        evaluation_hooks=evaluation_hooks,
        prediction_hooks=prediction_hooks)

  def _replace(self, **kwds):
    """Return a new EstimatorSpec replacing specified fields with new values."""
    if 'mode' in kwds:
      if self.mode != kwds['mode']:
        raise ValueError('mode of EstimatorSpec cannot be changed.')
    new_fields = map(kwds.pop, self._fields, list(self))
    return EstimatorSpec(*new_fields)


def _get_export_outputs(export_outputs, predictions):
  """Validate export_outputs or create default export_outputs.

  Args:
    export_outputs: Describes the output signatures to be exported to
      `SavedModel` and used during serving. Should be a dict or None.
    predictions:  Predictions `Tensor` or dict of `Tensor`.

  Returns:
    Valid export_outputs dict

  Raises:
    TypeError: if export_outputs is not a dict or its values are not
      ExportOutput instances.
  """
  if export_outputs is None:
    default_output = export_output_lib.PredictOutput(predictions)
    export_outputs = {
        signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: default_output}

  if not isinstance(export_outputs, dict):
    raise TypeError('export_outputs must be dict, given: {}'.format(
        export_outputs))
  for v in six.itervalues(export_outputs):
    if not isinstance(v, export_output_lib.ExportOutput):
      raise TypeError(
          'Values in export_outputs must be ExportOutput objects. '
          'Given: {}'.format(export_outputs))

  _maybe_add_default_serving_output(export_outputs)

  return export_outputs


def _maybe_add_default_serving_output(export_outputs):
  """Add a default serving output to the export_outputs if not present.

  Args:
    export_outputs: Describes the output signatures to be exported to
      `SavedModel` and used during serving. Should be a dict.

  Returns:
    export_outputs dict with default serving signature added if necessary

  Raises:
    ValueError: if multiple export_outputs were provided without a default
      serving key.
  """
  if len(export_outputs) == 1:
    (key, value), = export_outputs.items()
    if key != signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY:
      export_outputs[
          signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY] = value
  if len(export_outputs) > 1:
    if (signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY
        not in export_outputs):
      raise ValueError(
          'Multiple export_outputs were provided, but none of them is '
          'specified as the default.  Do this by naming one of them with '
          'signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY.')

  return export_outputs


class _TPUEstimatorSpec(
    collections.namedtuple('TPUEstimatorSpec', [
        'mode', 'predictions', 'loss', 'train_op', 'eval_metrics',
        'export_outputs', 'scaffold_fn', 'host_call', 'training_hooks',
        'evaluation_hooks', 'prediction_hooks'
    ])):
  """Ops and objects returned from a `model_fn` and passed to `TPUEstimator`.

  This is a simplified implementation of `tf.contrib.tpu.EstimatorSpec`. See
  tensorflow/contrib/tpu/python/tpu/tpu_estimator.py for more detailed
  documentation.
  """

  def __new__(cls,
              mode,
              predictions=None,
              loss=None,
              train_op=None,
              eval_metrics=None,
              export_outputs=None,
              scaffold_fn=None,
              host_call=None,
              training_hooks=None,
              evaluation_hooks=None,
              prediction_hooks=None):
    """Creates a `_TPUEstimatorSpec` instance."""
    train_op = _validate_estimator_spec_train_op(train_op, mode)
    loss = _validate_estimator_spec_loss(loss, mode)
    predictions = _validate_estimator_spec_predictions(predictions, mode)
    export_outputs = _validate_estimator_spec_export_outputs(
        export_outputs, predictions, mode)
    training_hooks = _validate_estimator_spec_hooks(training_hooks)
    evaluation_hooks = _validate_estimator_spec_hooks(evaluation_hooks)
    prediction_hooks = _validate_estimator_spec_hooks(prediction_hooks)
    return super(_TPUEstimatorSpec, cls).__new__(
        cls,
        mode=mode,
        predictions=predictions,
        loss=loss,
        train_op=train_op,
        eval_metrics=eval_metrics,
        export_outputs=export_outputs,
        scaffold_fn=scaffold_fn,
        host_call=host_call,
        training_hooks=training_hooks,
        evaluation_hooks=evaluation_hooks,
        prediction_hooks=prediction_hooks)

  def as_estimator_spec(self):
    """Creates an equivalent `EstimatorSpec` used by CPU train/eval."""
    if not self.eval_metrics:
      eval_metric_ops = None
    else:
      metric_fn, tensors = self.eval_metrics
      eval_metric_ops = metric_fn(**tensors)
    return EstimatorSpec(
        mode=self.mode,
        predictions=self.predictions,
        loss=self.loss,
        train_op=self.train_op,
        eval_metric_ops=eval_metric_ops,
        export_outputs=self.export_outputs,
        training_hooks=self.training_hooks,
        evaluation_hooks=self.evaluation_hooks,
        prediction_hooks=self.prediction_hooks)

# Used to generate possible error causes if the user provides a `Tensor` to an
# EstimatorSpec that is not in the default graph.
_default_graph_error_message_template = (
    '{0} with "{1}" must be from the default graph. '
    'Possible causes of this error include: \n\n'
    '1) {0} was created outside the context of the default graph.'
    '\n\n'
    '2) The object passed through to EstimatorSpec was not created '
    'in the most recent call to "model_fn".')


def _validate_estimator_spec_train_op(train_op, mode):
  """Validate train_op inputs for EstimatorSpec or TPUEstimatorSpec.

  Args:
    train_op: Op for the training step.
    mode: A `ModeKeys`. Used to determine whether the train_op is acceptable for
      use in the current mode; for example, if we are not training, this can be
      None.

  Returns:
    train_op: Op for the training step.

  Raises:
    ValueError: If no train_op is passed during training.
    TypeError:  If:
                - train_op is neither a `Tensor` nor an Op.
                - train_op is not part of the default graph.
  """
  if train_op is None:
    if mode == ModeKeys.TRAIN:
      raise ValueError('Missing train_op.')
  else:
    default_graph = ops.get_default_graph()
    _check_is_tensor_or_operation(train_op, 'train_op')
    if train_op.graph is not default_graph:
      raise ValueError(
          _default_graph_error_message_template.format('train_op',
                                                       train_op.name))
  return train_op


def _validate_estimator_spec_loss(loss, mode):
  """Validate loss inputs for EstimatorSpec or TPUEstimatorSpec.

  Args:
    loss: Training loss `Tensor`. Must either be scalar, or with shape `[1]`.
    mode: A `ModeKeys`. Used to determine whether the loss is acceptable for use
      in the current mode; for example, None is acceptable if we are not
      training or evaluating.

  Returns:
    loss: Training loss `Tensor`.

  Raises:
    ValueError: If the loss `Tensor` is not appropriately formatted.
    TypeError:  If:
                - a non-`Tensor`, non-None input is passed.
                - the loss `Tensor` is not part of the default graph.
  """
  if loss is None:
    if mode in (ModeKeys.TRAIN, ModeKeys.EVAL):
      raise ValueError('Missing loss.')
  else:
    default_graph = ops.get_default_graph()
    # Loss must be a tensor.
    loss = _check_is_tensor(loss, 'loss')
    loss_shape = loss.get_shape()
    if loss_shape.num_elements() not in (None, 1):
      raise ValueError('Loss must be scalar, given: {}'.format(loss))
    if not loss_shape.is_compatible_with(tensor_shape.scalar()):
      loss = array_ops.reshape(loss, [])
    if loss.graph is not default_graph:
      raise ValueError(
          _default_graph_error_message_template.format('loss', loss.name))
  return loss


def _validate_estimator_spec_predictions(predictions, mode):
  """Validate predictions inputs for EstimatorSpec or TPUEstimatorSpec.

  Args:
    predictions: Predictions `Tensor` or dict of `Tensor`.
    mode: A `ModeKeys`. Used to determine whether the predictions are acceptable
      for use in the current mode; None is acceptable if we are not making
      predictions.

  Returns:
    predictions: Predictions `Tensor` or dict of `Tensor`.

  Raises:
    ValueError: If:
      - predictions is None and we are in predict mode.
      - predictions `Tensor` is not in default_graph or else it is a dict of
        `Tensor` where at least one is not in default_graph.
    TypeError:  If predictions is not a `Tensor` or dict of `Tensor`.
  """
  if predictions is None:
    if mode == ModeKeys.PREDICT:
      raise ValueError('Missing predictions.')
    predictions = {}
  else:
    default_graph = ops.get_default_graph()
    if isinstance(predictions, dict):
      predictions = {
          k: _check_is_tensor(v, 'predictions[{}]'.format(k))
          for k, v in six.iteritems(predictions)
      }
      for key, value in six.iteritems(predictions):
        if value.graph is not default_graph:
          raise ValueError(
              _default_graph_error_message_template.format(
                  'prediction values', '{0}: {1}'.format(key, value.name)))
    else:
      # Predictions should be a tensor.
      predictions = _check_is_tensor(predictions, 'predictions')
      if predictions.graph is not default_graph:
        raise ValueError(
            _default_graph_error_message_template.format('prediction values',
                                                         predictions.name))
  return predictions


def _validate_estimator_spec_export_outputs(export_outputs, predictions, mode):
  """Validate export_outputs inputs for EstimatorSpec or TPUEstimatorSpec.

  Args:
    export_outputs: Describes the output signatures to be exported to
      `SavedModel` and used during serving.
      A dict `{name: output}` where:
      * name: An arbitrary name for this output.
      * output: an `ExportOutput` object such as `ClassificationOutput`
        `RegressionOutput`, or `PredictOutput`. Single-headed models should only
        need to specify one entry in this dictionary. Multi-headed models should
        specify one entry for each head, one of which must be named using
        signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY. If no entry is
        provided, a default `PredictOutput` mapping to predictions will be
        created.

    predictions: Predictions `Tensor` or dict of `Tensor`. Used in generation of
     default outputs.
    mode: A `ModeKeys`. Used to determine whether to validate at all; if the
      EstimatorSpec is not for making predictions we can skip validation.

  Returns:
    ValueError: If validation fails.
    TypeError: If the export_outputs is not a dict or the values of the dict are
               not instances of type `ExportOutput`.
  """
  if mode == ModeKeys.PREDICT:
    export_outputs = _get_export_outputs(export_outputs, predictions)
  return export_outputs


def _validate_estimator_spec_hooks(hooks):
  """Validate SessionRunHooks for use in EstimatorSpec or TPUEstimatorSpec.

  Args:
    hooks: Iterable of `tf.train.SessionRunHook` objects to run on all workers.

  Returns:
    hooks: Iterable of `tf.train.SessionRunHook` objects.

  Raises:
    ValueError: If validation fails.
    TypeError:  If any element of the iterable is not a SessionRunHook.
  """
  hooks = tuple(hooks or [])

  for hook in hooks:
    if not isinstance(hook, session_run_hook.SessionRunHook):
      raise TypeError(
          'All hooks must be SessionRunHook instances, given: {}'.format(hook))
  return hooks


def _validate_eval_metric_ops(eval_metric_ops):
  """Validate eval_metric_ops for use in EstimatorSpec.

  Args:
    eval_metric_ops: Dict of metric results keyed by name.
      The values of the dict can be one of the following:
      (1) instance of `Metric` class.
      (2) Results of calling a metric_function, namely a
      `(metric_tensor, update_op)` tuple. `metric_tensor` should
      be evaluated without any impact on state (typically it is a
      pure computation based on variables.). For example, it
      should not trigger the `update_op` or require any input
      fetching.

  Returns:
    eval_metric_ops: Dict of metric results keyed by name.

  Raises:
    ValueError:  If:
     - one of the eval_metric_ops `Metric` objects has no updates.
     - there is at least one `Metric` update or result, `Tensor`, or Op that is
       not in the default graph.
    TypeError:   If:
     - eval_metric_ops is not a dict or None.
     - an element of eval_metric_ops is not a `Metric` or a 2-tuple.
     - an element of eval_metric_ops has a sub-element that is not a `Tensor` or
       an Op.
  """
  if eval_metric_ops is None:
    eval_metric_ops = {}
  else:
    if not isinstance(eval_metric_ops, dict):
      raise TypeError(
          'eval_metric_ops must be a dict, given: {}'.format(eval_metric_ops))
    for key, value in six.iteritems(eval_metric_ops):
      # TODO(psv): When we deprecate the old metrics, throw an error here if
      # the value is not an instance of `Metric` class.
      if isinstance(value, Metric):
        if not value.updates:  # Check if metric updates are available.
          raise ValueError(
              'Please call update_state(...) on the "{metric_name}" metric'
              .format(metric_name=value.name))
      else:
        if not isinstance(value, tuple) or len(value) != 2:
          raise TypeError(
              'Values of eval_metric_ops must be (metric_value, update_op) '
              'tuples, given: {} for key: {}'.format(value, key))
  # Verify all tensors and ops are from default graph.
  default_graph = ops.get_default_graph()
  for key, value in list(six.iteritems(eval_metric_ops)):
    if isinstance(value, Metric):
      values_to_check = value.updates[:]
      values_to_check.append(value.result())
    else:
      values_to_check = nest.flatten(value)
    for val in values_to_check:
      if val.graph is not default_graph:
        raise ValueError(
            _default_graph_error_message_template.format(
                'eval_metric_ops', '{0}: {1}'.format(key, val.name)))
  # Add metric variables to the `LOCAL_VARIABLES` collection. Metric variables
  # are by default not added to any collections. We are doing this here, so that
  # metric variables get initialized.
  local_vars = set(ops.get_collection(ops.GraphKeys.LOCAL_VARIABLES))
  vars_to_add = set()
  for key, value in six.iteritems(eval_metric_ops):
    if isinstance(value, Metric):
      vars_to_add.update(value.variables)
      # Convert Metric instances to (value_tensor, update_op) tuple.
      eval_metric_ops[key] = (value.result(), value.updates[0])
  # Remove variables that are in the local variables collection already.
  vars_to_add = vars_to_add.difference(local_vars)
  for v in vars_to_add:
    ops.add_to_collection(ops.GraphKeys.LOCAL_VARIABLES, v)
  return eval_metric_ops


def _validate_scaffold(scaffold):
  """Validate scaffold input for EstimatorSpec.

  Args:
    scaffold: A `tf.train.Scaffold` object that can be used to set
      initialization, saver, and more to be used in training.

  Returns:
    scaffold: A `tf.train.Scaffold` object. If no scaffold is provided, then a
      default is generated.

  Raises:
    TypeError: If the scaffold is not of type `monitored_session.Scaffold`
      or None.
  """
  scaffold = scaffold or monitored_session.Scaffold()
  if not isinstance(scaffold, monitored_session.Scaffold):
    raise TypeError(
        'scaffold must be tf.train.Scaffold. Given: {}'.format(scaffold))
  return scaffold


def _check_is_tensor_or_operation(x, name):
  if not (isinstance(x, ops.Operation) or ops.is_dense_tensor_like(x)):
    raise TypeError('{} must be Operation or Tensor, given: {}'.format(name, x))


def _check_is_tensor(x, tensor_name):
  """Returns `x` if it is a `Tensor`, raises TypeError otherwise."""
  if not ops.is_dense_tensor_like(x):
    raise TypeError('{} must be Tensor, given: {}'.format(tensor_name, x))
  return x


def export_outputs_for_mode(
    mode, serving_export_outputs=None, predictions=None, loss=None,
    metrics=None):
  """Util function for constructing a `ExportOutput` dict given a mode.

  The returned dict can be directly passed to `build_all_signature_defs` helper
  function as the `export_outputs` argument, used for generating a SignatureDef
  map.

  Args:
    mode: A `ModeKeys` specifying the mode.
    serving_export_outputs: Describes the output signatures to be exported to
      `SavedModel` and used during serving. Should be a dict or None.
    predictions: A dict of Tensors or single Tensor representing model
        predictions. This argument is only used if serving_export_outputs is not
        set.
    loss: A dict of Tensors or single Tensor representing calculated loss.
    metrics: A dict of (metric_value, update_op) tuples, or a single tuple.
      metric_value must be a Tensor, and update_op must be a Tensor or Op

  Returns:
    Dictionary mapping the a key to an `tf.estimator.export.ExportOutput` object
    The key is the expected SignatureDef key for the mode.

  Raises:
    ValueError: if an appropriate ExportOutput cannot be found for the mode.
  """
  # TODO(b/113185250): move all model export helper functions into an util file.
  if mode == ModeKeys.PREDICT:
    return _get_export_outputs(serving_export_outputs, predictions)
  elif mode == ModeKeys.TRAIN:
    return {mode: export_output_lib.TrainOutput(
        loss=loss, predictions=predictions, metrics=metrics)}
  elif mode == ModeKeys.EVAL:
    return {mode: export_output_lib.EvalOutput(
        loss=loss, predictions=predictions, metrics=metrics)}
  else:
    raise ValueError(
        'Export output type not found for mode: {}'.format(mode))


@estimator_export('estimator.experimental.call_logit_fn')
def call_logit_fn(logit_fn, features, mode, params, config):
  """Calls logit_fn (experimental).

  THIS FUNCTION IS EXPERIMENTAL. Keras layers/models are the recommended APIs
  for logit and model composition.

  A utility function that calls the provided logit_fn with the relevant subset
  of provided arguments. Similar to tf.estimator._call_model_fn().

  Args:
    logit_fn: A logit_fn as defined above.
    features: The features dict.
    mode: TRAIN / EVAL / PREDICT ModeKeys.
    params: The hyperparameter dict.
    config: The configuration object.

  Returns:
    A logit Tensor, the output of logit_fn.

  Raises:
    ValueError: if logit_fn does not return a Tensor or a dictionary mapping
      strings to Tensors.
  """
  logit_fn_args = function_utils.fn_args(logit_fn)
  kwargs = {}
  if 'mode' in logit_fn_args:
    kwargs['mode'] = mode
  if 'params' in logit_fn_args:
    kwargs['params'] = params
  if 'config' in logit_fn_args:
    kwargs['config'] = config
  logit_fn_results = logit_fn(features=features, **kwargs)

  result_is_valid_dictionary = (
      isinstance(logit_fn_results, dict) and
      all([(isinstance(k, six.string_types) and isinstance(v, ops.Tensor))
           for k, v in six.iteritems(logit_fn_results)]))
  result_is_tensor = isinstance(logit_fn_results, ops.Tensor)

  if not (result_is_valid_dictionary or result_is_tensor):
    raise ValueError('logit_fn should return a Tensor or a dictionary mapping '
                     'strings to Tensors.  logit_fn returned: %s' %
                     logit_fn_results)

  return logit_fn_results
