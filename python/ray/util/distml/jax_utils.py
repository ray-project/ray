from typing import Any, Callable, Sequence, Optional

from jax.experimental import jax2tf  # type: ignore[import]
import tensorflow as tf  # type: ignore[import]


def convert_and_save_model(
    jax_fn: Callable[[Any, Any], Any],
    params,
    model_dir: str,
    *,
    input_signatures: Sequence[tf.TensorSpec],
    with_gradient: bool = False,
    enable_xla: bool = True,
    compile_model: bool = True,
    save_model_options: Optional[tf.saved_model.SaveOptions] = None):
  """Convert a JAX function and saves a SavedModel.

  This is an example, for serious uses you will likely want to copy and
  expand it as needed (see note at the top of the model).

  Use this function if you have a trained ML model that has both a prediction
  function and trained parameters, which you want to save separately from the
  function graph as variables (e.g., to avoid limits on the size of the
  GraphDef, or to enable fine-tuning.) If you don't have such parameters,
  you can still use this library function but probably don't need it
  (see jax2tf/README.md for some simple examples).

  In order to use this wrapper you must first convert your model to a function
  with two arguments: the parameters and the input on which you want to do
  inference. Both arguments may be np.ndarray or (nested)
  tuples/lists/dictionaries thereof.

  See the README.md for a discussion of how to prepare Flax and Haiku models.

  Args:
    jax_fn: a JAX function taking two arguments, the parameters and the inputs.
      Both arguments may be (nested) tuples/lists/dictionaries of np.ndarray.
    params: the parameters, to be used as first argument for `jax_fn`. These
      must be (nested) tuples/lists/dictionaries of np.ndarray, and will be
      saved as the variables of the SavedModel.
    model_dir: the directory where the model should be saved.
    input_signatures: the input signatures for the second argument of `jax_fn`
      (the input). A signature must be a `tensorflow.TensorSpec` instance, or a
      (nested) tuple/list/dictionary thereof with a structure matching the
      second argument of `jax_fn`. The first input_signature will be saved as
      the default serving signature. The additional signatures will be used
      only to ensure that the `jax_fn` is traced and converted to TF for the
      corresponding input shapes.
    with_gradient: whether the SavedModel should support gradients. If True,
      then a custom gradient is saved. If False, then a
      tf.raw_ops.PreventGradient is saved to error if a gradient is attempted.
      (At the moment due to a bug in SavedModel, custom gradients are not
      supported.)
    enable_xla: whether the jax2tf converter is allowed to use TFXLA ops. If
      False, the conversion tries harder to use purely TF ops and raises an
      exception if it is not possible. (default: True)
    compile_model: use TensorFlow jit_compiler on the SavedModel. This
      is needed if the SavedModel will be used for TensorFlow serving.
    save_model_options: options to pass to savedmodel.save.
  """
  if not input_signatures:
    raise ValueError("At least one input_signature must be given")
  tf_fn = jax2tf.convert(
    jax_fn,
    with_gradient=with_gradient,
    enable_xla=enable_xla)

  # Create tf.Variables for the parameters. If you want more useful variable
  # names, you can use `tree.map_structure_with_path` from the `dm-tree` package
  param_vars = tf.nest.map_structure(
    # Due to a bug in SavedModel it is not possible to use tf.GradientTape on
    # a function converted with jax2tf and loaded from SavedModel. Thus, we
    # mark the variables as non-trainable to ensure that users of the
    # SavedModel will not try to fine tune them.
    lambda param: tf.Variable(param, trainable=with_gradient),
    params)
  tf_graph = tf.function(lambda inputs: tf_fn(param_vars, inputs),
                         autograph=False,
                         experimental_compile=compile_model)

  signatures = {}
  # This signature is needed for TensorFlow Serving use.
  signatures[tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY] = \
    tf_graph.get_concrete_function(input_signatures[0])
  for input_signature in input_signatures[1:]:
    # If there are more signatures, trace and cache a TF function for each one
    tf_graph.get_concrete_function(input_signature)
  wrapper = _ReusableSavedModelWrapper(tf_graph, param_vars)
  tf.saved_model.save(wrapper, model_dir, signatures=signatures,
                      options=save_model_options)


class _ReusableSavedModelWrapper(tf.train.Checkpoint):
  """Wraps a function and its parameters for saving to a SavedModel.

  Implements the interface described at
  https://www.tensorflow.org/hub/reusable_saved_models.
  """

  def __init__(self, tf_graph, param_vars):
    """Args:

      tf_graph: a tf.function taking one argument (the inputs), which can be
         be tuples/lists/dictionaries of np.ndarray or tensors. The function
         may have references to the tf.Variables in `param_vars`.
      param_vars: the parameters, as tuples/lists/dictionaries of tf.Variable,
         to be saved as the variables of the SavedModel.
    """
    super().__init__()
    # Implement the interface from https://www.tensorflow.org/hub/reusable_saved_models
    self.variables = tf.nest.flatten(param_vars)
    self.trainable_variables = [v for v in self.variables if v.trainable]
    # If you intend to prescribe regularization terms for users of the model,
    # add them as @tf.functions with no inputs to this list. Else drop this.
    self.regularization_losses = []
    self.__call__ = tf_graph


def load_params_from_tf(checkpoint):
    return tf.saved_model.load(checkpoint)
