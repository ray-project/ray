import contextlib
import gym
from typing import List

from ray.util import log_once
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


@PublicAPI
class TFModelV2(ModelV2, tf.keras.models.Model if tf else object):
    """TF version of ModelV2, which is always also a keras Model.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):
        """Initialize a TFModelV2.

        Here is an example implementation for a subclass
        ``MyModelClass(TFModelV2)``::

            def __init__(self, *args, **kwargs):
                super(MyModelClass, self).__init__(*args, **kwargs)
                input_layer = tf.keras.layers.Input(...)
                hidden_layer = tf.keras.layers.Dense(...)(input_layer)
                output_layer = tf.keras.layers.Dense(...)(hidden_layer)
                value_layer = tf.keras.layers.Dense(...)(hidden_layer)
                self.base_model = tf.keras.Model(
                    input_layer, [output_layer, value_layer])
                self.register_variables(self.base_model.variables)
        """
        tf.keras.models.Model.__init__(self, name=name)
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="tf")

        # Deprecated: Since TFModelV2 are now keras Models, var_list is no
        # longer needed. Keras Models keep track automatically of their
        # inner Models' and layers' variables/trainable variables.
        self.var_list = []

        if tf1.executing_eagerly():
            self.graph = None
        else:
            self.graph = tf1.get_default_graph()

    def context(self) -> contextlib.AbstractContextManager:
        """Returns a contextmanager for the current TF graph."""
        if self.graph:
            return self.graph.as_default()
        else:
            return ModelV2.context(self)

    def update_ops(self) -> List[TensorType]:
        """Return the list of update ops for this model.

        For example, this should include any BatchNorm update ops."""
        return []

    def register_variables(self, variables: List[TensorType]) -> None:
        """Register the given list of variables with this model."""
        if log_once("deprecated_tfmodelv2_register_variables"):
            deprecation_warning(
                old="TFModelV2.register_variables", error=False)
        self.var_list.extend(variables)

    @override(ModelV2)
    def variables(self, as_dict: bool = False) -> List[TensorType]:
        #TODO: fix these.
        if as_dict:
            return {v.name: v for v in self.var_list}
        return list(self.var_list)

    @override(ModelV2)
    def trainable_variables(self, as_dict: bool = False) -> List[TensorType]:
        if as_dict:
            return {
                k: v
                for k, v in self.variables(as_dict=True).items() if v.trainable
            }
        return [v for v in self.variables() if v.trainable]

    @override(tf.keras.models.Model)
    def call(self, *args, **kwargs):
        return self.__call__(*args, **kwargs)
