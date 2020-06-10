import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf, try_import_torch

tf = try_import_tf()
torch, nn = try_import_torch()


class SharedWeightsModel1(TFModelV2):
    """Example of weight sharing between two different TFModelV2s.

    Here, we share the variables defined in the 'shared' variable scope
    by entering it explicitly with tf.AUTO_REUSE. This creates the
    variables for the 'fc1' layer in a global scope called 'shared'
    (outside of the Policy's normal variable scope).
    """

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        inputs = tf.keras.layers.Input(observation_space.shape)
        with tf.variable_scope(
                tf.VariableScope(tf.AUTO_REUSE, "shared"),
                reuse=tf.AUTO_REUSE,
                auxiliary_name_scope=False):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1")(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out")(last_layer)
        vf = tf.keras.layers.Dense(
            units=1, activation=None, name="value_out")(last_layer)
        self.base_model = tf.keras.models.Model(inputs, [output, vf])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class SharedWeightsModel2(TFModelV2):
    """The "other" TFModelV2 using the same shared space as the one above."""

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        super().__init__(observation_space, action_space, num_outputs,
                         model_config, name)

        inputs = tf.keras.layers.Input(observation_space.shape)

        # Weights shared with SharedWeightsModel1.
        with tf.variable_scope(
                tf.VariableScope(tf.AUTO_REUSE, "shared"),
                reuse=tf.AUTO_REUSE,
                auxiliary_name_scope=False):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1")(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out")(last_layer)
        vf = tf.keras.layers.Dense(
            units=1, activation=None, name="value_out")(last_layer)
        self.base_model = tf.keras.models.Model(inputs, [output, vf])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


TORCH_GLOBAL_SHARED_LAYER = None
if torch:
    TORCH_GLOBAL_SHARED_LAYER = SlimFC(
        64,
        64,
        activation_fn=nn.ReLU,
        initializer=torch.nn.init.xavier_uniform_,
    )


class TorchSharedWeightsModel(TorchModelV2, nn.Module):
    """Example of weight sharing between two different TorchModelV2s.

    The shared (single) layer is simply defined outside of the two Models,
    then used by both Models in their forward pass.
    """

    def __init__(self, observation_space, action_space, num_outputs,
                 model_config, name):
        TorchModelV2.__init__(self, observation_space, action_space,
                              num_outputs, model_config, name)
        nn.Module.__init__(self)

        # Non-shared initial layer.
        self.first_layer = SlimFC(
            int(np.product(observation_space.shape)),
            64,
            activation_fn=nn.ReLU,
            initializer=torch.nn.init.xavier_uniform_)

        # Non-shared final layer.
        self.last_layer = SlimFC(
            64,
            self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_)
        self.vf = SlimFC(
            64,
            1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )
        self._output = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out = self.first_layer(input_dict["obs"])
        self._output = TORCH_GLOBAL_SHARED_LAYER(out)
        model_out = self.last_layer(self._output)
        return model_out, []

    @override(ModelV2)
    def value_function(self):
        assert self._output is not None, "must call forward first!"
        return torch.reshape(self.vf(self._output), [-1])
