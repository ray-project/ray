import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

TF2_GLOBAL_SHARED_LAYER = None


class TF2SharedWeightsModel(TFModelV2):
    """Example of weight sharing between two different TFModelV2s.

    NOTE: This will only work for tf2.x. When running with config.framework=tf,
    use SharedWeightsModel1 and SharedWeightsModel2 below, instead!

    The shared (single) layer is simply defined outside of the two Models,
    then used by both Models in their forward pass.
    """

    def __init__(
        self, observation_space, action_space, num_outputs, model_config, name
    ):
        super().__init__(
            observation_space, action_space, num_outputs, model_config, name
        )

        global TF2_GLOBAL_SHARED_LAYER
        # The global, shared layer to be used by both models.
        if TF2_GLOBAL_SHARED_LAYER is None:
            TF2_GLOBAL_SHARED_LAYER = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1"
            )

        inputs = tf.keras.layers.Input(observation_space.shape)
        last_layer = TF2_GLOBAL_SHARED_LAYER(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out"
        )(last_layer)
        vf = tf.keras.layers.Dense(units=1, activation=None, name="value_out")(
            last_layer
        )
        self.base_model = tf.keras.models.Model(inputs, [output, vf])

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class SharedWeightsModel1(TFModelV2):
    """Example of weight sharing between two different TFModelV2s.

    NOTE: This will only work for tf1 (static graph). When running with
    config.framework=tf2, use TF2SharedWeightsModel, instead!

    Here, we share the variables defined in the 'shared' variable scope
    by entering it explicitly with tf1.AUTO_REUSE. This creates the
    variables for the 'fc1' layer in a global scope called 'shared'
    (outside of the Policy's normal variable scope).
    """

    def __init__(
        self, observation_space, action_space, num_outputs, model_config, name
    ):
        super().__init__(
            observation_space, action_space, num_outputs, model_config, name
        )

        inputs = tf.keras.layers.Input(observation_space.shape)
        with tf1.variable_scope(
            tf1.VariableScope(tf1.AUTO_REUSE, "shared"),
            reuse=tf1.AUTO_REUSE,
            auxiliary_name_scope=False,
        ):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1"
            )(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out"
        )(last_layer)
        vf = tf.keras.layers.Dense(units=1, activation=None, name="value_out")(
            last_layer
        )
        self.base_model = tf.keras.models.Model(inputs, [output, vf])

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class SharedWeightsModel2(TFModelV2):
    """The "other" TFModelV2 using the same shared space as the one above."""

    def __init__(
        self, observation_space, action_space, num_outputs, model_config, name
    ):
        super().__init__(
            observation_space, action_space, num_outputs, model_config, name
        )

        inputs = tf.keras.layers.Input(observation_space.shape)

        # Weights shared with SharedWeightsModel1.
        with tf1.variable_scope(
            tf1.VariableScope(tf1.AUTO_REUSE, "shared"),
            reuse=tf1.AUTO_REUSE,
            auxiliary_name_scope=False,
        ):
            last_layer = tf.keras.layers.Dense(
                units=64, activation=tf.nn.relu, name="fc1"
            )(inputs)
        output = tf.keras.layers.Dense(
            units=num_outputs, activation=None, name="fc_out"
        )(last_layer)
        vf = tf.keras.layers.Dense(units=1, activation=None, name="value_out")(
            last_layer
        )
        self.base_model = tf.keras.models.Model(inputs, [output, vf])

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


TORCH_GLOBAL_SHARED_LAYER = None
if torch:
    # The global, shared layer to be used by both models.
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

    def __init__(
        self, observation_space, action_space, num_outputs, model_config, name
    ):
        TorchModelV2.__init__(
            self, observation_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        # Non-shared initial layer.
        self.first_layer = SlimFC(
            int(np.product(observation_space.shape)),
            64,
            activation_fn=nn.ReLU,
            initializer=torch.nn.init.xavier_uniform_,
        )

        # Non-shared final layer.
        self.last_layer = SlimFC(
            64,
            self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )
        self.vf = SlimFC(
            64,
            1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )
        self._global_shared_layer = TORCH_GLOBAL_SHARED_LAYER
        self._output = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out = self.first_layer(input_dict["obs"])
        self._output = self._global_shared_layer(out)
        model_out = self.last_layer(self._output)
        return model_out, []

    @override(ModelV2)
    def value_function(self):
        assert self._output is not None, "must call forward first!"
        return torch.reshape(self.vf(self._output), [-1])
