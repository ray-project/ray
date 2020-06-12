import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import SlimFC, normc_initializer as \
    torch_normc_initializer
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils import try_import_tf, try_import_torch
from ray.rllib.utils.annotations import override

tf = try_import_tf()
torch, nn = try_import_torch()


class BatchNormModel(TFModelV2):
    """Example of a TFModelV2 that is built w/o using tf.keras.

    NOTE: This example does not work when using a keras-based TFModelV2 due
    to a bug in keras related to missing values for input placeholders, even
    though these input values have been provided in a forward pass through the
    actual keras Model.

    All Model logic (layers) is defined in the `forward` method (incl.
    the batch_normalization layers). Also, all variables are registered
    (only once) at the end of `forward`, so an optimizer knows which tensors
    to train on. A standard `value_function` override is used.
    """
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        # Have we registered our vars yet (see `forward`)?
        self._registered = False

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        last_layer = input_dict["obs"]
        hiddens = [256, 256]
        with tf.variable_scope("model", reuse=tf.AUTO_REUSE):
            for i, size in enumerate(hiddens):
                last_layer = tf.layers.dense(
                    last_layer,
                    size,
                    kernel_initializer=normc_initializer(1.0),
                    activation=tf.nn.tanh,
                    name="fc{}".format(i))
                # Add a batch norm layer
                last_layer = tf.layers.batch_normalization(
                    last_layer,
                    training=input_dict["is_training"],
                    name="bn_{}".format(i))

            output = tf.layers.dense(
                last_layer,
                self.num_outputs,
                kernel_initializer=normc_initializer(0.01),
                activation=None,
                name="out")
            self._value_out = tf.layers.dense(
                last_layer,
                1,
                kernel_initializer=normc_initializer(1.0),
                activation=None,
                name="vf")
        if not self._registered:
            self.register_variables(
                tf.get_collection(
                    tf.GraphKeys.TRAINABLE_VARIABLES, scope=".+/model/.+"))
            self._registered = True

        return output, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class KerasBatchNormModel(TFModelV2):
    """Keras version of above BatchNormModel with exactly the same structure.

    IMORTANT NOTE: This model will not work with PPO due to a bug in keras
    that surfaces when having more than one input placeholder (here: `inputs`
    and `is_training`) AND using the `make_tf_callable` helper (e.g. used by
    PPO), in which auto-placeholders are generated, then passed through the
    tf.keras. models.Model. In this last step, the connection between 1) the
    provided value in the auto-placeholder and 2) the keras `is_training`
    Input is broken and keras complains.
    Use the above `BatchNormModel` (a non-keras based TFModelV2), instead.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        inputs = tf.keras.layers.Input(shape=obs_space.shape, name="inputs")
        is_training = tf.keras.layers.Input(
            shape=(), dtype=tf.bool, batch_size=1, name="is_training")
        last_layer = inputs
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = tf.keras.layers.Dense(
                units=size,
                kernel_initializer=normc_initializer(1.0),
                activation=tf.nn.tanh,
                name=label)(last_layer)
            # Add a batch norm layer
            last_layer = tf.keras.layers.BatchNormalization()(
                last_layer, training=is_training[0])
        output = tf.keras.layers.Dense(
            units=self.num_outputs,
            kernel_initializer=normc_initializer(0.01),
            activation=None,
            name="fc_out")(last_layer)
        value_out = tf.keras.layers.Dense(
            units=1,
            kernel_initializer=normc_initializer(0.01),
            activation=None,
            name="value_out")(last_layer)

        self.base_model = tf.keras.models.Model(
            inputs=[inputs, is_training], outputs=[output, value_out])
        self.register_variables(self.base_model.variables)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(
            [input_dict["obs"], input_dict["is_training"]])
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class TorchBatchNormModel(TorchModelV2, nn.Module):
    """Example of a TorchModelV2 using batch normalization."""
    capture_index = 0

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, **kwargs):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)
        layers = []
        prev_layer_size = int(np.product(obs_space.shape))
        self._logits = None

        # Create layers 0 to second-last.
        for size in [256, 256]:
            layers.append(
                SlimFC(
                    in_size=prev_layer_size,
                    out_size=size,
                    initializer=torch_normc_initializer(1.0),
                    activation_fn=nn.ReLU))
            prev_layer_size = size
            # Add a batch norm layer.
            layers.append(nn.BatchNorm1d(prev_layer_size))

        self._logits = SlimFC(
            in_size=prev_layer_size,
            out_size=self.num_outputs,
            initializer=torch_normc_initializer(0.01),
            activation_fn=None)

        self._value_branch = SlimFC(
            in_size=prev_layer_size,
            out_size=1,
            initializer=torch_normc_initializer(1.0),
            activation_fn=None)

        self._hidden_layers = nn.Sequential(*layers)
        self._hidden_out = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Set the correct train-mode for our hidden module (only important
        # b/c we have some batch-norm layers).
        self._hidden_layers.train(mode=input_dict["is_training"])
        self._hidden_out = self._hidden_layers(input_dict["obs"])
        logits = self._logits(self._hidden_out)
        return logits, []

    @override(ModelV2)
    def value_function(self):
        assert self._hidden_out is not None, "must call forward first!"
        return torch.reshape(self._value_branch(self._hidden_out), [-1])
