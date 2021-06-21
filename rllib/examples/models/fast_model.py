from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class FastModel(TFModelV2):
    """An example for a non-Keras ModelV2 in tf that learns a single weight.

    Defines all network architecture in `forward` (not `__init__` as it's
    usually done for Keras-style TFModelV2s).
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)
        # Have we registered our vars yet (see `forward`)?
        self._registered = False

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        with tf1.variable_scope("model", reuse=tf1.AUTO_REUSE):
            bias = tf1.get_variable(
                dtype=tf.float32,
                name="bias",
                initializer=tf.keras.initializers.Zeros(),
                shape=())
            output = bias + \
                tf.zeros([tf.shape(input_dict["obs"])[0], self.num_outputs])
            self._value_out = tf.reduce_mean(output, -1)  # fake value

        if not self._registered:
            self.register_variables(
                tf1.get_collection(
                    tf1.GraphKeys.TRAINABLE_VARIABLES, scope=".+/model/.+"))
            self._registered = True

        return output, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class TorchFastModel(TorchModelV2, nn.Module):
    """Torch version of FastModel (tf)."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        self.bias = nn.Parameter(
            torch.tensor([0.0], dtype=torch.float32, requires_grad=True))

        # Only needed to give some params to the optimizer (even though,
        # they are never used anywhere).
        self.dummy_layer = SlimFC(1, 1)
        self._output = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        self._output = self.bias + torch.zeros(
            size=(input_dict["obs"].shape[0], self.num_outputs)).to(
                self.bias.device)
        return self._output, []

    @override(ModelV2)
    def value_function(self):
        assert self._output is not None, "must call forward first!"
        return torch.reshape(torch.mean(self._output, -1), [-1])
