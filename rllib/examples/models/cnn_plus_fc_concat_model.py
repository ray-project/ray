from gym.spaces import Discrete, Tuple

from ray.rllib.examples.models.impala_vision_nets import TorchImpalaVisionNet
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.misc import normc_initializer as \
    torch_normc_initializer, SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.utils import get_filter_config
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


# __sphinx_doc_begin__
class CNNPlusFCConcatModel(TFModelV2):
    """TFModelV2 concat'ing CNN outputs to flat input(s), followed by FC(s).

    Note: This model should be used for complex (Dict or Tuple) observation
    spaces that have one or more image components.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        # TODO: (sven) Support Dicts as well.
        assert isinstance(obs_space.original_space, (Tuple)), \
            "`obs_space.original_space` must be Tuple!"

        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        # Build the CNN(s) given obs_space's image components.
        self.cnns = {}
        concat_size = 0
        for i, component in enumerate(obs_space.original_space):
            # Image space.
            if len(component.shape) == 3:
                config = {
                    "conv_filters": model_config.get(
                        "conv_filters", get_filter_config(component.shape)),
                    "conv_activation": model_config.get("conv_activation"),
                }
                cnn = ModelCatalog.get_model_v2(
                    component,
                    action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="cnn_{}".format(i))
                concat_size += cnn.num_outputs
                self.cnns[i] = cnn
            # Discrete inputs -> One-hot encode.
            elif isinstance(component, Discrete):
                concat_size += component.n
            # TODO: (sven) Multidiscrete (see e.g. our auto-LSTM wrappers).
            # Everything else (1D Box).
            else:
                assert len(component.shape) == 1, \
                    "Only input Box 1D or 3D spaces allowed!"
                concat_size += component.shape[-1]

        self.logits_and_value_model = None
        self._value_out = None
        if num_outputs:
            # Action-distribution head.
            concat_layer = tf.keras.layers.Input((concat_size, ))
            logits_layer = tf.keras.layers.Dense(
                num_outputs,
                activation=tf.keras.activations.linear,
                name="logits")(concat_layer)

            # Create the value branch model.
            value_layer = tf.keras.layers.Dense(
                1,
                name="value_out",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(concat_layer)
            self.logits_and_value_model = tf.keras.models.Model(
                concat_layer, [logits_layer, value_layer])
        else:
            self.num_outputs = concat_size

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Push image observations through our CNNs.
        outs = []
        for i, component in enumerate(input_dict["obs"]):
            if i in self.cnns:
                cnn_out, _ = self.cnns[i]({"obs": component})
                outs.append(cnn_out)
            else:
                outs.append(component)
        # Concat all outputs and the non-image inputs.
        out = tf.concat(outs, axis=1)
        if not self.logits_and_value_model:
            return out, []

        # Value branch.
        logits, values = self.logits_and_value_model(out)
        self._value_out = tf.reshape(values, [-1])
        return logits, []

    @override(ModelV2)
    def value_function(self):
        return self._value_out


# __sphinx_doc_end__


class TorchCNNPlusFCConcatModel(TorchModelV2, nn.Module):
    """TorchModelV2 concat'ing CNN outputs to flat input(s), followed by FC(s).

    Note: This model should be used for complex (Dict or Tuple) observation
    spaces that have one or more image components.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        # TODO: (sven) Support Dicts as well.
        assert isinstance(obs_space.original_space, (Tuple)), \
            "`obs_space.original_space` must be Tuple!"

        nn.Module.__init__(self)
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)

        # Atari type CNNs or IMPALA type CNNs (with residual layers)?
        self.cnn_type = self.model_config["custom_model_config"].get(
            "conv_type", "atari")

        # Build the CNN(s) given obs_space's image components.
        self.cnns = {}
        concat_size = 0
        for i, component in enumerate(obs_space.original_space):
            # Image space.
            if len(component.shape) == 3:
                config = {
                    "conv_filters": model_config.get(
                        "conv_filters", get_filter_config(component.shape)),
                    "conv_activation": model_config.get("conv_activation"),
                }
                if self.cnn_type == "atari":
                    cnn = ModelCatalog.get_model_v2(
                        component,
                        action_space,
                        num_outputs=None,
                        model_config=config,
                        framework="torch",
                        name="cnn_{}".format(i))
                else:
                    cnn = TorchImpalaVisionNet(
                        component,
                        action_space,
                        num_outputs=None,
                        model_config=config,
                        name="cnn_{}".format(i))

                concat_size += cnn.num_outputs
                self.cnns[i] = cnn
                self.add_module("cnn_{}".format(i), cnn)
            # Discrete inputs -> One-hot encode.
            elif isinstance(component, Discrete):
                concat_size += component.n
            # TODO: (sven) Multidiscrete (see e.g. our auto-LSTM wrappers).
            # Everything else (1D Box).
            else:
                assert len(component.shape) == 1, \
                    "Only input Box 1D or 3D spaces allowed!"
                concat_size += component.shape[-1]

        self.logits_layer = None
        self.value_layer = None
        self._value_out = None

        if num_outputs:
            # Action-distribution head.
            self.logits_layer = SlimFC(
                in_size=concat_size,
                out_size=num_outputs,
                activation_fn=None,
            )
            # Create the value branch model.
            self.value_layer = SlimFC(
                in_size=concat_size,
                out_size=1,
                activation_fn=None,
                initializer=torch_normc_initializer(0.01))
        else:
            self.num_outputs = concat_size

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Push image observations through our CNNs.
        outs = []
        for i, component in enumerate(input_dict["obs"]):
            if i in self.cnns:
                cnn_out, _ = self.cnns[i]({"obs": component})
                outs.append(cnn_out)
            else:
                outs.append(component)
        # Concat all outputs and the non-image inputs.
        out = torch.cat(outs, dim=1)
        if self.logits_layer is None:
            return out, []

        # Value branch.
        logits, values = self.logits_layer(out), self.value_layer(out)
        self._value_out = torch.reshape(values, [-1])
        return logits, []

    @override(ModelV2)
    def value_function(self):
        return self._value_out
