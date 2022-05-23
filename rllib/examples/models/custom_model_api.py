from gym.spaces import Box

from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.torch.fcnet import (
    FullyConnectedNetwork as TorchFullyConnectedNetwork,
)
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


# __sphinx_doc_model_api_1_begin__
class DuelingQModel(TFModelV2):  # or: TorchModelV2
    """A simple, hard-coded dueling head model."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        super(DuelingQModel, self).__init__(
            obs_space, action_space, None, model_config, name
        )
        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the dueling head (see torch: SlimFC
        # below).

        # Construct advantage head ...
        self.A = tf.keras.layers.Dense(num_outputs)
        # torch:
        # self.A = SlimFC(
        #     in_size=self.num_outputs, out_size=num_outputs)

        # ... and value head.
        self.V = tf.keras.layers.Dense(1)
        # torch:
        # self.V = SlimFC(in_size=self.num_outputs, out_size=1)

    def get_q_values(self, underlying_output):
        # Calculate q-values following dueling logic:
        v = self.V(underlying_output)  # value
        a = self.A(underlying_output)  # advantages (per action)
        advantages_mean = tf.reduce_mean(a, 1)
        advantages_centered = a - tf.expand_dims(advantages_mean, 1)
        return v + advantages_centered  # q-values


# __sphinx_doc_model_api_1_end__


class TorchDuelingQModel(TorchModelV2):
    """A simple, hard-coded dueling head model."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        nn.Module.__init__(self)
        super(TorchDuelingQModel, self).__init__(
            obs_space, action_space, None, model_config, name
        )
        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the dueling head (see torch: SlimFC
        # below).

        # Construct advantage head ...
        self.A = SlimFC(in_size=self.num_outputs, out_size=num_outputs)

        # ... and value head.
        self.V = SlimFC(in_size=self.num_outputs, out_size=1)

    def get_q_values(self, underlying_output):
        # Calculate q-values following dueling logic:
        v = self.V(underlying_output)  # value
        a = self.A(underlying_output)  # advantages (per action)
        advantages_mean = torch.mean(a, 1)
        advantages_centered = a - torch.unsqueeze(advantages_mean, 1)
        return v + advantages_centered  # q-values


class ContActionQModel(TFModelV2):
    """A simple, q-value-from-cont-action model (for e.g. SAC type algos)."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        super(ContActionQModel, self).__init__(
            obs_space, action_space, None, model_config, name
        )

        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the single q-value computing head.

        # Nest an RLlib FullyConnectedNetwork (torch or tf) into this one here
        # to be used for Q-value calculation.
        # Use the current value of self.num_outputs, which is the wrapped
        # model's output layer size.
        combined_space = Box(-1.0, 1.0, (self.num_outputs + action_space.shape[0],))
        self.q_head = FullyConnectedNetwork(
            combined_space, action_space, 1, model_config, "q_head"
        )

        # Missing here: Probably still have to provide action output layer
        # and value layer and make sure self.num_outputs is correctly set.

    def get_single_q_value(self, underlying_output, action):
        # Calculate the q-value after concating the underlying output with
        # the given action.
        input_ = tf.concat([underlying_output, action], axis=-1)
        # Construct a simple input_dict (needed for self.q_head as it's an
        # RLlib ModelV2).
        input_dict = {"obs": input_}
        # Ignore state outputs.
        q_values, _ = self.q_head(input_dict)
        return q_values


# __sphinx_doc_model_api_2_begin__


class TorchContActionQModel(TorchModelV2):
    """A simple, q-value-from-cont-action model (for e.g. SAC type algos)."""

    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        nn.Module.__init__(self)
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        super(TorchContActionQModel, self).__init__(
            obs_space, action_space, None, model_config, name
        )

        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the single q-value computing head.

        # Nest an RLlib FullyConnectedNetwork (torch or tf) into this one here
        # to be used for Q-value calculation.
        # Use the current value of self.num_outputs, which is the wrapped
        # model's output layer size.
        combined_space = Box(-1.0, 1.0, (self.num_outputs + action_space.shape[0],))
        self.q_head = TorchFullyConnectedNetwork(
            combined_space, action_space, 1, model_config, "q_head"
        )

        # Missing here: Probably still have to provide action output layer
        # and value layer and make sure self.num_outputs is correctly set.

    def get_single_q_value(self, underlying_output, action):
        # Calculate the q-value after concating the underlying output with
        # the given action.
        input_ = torch.cat([underlying_output, action], dim=-1)
        # Construct a simple input_dict (needed for self.q_head as it's an
        # RLlib ModelV2).
        input_dict = {"obs": input_}
        # Ignore state outputs.
        q_values, _ = self.q_head(input_dict)
        return q_values


# __sphinx_doc_model_api_2_end__
