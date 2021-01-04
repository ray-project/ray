from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch


torch, _ = try_import_torch()


# __sphinx_doc_model_api_begin__

class ContActionQModel(TorchModelV2):  # or: TFModelV2
    """A simple, q-value-from-cont-action model (for e.g. SAC type algos)."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        # Pass num_outputs=None into super constructor (so that no action/
        # logits output layer is built).
        # Alternatively, you can pass in num_outputs=[last layer size of
        # config[model][fcnet_hiddens]] AND set no_last_linear=True, but
        # this seems more tedious as you will have to explain users of this
        # class that num_outputs is NOT the size of your Q-output layer.
        super(ContActionQModel, self).__init__(
            obs_space, action_space, None, model_config, name)

        # Now: self.num_outputs contains the last layer's size, which
        # we can use to construct the single q-value computing head.

        # Nest an RLlib FullyConnectedNetwork (torch or tf) into this one here
        # to be used for Q-value calculation.
        combined_space = Box(
            -1.0, 1.0, (self.num_outputs + action_space.shape[0], ))
        self.q_head = FullyConnectedNetwork(
            combined_space, action_space, 1, model_config, "q_head")

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

# __sphinx_doc_model_api_end__


if __name__ == "__main__":
    obs_space = Box(-1.0, 1.0, (3, ))
    action_space = Box(-1.0, -1.0, (2, ))

    # __sphinx_doc_model_construct_begin__
    my_cont_action_q_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=2,
        model_config=MODEL_DEFAULTS,
        framework="torch",  # or: "tf"
        # Providing the `model_interface` arg will make the factory
        # wrap the chosen default model with our new model API class
        # (DuelingQModel). This way, both `forward` and `get_q_values`
        # are available in the returned class.
        model_interface=ContActionQModel,
        name="cont_action_q_model",
    )
    # __sphinx_doc_model_construct_end__

    batch_size = 10
    input_ = torch.from_numpy(
        np.array([obs_space.sample() for _ in range(batch_size)]))
    input_dict = {
        "obs": input_,
        "is_training": False,
    }
    # Note that for PyTorch, you will have to provide torch tensors here.
    out, state_outs = my_cont_action_q_model(input_dict=input_dict)
    assert out.shape == (10, 256)
    # Pass `out` and an action into `my_cont_action_q_model`
    action = torch.from_numpy(
        np.array([action_space.sample() for _ in range(batch_size)]))
    q_value = my_cont_action_q_model.get_single_q_value(out, action)
    assert q_value.shape == (10, 1)
