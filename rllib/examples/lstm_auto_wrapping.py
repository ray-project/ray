import numpy as np

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

# __sphinx_doc_begin__


# The custom model that will be wrapped by an LSTM.
class MyCustomModel(TorchModelV2):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, num_outputs, model_config, name)
        self.num_outputs = int(np.product(self.obs_space.shape))
        self._last_batch_size = None

    # Implement your own forward logic, whose output will then be sent
    # through an LSTM.
    def forward(self, input_dict, state, seq_lens):
        obs = input_dict["obs_flat"]
        # Store last batch size for value_function output.
        self._last_batch_size = obs.shape[0]
        # Return 2x the obs (and empty states).
        # This will further be sent through an automatically provided
        # LSTM head (b/c we are setting use_lstm=True below).
        return obs * 2.0, []

    def value_function(self):
        return torch.from_numpy(np.zeros(shape=(self._last_batch_size,)))


if __name__ == "__main__":
    ray.init()

    # Register the above custom model.
    ModelCatalog.register_custom_model("my_torch_model", MyCustomModel)

    # Create the Algorithm from a config object.
    config = (
        ppo.PPOConfig()
        .environment("CartPole-v1")
        .framework("torch")
        .training(
            model={
                # Auto-wrap the custom(!) model with an LSTM.
                "use_lstm": True,
                # To further customize the LSTM auto-wrapper.
                "lstm_cell_size": 64,
                # Specify our custom model from above.
                "custom_model": "my_torch_model",
                # Extra kwargs to be passed to your model's c'tor.
                "custom_model_config": {},
            }
        )
    )
    algo = config.build()
    algo.train()
    algo.stop()

# __sphinx_doc_end__
