import torch

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule


class VPGTorchRLModule(TorchRLModule):
    """A simple VPG (vanilla policy gradient)-style RLModule for testing purposes.

    Use this as a minimum, bare-bones example implementation of a custom TorchRLModule.
    """

    def setup(self):
        # You have access here to the following already set attributes:
        # self.observation_space
        # self.action_space
        # self.inference_only
        # self.model_config  # <- a dict with custom settings
        input_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config["hidden_dim"]
        output_dim = self.action_space.n

        self._policy_net = torch.nn.Sequential(
            torch.nn.Linear(input_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, output_dim),
        )

    def _forward(self, batch, **kwargs):
        # Push the observations from the batch through our `self._policy_net`.
        action_logits = self._policy_net(batch[Columns.OBS])
        # Return parameters for the (default) action distribution, which is
        # `TorchCategorical` (due to our action space being `gym.spaces.Discrete`).
        return {Columns.ACTION_DIST_INPUTS: action_logits}

        # If you need more granularity between the different forward behaviors during
        # the different phases of the module's lifecycle, implement three different
        # forward methods. Thereby, it is recommended to put the inference and
        # exploration versions inside a `with torch.no_grad()` context for better
        # performance.
        # def _forward_train(self, batch):
        #    ...
        #
        # def _forward_inference(self, batch):
        #    with torch.no_grad():
        #        return self._forward_train(batch)
        #
        # def _forward_exploration(self, batch):
        #    with torch.no_grad():
        #        return self._forward_train(batch)
