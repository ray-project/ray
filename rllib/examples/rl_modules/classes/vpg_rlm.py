from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class VPGTorchRLModule(TorchRLModule):
    """A simple VPG (vanilla policy gradient)-style RLModule for testing purposes.

    Use this as a minimum, bare-bones example implementation of a custom TorchRLModule.
    """

    @override(TorchRLModule)
    def setup(self):
        input_dim = self.observation_space.shape[0]
        hidden_dim = self.model_config["hidden_dim"]
        output_dim = self.action_space.n

        self._policy_net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

    @override(TorchRLModule)
    def _forward_inference(self, batch, **kwargs):
        with torch.no_grad():
            return self._forward_train(batch)

    @override(TorchRLModule)
    def _forward_exploration(self, batch, **kwargs):
        with torch.no_grad():
            return self._forward_train(batch)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        action_logits = self._policy_net(batch[Columns.OBS])
        return {Columns.ACTION_DIST_INPUTS: action_logits}
