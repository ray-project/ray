from typing import TYPE_CHECKING

import lightning as L
import torch
from torch import nn

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.algorithms.ppo.lightning_ppo_learner import LightningPPOLearner
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.annotations import override

if TYPE_CHECKING:
    pass

parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=300000)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


class MyLightningModule(L.LightningModule):
    def __init__(self):
        super().__init__()
        self.automatic_optimization = False

        # Networks
        self.encoder = nn.Sequential(nn.Linear(4, 32), nn.ReLU())
        self.pi = nn.Linear(32, 2)
        self.vf = nn.Linear(32, 1)

    def configure_optimizers(self):
        # Separate optimizers for policy and value function
        # Note: encoder parameters are included in both (shared network)
        pi_params = list(self.encoder.parameters()) + list(self.pi.parameters())
        vf_params = list(self.encoder.parameters()) + list(self.vf.parameters())

        pi_opt = torch.optim.Adam(pi_params, lr=3e-4)
        vf_opt = torch.optim.Adam(vf_params, lr=1e-3)

        return (pi_opt, vf_opt)


# define the LightningRLModule
class LightningRLModule(PPOTorchRLModule):
    """RLModule that wraps a PyTorch Lightning module.

    This RLModule contains a LightningModule and uses its networks (encoder, pi, vf)
    for the PPO algorithm. The LightningModule's configure_optimizers() and gradient
    clipping will be used by LightningPPOLearner.
    """

    def __init__(self, *args, **kwargs):
        # Call parent init - this will create the default networks via setup()
        super().__init__(*args, **kwargs)
        self.lightning_module = MyLightningModule()

    @override(PPOTorchRLModule)
    def _forward(self, batch, **kwargs):
        """Inference/exploration forward pass."""
        output = {}
        obs = batch[Columns.OBS]
        encoder_out = self.lightning_module.encoder(obs)
        output[Columns.ACTION_DIST_INPUTS] = self.lightning_module.pi(encoder_out)
        return output

    @override(PPOTorchRLModule)
    def _forward_train(self, batch, **kwargs):
        """Train forward pass - keeps embeddings for value function."""
        output = {}
        obs = batch[Columns.OBS]
        encoder_out = self.lightning_module.encoder(obs)
        # Save embeddings for value computation in compute_loss_for_module
        output[Columns.EMBEDDINGS] = encoder_out
        output[Columns.ACTION_DIST_INPUTS] = self.lightning_module.pi(encoder_out)
        return output

    @override(PPOTorchRLModule)
    def compute_values(self, batch, embeddings=None):
        """Compute values using the Lightning module's value head."""
        if embeddings is None:
            obs = batch[Columns.OBS]
            embeddings = self.lightning_module.encoder(obs)

        # Value head
        vf_out = self.lightning_module.vf(embeddings)
        # Squeeze out last dimension
        return vf_out.squeeze(-1)


config = (
    PPOConfig()
    .environment("CartPole-v1")
    .training(
        lr=0.0003,  # Ignored - Lightning manages LR via configure_optimizers()
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .rl_module(
        rl_module_spec=RLModuleSpec(
            module_class=LightningRLModule,
        ),
    )
    .learners(
        learner_class=LightningPPOLearner,
    )
)

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
