from typing import TYPE_CHECKING

import lightning as L
import torch
from torch import nn
from torch.distributions import Categorical

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
    def __init__(
        self,
        lr: float = 3e-4,
        vf_lr: float = 1e-3,
        clip_eps: float = 0.2,
        vf_coef: float = 0.5,
        ent_coef: float = 0.01,
        grad_clip_val: float = 0.5,
    ):
        super().__init__()
        self.automatic_optimization = False
        self.save_hyperparameters()

        # Networks
        self.encoder = nn.Sequential(nn.Linear(4, 32), nn.ReLU())
        self.pi = nn.Linear(32, 2)
        self.vf = nn.Linear(32, 1)

    def configure_optimizers(self):
        # Separate optimizers for policy and value function
        # Policy network includes encoder + policy head
        policy_params = list(self.encoder.parameters()) + list(self.pi.parameters())
        pi_optimizer = torch.optim.Adam(policy_params, lr=self.hparams.lr)

        # Value function optimizer
        vf_optimizer = torch.optim.Adam(self.vf.parameters(), lr=self.hparams.vf_lr)

        # Store as instance attributes for use in training_step without Trainer
        self._pi_optimizer = pi_optimizer
        self._vf_optimizer = vf_optimizer

        return pi_optimizer, vf_optimizer

    def training_step(self, batch, batch_idx):
        # Get optimizers (stored in configure_optimizers since we're not using Trainer)
        pi_opt = self._pi_optimizer
        vf_opt = self._vf_optimizer

        # Extract batch components
        obs = batch[Columns.OBS]
        actions = batch[Columns.ACTIONS]
        old_logp = batch[Columns.ACTION_LOGP]
        advantages = batch[Columns.ADVANTAGES]
        value_targets = batch[Columns.VALUE_TARGETS]

        # Forward pass through networks
        encoder_out = self.encoder(obs)
        new_logits = self.pi(encoder_out)
        new_values = self.vf(encoder_out).squeeze(-1)

        # Compute policy loss (PPO clipped surrogate objective)
        new_dist = Categorical(logits=new_logits)
        new_logp = new_dist.log_prob(actions)
        ratio = torch.exp(new_logp - old_logp)
        surr1 = ratio * advantages
        surr2 = (
            torch.clamp(ratio, 1 - self.hparams.clip_eps, 1 + self.hparams.clip_eps)
            * advantages
        )
        policy_loss = -torch.min(surr1, surr2).mean()

        # Compute value function loss
        vf_loss = ((new_values - value_targets) ** 2).mean()

        # Compute entropy for exploration bonus
        entropy = new_dist.entropy().mean()

        # Policy update (policy loss - entropy bonus)
        pi_opt.zero_grad()
        pi_loss = policy_loss - self.hparams.ent_coef * entropy
        # retain_graph=True because encoder is shared with value function
        pi_loss.backward(retain_graph=True)
        # Clip gradients (manually since we don't have a Trainer)
        pi_params = [p for g in pi_opt.param_groups for p in g["params"]]
        torch.nn.utils.clip_grad_norm_(pi_params, self.hparams.grad_clip_val)
        pi_opt.step()

        # Value function update
        vf_opt.zero_grad()
        vf_loss_scaled = self.hparams.vf_coef * vf_loss
        vf_loss_scaled.backward()
        # Clip gradients (manually since we don't have a Trainer)
        vf_params = [p for g in vf_opt.param_groups for p in g["params"]]
        torch.nn.utils.clip_grad_norm_(vf_params, self.hparams.grad_clip_val)
        vf_opt.step()

        # Note: self.log() requires a Trainer, so we skip Lightning logging here.
        return pi_loss + vf_loss_scaled


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
