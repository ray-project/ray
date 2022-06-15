"""
Use one UnitTrainer and manage multi-agents inside on a centralized RLModule

Pros:
    - It is flexible in having a differentiable communication channels with shared
    modules like the value function here.
Cons:
    - The user have to overwrite the vf module, the RLModule, and also the PPOTrainer.
    So, in other words they are not reusing anything here.
"""

from typing import Dict

import torch
import torch.nn as nn
from rllib2.core.torch.torch_rl_module import TorchMARLModule, RLModuleConfig
from rllib2.core.torch.torch_unit_trainer import UnitTrainerConfig, TorchUnitTrainer
from rllib2.algorithms.ppo.torch.unit_trainer import PPOUnitTrainer
from rllib2.models.torch.pi_distribution import PiDistributionDict
from rllib2.models.torch.pi import Pi
from rllib2.models.torch.v_function import VNet, VFunctionOutput

from rllib.policy.sample_batch import concat_samples


class MultiAgentCentralizedVF(VNet):

    def __init__(self):
        ...

        self.net = nn.Linear(obs_dim * 2 + act_dim, 1)

    def forward(self, batch: SampleBatch, **kwargs) -> VFunctionOutput:
        agent_state = self.encoder(batch['obs']).state
        opponent_state = self.encoder(batch['opponent_obs']).state
        opponent_act = batch['opponent_act']

        value_input = torch.cat([agent_state, opponent_state, opponent_act], -1)
        value = self.net(value_input)
        return VFunctionOutput(values=[value])


if __name__ == '__main__':


    config = (
        PPOConfig()
        .multi_agent(
            policies={
                'pol1': (PPORLModule, pol1_config),
                'pol1': (PPORLModule, pol2_config),
            },

        )
    )