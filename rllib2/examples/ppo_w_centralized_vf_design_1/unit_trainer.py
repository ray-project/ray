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


class CentralizedVFMultiAgentPPOModule(TorchMARLModule):

    def __init__(self, config: RLModuleConfig):

        self.pis: Dict[str, Pi] = nn.ModuleDict({
            'pol1': model_catalog.make_pi(config.pi),
            'pol2': model_catalog.make_pi(config.pi)
        })

        self.vf = MultiAgentCentralizedVF(config.vf)

    def forward(self, batch: MultiAgentBatch, explore=False, **kwargs) -> PiDistributionDict:
        pi_dist = {}
        for pid, s_batch in batch.policy_batches.items():
            pi_dist[pid] = self.pis[pid].forward(s_batch, explore)

        return PiDistributionDict(pi_dist)


    def forward_train(self, batch: MultiAgentBatch, **kwargs) -> RLModuleOutputDict:
        """Note: I Assume the batch is processed correctly to include the following
        keys for computing the value function of agent i conditioned on
        state and action of agent j

        vf(s^i_t; s^j_t;a^j_t) to compute value of agent's i at time t you need to
        condition it on the observation and action of agent j at the same time
        """

        output = {}
        for pid in ['pol1', 'pol2']:
            train_batch = batch[pid]
            vf_input = {
                'opponent_obs': train_batch['opponent_obs'],
                'opponent_act': train_batch['opponent_act'],
                'obs': train_batch['obs'],
            }

            vf = self.vf(vf_input)
            pi_out_cur = self.pis[pid](train_batch)
            pi_out_prev = self.pis[pid](train_batch)

            output[pid] = PPOModuleOutput(
                pi_out_cur=pi_out_cur,
                pi_out_prev=pi_out_prev,
                vf=vf
            )

        return RLModuleOutputDict(output)





class CentralizedVFUnitTrainer(PPOUnitTrainer):

    def make_optimizer(self) -> Dict[str, Optimizer]:

    def loss(self, train_batch: MultiAgentBatch, fwd_train_dict) -> Dict[str, torch.Tensor]:

        losses = {}
        for pid in ['pol1', 'pol2']:
            s_batch = train_batch[pid]
            s_fwd_train_dict = fwd_train_dict[pid]
            losses[pid] = super().loss(s_batch, s_fwd_train_dict)

        return losses




