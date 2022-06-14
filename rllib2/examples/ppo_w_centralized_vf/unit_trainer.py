from typing import Dict

import torch
import torch.nn as nn
from rllib2.core.torch.torch_rl_module import TorchMARLModule, RLModuleConfig
from rllib2.models.torch.pi_distribution import PiDistributionDict
from rllib2.models.torch.pi import Pi
from rllib2.models.torch.v_function import VNet, VFunctionOutput

from rllib.policy.sample_batch import concat_sample


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


class CentralizedVFMultiAgentModule(TorchMARLModule):

    def __init__(self, config: RLModuleConfig):

        self.pis: Dict[str, Pi] = nn.ModuleDict({
            'pol1': model_catalog.make_pi(config.pi),
            'pol2': model_catalog.make_pi(config.pi)
        })

        self.vf: VFunction = MultiAgentCentralizedVF(config.vf)

    def forward(self, batch: MultiAgentBatch, explore=False, **kwargs) -> PiDistributionDict:
        pi_dist = {}
        for pid, s_batch in batch.policy_batches.items():
            pi_dist[pid] = self.pis[pid].forward(s_batch, explore)

        return PiDistributionDict(pi_dist)


    def forward_train(self, batch: MultiAgentBatch, **kwargs) -> RLModuleOutput:
        """Note: I Assume the batch is processed correctly to include the following
        keys for computing the value function of agent i conditioned on
        state and action of agent j"""
        # vf(s^i_t; s^j_t;a^j_t) to compute value of agent's i at time t you need to
        # condition it on the observation and action of agent j at the same time

        concat_sample()
        vf_input = {
            'opponent_obs': batch['opponent_obs'],
            'opponent_act': batch['opponent_act'],
            'obs': batch['obs'],
        }

        vf_output = self.vf(vf_input)[0]

        RLModuleOutputDict



class CentralizedVFUnitTrainer(MATrainer):


