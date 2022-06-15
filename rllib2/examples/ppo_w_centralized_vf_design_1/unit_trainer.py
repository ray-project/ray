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


    def default_rl_module(self) -> Union[str, Type[TorchMARLModule]]:
        return CentralizedVFMultiAgentPPOModule

    def make_optimizer(self) -> Dict[str, Optimizer]:
        optimizers = {}
        for pid in ['pol1', 'pol2']:
            optimizers[pid] = super().make_optimizer()

        return optimizers

    def loss(self, train_batch: MultiAgentBatch, fwd_train_dict) -> Dict[str, torch.Tensor]:

        losses = {}
        for pid in ['pol1', 'pol2']:
            s_batch = train_batch[pid]
            s_fwd_train_dict = fwd_train_dict[pid]
            losses[pid] = super().loss(s_batch, s_fwd_train_dict)

        return losses


    def update(self, train_batch: SampleBatch):




"""
Multi-Agent support means the following scenarios:

1. There can be independent Algorithms for updating each RLModule (Pi)
There is no agent communication during training.
During sampling the specific training portions of the algorithms will be ignored but
the other RLModules will be queried to get the samples for training the RLModule
of interest. See multi_agent_two_trainers.py.

# This function will be used during sampling to group samples by pid
policy_map_fn=lambda agent_id: 'ppo_policy' if agent_id == 0 else 'dqn_policy'

policies = {'ppo_policy': PPORLModule, 'dqn_policy': DQNRLModule}
# Both PPORLModule and DQNRLModule have the same forward() method so in principle they
# look identical during sampling an env

ppo = PPO(..., multi_agent={policy_map_fn, policies, policies_to_train=['ppo_policy']})
dqn = DQN(..., multi_agent={policy_map_fn, policies, policies_to_train=['dqn_policy']})


for iter in range(NUM_ITER):

    # runs forward() of both ppo and dqn modules for sample collection,
    # postprocesses according to PPO requirements,
    # and then updates only the ppo_policy according to ppo update rule
    ppo.train()

    # runs forward() of both ppo and dqn modules for sample collection,
    # postprocesses according to DQN requirements,
    # and then updates only the dqn_policy according to dqn update rule
    dqn.train()

    # update the ppo_policy weights of the dqn trainer with the update ppo_policy and vice versa
    ppo.update_weight('dqn_policy', dqn.get_weight('dqn_policy'))
    dqn.update_weight('ppo_policy', ppo.get_weight('ppo_policy'))

Action Items:
1.1. Algorithm should create a dictionary of UnitTrainers:
    self.unit_trainer_map = {'ppo_policy': PPOTrainer(PPORLModule), 'dqn_policy': PPOTrainer(DQNRLModule)}
1.2. Allow users to construct arbitrary RLModules inside the make_model of a unit_trainer.
But if there is a mismatch between the type of the output of forward_train()
and the expected output type from the UnitTrainer we should raise an error to inform
them about the mismatch.
1.3. Inside algorithm.update loop through the unit_trainer_map and only call .update()
on those policies that are included in policies_to_train

Notes:
1.1. In this scenario agent_k which is based on policy_m can still use data from the
perspective of other agents to encode its own observation into a latent state.
This is true for both forward() and forward_train() methods.
1.2 Can some part of the RLModules still be shared?
    1.2.1 They use a shared frozen ResNet model for encoding input images?
    This is a valid use-case. See below.
    1.2.2 They need to both train a share image encoder with two different algorithms?
    Very hard case. See below.

--> Sharing modules between different RLModules before construction

Config Schema:

multi-agent: {
    'policies': ['ppo_policy', 'dqn_policy'],
    'policies': {
        'ppo_policy': (PPORLModule, ppo_config),
        'dqn_policy': (DQMRLModule, dqn_config),
    },
    shared_modules: {
        'encoder': {
            'class': Encoder,
            'config': encoder_config,
            'shared_between': {'ppo_policy': 'encoder', 'dqn_policy': 'embedder'} # the renaming that needs to happen inside the RLModules
        },
        'dynamics': {
            'class': DynamicsModel,
            'config': dynamic_config,
            'shared_between': {'ppo_policy': 'dynamics_mdl', 'dqn_policy': 'dynamics'}
        }
    }
}

2.2
"""