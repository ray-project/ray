from copy import deepcopy
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType
from .model_base import ModelWithEncoder

from rllib2.utils import NNOutput
from rllib2.models.configs import ModelConfig

from .pi_distribution import (
    DeterministicDist,
    PiDistribution,
    SquashedDeterministicDist,
)

"""
Example:
    pi = Pi()
    
    # during training
        pi_output = pi(sample_batch)
        sampled_actions = pi_output.behavioral_sample(10)
        logp_sampled_actions = pi_output.log_prob(sampled_actions)
        selected_inds = logp_sampled_actions.argmax(-1)
        selected_actions = sampled_actions[selected_inds]
    
    # during inference
        pi_output = Pi({'obs': obs_tens[None]})
        # with exploration
        pi_output.target_sample((1,))
"""

@dataclass
class PiOutput(NNOutput):
    action_dist: Optional[PiDistribution] = None
    # TODO: Not sure if we want these. Most use-cases should be supported by outputting a action_dist directly
    action_logits: Optional[TensorType] = None
    action_sampled: Optional[TensorType] = None
    log_p: Optional[TensorType] = None

    def behavioral_sample(self, shape=(), **kwargs):
        """
        Sample from behavioral policy (exploration on)
        """
        pass

    def target_sample(self, shape=(), **kwargs):
        """
        Sample from target policy (exploration off)
        """
        pass

    def log_prob(self, value: TensorType) -> TensorType:
        pass

    def entropy(self) -> TensorType:
        pass


@dataclass
class PiConfig(ModelConfig):
    is_determintic: bool = False
    free_log_std: bool = False
    squash_actions: bool = False
    action_dist_class: Type[PiDistribution] = None


class PiBase(nn.Module):
    """Design requirements:
    * [x] Should support both stochastic and deterministic pi`s under one roof
        * Deterministic dists are just special case of stochastic dists with delta func.
        * log_prob would just not be implemented and would raise an error.
        * Under the hood, behavioral and target sample would behave the same
        * for deterministic pis entropy would be zero
    * [x] Should support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
        * Should we just create an encoder attribute and have the user encode themselves?
    * [] Support both Continuous and Discrete actions
        * For continuous you should output a continuous dist that can be sampled
        * For discrete you should output a categorical dist that can be sampled
        * Support composable output distribution heads for cases where we have a mixture
            of cont. and disc. action spaces
        * TODO: [x] How do we support mixed action spaces? Example below.
        * TODO: [x] How do we support auto-regressive action spaces? Example below.
        * TODO: [x] How do we support goal-conditioned policies? Example below.
        * TODO: [x] How do we support Recurrent Policies?
        * TODO: [x] Can we support Transformers?
        * TODO: [x] How do we support action-space masking?
    * [x] Should be able to switch between exploration = on / off --> this handled by who ever runs RLModule's forward_inference()
        * target_sample is used for inference (exploration = off)
        * behavioral_sample is used for sampling during training (exploration = true)
        * behavioral / target would be used based on the requirement of computing
        the algorithm's loss function during training
    * [x] Should be able to save/load very easily for serving (if needed)
    """

    def __init__(self, config: ModelConfig) -> None:
        super().__init__()
        self.config = config

    @abc.abstractmethod
    def forward(
        self, 
        input_dict: SampleBatch, 
        return_encoder_output: bool = False,
        **kwargs
    ) -> PiOutput:
        raise NotImplementedError


class SingleDistributionPi(PiBase, ModelWithEncoder):

    def __init__(self, config: PiConfig, **kwargs):
        super().__init__(config)

        # action distribution
        self.action_dist_class, self.logit_dim = self._make_action_dist()

        # output layer
        self.out_layer = self._make_output_layer()


    def _make_action_dist(self):
        dist_class, logit_dim = model_catalog.get_action_dist_class(self.config)
        return dist_class, logit_dim

    def _make_output_layer(self):
        return nn.Linear(self.encoder_out_dim, self.logit_dim)
    

    def forward(
        self, 
        input_dict: SampleBatch, 
        return_encoder_output: bool = False,
        **kwargs
    ) -> PiOutput:
        """Runs pi, Pi(input_dict) -> Pi(s_t)"""

        encoder_output = self.encoder(input_dict)
        logits = self.out_layer(encoder_output)
        action_dist = model_catalog.get_action_dist(
            input_dict, # uses action masking internally
            logits,
            self.action_dist_class, 
        )
        return PiOutput(
            action_dist=action_dist,
            action_logits=logits,
            encoder_output=encoder_output if return_encoder_output else None
        )

"""
Some examples of pre-defined RLlib standard policies
"""

#######################################################
########### Deterministic Continuous Policy a = Pi(s)
#######################################################
###########################################################
########### Stochastic Continuous Policy a ~ N(mu(s), std(s))
###########################################################


def test_continuous_pi():
    config = PiConfig(
        observation_space=Box(low=-1, high=1, shape=(10,)),
        action_space=Box(low=-1, high=1, shape=(2,)),
        is_deterministic=True,
        # free_log_std=True,
        # squash_actions=True,
    )
    pi = SingleDistributionPi(config)
    print(pi)


def test_discrete_pi():
    config = PiConfig(
        observation_space=Box(low=-1, high=1, shape=(10,)),
        action_space=Discrete(2),
        is_deterministic=True,
        # free_log_std=True,
    )
    pi = SingleDistributionPi(config)
    print(pi)


################################################################
########### Goal conditioned policies a ~ N(mu(s, g), std(s, g))
################################################################

def test_goal_conditioned_policy():
    # see how easy it is to modify the encoder
    # encode goal observation and current observation and concat them as the policy 
    # input
    class Encoder(nn.Module):
        def __init__(self, observation_space, action_space) -> None:
            super().__init__()
            self.encoding_layer = model_catalog.get_encoder(
                observation_space, action_space
            )
        
        def forward(self, input_dict):
            obs = input_dict['obs']
            goal = input_dict['goal']

            z_obs = self.encoding_layer(obs)
            z_goal = self.encoding_layer(goal)
            return torch.cat([z_obs, z_goal], -1)


    register_model('goal_conditioned_encoder', Encoder)
    config = PiConfig(
        encoder='goal_conditioned_encoder',
        observation_space=Box(low=-1, high=1, shape=(10,)),
        action_space=Discrete(2),
        is_deterministic=True,
        # free_log_std=True,
    )

    pi = SingleDistributionPi(config)
    print(pi)


###################################################################
########### Mixed action space policy
# action_space = {'torques': Box(-1, 1)^6, 'gripper': Discrete(n=2)}
# a ~ MixedDistribution({
#   'torques' ~ squashedNorm(mu(s), std(s)),
#   'gripper' ~ Categorical([p1(s), p2(s)])
# })
# example usage of the mixed_dist output
# >>> actor_loss = mixed_dist.log_prob(a)
# >>> {'torques': Tensor(0.63), 'gripper': Tensor(0.63)}
###################################################################

# see how easy it is to extend the base pi class to support mixed action spaces

@dataclass
class MixturePiConfig(PiConfig):
    pi_dict: Dict[str, Pi] = field(default_factory=dict)

class MixturePi(PiBase):

    def __init__(self, config: MixturePiConfig):
        super().__init__()
        self.config = config
        self.pis = nn.ModuleDict(pi_dict)

    def forward(self, input_dict: SampleBatch, **kwargs) -> PiOutput:
        pi_outputs = {}
        for pi_key, pi in self.pis.items():
            pi_outputs[pi_key] = pi(input_dict)

        act_dist_dict = {k: v.action_dist for k, v in pi_outputs.items()}
        return PiOutput(action_dist=MixDistribution(act_dist_dict))


###################################################################
########### Auto-regressive action space policy
# action_space = {'torques': Box(-1, 1)^6, 'gripper': Discrete(n=2)}
# a ~ MixedDistribution({
#   'torques' ~ squashedNorm(mu(s), std(s)),
#   'gripper' ~ Categorical([p1(s, torques), p2(s, torques)])
# })
# example usage of the mixed_dist with autoregressive outputs
# >>> actor_loss = mixed_dist.log_prob(a)
# >>> {'torques': Tensor(0.63), 'gripper': Tensor(0.63)}
###################################################################

class GripperObsTorqueEncoder(Encoder):

    def __init__(self, torque_out_dim: int, obs_encoder: nn.Module) -> None:
        super().__init__()
        self.obs_encoder = obs_encoder

        self.linear = nn.Linear(
            self.obs_encoder.out_dim + torque_out_dim, 
            64,
        )

    def forward(self, input_dict: SampleBatch) -> torch.Tensor:
        obs = input_dict['obs']
        torque = input_dict['torque']
        obs = self.obs_encoder(obs)
        z_t = torch.cat([obs, torque], -1)
        out = self.linear(z_t)
        return out

class CustomAutoregressivePi(PiBase):

    def __init__(self, config: PiConfig):
        super().__init__(config)

        torque_configs = deepcopy(config)
        torque_configs.action_space = config.action_space['torque']
        torque_configs.observation_space = config.observation_space['torque']
        self.torque_pi = SingleDistributionPi(torque_configs)


        gripper_configs = deepcopy(config)
        gripper_configs.action_space = config.action_space['gripper']
        gripper_configs.observation_space = config.observation_space['gripper']
        # get the obs encoder from torque's pi to share parameters with gripper's pi
        obs_encoder = self.torque_pi.encoder
        gripper_configs.encoder = GripperObsTorqueEncoder(
            torque_out_dim=torque_configs.fcnet_hiddens[-1], 
            encoder=obs_encoder
        )

        self.gripper_pi = SingleDistributionPi(gripper_configs)



    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:

        torque_output = self.torque_pi(batch)
        torque_actions = torque_output.sample()

        sample_batch = {'torque': torque_actions, 'obs': batch.obs}
        gripper_actions = self.gripper_pi(sample_batch)
        ...

        return PiOutput(action_dist=..., action_sampled=...)


###################################################################
########### Transformer policy
###################################################################
def test_transformer_pi():
    config = PiConfig(
        observation_space=Box(low=-1, high=1, shape=(84, 84, 3)),
        action_space=Discrete(2),
        is_transformer=True,
        # free_log_std=True,
    )
    pi = Pi(config)
    print(pi)


###################################################################
########### Recurrent Policies
###################################################################
def test_rnn_pi():
    config = PiConfig(
        observation_space=Box(low=-1, high=1, shape=(10,)),
        action_space=Discrete(2),
        is_rnn=True,
        # free_log_std=True,
    )
    pi = Pi(config)
    print(pi)


###################################################################
########### Action space masking # TODO
###################################################################
