from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType
from .model_base import ModelWithEncoder

from rllib2.utils import NNOutput
from rllib2.models.configs import ModelConfig

from .encoder import Encoder, WithEncoderMixin
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
class RecurrentPiOutput(PiOutput):
    last_state: Optional[TensorType] = None


@dataclass
class PiConfig(ModelConfig):
    is_determintic: bool = False
    free_log_std: bool = False
    squash_actions: bool = False


class Pi(ModelWithEncoder):
    """
    Design requirements:
    * Should support both stochastic and deterministic pi`s under one roof
        * Deterministic dists are just special case of stochastic dists with delta func.
        * log_prob would just not be implemented and would raise an error.
        * Under the hood, behavioral and target sample would behave the same
        * for deterministic pis entropy would be zero
    * Should support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
        * TODO: Should we just create an encoder attribute and have the user encode themselves?
            or should we also take care of the encoding during __call__ method?
    * Support both Continuous and Discrete actions
        * For continuous you should output a continuous dist that can be sampled
        * For discrete you should output a categorical dist that can be sampled
        * Support composable output distribution heads for cases where we have a mixture
            of cont. and disc. action spaces
        * TODO: [x] How do we support mixed action spaces? Example below.
        * TODO: [x] How do we support auto-regressive action spaces? Example below.
        * TODO: [x] How do we support goal-conditioned policies? Example below.
        * TODO: [] How do we support Recurrent Policies?
        * TODO: [] Can we support Decision Transformers?
        * TODO: [] How do we support action-space masking?
    * Should be able to switch between exploration = on / off
        * target_sample is used for inference (exploration = off)
        * behavioral_sample is used for sampling during training (exploration = true)
        * behavioral / target would be used based on the requirement of computing
        the algorithm's loss function during training

    * Should be able to save/load very easily for serving (if needed)
    * Should be able to create copies efficiently and perform arbitrary parameter updates in target_updates
    """

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
            self.action_dist_class, 
            logits
        )
        return PiOutput(
            action_dist=action_dist,
            action_logits=logits,
        )

"""
Some examples of pre-defined RLlib standard policies
"""

#######################################################
########### Deterministic Continuous Policy a = Pi(s)
#######################################################


class DetermnisticPi(Pi):
    """
    Both target and behavioral policy output a Deterministic distribution
    """

    def __init__(self, action_shape, encoder, squash_actions=False, **kwargs):
        super().__init__(encoder)
        self.policy_head = nn.Linear(encoder.num_features, action_shape)

    def forward(self, batch: SampleBatch, encoded_batch: Optional[EncoderOutput] = None, **kwargs) -> PiOutput:
        if encoded_batch:
            s = encoded_batch.out
        else:
            s = batch.obs

        logits = self.policy_head(s)
        if self.squash_actions:
            dist = SquashedDeterministicDist(logits)
        else:
            dist = DeterministicDist(logits)
        return PiOutput(action_dist=dist)


###########################################################
########### Stochastic Continuous Policy a ~ N(mu(s), std(s))
###########################################################

class NormalPi(Pi):
    """
    Behavioral Sample is sampled from Normal
    Target sample is the mean of the Normal
    """

    def __init__(self, encoder, action_shape, squash_actions=False,
                 log_std=None, use_parameter_log_std=False, **kwargs):
        super().__init__(encoder)

        output_shape = ...
        self.policy_head = nn.Linear(encoder.num_features, output_shape)


    def forward(self, batch: SampleBatch, encoded_batch: Optional[EncoderOutput] = None, **kwargs) -> PiOutput:
        if encoded_batch:
            s = encoded_batch.out
        else:
            s = batch.obs
        logits = self.policy_head(s)
        mean, std = ...

        if self.squash_actions:
            dist = SquashedNormalDist(mean, std)
        else:
            dist = NormalDist(mean, std)
        return PiOutput(action_dist=dist)


######################################################################
########### Stochastic Discrete Policy a ~ Categorical([p1, ..., pk](s))
######################################################################

class CategoricalPi(Pi):

    def __init__(self, encoder, action_classes):
        pass



class VectorGoalObsEncoder(Encoder):

    def __init__(self, encoder_config):
        super().__init__()
        self.net = MLP(
            in_channels=encoder_config.obs_dim + encoder_config.goal_dim,
            out_channels=encoder_config.latent_dim,
            hidden_dim = 256,
            n_layers = 3,
            activation = 'Relu'
        )

    def forward(self, batch: SampleBatch) -> EncoderOutput:
        obs = batch.obs
        goal = batch.goal
        obs_goal = torch.cat([obs, goal], -1)
        latent = self.net(obs_goal)
        return EncoderOutput(out=latent)


class GoalConditionedPi(NormalPi):

    def __init__(self, encoder: VectorGoalObsEncoder, *args, **kwargs):
        super().__init__(encoder, *args, **kwargs)


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


class MixturePi(Pi):

    def __init__(self, encoder, pi_dict: Dict[str, Pi]):
        super().__init__(encoder)
        self.pis = nn.ModuleDict(pi_dict)


    def forward(self, batch: SampleBatch, encoded_batch: Optional[EncoderOutput] = None, **kwargs) -> PiOutput:
        pi_outputs = {}
        for pi_key, pi in self.pis.items():
            pi_outputs[pi_key] = pi(batch, encoded_batch=encoded_batch, encode=False)

        act_dist_dict = {k: v.action_dist for k, v in pi_output.items()}
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

class _GripperPi(CategoricalPi):
    def __init__(self, encoder: VectorGoalObsEncoder, *args, **kwargs):
        super().__init__(encoder, *args, **kwargs)


class CustomAutoregressivePi(Pi):

    def __init__(self, encoder, action_space, obs_space):
        super().__init__(encoder)

        self.torques = NormalPi(action_space=action_space['torques'])
        self.gripper = _GripperPi(encoder=VectorGoalObsEncoder({'obs_dim': obs_space, 'goal_dim': self.torques.output_dim}))


    def forward(self, batch: SampleBatch, encoded_batch: Optional[EncoderOutput] = None, **kwargs) -> PiOutput:

        torque_output = self.torques(batch, encoded_batch=encoded_batch, encode=False)
        torque_actions = torque_output.sample()

        sample_batch = {'obs': batch.obs, 'goal': torque_actions}
        gripper_actions = self.gripper(sample_batch, encoded_batch, encoded_batch, encode=False)

        ...

        return PiOutput(action_dist=..., action_sampled=...)


###################################################################
########### Decision Transformer # TODO
###################################################################



###################################################################
########### Recurrent Policies # TODO
###################################################################



###################################################################
########### Action space masking # TODO
###################################################################
