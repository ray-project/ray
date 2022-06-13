from typing import Optional

import torch
import torch.nn as nn
from torch import TensorType
from dataclasses import dataclass
from .pi_distribution import (
    PiDistribution,
    DeterministicDist,
    SquashedDeterministicDist,
    ...
)

from rllib2.utils import NNOutput


"""
Example:
    pi = Pi()
    
    # during training
        pi_output = pi(sample_batch)
        sampled_actions = pi_output.behavioral_sample(10)
        logp_sampled_actions = pi_output.log_prob(samlped_actions)
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


class Pi(nn.Module):
    """
    Design requirements:
    * Should support both stochastic and deterministic pi`s under one roof
        * Deterministic dists are just special case of stochastic dists with delta func.
        * log_prob would just not be implemented and would raise an error.
        * Under the hood, behavioral and target sample would behave the same
        * for deterministic entropy would be zero
    * Should support arbitrary encoders (encode observations / history to s_t)
        * Encoder would be part of the model attributes
        * TODO: Should we just create an encoder attribute and have the user encode themselves?
            or should we also take care of the encoding during __call__ method?
    * Support both Continuous and Discrete actions
        * For continuous you should output a continuous dist that can be sampled
        * For discrete you should output a categorical dist that can be sampled
        * Support composable output distribution heads for cases where we have a mixture
            of cont. and disc. action spaces
        * TODO: How do we support auto-regressive action spaces?
        * TODO: How do we support Recurrent Policies?
        * TODO: How do we support action-space masking?
    * Support Goal conditioned policies?
    * Should be able to switch between exploration = on / off
        * target_sample is used for inference (exploration = off)
        * behavioral_sample is used for sampling during training (exploration = true)
        * behavioral / target would be used based on the requirement of computing
        the algorithm's loss function during training

    * Should be able to save/load very easily for serving (if needed)
    * Should be able to create copies efficiently and perform arbitrary parameter updates in target_updates
    """

    def __init__(self, encoder: nn.Module):
        super().__init__()
        self.encoder = encoder

    def __call__(self, batch, **kwargs):
        if self.encoder:
            output = self.encoder(batch, kwargs)
        else:
            output = batch
        return super().__call__(output)

    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:
        """
        Runs pi, Pi(input_dict) -> Pi(s_t)
        """
        pass


class DetermnisticPi(Pi):
    """
    Both target and behavioral policy output a Deterministic distribution
    """

    def __init__(self, action_shape, encoder, squash_actions=False, **kwargs):
        super().__init__(encoder)
        self.policy_head = nn.Linear(encoder.num_features, action_shape)

    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:
        s = batch['rl_state']
        logits = self.policy_head(s)
        if self.squash_actions:
            dist = SquashedDeterministicDist(logits)
        else:
            dist = DeterministicDist(logits)
        return PiOutput(action_dist=dist)


class NormalPi(Pi):
    """
    Behavioral Sample is sampled from Normal
    Target sample is the mean of the Normal
    """

    def __init__(self, action_shape, encoder, squash_actions=False,
                 log_std=None, use_parameter_log_std=False, **kwargs):
        super().__init__(encoder)

        output_shape = ...
        self.policy_head = nn.Linear(encoder.num_features, output_shape)


    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:
        s = batch['rl_state']
        logits = self.policy_head(s)
        mean, std = ...

        if self.squash_actions:
            dist = SquashedNormalDist(mean, std)
        else:
            dist = NormalDist(mean, std)
        return PiOutput(action_dist=dist)


class CategoricalPi(Pi):

    def __init__(self, action_classes):
        pass