from typing import Optional

import torch.nn as nn
from torch.distributions import Distribution
from torch import TensorType
from dataclasses import dataclass

from rllib2.utils import NNOutput

"""
class Distribution:
    
    def rsample(shape): # can compute gradients in backward()
        pass
    
    def sample(shape): # used in inference
        pass
    
    def log_prob(value):
        pass
        
    def entropy():
        pass
        
"""

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
    action_dist: Optional[Distribution] = None
    # TODO: Not sure if we want these. Most use-cases should be supported by outputting a action_dist directly
    action_logits: Optional[TensorType] = None
    action_sampled: Optional[TensorType] = None
    log_p: Optional[TensorType] = None

    def behavioral_sample(self, shape=()):
        """
        Sample from behavioral policy (exploration on)
        """
        pass

    def target_sample(self, shape=()):
        """
        Sample from target policy (exploration off)
        """
        pass

    def log_prob(self, value:TensorType) -> TensorType:
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
    * Support both Continuous and Discrete actions
        * For continuous you should output a continuous dist that can be sampled
        * For discrete you should output a categorical dist that can be sampled
    * Should be able to switch between exploration = on / off
        * target_sample is used for inference (exploration = off)
        * behavioral_sample is used for sampling during training (exploration = true)
        * behavioral / target would be used based on the requirement of computing
        the algorithm's loss function during training
    """

    def __init__(self):
        super().__init__()

    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:
        """
        Runs pi in inference mode, Pi(input_dict) -> P
        Returns:

        """
        pass
