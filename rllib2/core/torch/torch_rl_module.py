from dataclasses import dataclass

import torch.nn as nn

from rllib2.models.torch.pi import Pi, PiOutput
from rllib2.utils import NNOutput

"""Examples of TorchRLModules in RLlib --> See under algorithms"""

"""
Examples:
    
    configs: RLModuleConfig = ...
    self.model = PPOModule(configs)
    
    # The user of the following use-cases are RLlib methods. So they should have a 
    # pre-defined signature that is familiar to RLlib.
    
    # Inference during sampling env or during evaluating policy
    out = self.model({'obs': s[None]}, explore=True/False, inference=True)

    # During sample collection for training
    action = out.behavioral_sample()
    # During sample collection during evaluation
    action = out.target_sample()
    
    #TODO: I don't know if we'd need explore=True / False to change the behavior of sampling
    another alternative is to use explore and only have one sampling method action = out.sample()
    
    # computing (e.g. actor/critic) loss during policy update
    # The output in this use-case will be consumed by the loss function which is 
    # defined by the user (the author of the algorithm).
    # So the structure should flexible to accommodate the various user needs. 
    out = self.model(batch, explore=False, inference=False)
    
    # Note: user knows xyz should exist when forward_train() gets called
    print(out.xyz)
    
"""


@dataclass
class RLModuleConfig(NNConfig):
    """dataclass for holding the nested configuration parameters"""

    action_space: Optional[rllib.env.Space] = None
    obs_space: Optional[rllib.env.Space] = None


@dataclass
class RLModuleOutput(NNOutput):
    """dataclass for holding the outputs of RLModule forward_train() calls"""

    pass


class TorchRLModule(nn.Module, ModelIO):
    def __init__(self, configs=None):
        super().__init__()
        self.configs = configs

    def __call__(self, batch, *args, explore=False, inference=False, **kwargs):
        if inference:
            return self.forward(batch, explore=explore, **kwargs)
        return self.forward_train(batch, **kwargs)

    def forward(self, batch: SampleBatch, explore=False, **kwargs) -> PiDistribution:
        """Forward-pass during online sample collection
        Which could be either during training or evaluation based on explore parameter.
        """
        pass

    def forward_train(self, batch: SampleBatch, **kwargs) -> RLModuleOutput:
        """Forward-pass during computing loss function"""
        pass


class TorchMARLModule(TorchRLModule):
    def forward(
        self, batch: MultiAgentBatch, explore=False, **kwargs
    ) -> PiDistributionDict:
        pass

    def forward_train(self, batch: MultiAgentBatch, **kwargs) -> RLModuleOutput:
        pass
