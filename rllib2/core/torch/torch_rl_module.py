from dataclasses import dataclass
import torch.nn as nn

from rllib2.models.torch.pi import PiOutput, Pi
from rllib2.utils import NNOutput


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
    pass

@dataclass
class RLModuleOutput(NNOutput):
    """dataclass for holding the outputs of RLModule forward_train() calls"""
    pass


class TorchRLModule(nn.Module, ModelIO):

    def __init__(self, configs):
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


"""Examples of TorchRLModules in RLlib"""
####################################################
################# PPO RLModule
# This is what you customize when modifying TorchPPO
####################################################

# TODO: Make a decision about where explore=True / False should go?
@dataclass
class PPORLModuleConfig(RLModuleConfig):
    action_space: Optional[rllib.env.Space] = None
    obs_space: Optional[rllib.env.Space] = None
    pi: Optional[PiConfig] = None
    vf: Optional[VFuctionConfig] = None
    kl_coeff: Optional[float] = None

@dataclass
class PPOModuleOutput(RLModuleOutput):
    pi_out_cur: Optional[PiOutput] = None
    pi_out_prev: Optional[PiOutput] = None
    vf: Optional[VFOutput] = None


class PPOTorchRLModule(TorchRLModule):

    def __init__(self, config: PPORLModuleConfig):
        super().__init__(config)

        self.pi: Pi = model_catalog.make_pi(
            obs_space=config.obs_space,
            action_space=config.action_space,
            pi_config=config.pi
        )

        self.vf = None
        if config.use_ciritic:
            self.vf = model_catalog.make_vf(
                obs_space=config.obs_space,
                action_space=config.action_space,
                vf_config=config.vf
            )

        if config.kl_coeff is not None:
            kl_coeff = nn.Parameter(
                torch.Tensor(config.kl_coeff),
                requires_grad=True
            )
            self.register_parameter('kl_coeff', kl_coeff)

    def forward(self, batch: SampleBatch, explore=False, **kwargs) -> PiDistribution:

        # option 1: when pi_output has separate target_sample() and behavioral_sample()
        # in this case explore is totally ignored inside the forward() method.
        pi_output: PiOutput = self.pi(batch)

        # option 2: when pi_output has one sample() method whose behavior changes
        # depending on a flag
        pi_output: PiOutput = self.pi(batch, explore=explore)

        return pi_output

    def forward_train(self, batch: SampleBatch, **kwargs) -> RLModuleOutput:
        """Forward-pass during computing loss function"""
        pi_out_cur: PiOutput = self.pi(batch)
        pi_out_prev = self.pi({'obs': batch[SampleBatch.ACTION_DIST_INPUTS]})

        vf = None
        if self.vf:
            vf = self.vf(batch)

        return RLModuleOutput(pi_out_cur=pi_out_cur, pi_out_prev=pi_out_prev, vf=vf)


