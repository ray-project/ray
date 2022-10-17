from turtle import forward
from typing import Any, Mapping
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.core.rl_module import RLModule

torch, nn = try_import_torch()

class TorchRLModule(RLModule, nn.Module):

    def __init__(self, config) -> None:
        RLModule.__init__(self, config)
        nn.Module.__init__(self)
    
    def forward(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """This is only done because Torch requires a forward method to be 
        implemented"""
        return self.forward_train(batch, **kwargs)