from typing import Dict

from rllib2.core.torch.torch_rl_module import TorchRLModule, RLModuleOutput
from rllib2.models.torch.pi_distribution import PiDistributionDict

import torch.nn as nn


nn.Module
nn.Linear

class MARLModule(TorchRLModule):
    """Base class for MARL"""

    def __init__(self, rl_module_dict: Dict[str, TorchRLModule]):
        super(MARLModule, self).__init__()

        self.rl_module_dict = rl_module_dict


    def forward(self, batch: MultiAgentBatch, explore=False, **kwargs) -> PiDistributionDict:
        pass

    def forward_train(self, batch: MultiAgentBatch, **kwargs) -> MARLModuleOutput:
        pass


class DefaultMARLModule(MARLModule):
    """
    Independent Agent training for MARL
    """

    def forward(self, batch: MultiAgentBatch, explore=False, **kwargs) -> PiDistributionDict:
        out_dists = {}
        for mod_name, mod in self.rl_module_dict.items():
            out_dists[mod_name] = mod.forward(batch[mod_name], explore=explore, **kwargs)
        return PiDistributionDict(out_dists)

    def forward_train(self, batch: MultiAgentBatch, **kwargs) -> MARLModuleOutput:
        outputs = {}
        for mod_name, mod in self.rl_module_dict.items():
            outputs[mod_name] = mod.forward_train(batch[mod_name], **kwargs)
        return MARLModuleOutput(outputs)

