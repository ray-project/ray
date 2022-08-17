from calendar import c
from dataclasses import dataclass
from typing import Dict

from collections import defaultdict

import torch.nn as nn


from rllib2.models.torch.pi import Pi, PiOutput
from rllib2.models.torch.pi_distribution import PiDistributionDict
from rllib2.utils import NNOutput

from ....python.ray.rllib.policy.sample_batch import MultiAgentBatch
from ..trainer.rl_trainer import BatchType
from ray.rllib import policy

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


class RLModule:
    def __init__(self, configs=None):
        super().__init__()
        self.configs = configs

    def __call__(self, batch: BatchType, inference=False, **kwargs):
        if inference:
            return self.forward_inference(batch, **kwargs)
        return self.forward_train(batch, **kwargs)

    def forward_inference(self, batch: BatchType, **kwargs) -> PiDistribution:
        """Forward-pass during online sample collection
        Which could be either during training or evaluation based on explore parameter.
        """
        pass

    def forward_train(self, batch: BatchType, **kwargs) -> RLModuleOutput:
        """Forward-pass during computing loss function"""
        pass

class MARLModule(RLModule):
    def __init__(
        self,
        configs,  # multi_agent config
    ):

        self.configs = configs
        super().__init__(configs)

        self._shared_module_infos = self._make_shared_module_infos()
        self._modules = self._make_modules()

    def keys(self):
        return self._modules.keys()

    def forward_inference(
        self, batch: MultiAgentBatch, module_id: str = "default", **kwargs
    ) -> PiDistribution:
        """Forward-pass during online sample collection
        Which could be either during training or evaluation based on explore parameter.
        """
        self._check_module_exists(module_id)
        return self._modules[module_id].forward_inference(batch[module_id], **kwargs)

    def forward_train(
        self, batch: MultiAgentBatch, module_id: str = "default", **kwargs
    ) -> RLModuleOutput:
        """Forward-pass during computing loss function"""

        self._check_module_exists(module_id)
        return self._modules[module_id].forward_train(batch[module_id], **kwargs)

    def __getitem__(self, module_id):
        self._check_module_exists(module_id)
        return self._modules[module_id]

    def _make_shared_module_infos(self):
        config = self.configs
        shared_config = config["shared_submodules"]

        shared_mod_infos = defaultdict({})  # mapping from module_id to kwarg and value
        for mod_info in shared_config.values():
            mod_class = mod_info["class"]
            mod_config = mod_info["config"]
            mod_obj = mod_class(mod_config)

            for module_id, kw in mod_info["shared_between"].items():
                shared_mod_infos[module_id][kw] = mod_obj

        """
        shared_mod_infos = 'policy_kwargs'{
            'A': {'encoder': encoder, 'dynamics': dyna},
            'B': {'encoder': encoder, 'dynamics': dyna},
            '__all__': {'mixer': mixer}
        }
        """
        return shared_mod_infos

    def _make_modules(self):
        shared_mod_info = self.shared_module_infos
        policies = self.config["multi_agent"]["modules"]
        modules = {}
        for pid, pid_info in policies.items():
            # prepare the module parameters and class type
            rl_mod_class = pid_info["module_class"]
            rl_mod_config = pid_info["module_config"]
            kwargs = shared_mod_info[pid]
            rl_mod_config.update(**kwargs)

            # create the module instance
            rl_mod_obj = rl_mod_class(config=rl_mod_config)
            modules[pid] = rl_mod_obj

        return modules

    def _check_module_exists(self, module_id: str):
        if module_id not in self.modules:
            raise ValueError(
                f"Module with module_id {module_id} not found in ModuleDict"
            )
