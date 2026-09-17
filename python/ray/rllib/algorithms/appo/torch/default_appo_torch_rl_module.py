from ray.rllib.algorithms.appo.default_appo_rl_module import DefaultAPPORLModule
from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
    DefaultPPOTorchRLModule,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DefaultAPPOTorchRLModule(DefaultPPOTorchRLModule, DefaultAPPORLModule):
    pass
