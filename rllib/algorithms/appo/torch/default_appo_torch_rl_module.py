from ray.rllib.algorithms.appo.default_appo_rl_module import DefaultAPPORLModule
from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
    DefaultPPOTorchRLModule,
)


class DefaultAPPOTorchRLModule(DefaultPPOTorchRLModule, DefaultAPPORLModule):
    pass
