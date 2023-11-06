from ray.rllib.algorithms.bc.bc_rl_module import BCRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule


class BCTorchRLModule(TorchRLModule, BCRLModule):
    pass
