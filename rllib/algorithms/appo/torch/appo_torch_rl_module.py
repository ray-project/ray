from ray.rllib.algorithms.appo.appo_rl_module import APPORLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule


class APPOTorchRLModule(PPOTorchRLModule, APPORLModule):
    pass
