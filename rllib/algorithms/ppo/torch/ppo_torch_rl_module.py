# Backward compat import.
from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (  # noqa
    DefaultPPOTorchRLModule as PPOTorchRLModule,
)
from ray.rllib.utils.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module.PPOTorchRLModule",
    new="ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module."
    "DefaultPPOTorchRLModule",
    error=False,
)
