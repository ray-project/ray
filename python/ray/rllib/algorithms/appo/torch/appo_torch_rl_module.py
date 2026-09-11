# Backward compat import.
from ray.rllib.algorithms.appo.torch.default_appo_torch_rl_module import (  # noqa
    DefaultAPPOTorchRLModule as APPOTorchRLModule,
)
from ray._common.deprecation import deprecation_warning


deprecation_warning(
    old="ray.rllib.algorithms.appo.torch.appo_torch_rl_module.APPOTorchRLModule",
    new="ray.rllib.algorithms.appo.torch.default_appo_torch_rl_module."
    "DefaultAPPOTorchRLModule",
    error=False,
)
