"""PPO Learner that integrates with PyTorch Lightning modules.

This learner allows using a LightningModule's configure_optimizers() and gradient
clipping configuration within RLlib's PPO algorithm.
"""

from typing import TYPE_CHECKING

from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, ParamDict

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, nn = try_import_torch()


class LightningPPOLearner(PPOTorchLearner):
    """PPO Learner that uses PyTorch Lightning for optimizer configuration."""

    @override(PPOTorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: "AlgorithmConfig" = None
    ) -> None:
        """Configure optimizers for the given module."""
        module = self._module[module_id]
        pi_optimizer, vf_optimizer = module.lightning_module.configure_optimizers()
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="pi",
            optimizer=pi_optimizer,
            params=list(module.lightning_module.pi.parameters()),
        )
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="vf",
            optimizer=vf_optimizer,
            params=list(module.lightning_module.vf.parameters()),
        )

    @override(PPOTorchLearner)
    def postprocess_gradients_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig" = None,
        module_gradients_dict: ParamDict,
    ) -> ParamDict:
        """Apply gradient clipping using Lightning."""
        pi_optimizer = self.get_optimizer(module_id, optimizer_name="pi")
        self._module[module_id].lightning_module.clip_gradients(
            pi_optimizer, gradient_clip_val=0.5, gradient_clip_algorithm="norm"
        )

        vf_optimizer = self.get_optimizer(module_id, optimizer_name="vf")
        self._module[module_id].lightning_module.clip_gradients(
            vf_optimizer, gradient_clip_val=0.5, gradient_clip_algorithm="norm"
        )

        return module_gradients_dict
