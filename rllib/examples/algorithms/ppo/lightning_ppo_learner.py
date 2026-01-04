"""PPO Learner that integrates with PyTorch Lightning modules.

This can potentially be made a "LightningLearner(TorchLearner)" class.
"""

from typing import TYPE_CHECKING, Dict, Tuple

from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, nn = try_import_torch()


class LightningPPOLearner(PPOTorchLearner):
    """PPO Learner that uses PyTorch Lightning."""

    @override(TorchLearner)
    def _uncompiled_update(
        self,
        batch: Dict,
        **kwargs,
    ) -> Tuple[Dict, Dict, Dict]:
        """Custom update using Lightning module's training_step.

        This bypasses RLlib's standard update.

        Args:
            batch: Dictionary of batches by module_id

        Returns:
            Tuple of (fwd_out, loss_per_module, metrics)
        """
        # Track off-policy-ness (for metrics)
        self._compute_off_policyness(batch)

        loss_per_module = {}
        for module_id in self.module.keys():
            module_batch = batch[module_id]
            lightning_module = self._module[module_id].lightning_module
            total_loss = lightning_module.training_step(module_batch, batch_idx=-1)
            loss_per_module[module_id] = total_loss

        # Return empty fwd_out since we bypassed forward_train
        return {}, loss_per_module, {}

    @override(PPOTorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: "AlgorithmConfig" = None
    ) -> None:
        """Configure optimizers using the Lightning module's configure_optimizers().

        This method extracts the optimizers from the Lightning module and registers
        them with the Learner. The Lightning module returns separate optimizers for
        the policy network (encoder + policy head) and value function.

        Args:
            module_id: The ID of the module to configure optimizers for
            config: The algorithm configuration (unused, Lightning module manages LR)
        """
        module = self._module[module_id]
        module.lightning_module.configure_optimizers()
