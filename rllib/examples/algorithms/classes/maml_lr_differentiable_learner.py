from typing import Any, Dict, TYPE_CHECKING

from ray.rllib.core.learner.torch.torch_differentiable_learner import (
    TorchDifferentiableLearner,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, nn = try_import_torch()


class MAMLTorchDifferentiableLearner(TorchDifferentiableLearner):
    """A `TorchDifferentiableLearner` to perform MAML learning.

    This `TorchDifferentiableLearner`
    - defines a funcitonal MSE loss for learning simple (here non-linear)
    prediction.
    """

    @override(TorchDifferentiableLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Defines a simple MSE prediction loss for continuous task."""

        return nn.functional.mse_loss(fwd_out["y_pred"], batch["y"])
