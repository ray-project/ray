from typing import Any, Dict, List, TYPE_CHECKING

from ray.rllib.core.learner.torch.torch_meta_learner import TorchMetaLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

torch, nn = try_import_torch()


class MAMLTorchMetaLearner(TorchMetaLearner):
    """A `TorchMetaLearner` to perform MAML learning.

    This `TorchMetaLearner`
    - defines a MSE loss for learning simple (here non-linear) prediction.
    """

    @override(TorchMetaLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
        others_loss_per_module: List[Dict[ModuleID, TensorType]] = None,
    ) -> TensorType:
        """Defines a simple MSE prediction loss for continuous task.

        Note, MAML does not need the losses from the registered differentiable
        learners (contained in `others_loss_per_module`) b/c it computes a test
        loss on an unseen data batch.
        """
        # Use a simple MSE loss for the meta learning task.
        return torch.nn.functional.mse_loss(fwd_out["y_pred"], batch["y"])
