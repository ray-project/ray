import abc
from typing import Any, Dict, TYPE_CHECKING

from ray.rllib.utils.typing import ModuleID, TensorType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.learner.learner import Learner


@PublicAPI(stability="alpha")
class SelfSupervisedLossAPI(abc.ABC):
    """An API to be implemented by RLModules that bring their own self-supervised loss.

    Learners will call these model's `compute_self_supervised_loss()` method instead of
    the Learner's own `compute_loss_for_module()` method.
    The call signature is identical to the Learner's `compute_loss_for_module()` method
    except of an additional mandatory `learner` kwarg.
    """

    @abc.abstractmethod
    def compute_self_supervised_loss(
        self,
        *,
        learner: "Learner",
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Computes the loss for a single module.

        Think of this as computing loss for a single agent. For multi-agent use-cases
        that require more complicated computation for loss, consider overriding the
        `compute_losses` method instead.

        Args:
            learner: The Learner calling this loss method on the RLModule.
            module_id: The ID of the RLModule (within a MultiRLModule).
            config: The AlgorithmConfig specific to the given `module_id`.
            batch: The sample batch for this particular RLModule.
            fwd_out: The output of the forward pass for this particular RLModule.

        Returns:
            A single total loss tensor. If you have more than one optimizer on the
            provided `module_id` and would like to compute gradients separately using
            these different optimizers, simply add up the individual loss terms for
            each optimizer and return the sum. Also, for recording/logging any
            individual loss terms, you can use the `Learner.metrics.log_value(
            key=..., value=...)` or `Learner.metrics.log_dict()` APIs. See:
            :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger` for more
            information.
        """
