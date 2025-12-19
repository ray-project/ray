from typing import Any, Dict

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, TensorType

torch, _ = try_import_torch()


class PPOTorchLearnerWithWeightRegularizerLoss(PPOTorchLearner):
    """A custom PPO torch learner adding a weight regularizer term to the loss.

    We compute a naive regularizer term averaging over all parameters of the RLModule
    and add this mean value (multiplied by the regularizer coefficient) to the base PPO
    loss.
    The experiment shows that even with a large learning rate, our custom Learner is
    still able to learn properly as it's forced to keep the weights small.
    """

    @override(PPOTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        base_total_loss = super().compute_loss_for_module(
            module_id=module_id,
            config=config,
            batch=batch,
            fwd_out=fwd_out,
        )

        # Compute the mean of all the RLModule's weights.
        parameters = self.get_parameters(self.module[module_id])
        mean_weight = torch.mean(torch.stack([w.mean() for w in parameters]))

        self.metrics.log_value(
            key=(module_id, "mean_weight"),
            value=mean_weight,
            window=1,
        )

        total_loss = (
            base_total_loss
            + config.learner_config_dict["regularizer_coeff"] * mean_weight
        )

        return total_loss
