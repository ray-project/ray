import torch
from typing import Any, Dict, TYPE_CHECKING

import numpy as np

from ray.rllib.connectors.learner import ComputeReturnsToGo
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import ModuleID, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class VPGTorchLearner(TorchLearner):
    @override(TorchLearner)
    def build(self) -> None:
        super().build()

        # Prepend the returns-to-go connector piece to have that information
        # available in the train batch.
        if self.config.add_default_connectors_to_learner_pipeline:
            self._learner_connector.prepend(ComputeReturnsToGo(gamma=self.config.gamma))

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        rl_module = self.module[module_id]
        action_dist_inputs = fwd_out[Columns.ACTION_DIST_INPUTS]
        action_dist_class = rl_module.get_train_action_dist_cls()
        action_dist = action_dist_class.from_logits(action_dist_inputs)

        # Compute log probabilities of the actions taken
        log_probs = action_dist.logp(batch[Columns.ACTIONS])

        # Compute the policy gradient loss
        # Since we're not using a baseline, we use returns directly
        loss = -torch.mean(log_probs * batch[Columns.RETURNS_TO_GO])

        return loss

    @override(Learner)
    def after_gradient_based_update(self, *, timesteps):
        # This is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        if self.config.report_mean_weights:
            for module_id in self.module.keys():
                parameters = convert_to_numpy(
                    self.get_parameters(self.module[module_id])
                )
                mean_ws = np.mean([w.mean() for w in parameters])
                self.metrics.log_value((module_id, "mean_weight"), mean_ws, window=1)
