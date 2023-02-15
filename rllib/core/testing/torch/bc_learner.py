import torch
from typing import Any, Mapping

from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.testing.testing_learner import BaseTestingLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import TensorType


class BCTorchLearner(TorchLearner, BaseTestingLearner):
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> Mapping[str, Any]:

        action_dist = fwd_out["action_dist"]
        loss = -torch.mean(action_dist.log_prob(batch[SampleBatch.ACTIONS]))
        return {self.TOTAL_LOSS_KEY: loss}
