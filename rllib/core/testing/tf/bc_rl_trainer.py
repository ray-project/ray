import tensorflow as tf
from typing import Any, Mapping

from ray.rllib.core.rl_trainer.tf.tf_rl_trainer import TfRLTrainer
from ray.rllib.policy.sample_batch import SampleBatch

from ray.rllib.core.testing.testing_trainer import BaseTestingTrainer
from ray.rllib.utils.typing import TensorType


class BCTfRLTrainer(TfRLTrainer, BaseTestingTrainer):
    def _compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> Mapping[str, Any]:

        action_dist = fwd_out["action_dist"]
        loss = -tf.math.reduce_mean(action_dist.log_prob(batch[SampleBatch.ACTIONS]))

        return {self.TOTAL_LOSS_KEY: loss}
