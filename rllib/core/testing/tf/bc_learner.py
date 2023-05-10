import tensorflow as tf
from typing import Any, Mapping

from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.policy.sample_batch import SampleBatch

from ray.rllib.core.testing.testing_learner import BaseTestingLearner
from ray.rllib.utils.typing import TensorType


class BCTfLearner(TfLearner, BaseTestingLearner):
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> Mapping[str, Any]:

        action_dist_inputs = fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        action_dist_class = self._module[module_id].get_train_action_dist_cls()
        action_dist = action_dist_class.from_logits(action_dist_inputs)
        loss = -tf.math.reduce_mean(action_dist.logp(batch[SampleBatch.ACTIONS]))

        return {self.TOTAL_LOSS_KEY: loss}
