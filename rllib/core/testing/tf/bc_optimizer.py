import tensorflow as tf
from typing import Any, Mapping

from ray.rllib.core.optim.rl_optimizer import RLOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.nested_dict import NestedDict


class BCTFOptimizer(RLOptimizer):
    def __init__(self, module, config):
        super().__init__(module, config)

    def compute_loss(
        self, batch: NestedDict[tf.Tensor], fwd_out: Mapping[str, Any]
    ) -> tf.Tensor:
        """Compute a loss"""
        action_dist = fwd_out["action_dist"]
        loss = -tf.math.reduce_mean(action_dist.log_prob(batch[SampleBatch.ACTIONS]))
        return loss

    def get_state(self):
        return {
            key: optim.get_weights() for key, optim in self.get_optimizers().items()
        }

    def set_state(self, state: Mapping[str, Any]) -> None:
        assert set(state.keys()) == set(self.get_state().keys()) or not state
        for key, optim_dict in state.items():
            self.get_optimizers()[key].set_weights(optim_dict)

    def _configure_optimizers(self) -> Mapping[str, Any]:
        return {
            "policy": tf.keras.optimizers.Adam(
                learning_rate=self._config.get("lr", 1e-3)
            )
        }
