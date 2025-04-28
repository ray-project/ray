from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import (
    TensorType,
    try_import_tf,
)

tf1, tf, tfv = try_import_tf()


@OldAPIStack
class ThompsonSampling(Exploration):
    @override(Exploration)
    def get_exploration_action(
        self,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        if self.framework == "torch":
            return self._get_torch_exploration_action(action_distribution, explore)
        elif self.framework == "tf2":
            return self._get_tf_exploration_action(action_distribution, explore)
        else:
            raise NotImplementedError

    def _get_torch_exploration_action(self, action_dist, explore):
        if explore:
            return action_dist.inputs.argmax(dim=-1), None
        else:
            scores = self.model.predict(self.model.current_obs())
            return scores.argmax(dim=-1), None

    def _get_tf_exploration_action(self, action_dist, explore):
        action = tf.argmax(
            tf.cond(
                pred=explore,
                true_fn=lambda: action_dist.inputs,
                false_fn=lambda: self.model.predict(self.model.current_obs()),
            ),
            axis=-1,
        )
        return action, None
