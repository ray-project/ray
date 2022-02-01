from typing import Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import TensorType


class UpperConfidenceBound(Exploration):
    @override(Exploration)
    def get_exploration_action(
        self,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        if self.framework == "torch":
            return self._get_torch_exploration_action(action_distribution, explore)
        else:
            raise NotImplementedError

    def _get_torch_exploration_action(self, action_dist, explore):
        if explore:
            return action_dist.inputs.argmax(dim=-1), None
        else:
            scores = self.model.value_function()
            return scores.argmax(dim=-1), None
