from typing import Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import TensorType


class ThompsonSampling(Exploration):
    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        if self.framework == "torch":
            return self._get_torch_exploration_action(distribution_inputs,
                                                      explore, model)
        else:
            raise NotImplementedError

    def _get_torch_exploration_action(self, distribution_inputs, explore,
                                      model):
        if explore:
            return distribution_inputs.argmax(dim=1), None
        else:
            scores = model.predict(model.current_obs())
            return scores.argmax(dim=1), None


class UCB(Exploration):
    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        if self.framework == "torch":
            return self._get_torch_exploration_action(distribution_inputs,
                                                      explore, model)
        else:
            raise NotImplementedError

    def _get_torch_exploration_action(self, distribution_inputs, explore,
                                      model):
        if explore:
            return distribution_inputs.argmax(dim=1), None
        else:
            scores = model.value_function()
            return scores.argmax(dim=1), None
