from typing import Mapping, Any
from ray.rllib.core.rl_module import RLModule
import gymnasium as gym


class RandomRLModule(RLModule):
    def __init__(self, action_space):
        self.action_space = action_space

    def _forward_fn(self, *args, **kwargs):
        pass

    def _forward_inference(self, *args, **kwargs):
        pass

    def _forward_exploration(self, *args, **kwargs):
        pass

    def _forward_train(self, *args, **kwargs):
        pass

    @classmethod
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config: Mapping[str, Any],
    ) -> "RLModule":
        return cls(action_space)
