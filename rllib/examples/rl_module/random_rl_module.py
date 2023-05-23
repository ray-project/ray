from typing import Mapping, Any
from ray.rllib.core.rl_module import RLModule
import pathlib
import gymnasium as gym


class RandomRLModule(RLModule):
    def _forward_inference(self, batch, **kwargs):
        # TODO (Kourosh): Implement this when we completely replace RandomPolicy.
        raise NotImplementedError

    def _forward_exploration(self, batch, **kwargs):
        # TODO (Kourosh): Implement this when we completely replace RandomPolicy.
        raise NotImplementedError

    def _forward_train(self, *args, **kwargs):
        # TODO (Kourosh): Implement this when we completely replace RandomPolicy.
        raise NotImplementedError

    def get_state(self) -> Mapping[str, Any]:
        return {}

    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        pass

    def _module_state_file_name(self) -> pathlib.Path:
        return pathlib.Path("random_rl_module_dummy_state")

    def save_state(self, path) -> None:
        pass

    @classmethod
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config_dict: Mapping[str, Any],
    ) -> "RLModule":
        return cls(action_space)
