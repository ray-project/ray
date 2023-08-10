from typing import Mapping, Any
from ray.rllib.core.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
import tree
import pathlib
import gymnasium as gym


class RandomRLModule(RLModule):
    def _random_forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[SampleBatch.OBS])[0])
        actions = [self.config.action_space.sample() for _ in range(obs_batch_size)]
        return {SampleBatch.ACTIONS: actions}

    def _forward_inference(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    def _forward_exploration(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    def _forward_train(self, *args, **kwargs):
        raise NotImplementedError("This RLM should only run in evaluation.")

    def output_specs_inference(self):
        return [SampleBatch.ACTIONS]

    def output_specs_exploration(self):
        return [SampleBatch.ACTIONS]

    def get_state(self) -> Mapping[str, Any]:
        return {}

    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        pass

    def _module_state_file_name(self) -> pathlib.Path:
        return pathlib.Path("random_rl_module_dummy_state")

    def save_state(self, path) -> None:
        pass

    def compile(self, *args, **kwargs):
        """Dummy method for compatibility with TorchRLModule.

        This is hit when RolloutWorker tries to compile TorchRLModule."""
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
