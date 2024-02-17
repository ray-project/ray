import pathlib
from typing import Mapping, Any

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.core.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.spaces.space_utils import batch as create_batched_struct


class RandomRLModule(RLModule):
    def _random_forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[SampleBatch.OBS])[0])
        actions = create_batched_struct(
            [self.config.action_space.sample() for _ in range(obs_batch_size)]
        )
        return {SampleBatch.ACTIONS: actions}

    def _forward_inference(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    def _forward_exploration(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    def _forward_train(self, *args, **kwargs):
        raise NotImplementedError("Random RLModule: Should not be trained!")

    #def output_specs_inference(self):
    #    return [SampleBatch.ACTIONS]

    #def output_specs_exploration(self):
    #    return [SampleBatch.ACTIONS]

    def _module_state_file_name(self) -> pathlib.Path:
        return pathlib.Path("random_rl_module_dummy_state")

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
