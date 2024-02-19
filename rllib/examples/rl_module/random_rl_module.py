import pathlib
from typing import Mapping, Any

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.core.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch as create_batched_struct


class RandomRLModule(RLModule):
    @override(RLModule)
    def _forward_inference(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    @override(RLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._random_forward(batch, **kwargs)

    @override(RLModule)
    def _forward_train(self, *args, **kwargs):
        # RandomRLModule should always be configured as non-trainable.
        # To do so, set in your config:
        # `config.multi_agent(policies_to_train=[list of ModuleIDs to be trained,
        # NOT including the ModuleID of this RLModule])`
        raise NotImplementedError("Random RLModule: Should not be trained!")

    @override(RLModule)
    def output_specs_inference(self):
        return [SampleBatch.ACTIONS]

    @override(RLModule)
    def output_specs_exploration(self):
        return [SampleBatch.ACTIONS]

    def _random_forward(self, batch, **kwargs):
        obs_batch_size = len(tree.flatten(batch[SampleBatch.OBS])[0])
        actions = create_batched_struct(
            [self.config.action_space.sample() for _ in range(obs_batch_size)]
        )
        return {SampleBatch.ACTIONS: actions}

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
