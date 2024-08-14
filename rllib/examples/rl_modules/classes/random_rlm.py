from typing import Any, Dict

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch as batch_func


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
        actions = batch_func(
            [self.config.action_space.sample() for _ in range(obs_batch_size)]
        )
        return {SampleBatch.ACTIONS: actions}

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
        model_config_dict: Dict[str, Any],
    ) -> "RLModule":
        return cls(action_space)


class StatefulRandomRLModule(RandomRLModule):
    """A stateful RLModule that returns STATE_OUT from its forward methods.

    - Implements the `get_initial_state` method (returning a all-zeros dummy state).
    - Returns a dummy state under the `Columns.STATE_OUT` from its forward methods.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._internal_state_space = gym.spaces.Box(-1.0, 1.0, (1,))

    @override(RLModule)
    def get_initial_state(self):
        return {
            "state": np.zeros_like([self._internal_state_space.sample()]),
        }

    def _random_forward(self, batch, **kwargs):
        batch = super()._random_forward(batch, **kwargs)
        batch[Columns.STATE_OUT] = {
            "state": batch_func(
                [
                    self._internal_state_space.sample()
                    for _ in range(len(batch[Columns.ACTIONS]))
                ]
            ),
        }
        return batch
