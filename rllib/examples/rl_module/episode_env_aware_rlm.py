from typing import Mapping

import numpy as np
from gymnasium.spaces import Box

from ray.rllib.examples.rl_module.random_rl_module import RandomRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.typing import ViewRequirementsDict


class StatefulRandomRLModule(RandomRLModule):
    """An RLModule that always knows the current EpisodeID and EnvID and
    returns these in its actions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_space = Box(-1.0, 1.0, (1,))

    def is_recurrent(self):
        return True

    def get_initial_state(self):
        return np.zeros_like([self.state_space.sample()])

    def _random_forward(self, batch, **kwargs):
        batch = super()._random_forward(batch, **kwargs)
        batch[SampleBatch.ACTIONS] = np.array([x for x in batch[SampleBatch.ACTIONS]])
        batch[STATE_OUT] = np.array(
            [[self.state_space.sample()] for a in batch[SampleBatch.ACTIONS]]
        )
        return batch
