import logging
import uuid

from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class MemoryLeakingEnv(RandomEnv):
    """An env that leaks very little memory.

    Useful for proving that our memory-leak tests can catch the
    slightest leaks.
    """

    def __init__(self, config=None):
        super().__init__(config)
        self._leak = {}
        self._steps_after_reset = 0

    @override(RandomEnv)
    def reset(self):
        self._steps_after_reset = 0
        return super().reset()

    @override(RandomEnv)
    def step(self, action):
        self._steps_after_reset += 1

        # Only leak once an episode.
        if self._steps_after_reset == 2:
            self._leak[uuid.uuid4().hex.upper()] = 1

        return super().step(action)
