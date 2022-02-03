import gym
import random

from ray.rllib.env.apis.task_settable_env import TaskSettableEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.utils.annotations import override


class CurriculumCapableEnv(TaskSettableEnv):
    """Example of a curriculum learning capable env.

    This simply wraps a FrozenLake-v1 env and makes it harder with each
    task. Task (difficulty levels) can range from 1 to 10."""

    # Defining the different maps (all same size) for the different
    # tasks. Theme here is to move the goal further and further away and
    # to add more and more holes along the way.
    MAPS = [
        ["SFFFFFF", "FFFFFFF", "FFFFFFF", "HHFFFFG", "FFFFFFF", "FFFFFFF"],
        ["SFFFFFF", "FFFHFFF", "FFFFFFF", "HHHFFFF", "FFFFFFG", "FFFFFFF"],
        ["SFFFFFF", "FFHHFFF", "FFFFFFF", "HHHHFFF", "FFFFFFF", "FFFFFFG"],
        ["SFFFFFF", "FHHHFFF", "FFFFFFF", "HHHHHFF", "FFFFFFF", "FFFFFGF"],
        ["SFFFFFF", "FFFHHFF", "FHFFFFF", "HHHHHHF", "FFHFFHF", "FFFGFFF"],
    ]

    def __init__(self, config: EnvContext):
        self.cur_level = config.get("start_level", 1)
        self.max_timesteps = config.get("max_timesteps", 18)
        self.frozen_lake = None
        self._make_lake()  # create self.frozen_lake
        self.observation_space = self.frozen_lake.observation_space
        self.action_space = self.frozen_lake.action_space
        self.switch_env = False
        self._timesteps = 0

    def reset(self):
        if self.switch_env:
            self.switch_env = False
            self._make_lake()
        self._timesteps = 0
        return self.frozen_lake.reset()

    def step(self, action):
        self._timesteps += 1
        s, r, d, i = self.frozen_lake.step(action)
        # Make rewards scale with the level exponentially:
        # Level 1: x1
        # Level 2: x10
        # Level 3: x100, etc..
        r *= 10 ** (self.cur_level - 1)
        if self._timesteps >= self.max_timesteps:
            d = True
        return s, r, d, i

    @override(TaskSettableEnv)
    def sample_tasks(self, n_tasks):
        """Implement this to sample n random tasks."""
        return [random.randint(1, 10) for _ in range(n_tasks)]

    @override(TaskSettableEnv)
    def get_task(self):
        """Implement this to get the current task (curriculum level)."""
        return self.cur_level

    @override(TaskSettableEnv)
    def set_task(self, task):
        """Implement this to set the task (curriculum level) for this env."""
        self.cur_level = task
        self.switch_env = True

    def _make_lake(self):
        self.frozen_lake = gym.make(
            "FrozenLake-v1", desc=self.MAPS[self.cur_level - 1], is_slippery=False
        )
