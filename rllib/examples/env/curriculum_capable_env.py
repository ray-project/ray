import gym
from gym.spaces import Discrete, Box
import random

from ray.rllib.env.apis.task_settable_env import TaskSettableEnv
from ray.rllib.env.env_context import EnvContext


class CurriculumCapableEnv(TaskSettableEnv):
    """Example of a curriculum learning capable env.

    This simply wraps a FrozenLake-v0 env and makes it harder with each
    task. Task (difficulty levels) can range from 1 to 10."""

    # Defining the different maps (all same size) for the different
    # tasks. Theme here is to move the goal further and further away and
    # to add more holes along the way.
    MAPS = [["SFFFFF",
             "FFFFFF",
             "FFFFFF",
             "HFFFFG",
             "FFFFFF",
             "FFFFFF"],
            ["SFFFFF",
             "FFFFFF",
             "FFFFFF",
             "HHFFFF",
             "FFFFFG",
             "FFFFFF"],
            ["SFFFFF",
             "FFFFFF",
             "FFFFFF",
             "HHHFFF",
             "FFFFFF",
             "FFFFFG"],
            ["SFFFFF",
             "FFFFFF",
             "FFFFFF",
             "HHHHFF",
             "FFFFFF",
             "FFFFFG"],
            ["SFFFFF",
             "FFFHFF",
             "FFFFFF",
             "HHHHHF",
             "FFFFFF",
             "FFFFGF"],
            ]

    def __init__(self, config: EnvContext):
        self.cur_level = config.get("start_level", 1)
        self.frozen_lake = None
        self._make_lake()  # create self.frozen_lake
        self.observation_space = self.frozen_lake.observation_space
        self.action_space = self.frozen_lake.action_space
        self.switch_env = False

    def reset(self):
        if self.switch_env:
            self.switch_env = False
            self._make_lake()
        return self.frozen_lake.reset()

    def step(self, action):
        return self.frozen_lake.step(action)

    def sample_tasks(self, n_tasks):
        """Implement this to sample n random tasks."""
        return [random.randint(1, 10) for _ in range(n_tasks)]

    def get_task(self):
        """Implement this to get the current task (curriculum level)."""
        return self.cur_level

    def set_task(self, task):
        """Implement this to set the task (curriculum level) for this env."""
        self.cur_level = task
        self.switch_env = True

    def _make_lake(self):
        self.frozen_lake = gym.make(
            "FrozenLake-v0",
            desc=self.MAPS[self.cur_level - 1],
            is_slippery=False
        )
