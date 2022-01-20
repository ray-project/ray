import gym
from typing import Callable, Optional, Type, Union

from ray.rllib.env.env_context import EnvContext
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EnvType


def make_recsim_env(
        env_name_or_creator: Union[str, Callable[[EnvContext], EnvType]],
) -> Type[gym.Env]:

    class RecSimEnv(gym.Env):
        def __init__(self, config: Optional[EnvContext] = None):
            MultiAgentEnv.__init__(self)
            config = config or {}

            num = config.pop("num_agents", 1)
            if isinstance(env_name_or_creator, str):
                self.agents = [
                    gym.make(env_name_or_creator) for _ in range(num)
                ]
            else:
                self.agents = [env_name_or_creator(config) for _ in range(num)]

            self.dones = set()
            self.observation_space = self.agents[0].observation_space
            self.action_space = self.agents[0].action_space
            self._agent_ids = set(range(num))

        @override(gym.Env)
        def reset(self):
            self.dones = set()
            return {i: a.reset() for i, a in enumerate(self.agents)}

        @override(gym.Env)
        def step(self, action_dict):
            obs, rew, done, info = {}, {}, {}, {}
            for i, action in action_dict.items():
                obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
                if done[i]:
                    self.dones.add(i)
            done["__all__"] = len(self.dones) == len(self.agents)
            return obs, rew, done, info

    return RecSimEnv
