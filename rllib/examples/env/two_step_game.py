from gym.spaces import Dict, Discrete, MultiDiscrete, Tuple
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv, ENV_STATE


class TwoStepGame(MultiAgentEnv):
    action_space = Discrete(2)

    def __init__(self, env_config):
        super().__init__()
        self.action_space = Discrete(2)
        self.state = None
        self.agent_1 = 0
        self.agent_2 = 1
        # MADDPG emits action logits instead of actual discrete actions
        self.actions_are_logits = env_config.get("actions_are_logits", False)
        self.one_hot_state_encoding = env_config.get("one_hot_state_encoding",
                                                     False)
        self.with_state = env_config.get("separate_state_space", False)

        if not self.one_hot_state_encoding:
            self.observation_space = Discrete(6)
            self.with_state = False
        else:
            # Each agent gets the full state (one-hot encoding of which of the
            # three states are active) as input with the receiving agent's
            # ID (1 or 2) concatenated onto the end.
            if self.with_state:
                self.observation_space = Dict({
                    "obs": MultiDiscrete([2, 2, 2, 3]),
                    ENV_STATE: MultiDiscrete([2, 2, 2])
                })
            else:
                self.observation_space = MultiDiscrete([2, 2, 2, 3])

    def seed(self, seed=None):
        if seed:
            np.random.seed(seed)

    def reset(self):
        self.state = np.array([1, 0, 0])
        return self._obs()

    def step(self, action_dict):
        if self.actions_are_logits:
            action_dict = {
                k: np.random.choice([0, 1], p=v)
                for k, v in action_dict.items()
            }

        state_index = np.flatnonzero(self.state)
        if state_index == 0:
            action = action_dict[self.agent_1]
            assert action in [0, 1], action
            if action == 0:
                self.state = np.array([0, 1, 0])
            else:
                self.state = np.array([0, 0, 1])
            global_rew = 0
            done = False
        elif state_index == 1:
            global_rew = 7
            done = True
        else:
            if action_dict[self.agent_1] == 0 and action_dict[self.
                                                              agent_2] == 0:
                global_rew = 0
            elif action_dict[self.agent_1] == 1 and action_dict[self.
                                                                agent_2] == 1:
                global_rew = 8
            else:
                global_rew = 1
            done = True

        rewards = {
            self.agent_1: global_rew / 2.0,
            self.agent_2: global_rew / 2.0
        }
        obs = self._obs()
        dones = {"__all__": done}
        infos = {}
        return obs, rewards, dones, infos

    def _obs(self):
        if self.with_state:
            return {
                self.agent_1: {
                    "obs": self.agent_1_obs(),
                    ENV_STATE: self.state
                },
                self.agent_2: {
                    "obs": self.agent_2_obs(),
                    ENV_STATE: self.state
                }
            }
        else:
            return {
                self.agent_1: self.agent_1_obs(),
                self.agent_2: self.agent_2_obs()
            }

    def agent_1_obs(self):
        if self.one_hot_state_encoding:
            return np.concatenate([self.state, [1]])
        else:
            return np.flatnonzero(self.state)[0]

    def agent_2_obs(self):
        if self.one_hot_state_encoding:
            return np.concatenate([self.state, [2]])
        else:
            return np.flatnonzero(self.state)[0] + 3


class TwoStepGameWithGroupedAgents(MultiAgentEnv):
    def __init__(self, env_config):
        super().__init__()
        env = TwoStepGame(env_config)
        tuple_obs_space = Tuple([env.observation_space, env.observation_space])
        tuple_act_space = Tuple([env.action_space, env.action_space])

        self.env = env.with_agent_groups(
            groups={"agents": [0, 1]},
            obs_space=tuple_obs_space,
            act_space=tuple_act_space)
        self.observation_space = self.env.observation_space
        self.action_space = self.env.action_space

    def reset(self):
        return self.env.reset()

    def step(self, actions):
        return self.env.step(actions)
