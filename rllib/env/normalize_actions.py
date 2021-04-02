import gym
from gym import spaces
import numpy as np
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class NormalizeActionWrapper(gym.ActionWrapper):
    """Rescale the action space of the environment."""

    def action(self, action):
        if not isinstance(self.env.action_space, spaces.Box):
            return action

        # rescale the action
        low, high = self.env.action_space.low, self.env.action_space.high
        scaled_action = low + (action + 1.0) * (high - low) / 2.0
        scaled_action = np.clip(scaled_action, low, high)

        return scaled_action

    def reverse_action(self, action):
        raise NotImplementedError


class NormalizeMultiAgentActionWrapper(MultiAgentEnv):
    """Rescale the action space of multi agent environment"""

    def __init__(self, env, config):
        self.env = env
        self.config = config
        self.action_space = self.env.action_space
        self.observation_space = self.env.observation_space
        self.action_space_map = None

    def step(self, action):
        return self.env.step(self.action(action))

    def reset(self):
        return self.env.reset()

    def action(self, action):
        if not isinstance(self.env.action_space, spaces.Box):
            return action

        if self.action_space_map is None:
            self.action_space_map = self.get_action_space_map(action)

        for agent_id in action.keys():
            action[agent_id] = self.rescale_action(
                action[agent_id], self.action_space_map[agent_id])

        return action

    def rescale_action(self, action, action_space):
        low, high = action_space.low, action_space.high
        scaled_action = low + (action + 1.0) * (high - low) / 2.0
        scaled_action = np.clip(scaled_action, low, high)
        return scaled_action

    def get_action_space_map(self, action):
        action_space_map = {}
        policies = self.config["multiagent"]["policies"]
        policy_mapping_fn = self.config["multiagent"]["policy_mapping_fn"]

        if len(policies) > 0 and policy_mapping_fn:
            for agent_id in action.keys():
                policy_id = policy_mapping_fn(agent_id)
                policy_config = policies[policy_id]
                action_space_map[agent_id] = policy_config[2]
        else:
            # When multiagent config is not given, use default action space
            for agent_id in action.keys():
                action_space_map[agent_id] = self.env.action_space
        return action_space_map
