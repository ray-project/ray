from gym.spaces import Box, Discrete
import numpy as np
import pyspiel

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class OpenSpielEnv(MultiAgentEnv):

    def __init__(self, env):
        self.env = env

        # Agent IDs are ints, starting from 0.
        self.num_agents = self.env.num_players()
        # Store the open-spiel game type.
        self.type = self.env.get_type()
        # Stores the current open-spiel game state.
        self.state = None

        # Extract observation- and action spaces from game.
        self.observation_space = Box(float("-inf"), float("inf"),
                                     self.env.observation_tensor_shape())
        self.action_space = Discrete(self.env.num_distinct_actions())

    def reset(self):
        self.state = self.env.new_initial_state()
        return self._get_obs()

    def step(self, action):
        # Sequential game:
        if str(self.type.dynamics) == "Dynamics.SEQUENTIAL":
            curr_player = self.state.current_player()
            assert curr_player in action
            self.state.apply_action(action[curr_player])
            # Compile rewards dict.
            rewards = {curr_player: self.state.returns()[curr_player]}
            # Are we done?
            dones = {curr_player: self.state.is_terminal(),
                     "__all__": self.state.is_terminal()}
        # Simultaneous game.
        else:
            raise NotImplementedError

        return self._get_obs, rewards, dones, {}

    def _get_obs(self):
        # Sequential game:
        if str(self.type.dynamics) == "Dynamics.SEQUENTIAL":
            return {
                self.state.current_player(): np.array(self.state.observation_tensor())
            }
        # Simultaneous game.
        else:
            raise NotImplementedError

    def close(self):
        self.env.close()

    def seed(self, seed=None):
        self.env.seed(seed)

    def render(self, mode="human"):
        return self.env.render(mode)
