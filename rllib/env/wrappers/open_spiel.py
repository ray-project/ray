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
        self.observation_space = Box(
            float("-inf"), float("inf"),
            (self.env.observation_tensor_size(), ))
        self.action_space = Discrete(self.env.num_distinct_actions())

    def reset(self):
        self.state = self.env.new_initial_state()
        return self._get_obs()

    def step(self, action):
        # Sequential game:
        if str(self.type.dynamics) == "Dynamics.SEQUENTIAL":
            curr_player = self.state.current_player()
            assert curr_player in action
            penalty = None
            try:
                self.state.apply_action(action[curr_player])
            # TODO: (sven) resolve this hack by publishing legal actions
            #  with each step.
            except pyspiel.SpielError:
                self.state.apply_action(
                    np.random.choice(self.state.legal_actions()))
                penalty = -0.1

            # Are we done?
            is_done = self.state.is_terminal()
            dones = dict({ag: is_done
                          for ag in range(self.num_agents)},
                         **{"__all__": is_done})

            # Compile rewards dict.
            rewards = {ag: r for ag, r in enumerate(self.state.returns())}
            if penalty:
                rewards[curr_player] += penalty
        # Simultaneous game.
        else:
            raise NotImplementedError

        return self._get_obs(), rewards, dones, {}

    def _get_obs(self):
        curr_player = self.state.current_player()
        # Sequential game:
        if str(self.type.dynamics) == "Dynamics.SEQUENTIAL":
            if self.state.is_terminal():
                return {}
            return {
                curr_player: np.reshape(self.state.observation_tensor(), [-1])
            }
        # Simultaneous game.
        else:
            raise NotImplementedError

    def render(self, mode=None) -> None:
        if mode == "human":
            print(self.state)
