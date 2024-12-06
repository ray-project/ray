import logging
import random

import gymnasium as gym

from ray.rllib.env import MultiAgentEnv

logger = logging.getLogger(__name__)

# Agent has to traverse the maze from the starting position S -> F
# Observation space [x_pos, y_pos, wind_direction]
# Action space: stay still OR move in current wind direction
MAP_DATA = """
#########
#S      #
####### #
      # #
      # #
####### #
#F      #
#########"""


class WindyMazeEnv(gym.Env):
    def __init__(self, config=None):
        self.map = [m for m in MAP_DATA.split("\n") if m]
        self.x_dim = len(self.map)
        self.y_dim = len(self.map[0])
        logger.info(f"Loaded map {self.x_dim} {self.y_dim}")
        for x in range(self.x_dim):
            for y in range(self.y_dim):
                if self.map[x][y] == "S":
                    self.start_pos = (x, y)
                elif self.map[x][y] == "F":
                    self.end_pos = (x, y)
        logger.info(f"Start pos {self.start_pos} end pos {self.end_pos}")
        self.observation_space = gym.spaces.Tuple(
            [
                gym.spaces.Box(0, 100, shape=(2,)),  # (x, y)
                gym.spaces.Discrete(4),  # wind direction (N, E, S, W)
            ]
        )
        self.action_space = gym.spaces.Discrete(2)  # whether to move or not

    def reset(self, *, seed=None, options=None):
        self.wind_direction = random.choice([0, 1, 2, 3])
        self.pos = self.start_pos
        self.num_steps = 0
        return [[self.pos[0], self.pos[1]], self.wind_direction], {}

    def step(self, action):
        if action == 1:
            self.pos = self._get_new_pos(self.pos, self.wind_direction)
        self.num_steps += 1
        self.wind_direction = random.choice([0, 1, 2, 3])
        at_goal = self.pos == self.end_pos
        truncated = self.num_steps >= 200
        done = at_goal or truncated
        return (
            [[self.pos[0], self.pos[1]], self.wind_direction],
            100 * int(at_goal),
            done,
            truncated,
            {},
        )

    def _get_new_pos(self, pos, direction):
        if direction == 0:
            new_pos = (pos[0] - 1, pos[1])
        elif direction == 1:
            new_pos = (pos[0], pos[1] + 1)
        elif direction == 2:
            new_pos = (pos[0] + 1, pos[1])
        elif direction == 3:
            new_pos = (pos[0], pos[1] - 1)
        if (
            new_pos[0] >= 0
            and new_pos[0] < self.x_dim
            and new_pos[1] >= 0
            and new_pos[1] < self.y_dim
            and self.map[new_pos[0]][new_pos[1]] != "#"
        ):
            return new_pos
        else:
            return pos  # did not move


class HierarchicalWindyMazeEnv(MultiAgentEnv):
    def __init__(self, config=None):
        super().__init__()
        self.flat_env = WindyMazeEnv(config)

        obs = {
            f"low_level_{aid}": gym.spaces.Tuple(
                [
                    # Flat observation space.
                    self.flat_env.observation_space,
                    # Goal (move up/down/left/right).
                    gym.spaces.Discrete(4),
                ]
            )
            for aid in range(25)
        }
        obs.update(
            {
                "high_level_agent": self.flat_env.observation_space,
            }
        )
        self.observation_spaces = gym.spaces.Dict(obs)

        act = {
            # Flat action space.
            f"low_level_{aid}": self.flat_env.action_space
            for aid in range(25)
        }
        act.update(
            {
                # Goal.
                "high_level_agent": gym.spaces.Discrete(4),
            }
        )
        self.action_spaces = gym.spaces.Dict(act)
        self.agents = [f"low_level_{aid}" for aid in range(25)]
        self.agents.append("high_level_agent")
        self.possible_agents = self.agents.copy()

    def reset(self, *, seed=None, options=None):
        self.cur_obs, infos = self.flat_env.reset()
        self.current_goal = None
        self.steps_remaining_at_level = None
        self.num_high_level_steps = 0
        # current low level agent id. This must be unique for each high level
        # step since agent ids cannot be reused.
        self.low_level_agent_id = f"low_level_{self.num_high_level_steps}"
        return {
            "high_level_agent": self.cur_obs,
        }, {"high_level_agent": infos}

    def step(self, action_dict):
        assert len(action_dict) == 1, action_dict
        if "high_level_agent" in action_dict:
            return self._high_level_step(action_dict["high_level_agent"])
        else:
            return self._low_level_step(list(action_dict.values())[0])

    def _high_level_step(self, action):
        logger.debug("High level agent sets goal")
        self.current_goal = action
        self.steps_remaining_at_level = 25
        self.num_high_level_steps += 1
        self.low_level_agent_id = f"low_level_{self.num_high_level_steps}"
        obs = {self.low_level_agent_id: [self.cur_obs, self.current_goal]}
        rew = {self.low_level_agent_id: 0}
        done = truncated = {"__all__": False}
        return obs, rew, done, truncated, {}

    def _low_level_step(self, action):
        logger.debug(f"Low level agent step {action}")
        self.steps_remaining_at_level -= 1
        cur_pos = tuple(self.cur_obs[0])
        goal_pos = self.flat_env._get_new_pos(cur_pos, self.current_goal)

        # Step in the actual env
        f_obs, f_rew, f_terminated, f_truncated, info = self.flat_env.step(action)
        new_pos = tuple(f_obs[0])
        self.cur_obs = f_obs

        # Calculate low-level agent observation and reward
        obs = {self.low_level_agent_id: [f_obs, self.current_goal]}
        if new_pos != cur_pos:
            if new_pos == goal_pos:
                rew = {self.low_level_agent_id: 1}
            else:
                rew = {self.low_level_agent_id: -1}
        else:
            rew = {self.low_level_agent_id: 0}

        # Handle env termination & transitions back to higher level.
        terminated = {"__all__": False}
        truncated = {"__all__": False}
        if f_terminated or f_truncated:
            terminated["__all__"] = f_terminated
            truncated["__all__"] = f_truncated
            logger.debug(f"high level final reward {f_rew}")
            rew["high_level_agent"] = f_rew
            obs["high_level_agent"] = f_obs
        elif self.steps_remaining_at_level == 0:
            terminated[self.low_level_agent_id] = True
            truncated[self.low_level_agent_id] = False
            rew["high_level_agent"] = 0
            obs["high_level_agent"] = f_obs

        return obs, rew, terminated, truncated, {self.low_level_agent_id: info}
