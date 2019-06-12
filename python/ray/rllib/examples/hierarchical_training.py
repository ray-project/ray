"""Example of hierarchical training using the multi-agent API.

The example env is that of a "windy maze". The agent observes the current wind
direction and can either choose to stand still, or move in that direction.

You can try out the env directly with:

    $ python hierarchical_training.py --flat

A simple hierarchical formulation involves a high-level agent that issues goals
(i.e., go north / south / east / west), and a low-level agent that executes
these goals over a number of time-steps. This can be implemented as a
multi-agent environment with a top-level agent and low-level agents spawned
for each higher-level action. The lower level agent is rewarded for moving
in the right direction.

You can try this formulation with:

    $ python hierarchical_training.py  # gets ~100 rew after ~100k timesteps

Note that the hierarchical formulation actually converges slightly slower than
using --flat in this example.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import random
import gym
from gym.spaces import Box, Discrete, Tuple
import logging

import ray
from ray import tune
from ray.tune import function
from ray.rllib.env import MultiAgentEnv

parser = argparse.ArgumentParser()
parser.add_argument("--flat", action="store_true")

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

logger = logging.getLogger(__name__)


class WindyMazeEnv(gym.Env):
    def __init__(self, env_config):
        self.map = [m for m in MAP_DATA.split("\n") if m]
        self.x_dim = len(self.map)
        self.y_dim = len(self.map[0])
        logger.info("Loaded map {} {}".format(self.x_dim, self.y_dim))
        for x in range(self.x_dim):
            for y in range(self.y_dim):
                if self.map[x][y] == "S":
                    self.start_pos = (x, y)
                elif self.map[x][y] == "F":
                    self.end_pos = (x, y)
        logger.info("Start pos {} end pos {}".format(self.start_pos,
                                                     self.end_pos))
        self.observation_space = Tuple([
            Box(0, 100, shape=(2, )),  # (x, y)
            Discrete(4),  # wind direction (N, E, S, W)
        ])
        self.action_space = Discrete(2)  # whether to move or not

    def reset(self):
        self.wind_direction = random.choice([0, 1, 2, 3])
        self.pos = self.start_pos
        self.num_steps = 0
        return [[self.pos[0], self.pos[1]], self.wind_direction]

    def step(self, action):
        if action == 1:
            self.pos = self._get_new_pos(self.pos, self.wind_direction)
        self.num_steps += 1
        self.wind_direction = random.choice([0, 1, 2, 3])
        at_goal = self.pos == self.end_pos
        done = at_goal or self.num_steps >= 200
        return ([[self.pos[0], self.pos[1]], self.wind_direction],
                100 * int(at_goal), done, {})

    def _get_new_pos(self, pos, direction):
        if direction == 0:
            new_pos = (pos[0] - 1, pos[1])
        elif direction == 1:
            new_pos = (pos[0], pos[1] + 1)
        elif direction == 2:
            new_pos = (pos[0] + 1, pos[1])
        elif direction == 3:
            new_pos = (pos[0], pos[1] - 1)
        if (new_pos[0] >= 0 and new_pos[0] < self.x_dim and new_pos[1] >= 0
                and new_pos[1] < self.y_dim
                and self.map[new_pos[0]][new_pos[1]] != "#"):
            return new_pos
        else:
            return pos  # did not move


class HierarchicalWindyMazeEnv(MultiAgentEnv):
    def __init__(self, env_config):
        self.flat_env = WindyMazeEnv(env_config)

    def reset(self):
        self.cur_obs = self.flat_env.reset()
        self.current_goal = None
        self.steps_remaining_at_level = None
        self.num_high_level_steps = 0
        # current low level agent id. This must be unique for each high level
        # step since agent ids cannot be reused.
        self.low_level_agent_id = "low_level_{}".format(
            self.num_high_level_steps)
        return {
            "high_level_agent": self.cur_obs,
        }

    def step(self, action_dict):
        assert len(action_dict) == 1, action_dict
        if "high_level_agent" in action_dict:
            return self._high_level_step(action_dict["high_level_agent"])
        else:
            return self._low_level_step(list(action_dict.values())[0])

    def _high_level_step(self, action):
        logger.debug("High level agent sets goal".format(action))
        self.current_goal = action
        self.steps_remaining_at_level = 25
        self.num_high_level_steps += 1
        self.low_level_agent_id = "low_level_{}".format(
            self.num_high_level_steps)
        obs = {self.low_level_agent_id: [self.cur_obs, self.current_goal]}
        rew = {self.low_level_agent_id: 0}
        done = {"__all__": False}
        return obs, rew, done, {}

    def _low_level_step(self, action):
        logger.debug("Low level agent step {}".format(action))
        self.steps_remaining_at_level -= 1
        cur_pos = tuple(self.cur_obs[0])
        goal_pos = self.flat_env._get_new_pos(cur_pos, self.current_goal)

        # Step in the actual env
        f_obs, f_rew, f_done, _ = self.flat_env.step(action)
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

        # Handle env termination & transitions back to higher level
        done = {"__all__": False}
        if f_done:
            done["__all__"] = True
            logger.debug("high level final reward {}".format(f_rew))
            rew["high_level_agent"] = f_rew
            obs["high_level_agent"] = f_obs
        elif self.steps_remaining_at_level == 0:
            done[self.low_level_agent_id] = True
            rew["high_level_agent"] = 0
            obs["high_level_agent"] = f_obs

        return obs, rew, done, {}


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()
    if args.flat:
        tune.run(
            "PPO",
            config={
                "env": WindyMazeEnv,
                "num_workers": 0,
            },
        )
    else:
        maze = WindyMazeEnv(None)

        def policy_mapping_fn(agent_id):
            if agent_id.startswith("low_level_"):
                return "low_level_policy"
            else:
                return "high_level_policy"

        tune.run(
            "PPO",
            config={
                "env": HierarchicalWindyMazeEnv,
                "num_workers": 0,
                "log_level": "INFO",
                "entropy_coeff": 0.01,
                "multiagent": {
                    "policies": {
                        "high_level_policy": (None, maze.observation_space,
                                              Discrete(4), {
                                                  "gamma": 0.9
                                              }),
                        "low_level_policy": (None,
                                             Tuple([
                                                 maze.observation_space,
                                                 Discrete(4)
                                             ]), maze.action_space, {
                                                 "gamma": 0.0
                                             }),
                    },
                    "policy_mapping_fn": function(policy_mapping_fn),
                },
            },
        )
