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

import argparse
from gym.spaces import Discrete, Tuple
import logging

import ray
from ray import tune
from ray.rllib.examples.env.windy_maze_env import WindyMazeEnv, \
    HierarchicalWindyMazeEnv
from ray.tune import function

parser = argparse.ArgumentParser()
parser.add_argument("--flat", action="store_true")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-reward", type=float, default=0.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    if args.flat:
        results = tune.run(
            "PPO",
            stop=stop,
            config={
                "env": WindyMazeEnv,
                "num_workers": 0,
                "use_pytorch": args.torch,
            },
        )
    else:
        maze = WindyMazeEnv(None)

        def policy_mapping_fn(agent_id):
            if agent_id.startswith("low_level_"):
                return "low_level_policy"
            else:
                return "high_level_policy"

        config = {
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
            "use_pytorch": args.torch,
        }

        results = tune.run("PPO", stop=stop, config=config)

    # Error if stop-reward not reached.
    if results.trials[0].last_result["episode_reward_mean"] < \
            args.stop_reward:
        raise ValueError("`stop-reward` of {} not reached!".format(
            args.stop_reward))
    print("ok")
