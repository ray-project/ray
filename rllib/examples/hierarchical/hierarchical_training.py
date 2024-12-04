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

import logging

import gymnasium as gym

from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.examples.envs.classes.six_room_env import SixRoomEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

logger = logging.getLogger(__name__)

parser = add_rllib_example_script_args(
    default_reward=0.0,
    default_timesteps=100000,
    default_iters=200,
)
parser.add_argument("--flat", action="store_true")
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    # Run the flat (non-hierarchical env).
    if args.flat:
        tune.register_env("env", lambda cfg: SixRoomEnv({"flat": True}))
        base_config = (
            PPOConfig()
            .environment(SixRoomEnv)
            # .env_runners(num_env_runners=0)
        )
    # Run in hierarchical mode.
    else:
        maze = SixRoomEnv()

        def policy_mapping_fn(agent_id, episode, **kwargs):
            if agent_id.startswith("lower_level_"):
                return "low_level_policy"
            else:
                return "high_level_policy"

        base_config = (
            PPOConfig()
            .environment(HierarchicalWindyMazeEnv)
            .env_runners(
                env_to_module_connector=lambda env: FlattenObservations(multi_agent=True),
                # num_env_runners=0,
            )
            .training(entropy_coeff=0.01)
            .multi_agent(
                policy_mapping_fn=policy_mapping_fn,
                policies={"high_level_policy", "low_level_policy"},
                algorithm_config_overrides_per_module={
                    "high_level_policy": PPOConfig.overrides(gamma=0.9),
                    "low_level_policy": PPOConfig.overrides(gamma=0.0),
                },
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(rl_module_specs={
                    "high_level_policy": RLModuleSpec(
                        observation_space=maze.observation_space,
                        action_space=gym.spaces.Discrete(4),
                    ),
                    "low_level_policy": RLModuleSpec(
                        observation_space=gym.spaces.Tuple(
                            [maze.observation_space, gym.spaces.Discrete(4)]
                        ),
                        action_space=maze.action_space,
                    ),
                }),
            )
        )

    run_rllib_example_script_experiment(base_config, args)
