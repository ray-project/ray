"""Example of setting up a hierarchical RL training using RLlib's multi-agent API.

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
from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.examples.envs.classes.six_room_env import (
    HierarchicalSixRoomEnv,
    SixRoomEnv,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=100.0,
    default_timesteps=1000000,
    default_iters=200,
)
parser.add_argument(
    "--flat",
    action="store_true",
    help="Use the non-hierarchical, single-agent flat `SixRoomEnv` instead.",
)
parser.add_argument(
    "--map",
    type=str,
    choices=["small", "medium", "large"],
    default="small",
    help="The built-in map to use.",
)
parser.add_argument(
    "--time-limit",
    type=int,
    default=100,
    help="The max. number of (primitive) timesteps per episode.",
)
parser.add_argument(
    "--max-steps-low-level",
    type=int,
    default=15,
    help="The max. number of steps a low-level policy can take after having been "
    "picked by the high level policy. After this number of timesteps, control is "
    "handed back to the high-level policy (to pick a next goal position plus the next "
    "low level policy).",
)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    # Run the flat (non-hierarchical env).
    if args.flat:
        cls = SixRoomEnv
    # Run in hierarchical mode.
    else:
        cls = HierarchicalSixRoomEnv

    tune.register_env("env", lambda cfg: cls(config=cfg))

    base_config = (
        PPOConfig()
        .environment(
            "env",
            env_config={
                "map": args.map,
                "max_steps_low_level": args.max_steps_low_level,
                "max_ts": args.time_limit,
            },
        )
        .env_runners(
            env_to_module_connector=(
                lambda env: FlattenObservations(multi_agent=not args.flat)
            ),
        )
        .training(
            num_epochs=10,
            entropy_coeff=0.01,
        )
    )

    # Configure a proper multi-agent setup for the hierarchical env.
    if not args.flat:

        def policy_mapping_fn(agent_id, episode, **kwargs):
            # Map each low level agent to its respective (low-level) policy.
            if agent_id.startswith("low_level_"):
                return f"low_level_policy_{agent_id[-1]}"
            # Map the high level agent to the high level policy.
            else:
                return "high_level_policy"

        base_config.multi_agent(
            policy_mapping_fn=policy_mapping_fn,
            policies={
                "high_level_policy",
                "low_level_policy_0",
                "low_level_policy_1",
                "low_level_policy_2",
            },
        )

    run_rllib_example_script_experiment(base_config, args)
