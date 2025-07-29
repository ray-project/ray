"""Example of running a hierarchichal training setup in RLlib using its multi-agent API.

This example is very loosely based on this paper:
[1] Hierarchical RL Based on Subgoal Discovery and Subpolicy Specialization -
B. Bakker & J. Schmidhuber - 2003

The approach features one high level policy, which picks the next target state to be
reached by one of three low level policies as well as the actual low level policy to
take over control.
A low level policy - once chosen by the high level one - has up to 10 primitive
timesteps to reach the given target state. If it reaches it, both high level and low
level policy are rewarded and the high level policy takes another action (choses a new
target state and a new low level policy).
A global goal state must be reached to deem the overall task to be solved. Once one
of the lower level policies reaches that goal state, the high level policy receives
a large reward and the episode ends.
The approach utilizes the possibility for low level policies to specialize in reaching
certain sub-goals and the high level policy to know, which sub goals to pick next and
which "expert" (low level policy) to allow to reach the subgoal.

This example:
    - demonstrates how to write a relatively simple custom multi-agent environment and
    have it behave, such that it mimics a hierarchical RL setup with higher- and lower
    level agents acting on different abstract time axes (the higher level policy
    only acts occasionally, picking a new lower level policy and the lower level
    policies have each n primitive timesteps to reach the given target state, after
    which control is handed back to the high level policy for the next pick).
    - shows how to setup a plain multi-agent RL algo (here: PPO) to learn in this
    hierarchical setup and solve tasks that are otherwise very difficult to solve
    only with a single, primitive-action picking low level policy.

We use the `SixRoomEnv` and `HierarchicalSixRoomEnv`, both sharing the same built-in
maps. The envs are similar to the FrozenLake-v1 env, but support walls (inner and outer)
through which the agent cannot walk.


How to run this script
----------------------
`python [script file name].py --map=large --time-limit=50`

Use the `--flat` option to disable the hierarchical setup and learn the simple (flat)
SixRoomEnv with only one policy. You should observe that it's much harder for the algo
to reach the global goal state in this setting.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that only a PPO algorithm that uses hierarchical
training (`--flat` flag is NOT set) can actually learn with the command line options
`--map=large --time-limit=500 --max-steps-low-level=40 --num-low-level-agents=3`.

4 policies in a hierarchical setup (1 high level "manager", 3 low level "experts"):
+---------------------+----------+--------+------------------+
| Trial name          | status   |   iter |   total time (s) |
|                     |          |        |                  |
|---------------------+----------+--------+------------------+
| PPO_env_58b78_00000 | RUNNING  |    100 |           278.23 |
+---------------------+----------+--------+------------------+
+-------------------+--------------------------+---------------------------+ ...
|   combined return | return high_level_policy | return low_level_policy_0 |
|-------------------+--------------------------+---------------------------+ ...
|              -8.4 |                     -5.2 |                     -1.19 |
+-------------------+--------------------------+---------------------------+ ...
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
    default_reward=7.0,
    default_timesteps=4000000,
    default_iters=800,
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
    default="medium",
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
parser.add_argument(
    "--num-low-level-agents",
    type=int,
    default=3,
    help="The number of low-level agents/policies to use.",
)


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
                "time_limit": args.time_limit,
                "num_low_level_agents": args.num_low_level_agents,
            },
        )
        .env_runners(
            # num_envs_per_env_runner=10,
            env_to_module_connector=(
                lambda env, spaces, device: (
                    FlattenObservations(multi_agent=not args.flat)
                )
            ),
        )
        .training(
            train_batch_size_per_learner=4000,
            minibatch_size=512,
            lr=0.0003,
            num_epochs=20,
            entropy_coeff=0.025,
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
            policies={"high_level_policy"}
            | {f"low_level_policy_{i}" for i in range(args.num_low_level_agents)},
        )

    run_rllib_example_script_experiment(base_config, args)
