"""Example of running a multi-agent experiment w/ agents always acting simultaneously.

This example:
    - demonstrates how to write your own (multi-agent) environment using RLlib's
    MultiAgentEnv API.
    - shows how to implement the `reset()` and `step()` methods of the env such that
    the agents act simultaneously.
    - shows how to configure and setup this environment class within an RLlib
    Algorithm config.
    - runs the experiment with the configured algo, trying to solve the environment.


How to run this script
----------------------
`python [script file name].py --sheldon-cooper-mode`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see results similar to the following in your console output:

+-----------------------------------+----------+--------+------------------+-------+
| Trial name                        | status   |   iter |   total time (s) |    ts |
|-----------------------------------+----------+--------+------------------+-------+
| PPO_RockPaperScissors_8cef7_00000 | RUNNING  |      3 |          16.5348 | 12000 |
+-----------------------------------+----------+--------+------------------+-------+
+-------------------+------------------+------------------+
|   combined return |   return player2 |   return player1 |
|-------------------+------------------+------------------|
|                 0 |            -0.15 |             0.15 |
+-------------------+------------------+------------------+

Note that b/c we are playing a zero-sum game, the overall return remains 0.0 at
all times.
"""
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.examples.envs.classes.multi_agent.rock_paper_scissors import (
    RockPaperScissors,
)
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env  # noqa

parser = add_rllib_example_script_args(
    default_reward=0.9, default_iters=50, default_timesteps=100000
)
parser.set_defaults(
    num_agents=2,
)
parser.add_argument(
    "--sheldon-cooper-mode",
    action="store_true",
    help="Whether to add two more actions to the game: Lizard and Spock. "
    "Watch here for more details :) https://www.youtube.com/watch?v=x5Q6-wMx-K8",
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents == 2, "Must set --num-agents=2 when running this script!"

    # You can also register the env creator function explicitly with:
    # register_env("env", lambda cfg: RockPaperScissors({"sheldon_cooper_mode": False}))

    # Or you can hard code certain settings into the Env's constructor (`config`).
    # register_env(
    #    "rock-paper-scissors-w-sheldon-mode-activated",
    #    lambda config: RockPaperScissors({**config, **{"sheldon_cooper_mode": True}}),
    # )

    # Or allow the RLlib user to set more c'tor options via their algo config:
    # config.environment(env_config={[c'tor arg name]: [value]})
    # register_env("rock-paper-scissors", lambda cfg: RockPaperScissors(cfg))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            RockPaperScissors,
            env_config={"sheldon_cooper_mode": args.sheldon_cooper_mode},
        )
        .env_runners(
            env_to_module_connector=(
                lambda env, spaces, device: FlattenObservations(multi_agent=True)
            ),
        )
        .multi_agent(
            # Define two policies.
            policies={"player1", "player2"},
            # Map agent "player1" to policy "player1" and agent "player2" to policy
            # "player2".
            policy_mapping_fn=lambda agent_id, episode, **kw: agent_id,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
