"""Example of running a multi-agent experiment w/ agents taking turns (sequence).

This example:
    - demonstrates how to write your own (multi-agent) environment using RLlib's
    MultiAgentEnv API.
    - shows how to implement the `reset()` and `step()` methods of the env such that
    the agents act in a fixed sequence (taking turns).
    - shows how to configure and setup this environment class within an RLlib
    Algorithm config.
    - runs the experiment with the configured algo, trying to solve the environment.


How to run this script
----------------------
`python [script file name].py`

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
+---------------------------+----------+--------+------------------+--------+
| Trial name                | status   |   iter |   total time (s) |     ts |
|---------------------------+----------+--------+------------------+--------+
| PPO_TicTacToe_957aa_00000 | RUNNING  |     25 |          96.7452 | 100000 |
+---------------------------+----------+--------+------------------+--------+
+-------------------+------------------+------------------+
|   combined return |   return player2 |   return player1 |
|-------------------+------------------+------------------|
|                -2 |             1.15 |            -0.85 |
+-------------------+------------------+------------------+

Note that even though we are playing a zero-sum game, the overall return should start
at some negative values due to the misplacement penalty of our (simplified) TicTacToe
game.
"""
from ray.rllib.examples.envs.classes.multi_agent.tic_tac_toe import TicTacToe
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env  # noqa


parser = add_rllib_example_script_args(
    default_reward=-4.0, default_iters=50, default_timesteps=100000
)
parser.set_defaults(
    num_agents=2,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents == 2, "Must set --num-agents=2 when running this script!"

    # You can also register the env creator function explicitly with:
    # register_env("tic_tac_toe", lambda cfg: TicTacToe())

    # Or allow the RLlib user to set more c'tor options via their algo config:
    # config.environment(env_config={[c'tor arg name]: [value]})
    # register_env("tic_tac_toe", lambda cfg: TicTacToe(cfg))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(TicTacToe)
        .multi_agent(
            # Define two policies.
            policies={"player1", "player2"},
            # Map agent "player1" to policy "player1" and agent "player2" to policy
            # "player2".
            policy_mapping_fn=lambda agent_id, episode, **kw: agent_id,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
