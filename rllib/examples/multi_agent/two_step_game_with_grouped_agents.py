"""The two-step game from the QMIX paper:
https://arxiv.org/pdf/1803.11485.pdf

See also: rllib/examples/centralized_critic.py for centralized critic PPO on this game.

How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2`

Note that in this script, we use an multi-agent environment in which both
agents that normally play this game have been merged into one agent with ID
"agents" and observation- and action-spaces being 2-tupled (1 item for each
agent). The "agents" agent is mapped to the policy with ID "p0".

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
Which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should expect a reward of 8.0 (the max to reach in thie game) eventually
being achieved by a simple PPO policy (no tuning, just using RLlib's default settings):

+---------------------------------+------------+-----------------+--------+
| Trial name                      | status     | loc             |   iter |
|---------------------------------+------------+-----------------+--------+
| PPO_grouped_twostep_4354b_00000 | TERMINATED | 127.0.0.1:42602 |     20 |
+---------------------------------+------------+-----------------+--------+

+------------------+-------+-------------------+-------------+
|   total time (s) |    ts |   combined reward |   reward p0 |
+------------------+-------+-------------------+-------------|
|          87.5756 | 80000 |                 8 |           8 |
+------------------+-------+-------------------+-------------+
"""

from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.envs.classes.two_step_game import TwoStepGameWithGroupedAgents
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env, get_trainable_cls


parser = add_rllib_example_script_args(default_reward=7.0)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents == 2, "Must set --num-agents=2 when running this script!"
    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    register_env(
        "grouped_twostep",
        lambda config: TwoStepGameWithGroupedAgents(config),
    )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("grouped_twostep")
        .env_runners(
            env_to_module_connector=lambda env: FlattenObservations(multi_agent=True),
        )
        .multi_agent(
            policies={"p0"},
            policy_mapping_fn=lambda aid, *a, **kw: "p0",
        )
        .rl_module(
            rl_module_spec=MultiAgentRLModuleSpec(
                module_specs={
                    "p0": SingleAgentRLModuleSpec(),
                },
            )
        )
    )

    run_rllib_example_script_experiment(base_config, args)
