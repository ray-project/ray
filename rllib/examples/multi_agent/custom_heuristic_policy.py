"""Example of running a custom heuristic (hand-coded) policy alongside trainable ones.

This example has two RLModules (as action computing policies):
    (1) one trained by a PPOLearner
    (2) one hand-coded policy that acts at random in the env (doesn't learn).

The environment is MultiAgentCartPole, in which there are n agents both policies


How to run this script
----------------------
`python [script file name].py --num-agents=2`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see the PPO policy ("learnable_policy") does much
better than "random":

+-------------------+------------+----------+------+----------------+
| Trial name        | status     | loc      | iter | total time (s) |
|                   |            |          |      |                |
|-------------------+------------+----------+------+----------------+
| PPO_multi_agen... | TERMINATED | 127. ... |   20 |         58.646 |
+-------------------+------------+----------+------+----------------+

+--------+-------------------+-----------------+--------------------+
|     ts |   combined reward |   reward random |             reward |
|        |                   |                 |   learnable_policy |
+--------+-------------------+-----------------+--------------------|
|  80000 |            481.26 |           78.41 |             464.41 |
+--------+-------------------+-----------------+--------------------+
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_iters=40, default_reward=500.0, default_timesteps=200000
)
parser.set_defaults(
    num_agents=2,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents == 2, "Must set --num-agents=2 when running this script!"

    # Simple environment with n independent cartpole entities.
    register_env(
        "multi_agent_cartpole",
        lambda _: MultiAgentCartPole({"num_agents": args.num_agents}),
    )

    base_config = (
        PPOConfig()
        .environment("multi_agent_cartpole")
        .multi_agent(
            policies={"learnable_policy", "random"},
            # Map to either random behavior or PPO learning behavior based on
            # the agent's ID.
            policy_mapping_fn=lambda agent_id, *args, **kwargs: [
                "learnable_policy",
                "random",
            ][agent_id % 2],
            # We need to specify this here, b/c the `forward_train` method of
            # `RandomRLModule` (ModuleID="random") throws a not-implemented error.
            policies_to_train=["learnable_policy"],
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs={
                    "learnable_policy": RLModuleSpec(),
                    "random": RLModuleSpec(module_class=RandomRLModule),
                }
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
