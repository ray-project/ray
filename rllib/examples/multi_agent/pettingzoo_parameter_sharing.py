"""Runs the PettingZoo Waterworld multi-agent env in RLlib using single policy learning.

Other than the `pettingzoo_independent_learning.py` example (in this same folder),
this example simply trains a single policy (shared by all agents).

See: https://pettingzoo.farama.org/environments/sisl/waterworld/
for more details on the environment.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2`

Control the number of agents and policies (RLModules) via --num-agents and
--num-policies.

This works with hundreds of agents and policies, but note that initializing
many policies might take some time.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
The above options can reach a combined reward of roughly ~0.0 after about 500k-1M env
timesteps. Keep in mind, though, that in this setup, the agents do not have the
opportunity to benefit from or even out other agents' mistakes (and behavior in general)
as everyone is using the same policy. Hence, this example learns a more generic policy,
which might be less specialized to certain "niche exploitation opportunities" inside
the env:

+---------------------+----------+-----------------+--------+-----------------+
| Trial name          | status   | loc             |   iter |  total time (s) |
|---------------------+----------+-----------------+--------+-----------------+
| PPO_env_91f49_00000 | RUNNING  | 127.0.0.1:63676 |    200 |         605.176 |
+---------------------+----------+-----------------+--------+-----------------+

+--------+-------------------+-------------+
|     ts |   combined reward |   reward p0 |
+--------+-------------------+-------------|
| 800000 |          0.323752 |    0.161876 |
+--------+-------------------+-------------+
"""
from pettingzoo.sisl import waterworld_v4

from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env


parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=1000000,
    default_reward=0.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents > 0, "Must set --num-agents > 0 when running this script!"
    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    # Here, we use the "Agent Environment Cycle" (AEC) PettingZoo environment type.
    # For a "Parallel" environment example, see the rock paper scissors examples
    # in this same repository folder.
    register_env("env", lambda _: PettingZooEnv(waterworld_v4.env()))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env")
        .multi_agent(
            policies={"p0"},
            # All agents map to the exact same policy.
            policy_mapping_fn=(lambda aid, *args, **kwargs: "p0"),
        )
        .training(
            model={
                "vf_share_layers": True,
            },
            vf_loss_coeff=0.005,
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs={"p0": RLModuleSpec()},
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
