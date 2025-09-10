"""Runs the PettingZoo Waterworld env in RLlib using a shared critic.

See: https://pettingzoo.farama.org/environments/sisl/waterworld/
for more details on the environment.


How to run this script
----------------------
`python [script file name].py --num-agents=2`

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
The above options will typically reach a combined reward of 0 or more before 500k env timesteps. Keep in mind, though, that due to the separate learned policies in general, one agent's gain (in per-agent reward) might cause the other agent's reward to decrease at the same time. However, over time, both agents should simply improve, with the shared critic stabilizing this process significantly.

+-----------------------+------------+--------------------+--------+------------------+--------+-------------------+--------------------+--------------------+
| Trial name            | status     | loc                |   iter |   total time (s) |     ts |   combined return |   return pursuer_0 |   return pursuer_1 |
|-----------------------+------------+--------------------+--------+------------------+--------+-------------------+--------------------+--------------------|
| MAPPO_env_39b0c_00000 | TERMINATED | 172.29.87.208:9993 |    148 |          2690.21 | 592000 |           2.06999 |            38.2254 |           -36.1554 |
+-----------------------+------------+--------------------+--------+------------------+--------+-------------------+--------------------+--------------------+

+--------+-------------------+--------------------+--------------------+
|     ts |   combined return |   return pursuer_0 |   return pursuer_1 |
+--------+-------------------+--------------------+--------------------|
| 592000 |           2.06999 |            38.2254 |           -36.1554 |
+--------+-------------------+--------------------+--------------------+

Note that the two agents (`pursuer_0` and `pursuer_1`) are optimized on the exact same
objective and thus differences in the rewards can be attributed to weight initialization
(and sampling randomness) only.
"""

from pettingzoo.sisl import waterworld_v4

from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
    SHARED_CRITIC_ID,
)
from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import (
    SharedCriticTorchRLModule,
)


parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=1000000,
    default_reward=0.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents > 0, "Must set --num-agents > 0 when running this script!"

    # Here, we use the "Parallel" PettingZoo environment type.
    # This allows MAPPO's global observations to be constructed more neatly.
    def get_env(_):
        return ParallelPettingZooEnv(waterworld_v4.parallel_env())

    register_env("env", get_env)

    # Policies are called just like the agents (exact 1:1 mapping).
    policies = [f"pursuer_{i}" for i in range(args.num_agents)]

    # An agent for each of our policies, and a single shared critic
    env_instantiated = get_env({})  # neccessary for non-agent modules
    specs = {p: RLModuleSpec() for p in policies}
    specs[SHARED_CRITIC_ID] = RLModuleSpec(
        module_class=SharedCriticTorchRLModule,
        observation_space=env_instantiated.observation_space[policies[0]],
        action_space=env_instantiated.action_space[policies[0]],
        learner_only=True,  # Only build on learner
        model_config={"observation_spaces": env_instantiated.observation_space},
    )

    base_config = (
        MAPPOConfig()
        .environment("env")
        .multi_agent(
            policies=policies + [SHARED_CRITIC_ID],
            # Exact 1:1 mapping from AgentID to ModuleID.
            policy_mapping_fn=(lambda aid, *args, **kwargs: aid),
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs=specs,
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
