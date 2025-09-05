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
The above options can reach a combined reward of _ or more after about _ env timesteps. Keep in mind, though, that due to the separate learned policies in general,  one agent's gain (in per-agent reward) might cause the other agent's reward to decrease at the same time. However, over time, both agents should simply improve. For reasons similar to those described in pettingzoo_parameter_sharing.py, learning may take slightly longer than in fully-independent settings, as agents are less inclined to specialize and thereby balance out one anothers' mistakes.

+---------------------+------------+--------------------+--------+------------------+
| Trial name          | status     | loc                |   iter |   total time (s) |
|---------------------+------------+--------------------+--------+------------------+
| PPO_env_c90f4_00000 | TERMINATED | 172.29.87.208:6322 |    101 |          1269.24 |
+---------------------+------------+--------------------+--------+------------------+

--------+-------------------+--------------------+--------------------+
     ts |   combined return |   return pursuer_0 |   return pursuer_1 |
--------+-------------------+--------------------+--------------------|
 404000 |           1.31496 |            48.0908 |           -46.7758 |
--------+-------------------+--------------------+--------------------+

Note that the two agents (`pursuer_0` and `pursuer_1`) are optimized on the exact same
objective and thus differences in the rewards can be attributed to weight initialization
(and sampling randomness) only.
"""

from pettingzoo.sisl import waterworld_v4

from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

from ray.rllib.examples.algorithms.mappo.mappo import MAPPOConfig
from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import SHARED_CRITIC_ID
from ray.rllib.examples.algorithms.mappo.torch.shared_critic_torch_rl_module import SharedCriticTorchRLModule


parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=1000000,
    default_reward=0.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.num_agents > 0, "Must set --num-agents > 0 when running this script!"

    # Here, we use the "Agent Environment Cycle" (AEC) PettingZoo environment type.
    # For a "Parallel" environment example, see the rock paper scissors examples
    # in this same repository folder.
    get_env = lambda _: PettingZooEnv(waterworld_v4.env())
    register_env("env", get_env)

    # Policies are called just like the agents (exact 1:1 mapping).
    policies = [f"pursuer_{i}" for i in range(args.num_agents)]
    
    # An agent for each of our policies, and a single shared critic
    env_instantiated = get_env({}) # neccessary for non-agent modules
    specs = {p: RLModuleSpec() for p in policies}
    specs[SHARED_CRITIC_ID] = RLModuleSpec(
        module_class=SharedCriticTorchRLModule,
        observation_space=env_instantiated.observation_spaces[policies[0]],
        action_space=env_instantiated.action_spaces[policies[0]],
        learner_only=True, # Only build on learner
        model_config={},
    )

    base_config = (
        MAPPOConfig()
        .environment("env")
        .multi_agent(
            policies=policies,
            # Exact 1:1 mapping from AgentID to ModuleID.
            policy_mapping_fn=(lambda aid, *args, **kwargs: aid),
        )
        .training(
            vf_loss_coeff=0.005,
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs=specs,
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
