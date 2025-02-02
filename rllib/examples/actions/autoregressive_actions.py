"""Example on how to define and run with an RLModule with a dependent action space.

This examples:
    - Shows how to write a custom RLModule outputting autoregressive actions.
    The RLModule class used here implements a prior distribution for the first couple
    of actions and then uses the sampled actions to compute the parameters for and
    sample from a posterior distribution.
    - Shows how to configure a PPO algorithm to use the custom RLModule.
    - Stops the training after 100k steps or when the mean episode return
    exceeds -0.012 in evaluation, i.e. if the agent has learned to
    synchronize its actions.

For details on the environment used, take a look at the `CorrelatedActionsEnv`
class. To receive an episode return over 100, the agent must learn how to synchronize
its actions.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-env-runners 2`

Control the number of `EnvRunner`s with the `--num-env-runners` flag. This
will increase the sampling speed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should reach an episode return of better than -0.5 quickly through a simple PPO
policy. The logic behind beating the env is roughly:

OBS:  optimal a1:   r1:  optimal a2:   r2:
-1      2            0      -1.0        0
-0.5    1/2       -0.5   -0.5/-1.5      0
0       1            0      -1.0        0
0.5     0/1       -0.5   -0.5/-1.5      0
1       0            0      -1.0        0

Meaning, most of the time, you would receive a reward better than -0.5, but worse than
0.0.

+--------------------------------------+------------+--------+------------------+
| Trial name                           | status     |   iter |   total time (s) |
|                                      |            |        |                  |
|--------------------------------------+------------+--------+------------------+
| PPO_CorrelatedActionsEnv_6660d_00000 | TERMINATED |     76 |          132.438 |
+--------------------------------------+------------+--------+------------------+
+------------------------+------------------------+------------------------+
|    episode_return_mean |   num_env_steps_sample |   ...env_steps_sampled |
|                        |             d_lifetime |   _lifetime_throughput |
|------------------------+------------------------+------------------------|
|                  -0.43 |                 152000 |                1283.48 |
+------------------------+------------------------+------------------------+
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.examples.rl_modules.classes.autoregressive_actions_rlm import (
    AutoregressiveActionsRLM,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_iters=1000,
    default_timesteps=2000000,
    default_reward=-0.45,
)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    if args.algo != "PPO":
        raise ValueError(
            "This example script only runs with PPO! Set --algo=PPO on the command "
            "line."
        )

    base_config = (
        PPOConfig()
        .environment(CorrelatedActionsEnv)
        .training(
            train_batch_size_per_learner=2000,
            num_epochs=12,
            minibatch_size=256,
            entropy_coeff=0.005,
            lr=0.0003,
        )
        # Specify the RLModule class to be used.
        .rl_module(
            rl_module_spec=RLModuleSpec(module_class=AutoregressiveActionsRLM),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
