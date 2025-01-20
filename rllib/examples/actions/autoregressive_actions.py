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
You should reach an episode return of around 155-160 after ~36,000 timesteps sampled and
trained by a simple PPO policy (no tuning, just using RLlib's
default settings).
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.correlated_actions_env import (
    AutoRegressiveActionEnv,
)
from ray.rllib.examples.rl_modules.classes.autoregressive_actions_rlm import (
    AutoregressiveActionsRLM,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=150.0,
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
        .environment(AutoRegressiveActionEnv)
        .rl_module(
            # We need to explicitly specify here RLModule to use and
            # the catalog needed to build it.
            rl_module_spec=RLModuleSpec(
                module_class=AutoregressiveActionsRLM,
                model_config={
                    "head_fcnet_hiddens": [64, 64],
                    "head_fcnet_activation": "relu",
                },
                catalog_class=PPOCatalog,
            ),
        )
    )

    # Run the example (with Tune).
    run_rllib_example_script_experiment(base_config, args)
