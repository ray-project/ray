"""An example script showing how to define and load an `RLModule` with
a dependent action space.

This examples:
    - Defines an `RLModule` with autoregressive actions.
    - It does so by implementing a prior distribution for the first couple
        of actions and then using these actions in a posterior distribution.
    - Furthermore, it uses in the `RLModule` our simple base `Catalog` class
        to build the distributions.
    - Uses this `RLModule` in a PPO training run on a simple environment
        that rewards synchronized actions.
    - Stops the training after 100k steps or when the mean episode return
        exceeds 150 in evaluation, i.e. if the agent has learned to
        synchronize its actions.

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
You should expect a reward of around 155-160 after ~36,000 timesteps sampled
(trained) being achieved by a simple PPO policy (no tuning, just using RLlib's
default settings). For details take also a closer look into the
`CorrelatedActionsEnv` environment. Rewards are such that to receive a return
over 100, the agent must learn to synchronize its actions.
"""


from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.envs.classes.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.examples.rl_modules.classes.autoregressive_actions_rlm import (
    AutoregressiveActionsTorchRLM,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env


register_env("correlated_actions_env", lambda _: CorrelatedActionsEnv(_))

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=150.0,
)

if __name__ == "__main__":
    args = parser.parse_args()

    if args.algo != "PPO":
        raise ValueError("This example only supports PPO. Please use --algo=PPO.")

    base_config = (
        PPOConfig()
        .environment(env="correlated_actions_env")
        .rl_module(
            model_config_dict={
                "post_fcnet_hiddens": [64, 64],
                "post_fcnet_activation": "relu",
            },
            # We need to explicitly specify here RLModule to use and
            # the catalog needed to build it.
            rl_module_spec=SingleAgentRLModuleSpec(
                module_class=AutoregressiveActionsTorchRLM,
                catalog_class=Catalog,
            ),
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            # Run evaluation parallel to training to speed up the example.
            evaluation_parallel_to_training=True,
        )
    )

    # Let's stop the training after 100k steps or when the mean episode return
    # exceeds 150 in evaluation.
    stop = {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 100000,
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 150.0,
    }

    # Run the example (with Tune).
    run_rllib_example_script_experiment(base_config, args, stop=stop)
