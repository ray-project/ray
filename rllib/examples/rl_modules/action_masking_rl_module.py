"""An example script showing how to define and load an `RLModule` that applies
action masking

This example:
    - Defines an `RLModule` that applies action masking.
    - It does so by using a `gymnasium.spaces.dict.Dict` observation space
        with two keys, namely `"observations"`, holding the original observations
        and `"action_mask"` defining the action mask for the current environment
        state. Note, by this definition you can wrap any `gymnasium` environment
        and use it for this module.
    - Furthermore, it derives its `TorchRLModule` from the `PPOTorchRLModule` and
        can therefore be easily plugged into our `PPO` algorithm.
    - It overrides the `forward` methods of the `PPOTorchRLModule` to apply the
        action masking and it overrides the `_compute_values` method for GAE
        computation to extract the `"observations"` from the batch `Columns.OBS`
        key.
    - It uses the custom `ActionMaskEnv` that defines for each step a new action
        mask that defines actions that are allowed (1.0) and others that are not
        (0.0).
    - It runs 10 iterations with PPO and finishes.

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
You should expect a mean episode reward of around 0.35. The environment is a random
environment paying out random rewards - so the agent cannot learn, but it can obey the
action mask and should do so (no `AssertionError` should happen).
After 40,000 environment steps and 10 training iterations the run should stop
successfully:

+-------------------------------+------------+----------------------+--------+
| Trial name                    | status     | loc                  |   iter |
|                               |            |                      |        |
|-------------------------------+------------+----------------------+--------+
| PPO_ActionMaskEnv_dedc8_00000 | TERMINATED | 192.168.1.178:103298 |     10 |
+-------------------------------+------------+----------------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |   num_env_steps_sample |   num_env_steps_traine |
|                  |             d_lifetime |             d_lifetime |
+------------------+------------------------+------------------------+
|          57.9207 |                  40000 |                  40000 |
+------------------+------------------------+------------------------+
*------------------------+
|   num_episodes_lifetim |
|                      e |
+------------------------|
|                   3898 |
+------------------------+
"""
from gymnasium.spaces import Box, Discrete

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.action_mask_env import ActionMaskEnv
from ray.rllib.examples.rl_modules.classes.action_masking_rlm import (
    ActionMaskingTorchRLModule,
)

from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_iters=10,
    default_timesteps=100000,
    default_reward=150.0,
)

if __name__ == "__main__":
    args = parser.parse_args()

    if args.algo != "PPO":
        raise ValueError("This example only supports PPO. Please use --algo=PPO.")

    base_config = (
        PPOConfig()
        .environment(
            env=ActionMaskEnv,
            env_config={
                "action_space": Discrete(100),
                # This defines the 'original' observation space that is used in the
                # `RLModule`. The environment will wrap this space into a
                # `gym.spaces.Dict` together with an 'action_mask' that signals the
                # `RLModule` to adapt the action distribution inputs for the underlying
                # `PPORLModule`.
                "observation_space": Box(-1.0, 1.0, (5,)),
            },
        )
        .rl_module(
            # We need to explicitly specify here RLModule to use and
            # the catalog needed to build it.
            rl_module_spec=RLModuleSpec(
                module_class=ActionMaskingTorchRLModule,
                model_config={
                    "head_fcnet_hiddens": [64, 64],
                    "head_fcnet_activation": "relu",
                },
            ),
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            # Run evaluation parallel to training to speed up the example.
            evaluation_parallel_to_training=True,
        )
    )

    # Run the example (with Tune).
    run_rllib_example_script_experiment(base_config, args)
