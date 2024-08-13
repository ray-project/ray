"""Example of using automatic mixed precision training on a torch RLModule.

This example:
  - shows how to set up an Algorithm with mixed precision training enabled.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

Note that the shown GPU settings in this script also work in case you are not
running via tune, but instead are using the `--no-tune` command line option.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------
In the console output, you should see something like this:
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_0005e_00000 | TERMINATED | 127.0.0.1:34881 |     19 |
+-----------------------------+------------+-----------------+--------+
+----------------+------------------------+------------------------+
| total time (s) |   episode_return_mean  |   num_episodes_lifetim |
|                |                        |                      e |
+----------------+------------------------+------------------------+
|        45.8078 |                 454.28 |                    589 |
+----------------+------------------------+------------------------+
"""
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_iters=200, default_reward=450.0, default_timesteps=200000
)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # This script only works on the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment("CartPole-v1")
        # Activate mixed-precision training for our torch RLModule.
        .experimental(_enable_torch_mixed_precision_training=True)
    )

    run_rllib_example_script_experiment(base_config, args, keep_config=True)
