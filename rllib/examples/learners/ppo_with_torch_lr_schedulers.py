"""Example of how to use PyTorch's learning rate schedulers to design a complex
learning rate schedule for training.

Two learning rate schedules are applied in sequence to the learning rate of the
optimizer. In this way even more complex learning rate schedules can be assembled.

This example shows:
    - how to configure multiple learning rate schedulers, as a chained pipeline, in
    PyTorch using partial initialization with `functools.partial`.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --lr-const-factor=0.1
--lr-const-iters=10 --lr-exp-decay=0.3`

Use the `--lr-const-factor` to define the facotr by which to multiply the
learning rate in the first `--lr-const-iters` iterations. Use the
`--lr-const-iters` to set the number of iterations in which the learning rate
should be adapted by the `--lr-const-factor`. Use `--lr-exp-decay` to define
the learning rate decay to be applied after the constant factor multiplication.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should expect to observe decent learning behavior from your console output:

With `--lr-const-factor=0.1`, `--lr-const-iters=10, and `--lr-exp_decay=0.3`.
+-----------------------------+------------+--------+------------------+
| Trial name                  | status     |   iter |   total time (s) |
|                             |            |        |                  |
|-----------------------------+------------+--------+------------------+
| PPO_CartPole-v1_7fc44_00000 | TERMINATED |     50 |          59.6542 |
+-----------------------------+------------+--------+------------------+
+------------------------+------------------------+------------------------+
|    episode_return_mean |  num_episodes_lifetime |   num_env_steps_traine |
|                        |                        |             d_lifetime |
+------------------------+------------------------+------------------------|
|                  451.2 |                   9952 |                 210047 |
+------------------------+------------------------+------------------------+
"""
import functools

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import add_rllib_example_script_args

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=200000)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--lr-const-factor",
    type=float,
    default=0.1,
    help="The factor by which the learning rate should be multiplied.",
)
parser.add_argument(
    "--lr-const-iters",
    type=int,
    default=10,
    help=(
        "The number of iterations by which the learning rate should be "
        "multiplied by the factor."
    ),
)
parser.add_argument(
    "--lr-exp-decay",
    type=float,
    default=0.3,
    help="The rate by which the learning rate should exponentially decay.",
)

if __name__ == "__main__":
    # Use `parser` to add your own custom command line options to this script
    # and (if needed) use their values toset up `config` below.
    args = parser.parse_args()

    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .training(
            lr=0.0003,
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                vf_share_layers=True,
            ),
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_parallel_to_training=True,
            evaluation_config=PPOConfig.overrides(exploration=False),
        )
        .experimental(
            # Add two learning rate schedulers to be applied in sequence.
            _torch_lr_scheduler_classes=[
                # Multiplies the learning rate by a factor of 0.1 for 10 iterations.
                functools.partial(
                    torch.optim.lr_scheduler.ConstantLR,
                    factor=args.lr_const_factor,
                    total_iters=args.lr_const_iters,
                ),
                # Decays the learning rate after each gradients step by
                # `args.lr_exp_decay`.
                functools.partial(
                    torch.optim.lr_scheduler.ExponentialLR, gamma=args.lr_exp_decay
                ),
            ]
        )
    )

    stop = {
        f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": args.stop_timesteps,
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
            args.stop_reward
        ),
    }

    if __name__ == "__main__":
        from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

        run_rllib_example_script_experiment(config, args, stop=stop)
