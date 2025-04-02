"""Example of how to use PyTorch's learning rate schedulers to design a complex
learning rate schedule for training.

Two learning rate schedules are applied in sequence to the learning rate of the
optimizer. In this way even more complex learning rate schedules can be assembled.

This example shows:
    - how to configure multiple learning rate schedulers, as a chained pipeline, in
    PyTorch using partial initialization with `functools.partial`.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --lr-const-factor=0.9
--lr-const-iters=10 --lr-exp-decay=0.9`

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
import numpy as np
from typing import Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import DEFAULT_OPTIMIZER
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import add_rllib_example_script_args

torch, _ = try_import_torch()


class LRChecker(RLlibCallback):
    def on_algorithm_init(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        **kwargs,
    ) -> None:
        # Store the expected learning rates for each iteration.
        self.lr = []
        # Retrieve the chosen configuration parameters from the config.
        lr_factor = algorithm.config._torch_lr_scheduler_classes[0].keywords["factor"]
        lr_total_iters = algorithm.config._torch_lr_scheduler_classes[0].keywords[
            "total_iters"
        ]
        lr_gamma = algorithm.config._torch_lr_scheduler_classes[1].keywords["gamma"]
        # Compute the learning rates for all iterations up to `lr_const_iters`.
        for i in range(1, lr_total_iters + 1):
            # The initial learning rate.
            lr = algorithm.config.lr
            # In the first 10 iterations we multiply by `lr_const_factor`.
            if i < lr_total_iters:
                lr *= lr_factor
            # Finally, we have an exponential decay of `lr_exp_decay`.
            lr *= lr_gamma**i
            self.lr.append(lr)

    def on_train_result(
        self,
        *,
        algorithm: "Algorithm",
        metrics_logger: Optional[MetricsLogger] = None,
        result: dict,
        **kwargs,
    ) -> None:

        # Check for the first `lr_total_iters + 1` iterations, if expected
        # and actual learning rates correspond.
        if (
            algorithm.training_iteration
            <= algorithm.config._torch_lr_scheduler_classes[0].keywords["total_iters"]
        ):
            actual_lr = algorithm.learner_group._learner.get_optimizer(
                DEFAULT_MODULE_ID, DEFAULT_OPTIMIZER
            ).param_groups[0]["lr"]
            # Assert the learning rates are close enough.
            assert np.isclose(
                actual_lr,
                self.lr[algorithm.training_iteration - 1],
                atol=1e-9,
                rtol=1e-9,
            )


parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=250000)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--lr-const-factor",
    type=float,
    default=0.9,
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
    default=0.99,
    help="The rate by which the learning rate should exponentially decay.",
)

if __name__ == "__main__":
    # Use `parser` to add your own custom command line options to this script
    # and (if needed) use their values to set up `config` below.
    args = parser.parse_args()

    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .training(
            lr=0.03,
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
        .callbacks(
            LRChecker,
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
