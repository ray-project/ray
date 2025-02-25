"""Example of how to write a custom loss function, custom rl_module and custom config.

This script implements a new critic network that models the value function as a mixture of gaussians (MoG).
By replacing the traditional single-point estimate of the value function with a distribution,
the critic gains greater expressivity to capture multimodal or non-linear dynamics in the environment.
This is particularly useful for environments with complex reward structures or non-stationary transitions,
where modeling the uncertainty and variability of the value function is necessary.

As investigated in [Shahriari et al., 2022] from DeepMind, this expressiveness can improve
policy effectiveness through two mechanisms: adaptive Mahalanobis reweighting and improved
feature learning. Their findings concluded that both mechanisms contribute to the observed
performance gains.

Paper: https://arxiv.org/pdf/2204.10256

Key difference: While the paper uses a cross-entropy loss, this implementation focuses on
a form of the negative log-likelihood loss.

The parameter --num-gaussians allows the user to specify the number of gaussian components.
This controls the flexibility of the value function representation. A value between 3-5 generally performs well.
The balance is computation efficiency and maintaining expressivity.

This example shows:
    - how to subclass an existing (torch) Learner and override its `compute_loss_for_module()` method.
    - how you can add your own loss to a custom (or non-custom) RLModule using compute_loss_for_module.
    - how to add custom pipeline components to a custom config class using build_learner_connector.
    - how to plug in a custom Learner, RLModule, its loss, and use a custom config to train with.

See the :py:class:`~ray.rllib.examples.learners.classes.mixture_of_gaussian_learner.PPOTorchLearnerWithMOGLoss`  # noqa
class for details on how to override the main (PPO) loss function.

How to run this script
----------------------
`python mixture_of_gaussian.py --enable-new-api-stack --num-gaussians=3 --lr=0.001`


Results to expect
-----------------
In general with environments, the distributional critic should perform better than a normal critic given
the same hyperparameters. Some hiccups have been the alphas going too small which causes nans in the loss equation.
This has been fixed by using the log_softmax(alphas) instead of using the softmax on the alphas once out of
the module.

With --num-gaussians=2 and --lr=0.0001
(trying to reach 250.0 return on CartPole in 100k env steps):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_9cda9_00000 | TERMINATED | 127.0.0.1:54584 |     10 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          100.105 |                 44_000 |              252.31 |
+------------------+------------------------+---------------------+



With --num-gaussians=2 and --lr=0.0003
(trying to reach 500.0 return on HalfCheetah in 2M env steps):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| HalfCheetah-v5              | TERMINATED | 127.0.0.1:8888  |    317 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          ------- |                1272000 |              500.2  |
+------------------+------------------------+---------------------+
"""
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.examples.learners.classes.mixture_of_gaussian_config import (
    PPOConfigWithMOG,
)
from ray.rllib.utils.metrics import (
    EVALUATION_RESULTS,
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

torch, _ = try_import_torch()


parser = add_rllib_example_script_args(
    default_reward=500.0,
    default_timesteps=10_000_000,
)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--num-gaussians",
    type=int,
    default=3,
    help="Number of gaussians to represent the value function",
)
parser.add_argument(
    "--lr",
    type=float,
    default=0.0003,
    help="Learning rate",
)

if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"
    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        PPOConfigWithMOG()
        .environment(
            env="HalfCheetah-v5",
        )
        .training(
            lr=args.lr,
            gamma=0.995,
            lambda_=0.995,
            num_gaussians=args.num_gaussians,
        )
    )

    stop = {
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 500.0,
        TRAINING_ITERATION: 1000,
    }

    run_rllib_example_script_experiment(base_config=base_config, args=args, stop=stop)
