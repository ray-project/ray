"""Example of using fractional GPUs (< 1.0) per Learner worker.

This example:


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------
"""
from typing import Optional

import gymnasium as gym
import numpy as np
import torch

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_iters=50, default_reward=150.0, default_timesteps=100000
)
parser.set_defaults(
    enable_new_api_stack=True,
)


class Float16GradScaler:
    def __init__(
        self,
        init_scale=1000.0,
        growth_factor=2.0,
        backoff_factor=0.5,
        growth_interval=2000,
    ):
        self._scale = init_scale
        self.growth_factor = growth_factor
        self.backoff_factor = backoff_factor
        self.growth_interval = growth_interval
        self._found_inf_or_nan = False
        self.steps_since_growth = 0

    def scale(self, loss):
        return loss * self._scale

    def get_scale(self):
        return self._scale

    def step(self, optimizer):
        """Unscale the gradients for all model parameters and apply."""
        for group in optimizer.param_groups:
            for param in group["params"]:
                if param.grad is not None:
                    param.grad.data.div_(self._scale)
                    if torch.isinf(param.grad).any() or torch.isnan(param.grad).any():
                        self._found_inf_or_nan = True
                        break
            if self._found_inf_or_nan:
                break
        # Only step if no inf/NaN grad found.
        if not self._found_inf_or_nan:
            optimizer.step()

    def update(self):
        # If gradients are found to be inf/NaN, reduce the scale.
        if self._found_inf_or_nan:
            self._scale *= self.backoff_factor
            self.steps_since_growth = 0
        # Increase the scale after a set number of steps without inf/NaN.
        else:
            self.steps_since_growth += 1
            if self.steps_since_growth >= self.growth_interval:
                self._scale *= self.growth_factor
                self.steps_since_growth = 0
        # Reset inf/NaN flag.
        self._found_inf_or_nan = False


class Float16InitCallback(DefaultCallbacks):
    """Callback making sure that all RLModules in the algo are `half()`'ed."""

    def on_algorithm_init(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger: Optional[MetricsLogger] = None,
        **kwargs,
    ) -> None:
        # Switch all Learner RLModules to float16.
        algorithm.learner_group.foreach_learner(
            lambda learner: learner.module.foreach_module(lambda mid, mod: mod.half())
        )
        # Switch all EnvRunner RLModules (assuming single RLModules) to float16.
        algorithm.env_runner_group.foreach_worker(
            lambda env_runner: env_runner.module.half()
        )
        if algorithm.eval_env_runner_group:
            algorithm.eval_env_runner_group.foreach_worker(
                lambda env_runner: env_runner.module.half()
            )


class Float16Connector(ConnectorV2):
    """ConnectorV2 piece preprocessing observations and rewards to be float16.

    Note that users can also write a gymnasium.Wrapper for observations and rewards
    to achieve the same thing.
    """

    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ):
        return gym.spaces.Box(
            input_observation_space.low.astype(np.float16),
            input_observation_space.high.astype(np.float16),
            input_observation_space.shape,
            np.float16,
        )

    def __call__(self, *, rl_module, batch, episodes, **kwargs):
        for sa_episode in self.single_agent_episode_iterator(episodes):
            obs = sa_episode.get_observations(-1)
            float16_obs = obs.astype(np.float16)
            sa_episode.set_observations(new_data=float16_obs, at_indices=-1)
            if len(sa_episode) > 0:
                rew = sa_episode.get_rewards(-1).astype(np.float16)
                sa_episode.set_rewards(new_data=rew, at_indices=-1)
        return batch


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        # Plug in our custom loss scaler class.
        .experimental(_torch_grad_scaler_class=Float16GradScaler)
        .env_runners(env_to_module_connector=lambda env: Float16Connector())
        .callbacks(Float16InitCallback)
        .training(
            grad_clip=None,  # switch off grad clipping entirely as we use our custom scaler
            gamma=0.99,
            lr=0.0003,
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
            use_kl_loss=True,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
