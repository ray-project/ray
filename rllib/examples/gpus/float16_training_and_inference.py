"""Example of using float16 precision for training and inference.

This example:
    - shows how to write a custom callback for RLlib to convert all RLModules
    (on the EnvRunners and Learners) to float16 precision.
    - shows how to write a custom env-to-module ConnectorV2 piece to convert all
    observations and rewards in the collected trajectories to float16 (numpy) arrays.
    - shows how to write a custom grad scaler for torch that is necessary to stabilize
    learning with float16 weight matrices and gradients. This custom scaler behaves
    exactly like the torch built-in `torch.amp.GradScaler` but also works for float16
    gradients (which the torch built-in one doesn't).
    - shows how to write a custom TorchLearner to change the epsilon setting (to the
    much larger 1e-4 to stabilize learning) on the default optimizer (Adam) registered
    for each RLModule.
    - demonstrates how to plug in all the above custom components into an
    `AlgorithmConfig` instance and start training (and inference) with float16
    precision.


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
You should see something similar to the following on your terminal, when running this
script with the above recommended options:

+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_437ee_00000 | TERMINATED | 127.0.0.1:81045 |      6 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |  num_episodes_lifetime |
|                  |                        |                        |
|------------------+------------------------+------------------------+
|          71.3123 |                 153.79 |                    358 |
+------------------+------------------------+------------------------+
"""
from typing import Optional

import gymnasium as gym
import numpy as np
import torch

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
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


class MakeAllRLModulesFloat16(DefaultCallbacks):
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


class WriteObsAndRewardsAsFloat16(ConnectorV2):
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


class Float16GradScaler:
    """Custom grad scaler for `TorchLearner`.

    This class is utilizing the experimental support for the `TorchLearner`'s support
    for loss/gradient scaling (analogous to how a `torch.amp.GradScaler` would work).

    TorchLearner performs the following steps using this class (`scaler`):
    - loss_per_module = TorchLearner.compute_losses()
    - for L in loss_per_module: L = scaler.scale(L)
    - grads = TorchLearner.compute_gradients()  # L.backward() on scaled loss
    - TorchLearner.apply_gradients(grads):
        for optim in optimizers:
            scaler.step(optim)  # <- grads should get unscaled
            scaler.update()  # <- update scaling factor
    """

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
        # Scale the loss by `self._scale`.
        return loss * self._scale

    def get_scale(self):
        return self._scale

    def step(self, optimizer):
        # Unscale the gradients for all model parameters and apply.
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


class LargeEpsAdamTorchLearner(PPOTorchLearner):
    """A TorchLearner overriding the default optimizer (Adam) to use non-default eps."""

    @override(TorchLearner)
    def configure_optimizers_for_module(self, module_id, config):
        """Registers an Adam optimizer with a larg epsilon under the given module_id."""
        params = list(self._module[module_id].parameters())

        # Register one Adam optimizer (under the default optimizer name:
        # DEFAULT_OPTIMIZER) for the `module_id`.
        self.register_optimizer(
            module_id=module_id,
            # Create an Adam optimizer with a different eps for better float16
            # stability.
            optimizer=torch.optim.Adam(params, eps=1e-4),
            params=params,
            # Let RLlib handle the learning rate/learning rate schedule.
            # You can leave `lr_or_lr_schedule` at None, but then you should
            # pass a fixed learning rate into the Adam constructor above.
            lr_or_lr_schedule=config.lr,
        )


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        # Plug in our custom callback (on_algorithm_init) to make all RLModules
        # float16 models.
        .callbacks(MakeAllRLModulesFloat16)
        # Plug in our custom loss scaler class to stabilize gradient computations
        # (by scaling the loss, then unscaling the gradients before applying them).
        # This is using the built-in, experimental feature of TorchLearner.
        .experimental(_torch_grad_scaler_class=Float16GradScaler)
        # Plug in our custom env-to-module ConnectorV2 piece to convert all observations
        # and reward in the episodes (permanently) to float16.
        .env_runners(env_to_module_connector=lambda env: WriteObsAndRewardsAsFloat16())
        .training(
            # Plug in our custom TorchLearner (using a much larger, stabilizing epsilon
            # on the Adam optimizer).
            learner_class=LargeEpsAdamTorchLearner,
            # Switch off grad clipping entirely b/c we use our custom grad scaler with
            # built-in inf/nan detection (see `step` method of `Float16GradScaler`).
            grad_clip=None,
            # Typical CartPole-v1 hyperparams known to work well:
            gamma=0.99,
            lr=0.0003,
            num_epochs=6,
            vf_loss_coeff=0.01,
            use_kl_loss=True,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
