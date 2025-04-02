"""Example of using automatic mixed precision training on a torch RLModule.

This example:
    - shows how to write a custom callback for RLlib to convert those RLModules
    only(!) on the EnvRunners to float16 precision.
    - shows how to write a custom env-to-module ConnectorV2 piece to add float16
    observations to the action computing forward batch on the EnvRunners, but NOT
    permanently write these changes into the episodes, such that on the
    Learner side, the original float32 observations will be used (for the mixed
    precision `forward_train` and `loss` computations).
    - shows how to plugin torch's built-in `GradScaler` class to be used by the
    TorchLearner to scale losses and unscale gradients in order to gain more stability
    when training with mixed precision.
    - shows how to write a custom TorchLearner to run the update step (overrides
    `_update()`) within a `torch.amp.autocast()` context. This makes sure that .
    - demonstrates how to plug in all the above custom components into an
    `AlgorithmConfig` instance and start training with mixed-precision while
    performing the inference on the EnvRunners with float16 precision.


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
| PPO_CartPole-v1_485af_00000 | TERMINATED | 127.0.0.1:81045 |     22 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |  num_episodes_lifetime |
|                  |                        |                        |
|------------------+------------------------+------------------------+
|         281.3231 |                 455.81 |                   1426 |
+------------------+------------------------+------------------------+
"""
import gymnasium as gym
import numpy as np
import torch

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_iters=200, default_reward=450.0, default_timesteps=200000
)
parser.set_defaults(
    algo="PPO",
    enable_new_api_stack=True,
)


def on_algorithm_init(
    algorithm: Algorithm,
    **kwargs,
) -> None:
    """Callback making sure that all RLModules in the algo are `half()`'ed."""

    # Switch all EnvRunner RLModules (assuming single RLModules) to float16.
    algorithm.env_runner_group.foreach_env_runner(
        lambda env_runner: env_runner.module.half()
    )
    if algorithm.eval_env_runner_group:
        algorithm.eval_env_runner_group.foreach_env_runner(
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
            self.add_batch_item(
                batch,
                column="obs",
                item_to_add=float16_obs,
                single_agent_episode=sa_episode,
            )
        return batch


class PPOTorchMixedPrecisionLearner(PPOTorchLearner):
    def _update(self, *args, **kwargs):
        with torch.cuda.amp.autocast():
            results = super()._update(*args, **kwargs)
        return results


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"
    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        (PPOConfig().environment("CartPole-v1"))
        .env_runners(env_to_module_connector=lambda env: Float16Connector())
        # Plug in our custom callback (on_algorithm_init) to make EnvRunner RLModules
        # float16 models.
        .callbacks(on_algorithm_init=on_algorithm_init)
        # Plug in the torch built-int loss scaler class to stabilize gradient
        # computations (by scaling the loss, then unscaling the gradients before
        # applying them). This is using the built-in, experimental feature of
        # TorchLearner.
        .experimental(_torch_grad_scaler_class=torch.cuda.amp.GradScaler)
        .training(
            # Plug in the custom Learner class to activate mixed-precision training for
            # our torch RLModule (uses `torch.amp.autocast()`).
            learner_class=PPOTorchMixedPrecisionLearner,
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
