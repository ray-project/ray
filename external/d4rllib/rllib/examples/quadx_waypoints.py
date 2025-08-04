"""An example showing how to use PyFlyt gymnasium environment to train a UAV to
reach waypoints.

For more infos about the PyFlyt gymnasium environment see the GitHub Repository:
https://github.com/jjshoots/PyFlyt/tree/master/PyFlyt

This example
    - Runs a single-agent `PyFlyt/QuadX-Waypoints-v1` experiment.
    - Uses a gymnasium reward wrapper for reward scaling.
    - Stops the experiment, if either `--stop-iters` (default is 200) or
        `--stop-reward` (default is 90.0) is reached.


How to run this script
----------------------
`python [script file name].py`

Control the number of environments per `EnvRunner` via `--num-envs-per-env-runner`.
This will increase sampling speed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0` which should allow you to set breakpoints
anywhere in the RLlib code and have the execution stop there for inspection
and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""
import gymnasium as gym
import sys

from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    TRAINING_ITERATION_TIMER,
)
from ray.tune.registry import get_trainable_cls, register_env

sys.setrecursionlimit(3000)

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=90.0,
)
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--env-name", type=str, default="quadx_waypoints")
parser.add_argument("--num-envs-per-env-runner", type=int, default=4)


class RewardWrapper(gym.RewardWrapper):
    def __init__(self, env):
        super().__init__(env)

    def reward(self, reward):
        # Scale rewards:
        if reward >= 99.0 or reward <= -99.0:
            return reward / 10
        return reward


def create_quadx_waypoints_env(env_config):
    import PyFlyt.gym_envs  # noqa
    from PyFlyt.gym_envs import FlattenWaypointEnv

    env = gym.make("PyFlyt/QuadX-Waypoints-v1")
    # Wrap Environment to use max 10 and -10 for rewards
    env = RewardWrapper(env)

    return FlattenWaypointEnv(env, context_length=1)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register the environment with tune.
    register_env(args.env_name, env_creator=create_quadx_waypoints_env)

    # Get the algorithm class to use for training.
    algo_cls = get_trainable_cls(args.run)
    config = (
        algo_cls.get_default_config()
        .environment(env=args.env_name)
        .env_runners(
            num_envs_per_env_runner=args.num_envs_per_env_runner,
        )
        .reporting(min_time_s_per_iteration=0.1)
    )

    # If PPO set additional configurations.
    if args.run == "PPO":
        config.rl_module(
            model_config={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
            }
        )
        config.training(
            minibatch_size=128,
            train_batch_size_per_learner=10000,
        )
    # If IMPALA set additional arguments.
    elif args.run == "IMPALA":
        config.env_runners(num_env_runners=2)
        config.learners(num_gpus_per_learner=0)
        config.training(vf_loss_coeff=0.01)

    # Set the stopping arguments.
    EPISODE_RETURN_MEAN_KEY = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    stop = {
        TRAINING_ITERATION_TIMER: args.stop_iters,
        EPISODE_RETURN_MEAN_KEY: args.stop_reward,
    }

    # Run the experiment.
    run_rllib_example_script_experiment(
        config,
        args,
        stop=stop,
        success_metric={
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        },
    )
