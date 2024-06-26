# @OldAPIStack
"""
Example of interfacing with an environment that produces 2D observations.

This example shows how turning 2D observations with shape (A, B) into a 3D
observations with shape (C, D, 1) can enable usage of RLlib's default models.
RLlib's default Catalog class does not provide default models for 2D observation
spaces, but it does so for 3D observations.
Therefore, one can either write a custom model or transform the 2D observations into 3D
observations. This enables RLlib to use one of the default CNN filters, even though the
original observation space of the environment does not fit them.

This simple example should reach rewards of 50 within 150k timesteps.
"""

from numpy import float32
import argparse
from pettingzoo.butterfly import pistonball_v6
from supersuit import (
    normalize_obs_v0,
    dtype_v0,
    color_reduction_v0,
    reshape_v0,
    resize_v1,
)

from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env import PettingZooEnv
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune.registry import register_env
from ray import tune
from ray import air


parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a compilation test.",
)
parser.add_argument(
    "--stop-iters", type=int, default=150, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=1000000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=50, help="Reward at which we stop training."
)

args = parser.parse_args()


# The space we down-sample and transform the greyscale pistonball images to.
# Other spaces supported by RLlib can be chosen here.
TRANSFORMED_OBS_SPACE = (42, 42, 1)


def env_creator(config):
    env = pistonball_v6.env(n_pistons=5)
    env = dtype_v0(env, dtype=float32)
    # This gives us greyscale images for the color red
    env = color_reduction_v0(env, mode="R")
    env = normalize_obs_v0(env)
    # This gives us images that are upsampled to the number of pixels in the
    # default CNN filter
    env = resize_v1(
        env, x_size=TRANSFORMED_OBS_SPACE[0], y_size=TRANSFORMED_OBS_SPACE[1]
    )
    # This gives us 3D images for which we have default filters
    env = reshape_v0(env, shape=TRANSFORMED_OBS_SPACE)
    return env


# Register env
register_env("pistonball", lambda config: PettingZooEnv(env_creator(config)))

config = (
    PPOConfig()
    .environment("pistonball", env_config={"local_ratio": 0.5}, clip_rewards=True)
    .env_runners(
        num_env_runners=15 if not args.as_test else 2,
        num_envs_per_env_runner=1,
        observation_filter="NoFilter",
        rollout_fragment_length="auto",
    )
    .framework("torch")
    .training(
        entropy_coeff=0.01,
        vf_loss_coeff=0.1,
        clip_param=0.1,
        vf_clip_param=10.0,
        num_sgd_iter=10,
        kl_coeff=0.5,
        lr=0.0001,
        grad_clip=100,
        sgd_minibatch_size=500,
        train_batch_size=5000 if not args.as_test else 1000,
        model={"vf_share_layers": True},
    )
    .resources(num_gpus=1 if not args.as_test else 0)
    .reporting(min_time_s_per_iteration=30)
)

tune.Tuner(
    "PPO",
    param_space=config.to_dict(),
    run_config=air.RunConfig(
        stop={
            TRAINING_ITERATION: args.stop_iters,
            NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        },
        verbose=2,
    ),
).fit()
