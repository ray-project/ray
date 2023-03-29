"""
Example of interfacing with an environment that produces 2D observations.

This example shows how to turn a pettingzoo environment with a 2D observations with
shape (A, B) into a 3D observations with shape (C, D, 1).
This can be useful in order to use RLlib's default CNN filters, even though the
original observation space of the environment does not fit them.
"""

from numpy import float32
from pettingzoo.butterfly import pistonball_v6
from supersuit import normalize_obs_v0, dtype_v0, color_reduction_v0, reshape_v0, resize_v1

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env import PettingZooEnv
from ray.tune.registry import register_env
from ray import tune
from ray import air

# The space we downsample and transform the greyscaled pistonball images to.
# Other spaces supported by Rllib can be chosen here.
TRANSFORMED_OBS_SPACE = (84, 84, 1)


def env_creator(config):
    env = pistonball_v6.env(n_pistons=5)
    env = dtype_v0(env, dtype=float32)
    # This step gives us a greyscale image
    env = color_reduction_v0(env, mode="R")
    env = normalize_obs_v0(env)
    # This step gives us an image that is upsampled to the number of pixels in the
    # default CNN filter
    env = resize_v1(env, x_size=TRANSFORMED_OBS_SPACE[0],
                    y_size=TRANSFORMED_OBS_SPACE[1])
    # This step gives us an image that fits the default CNN filters
    env = reshape_v0(env, shape=TRANSFORMED_OBS_SPACE)
    return env


# Register env
register_env("pistonball", lambda config: PettingZooEnv(env_creator(config)))


config = (
    PPOConfig()
    .environment("pistonball", env_config={"local_ratio": 0.5}, clip_rewards=True)
    .rollouts(
        num_rollout_workers=15, num_envs_per_worker=1, observation_filter="NoFilter", rollout_fragment_length="auto")
    .framework("torch")
    .training(entropy_coeff=0.01, vf_loss_coeff=0.1, clip_param=0.1,
              vf_clip_param=10.0, num_sgd_iter=10, kl_coeff=0.5, lr=0.0001,
              grad_clip=100, sgd_minibatch_size=500, train_batch_size=5000,
              model={"vf_share_layers": True})
    .resources(num_gpus=1)
    .reporting(min_time_s_per_iteration=30)
)

tune.Tuner(
    "PPO",
    param_space=config.to_dict(),
    run_config=air.RunConfig(
        stop={
            "training_iteration": 10,
        },
    verbose=2
    ),
).fit()

