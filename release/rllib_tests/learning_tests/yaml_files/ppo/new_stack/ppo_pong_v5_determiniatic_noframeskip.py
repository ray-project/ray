from functools import partial

import gymnasium as gym
from supersuit.generic_wrappers import resize_v1

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.wrappers.atari_wrappers import (
    MaxAndSkipEnv,
    NoopResetEnv,
    NormalizedImageEnv,
)
from ray import tune


tune.register_env(
    "env",
    (
        lambda cfg: (
            MaxAndSkipEnv(NoopResetEnv(NormalizedImageEnv(
                partial(resize_v1, x_size=64, y_size=64)(
                    partial(gym.wrappers.TimeLimit, max_episode_steps=108000)(
                        gym.make("ALE/Pong-v5", **dict(
                            cfg, **{"render_mode": "rgb_array"}
                        ))
                    )
                )
            )))
        )
    ),
)


config = (
    PPOConfig()
    .environment(
        "env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .experimental(_enable_new_api_stack=True)
    .rollouts(
        env_runner_cls=SingleAgentEnvRunner,
        num_rollout_workers=59,
        num_envs_per_worker=1,
    )
    .training(
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        train_batch_size_per_learner=4000,
        mini_batch_size_per_learner=256,
        num_sgd_iter=10,
        lr=0.0008,  # needs to be adjusted: `lr=0.0001*num_learner_workers`
        grad_clip=100.0,
        grad_clip_by="global_norm",
        model={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
        },
    )
    .resources(
        num_learner_workers=8,
        num_gpus_per_learner_worker=1,
        num_gpus=0,
        num_cpus_for_local_worker=1,
    )
)


if __name__ == "__main__":
    from ray import tune

    tuner = tune.Tuner(
        trainable=config.algo_class.__name__,
        param_space=config,
    )
    tuner.fit()
