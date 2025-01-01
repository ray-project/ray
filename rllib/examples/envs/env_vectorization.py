import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray import train, tune


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make("ale_py:ALE/Pong-v5", **cfg, render_mode="rgb_array"),
        framestack=4,
    )

tune.register_env("env", _env_creator)


if __name__ == '__main__':
    config = (
        PPOConfig()
        .environment("env")
        .env_runners(
            num_env_runners=1,
            num_envs_per_env_runner=6,
            remote_worker_envs=tune.grid_search([True, False]),
        )
        .rl_module(
            model_config=DefaultModelConfig(
                conv_filters=[[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                conv_activation="relu",
                head_fcnet_hiddens=[256],
                vf_share_layers=True,
            ),
        )
        .training(
            train_batch_size_per_learner=4000,
            minibatch_size=128,
            lambda_=0.95,
            kl_coeff=0.5,
            clip_param=0.1,
            vf_clip_param=10.0,
            entropy_coeff=0.01,
            num_epochs=10,
            lr=0.00015,
            grad_clip=100.0,
            grad_clip_by="global_norm",
        )
    )

    results = tune.Tuner(
        "PPO",
        param_space=config,
        run_config=train.RunConfig(stop={"num_env_steps_sampled_lifetime": 100000}),
    ).fit()

    print(results[0].metrics)
