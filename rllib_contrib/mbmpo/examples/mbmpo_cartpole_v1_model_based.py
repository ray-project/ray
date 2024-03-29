import argparse

from gymnasium.wrappers import TimeLimit
from rllib_mbmpo.env.mbmpo_env import CartPoleWrapper
from rllib_mbmpo.mbmpo import MBMPO, MBMPOConfig

import ray
from ray import air, tune
from ray.tune.registry import register_env


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-as-test", action="store_true", default=False)
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init()
    register_env(
        "cartpole-mbmpo",
        lambda env_ctx: TimeLimit(CartPoleWrapper(), max_episode_steps=200),
    )

    config = (
        MBMPOConfig()
        # .rollouts(num_rollout_workers=7, num_envs_per_worker=20)
        .framework("torch")
        .environment("cartpole-mbmpo")
        .rollouts(num_rollout_workers=4)
        # .training(dynamics_model={"ensemble_size": 2})
        # )
        .training(
            inner_adaptation_steps=1,
            maml_optimizer_steps=8,
            gamma=0.99,
            lambda_=1.0,
            lr=0.001,
            clip_param=0.5,
            kl_target=0.003,
            kl_coeff=0.0000000001,
            inner_lr=0.001,
            num_maml_steps=15,
            model={"fcnet_hiddens": [32, 32], "free_log_std": True},
        )
    )
    if args.run_as_test:
        stop = {
            "episode_reward_mean": 190,
            "training_iteration": 20,
        }
    else:
        stop = {"training_iteration": 1}

    tuner = tune.Tuner(
        MBMPO,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
