import argparse

from rllib_ddppo.ddppo import DDPPO, DDPPOConfig

import ray
from ray import air, tune
from ray.rllib.utils.test_utils import check_learning_achieved


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

    config = (
        DDPPOConfig()
        .rollouts(
            num_rollout_workers=4,
            num_envs_per_worker=10,
            observation_filter="MeanStdFilter",
        )
        .environment("Pendulum-v1")
        .training(
            train_batch_size=2500,
            gamma=0.95,
            sgd_minibatch_size=50,
            num_sgd_iter=5,
            clip_param=0.4,
            vf_clip_param=10.0,
            lambda_=0.1,
            lr=0.00015,
        )
        .resources(num_gpus_per_worker=0)
        .reporting(min_sample_timesteps_per_iteration=1000, min_time_s_per_iteration=5)
    )

    stop_reward = -700

    tuner = tune.Tuner(
        DDPPO,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 1500000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(results, stop_reward)
