import argparse

from rllib_ars.ars import ARS, ARSConfig

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
        ARSConfig()
        .rollouts(num_rollout_workers=2)
        .framework("torch")
        .environment("CartPole-v1")
        .training(
            noise_stdev=0.02,
            rollouts_used=23,
            num_rollouts=50,
            sgd_stepsize=0.01,
            noise_size=250000000,
            eval_prob=0.5,
        )
    )

    stop_reward = 150

    tuner = tune.Tuner(
        ARS,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 1000000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(
            results, stop_reward, metric="sampler_results/episode_reward_mean"
        )
