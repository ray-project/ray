import argparse

from rllib_es.es import ES, ESConfig

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
        ESConfig()
        .rollouts(num_rollout_workers=2)
        .framework("torch")
        .environment("CartPole-v1")
        .training(noise_size=25000000, episodes_per_batch=50)
    )

    stop_reward = 100

    tuner = tune.Tuner(
        ES,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 500000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(
            results, stop_reward, metric="sampler_results/episode_reward_mean"
        )
