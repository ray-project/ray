import argparse

from rllib_a2c.a2c import A2C, A2CConfig

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
        A2CConfig()
        .rollouts(num_rollout_workers=0)
        .framework("torch")
        .environment("CartPole-v1")
        .training(lr=0.001, train_batch_size=40)
    )

    stop_reward = 150

    tuner = tune.Tuner(
        A2C,
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
        check_learning_achieved(results, stop_reward)
