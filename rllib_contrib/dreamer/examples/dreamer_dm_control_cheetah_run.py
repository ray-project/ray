import argparse

from rllib_dreamer.dreamer import Dreamer, DreamerConfig

import ray
from ray import air, tune


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
        DreamerConfig()
        .rollouts(
            num_rollout_workers=0,
            batch_mode="complete_episodes",
        )
        .environment("ray.rllib.examples.env.dm_control_suite.cheetah_run")
        .training(
            td_model_lr=0.0006,
            actor_lr=0.00008,
            critic_lr=0.00008,
            gamma=0.99,
            lambda_=0.95,
            dreamer_train_iters=10,
            batch_size=50,
            batch_length=50,
            imagine_horizon=15,
            free_nats=3.0,
        )
        .resources(num_gpus=0)
        .reporting(min_sample_timesteps_per_iteration=1000, min_time_s_per_iteration=5)
    )
    if args.run_as_test:
        timesteps_total = 100
    else:
        timesteps_total = 1000000

    tuner = tune.Tuner(
        Dreamer,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "timesteps_total": timesteps_total,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
