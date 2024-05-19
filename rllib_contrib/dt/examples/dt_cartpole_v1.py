import argparse

from rllib_dt.dt import DT, DTConfig

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
        DTConfig()
        .environment(env="CartPole-v1", clip_actions=True)
        .framework("torch")
        .offline_data(
            input_="dataset",
            input_config={
                "format": "json",
                "paths": ["s3://anonymous@air-example-data/rllib/cartpole/large.json"],
            },
            actions_in_input_normalized=True,
        )
        .training(
            train_batch_size=512,
            lr=0.01,
            optimizer={
                "weight_decay": 0.1,
                "betas": [0.9, 0.999],
            },
            replay_buffer_config={"capacity": 20},
            # model
            model={"max_seq_len": 3},
            num_layers=1,
            num_heads=1,
            embed_dim=64,
            horizon=500,
        )
        .evaluation(
            evaluation_interval=1,
            evaluation_num_workers=1,
            evaluation_duration=10,
            target_return=200,
            evaluation_duration_unit="episodes",
            evaluation_parallel_to_training=True,
            evaluation_config=DTConfig.overrides(input_="sampler", explore=False),
        )
        # Episode horizon: Must match environment's time limit, if any.
        .rollouts(num_rollout_workers=3)
        .reporting(min_train_timesteps_per_iteration=5000)
    )

    stop_reward = 200

    tuner = tune.Tuner(
        DT,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "evaluation/sampler_results/episode_reward_mean": stop_reward,
                "training_iteration": 100,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(
            results,
            stop_reward,
            metric="evaluation/sampler_results/episode_reward_mean",
        )
