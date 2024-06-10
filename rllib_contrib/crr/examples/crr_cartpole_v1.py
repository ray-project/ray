import argparse

from rllib_crr.crr import CRR, CRRConfig

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
        CRRConfig()
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
            twin_q=True,
            weight_type="exp",
            advantage_type="mean",
            n_action_sample=4,
            target_network_update_freq=10000,
            tau=0.0005,
            gamma=0.99,
            train_batch_size=2048,
            critic_hidden_activation="tanh",
            critic_hiddens=[128, 128, 128],
            critic_lr=0.0003,
            actor_hidden_activation="tanh",
            actor_hiddens=[128, 128, 128],
            actor_lr=0.0003,
            temperature=1.0,
            max_weight=20.0,
        )
        .evaluation(
            evaluation_interval=1,
            evaluation_num_workers=1,
            evaluation_duration=10,
            evaluation_duration_unit="episodes",
            evaluation_parallel_to_training=True,
            evaluation_config=CRRConfig.overrides(input_="sampler", explore=False),
        )
        .rollouts(num_rollout_workers=3)
    )

    stop_reward = 200

    tuner = tune.Tuner(
        CRR,
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
