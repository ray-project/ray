import argparse

from rllib_apex_dqn.apex_dqn import ApexDQN, ApexDQNConfig

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
        ApexDQNConfig()
        .rollouts(num_rollout_workers=3)
        .environment("CartPole-v1")
        .training(
            replay_buffer_config={
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 20000,
            },
            num_steps_sampled_before_learning_starts=1000,
            optimizer={"num_replay_buffer_shards": 2},
            target_network_update_freq=500,
            training_intensity=4,
        )
        .resources(num_gpus=0)
        .reporting(min_sample_timesteps_per_iteration=1000, min_time_s_per_iteration=5)
    )

    stop_reward = 150.0

    tuner = tune.Tuner(
        ApexDQN,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 250000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(results, stop_reward)
