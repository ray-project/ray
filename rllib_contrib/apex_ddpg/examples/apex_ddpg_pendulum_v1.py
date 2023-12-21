import argparse

from rllib_apex_ddpg.apex_ddpg import ApexDDPG, ApexDDPGConfig

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
        ApexDDPGConfig()
        .rollouts(num_rollout_workers=3)
        .framework("torch")
        .environment("Pendulum-v1", clip_rewards=False)
        .training(n_step=1, target_network_update_freq=50000, tau=1.0, use_huber=True)
        .evaluation(evaluation_interval=5, evaluation_duration=10)
    )

    stop_reward = -320

    tuner = tune.Tuner(
        ApexDDPG,
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
