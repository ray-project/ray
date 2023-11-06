import argparse

from rllib_td3.td3 import TD3, TD3Config

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
        TD3Config()
        .framework("torch")
        .environment("Pendulum-v1")
        .training(
            actor_hiddens=[64, 64],
            critic_hiddens=[64, 64],
            replay_buffer_config={"type": "MultiAgentReplayBuffer"},
            num_steps_sampled_before_learning_starts=5000,
        )
        .exploration(exploration_config={"random_timesteps": 5000})
    )

    stop_reward = -900

    tuner = tune.Tuner(
        TD3,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 100000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(results, stop_reward)
