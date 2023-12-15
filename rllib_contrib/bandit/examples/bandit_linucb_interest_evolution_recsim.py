import argparse

from rllib_bandit.bandit import BanditLinUCB, BanditLinUCBConfig

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

    ray.init(local_mode=True)

    config = (
        BanditLinUCBConfig()
        .framework("torch")
        .environment(
            "ray.rllib.examples.env.recommender_system_envs_with_recsim.InterestEvolutionRecSimEnv",  # noqa
            env_config={
                "config": {
                    # Each step, sample `num_candidates` documents using the
                    # env-internal
                    # document sampler model (a logic that creates n documents to select
                    # the slate from).
                    "resample_documents": True,
                    "num_candidates": 100,
                    # How many documents to recommend (out of `num_candidates`) each
                    # timestep
                    "slate_size": 2,
                    "convert_to_discrete_action_space": True,
                    "wrap_for_bandits": True,
                }
            },
        )
        .reporting(metrics_num_episodes_for_smoothing=500)
        .debugging(seed=0)
    )

    stop_reward = 180

    tuner = tune.Tuner(
        BanditLinUCB,
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
