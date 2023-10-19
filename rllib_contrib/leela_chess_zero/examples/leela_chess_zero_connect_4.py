import argparse

from custom_model import DenseModel, PolicyMappingFn
from rllib_leela_chess_zero.leela_chess_zero import LeelaChessZero, LeelaChessZeroConfig

import ray
from ray import air, tune
from ray.rllib.examples.env.pettingzoo_connect4 import MultiAgentConnect4


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
        LeelaChessZeroConfig()
        .rollouts(num_rollout_workers=7)
        .framework("torch")
        .environment(MultiAgentConnect4)
        .training(model={"custom_model": DenseModel, "max_seq_len": 0})
        .multi_agent(
            policies=["p_0", "p_1"],
            policies_to_train=["p_0"],
            policy_mapping_fn={"type": PolicyMappingFn},
        )
    )

    if args.run_as_test:
        stop = {"timesteps_total": 10000}
    else:
        stop = {
            "policy_reward_mean/p_0": 0.9,
            "timesteps_total": 1000000,
        }

    tuner = tune.Tuner(
        LeelaChessZero,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
