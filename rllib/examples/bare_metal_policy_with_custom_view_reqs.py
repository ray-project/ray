import argparse
import os

import ray
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.examples.policy.bare_metal_policy_with_custom_view_reqs import (
    BareMetalPolicyWithCustomViewReqs,
)
from ray import air, tune


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # general args
    parser.add_argument(
        "--run", default="PPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument("--num-cpus", type=int, default=3)
    parser.add_argument(
        "--stop-iters", type=int, default=200, help="Number of iterations to train."
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=100000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # Create q custom Algorithm class using our custom Policy.
    class BareMetalPolicyAlgorithm(Algorithm):
        @classmethod
        def get_default_policy_class(cls, config):
            return BareMetalPolicyWithCustomViewReqs

    config = (
        AlgorithmConfig()
        .environment("CartPole-v1")
        .rollouts(num_rollout_workers=1, create_env_on_local_worker=True)
        .training(
            model={
                # Necessary to get the whole trajectory of 'state_in_0' in the
                # sample batch.
                "max_seq_len": 1,
            }
        )
        .debugging(log_level="DEBUG")
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )
    # NOTE: Does this have consequences?
    # I use it for not loading tensorflow/pytorch.
    config.framework_str = None

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Train the Algorithm with our policy.
    results = tune.Tuner(
        BareMetalPolicyAlgorithm,
        param_space=config,
        run_config=air.RunConfig(stop=stop),
    ).fit()
    print(results.get_best_result())
