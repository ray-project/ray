import argparse
import os

import ray
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.examples.policy.bare_metal_policy_with_custom_view_reqs import (
    BareMetalPolicyWithCustomViewReqs,
)
from ray import tune


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

    # Create q custom Trainer class using our custom Policy.
    BareMetalPolicyTrainer = build_trainer(
        name="MyPolicy", default_policy=BareMetalPolicyWithCustomViewReqs
    )

    config = {
        "env": "CartPole-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "model": {
            # Necessary to get the whole trajectory of 'state_in_0' in the
            # sample batch.
            "max_seq_len": 1,
        },
        "num_workers": 1,
        # NOTE: Does this have consequences?
        # I use it for not loading tensorflow/pytorch.
        "framework": None,
        "log_level": "DEBUG",
        "create_env_on_driver": True,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Train the Trainer with our policy.
    results = tune.run(BareMetalPolicyTrainer, config=config, stop=stop)
    print(results)
