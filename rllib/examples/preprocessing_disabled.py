"""Example of setting preprocessor_pref=None to disable all preprocessing.

This example shows:
  - How a complex observation space from the env is handled directly by the
    model.
  - Complex observations are flattened into lists of tensors and as such
    stored by the SampleCollectors.
  - This has the advantage that preprocessing happens in batched fashion
    (in the model).
"""
import argparse
from gym.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import numpy as np
import os

import ray
from ray import tune


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    parser.add_argument(
        "--no-attention",
        action="store_true",
        help="Do NOT use attention. For comparison: The agent will not learn.")

    # general args
    parser.add_argument(
        "--run", default="PPO", help="The RLlib-registered algorithm to use.")
    parser.add_argument("--num-cpus", type=int, default=3)
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "tfe", "torch"],
        default="tf",
        help="The DL framework specifier.")
    parser.add_argument(
        "--stop-iters",
        type=int,
        default=200,
        help="Number of iterations to train.")
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=500000,
        help="Number of timesteps to train.")
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=80.0,
        help="Reward at which we stop training.")
    parser.add_argument(
        "--as-test",
        action="store_true",
        help="Whether this script should be run as a test: --stop-reward must "
        "be achieved within --stop-timesteps AND --stop-iters.")
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.")
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.")

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()
    assert args.framework == "tf",\
        "No-preprocessing only working for tf so far! Complex input " \
        "model must be changed to accept Dict spaces as well (besides " \
        "Tuples)."

    ray.init(local_mode=args.local_mode)

    config = {
        "env": "ray.rllib.examples.env.random_env.RandomEnv",
        "env_config": {
            "config": {
                "observation_space": Dict({
                    "a": Discrete(2),
                    "b": Dict({
                        "ba": Discrete(3),
                        "bb": Box(-1.0, 1.0, (2, 3), dtype=np.float32)
                    }),
                    "c": Tuple((MultiDiscrete([2, 3]), Discrete(2))),
                    "d": Box(-1.0, 1.0, (2, ), dtype=np.int32),
                }),
            },
        },
        # Set this to None to enforce no preprocessors being used.
        # Complex observations now arrive directly in the model as
        # structures of batches, e.g. {"a": tensor, "b": [tensor, tensor]}
        # for obs-space=Dict(a=..., b=Tuple(..., ...)).
        "preprocessor_pref": None,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", 0)),
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    ray.shutdown()
