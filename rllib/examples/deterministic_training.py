"""
Example of a fully deterministic, repeatable RLlib train run using
the "seed" config key.
"""
import argparse
import os

import ray
from ray import tune
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    config = {
        "env": "CartPole-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 2,  # parallelism
        "framework": args.framework,
        "seed": args.seed,
    }

    stop = {
        "training_iteration": args.stop_iters,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=1)
    results2 = tune.run(args.run, config=config, stop=stop, verbose=1)

    if args.as_test:
        check(
            list(results.results.values())[0]["hist_stats"],
            list(results2.results.values())[0]["hist_stats"])
    ray.shutdown()
