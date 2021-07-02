"""
Example of a fully deterministic, repeatable RLlib train run using
the "seed" config key.
"""
import argparse
import os

import ray
from ray import tune
from ray.rllib.examples.env.env_using_remote_actor import \
    CartPoleWithRemoteParamServer, ParameterStorage
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

    param_storage = ParameterStorage.options(name="param-server").remote()

    config = {
        "env": CartPoleWithRemoteParamServer,
        "env_config": {
            "param_server": "param-server",
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 2,  # parallelism
        "framework": args.framework,
        "seed": args.seed,

        # Simplify to run this example script faster.
        "train_batch_size": 100,
        "sgd_minibatch_size": 10,
        "num_sgd_iter": 5,
        "rollout_fragment_length": 50,
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
