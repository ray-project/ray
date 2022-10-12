"""Example of using a custom image env and model.

Both the model and env are trivial (and super-fast), so they are useful
for running perf microbenchmarks.
"""

import argparse
import os

import ray
from ray import air, tune
from ray.tune import sample_from
from ray.rllib.examples.env.fast_image_env import FastImageEnv
from ray.rllib.examples.models.fast_model import FastModel, TorchFastModel
from ray.rllib.models import ModelCatalog

parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=4)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)

    ModelCatalog.register_custom_model(
        "fast_model", TorchFastModel if args.framework == "torch" else FastModel
    )

    config = {
        "env": FastImageEnv,
        "compress_observations": True,
        "model": {"custom_model": "fast_model"},
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 2,
        "num_envs_per_worker": 10,
        "num_multi_gpu_tower_stacks": 1,
        "num_aggregation_workers": 1,
        "broadcast_interval": 50,
        "rollout_fragment_length": 100,
        "train_batch_size": sample_from(
            lambda spec: 1000 * max(1, spec.config.num_gpus or 1)
        ),
        "fake_sampler": True,
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
    }

    tuner = tune.Tuner(
        "IMPALA", param_space=config, run_config=air.RunConfig(stop=stop, verbose=1)
    )
    tuner.fit()

    ray.shutdown()
