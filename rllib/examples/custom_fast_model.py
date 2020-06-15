"""Example of using a custom image env and model.

Both the model and env are trivial (and super-fast), so they are useful
for running perf microbenchmarks.
"""

import argparse

import ray
import ray.tune as tune
from ray.tune import sample_from
from ray.rllib.examples.env.fast_image_env import FastImageEnv
from ray.rllib.examples.models.fast_model import FastModel, TorchFastModel
from ray.rllib.models import ModelCatalog

parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=2)
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)

    ModelCatalog.register_custom_model(
        "fast_model", TorchFastModel if args.torch else FastModel)

    config = {
        "env": FastImageEnv,
        "compress_observations": True,
        "model": {
            "custom_model": "fast_model"
        },
        "num_gpus": 0,
        "num_workers": 2,
        "num_envs_per_worker": 10,
        "num_data_loader_buffers": 1,
        "num_aggregation_workers": 1,
        "broadcast_interval": 50,
        "rollout_fragment_length": 100,
        "train_batch_size": sample_from(
            lambda spec: 1000 * max(1, spec.config.num_gpus)),
        "fake_sampler": True,
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
    }

    tune.run("IMPALA", config=config, stop=stop)

    ray.shutdown()
