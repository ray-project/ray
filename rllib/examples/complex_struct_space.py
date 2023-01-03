"""Example of using variable-length Repeated / struct observation spaces.

This example shows:
  - using a custom environment with Repeated / struct observations
  - using a custom model to view the batched list observations

For PyTorch / TF eager mode, use the `--framework=[torch|tf2]` flag.
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.models import ModelCatalog
from ray.rllib.examples.env.simple_rpg import SimpleRPG
from ray.rllib.examples.models.simple_rpg_model import (
    CustomTorchRPGModel,
    CustomTFRPGModel,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf2",
    help="The DL framework specifier.",
)

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    if args.framework == "torch":
        ModelCatalog.register_custom_model("my_model", CustomTorchRPGModel)
    else:
        ModelCatalog.register_custom_model("my_model", CustomTFRPGModel)

    config = (
        PGConfig()
        .environment(SimpleRPG)
        .framework(args.framework)
        .rollouts(rollout_fragment_length=1, num_rollout_workers=0)
        .training(train_batch_size=2, model={"custom_model": "my_model"})
        .experimental(_disable_preprocessor_api=False)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "timesteps_total": 1,
    }

    tuner = tune.Tuner(
        "PG",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
