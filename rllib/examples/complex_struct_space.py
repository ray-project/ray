"""Example of using variable-length Repeated / struct observation spaces.

This example shows:
  - using a custom environment with Repeated / struct observations
  - using a custom model to view the batched list observations

For PyTorch / TF eager mode, use the --torch and --eager flags.
"""

import argparse

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.examples.env.simple_rpg import SimpleRPG
from ray.rllib.examples.models.simple_rpg_model import CustomTorchRPGModel, \
    CustomTFRPGModel

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework", choices=["tf", "tfe", "torch"], default="tf")
parser.add_argument("--eager", action="store_true")

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()
    if args.framework == "torch":
        ModelCatalog.register_custom_model("my_model", CustomTorchRPGModel)
    else:
        ModelCatalog.register_custom_model("my_model", CustomTFRPGModel)
    tune.run(
        "PG",
        stop={
            "timesteps_total": 1,
        },
        config={
            "framework": args.framework,
            "eager": args.eager,
            "env": SimpleRPG,
            "rollout_fragment_length": 1,
            "train_batch_size": 2,
            "num_workers": 0,
            "model": {
                "custom_model": "my_model",
            },
        },
    )
