# @OldAPIStack
"""Example of using variable-length Repeated or struct observation spaces.

This example demonstrates the following:
  - using a custom environment with Repeated / struct observations
  - using a custom model to view the batched list observations

For PyTorch / TF eager mode, use the `--framework=[torch|tf2]` flag.
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.models import ModelCatalog
from ray.rllib.examples.envs.classes.simple_rpg import SimpleRPG
from ray.rllib.examples._old_api_stack.models.simple_rpg_model import (
    CustomTorchRPGModel,
    CustomTFRPGModel,
)
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME

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
        PPOConfig()
        .environment(SimpleRPG)
        .framework(args.framework)
        .env_runners(rollout_fragment_length=1, num_env_runners=0)
        .training(train_batch_size=2, model={"custom_model": "my_model"})
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {NUM_ENV_STEPS_SAMPLED_LIFETIME: 1}

    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
