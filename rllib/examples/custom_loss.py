"""Example of using custom_loss() with an imitation learning loss.

The default input file is too small to learn a good policy, but you can
generate new experiences for IL training as follows:

To generate experiences:
$ ./train.py --run=PG --config='{"output": "/tmp/cartpole"}' --env=CartPole-v0

To train on experiences with joint PG + IL loss:
$ python custom_loss.py --input-files=/tmp/cartpole
"""

import argparse
from pathlib import Path
import os

import ray
from ray import tune
from ray.rllib.examples.models.custom_loss_model import CustomLossModel, \
    TorchCustomLossModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument(
    "--input-files",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../tests/data/cartpole/small.json"))

if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()

    # Bazel makes it hard to find files specified in `args` (and `data`).
    # Look for them here.
    if not os.path.exists(args.input_files):
        # This script runs in the ray/rllib/examples dir.
        rllib_dir = Path(__file__).parent.parent
        input_dir = rllib_dir.absolute().joinpath(args.input_files)
        args.input_files = str(input_dir)

    ModelCatalog.register_custom_model(
        "custom_loss", TorchCustomLossModel if args.torch else CustomLossModel)

    config = {
        "env": "CartPole-v0",
        "num_workers": 0,
        "model": {
            "custom_model": "custom_loss",
            "custom_model_config": {
                "input_files": args.input_files,
            },
        },
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
    }

    tune.run("PG", config=config, stop=stop)
