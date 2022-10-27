"""Example of using custom_loss() with an imitation learning loss.

The default input file is too small to learn a good policy, but you can
generate new experiences for IL training as follows:

To generate experiences:
$ ./train.py --run=PG --config='{"output": "/tmp/cartpole"}' --env=CartPole-v1

To train on experiences with joint PG + IL loss:
$ python custom_loss.py --input-files=/tmp/cartpole
"""

import argparse
from pathlib import Path
import os

import ray
from ray import air, tune
from ray.rllib.examples.models.custom_loss_model import (
    CustomLossModel,
    TorchCustomLossModel,
)
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PG", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument(
    "--input-files",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../tests/data/cartpole/small.json"
    ),
)

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
        "custom_loss",
        TorchCustomLossModel if args.framework == "torch" else CustomLossModel,
    )

    config = {
        "env": "CartPole-v1",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "model": {
            "custom_model": "custom_loss",
            "custom_model_config": {
                "input_files": args.input_files,
            },
        },
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
    }

    tuner = tune.Tuner(
        args.run, param_space=config, run_config=air.RunConfig(stop=stop, verbose=1)
    )
    results = tuner.fit()
    info = results.get_best_result().metrics["info"]

    # Torch metrics structure.
    if args.framework == "torch":
        assert LEARNER_STATS_KEY in info[LEARNER_INFO][DEFAULT_POLICY_ID]
        assert "model" in info[LEARNER_INFO][DEFAULT_POLICY_ID]
        assert "custom_metrics" in info[LEARNER_INFO][DEFAULT_POLICY_ID]

    # TODO: (sven) Make sure the metrics structure gets unified between
    #  tf and torch. Tf should work like current torch:
    #  info:
    #    learner:
    #      [policy_id]
    #        learner_stats: [return values of policy's `stats_fn`]
    #        model: [return values of ModelV2's `metrics` method]
    #        custom_metrics: [return values of callback: `on_learn_on_batch`]
    else:
        assert "model" in info[LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]
