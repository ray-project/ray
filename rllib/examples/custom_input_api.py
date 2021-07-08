"""Example of creating a custom input api

To train CQL on the pendulum environment:
$ python custom_input_api.py --input-files=../tests/data/pendulum/enormous.zip
"""

import argparse
import os
from pathlib import Path

import ray
from ray import tune
from ray.rllib.offline import JsonReader, ShuffledInput, IOContext, InputReader
from ray.tune.registry import register_input

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="CQL",
    help="The RLlib-registered algorithm to use.")
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument("--stop-iters", type=int, default=100)
parser.add_argument(
    "--input-files",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../tests/data/pendulum/small.json"))


# example class that subclasses InputReader (from JsonReader)
# this gets wrapped in ShuffledInput to comply with offline rl algorithms
class CustomJsonReader(JsonReader):
    def __init__(self, ioctx: IOContext = None):
        """
        The constructor must take an IOContext to be used in the input config.
        """
        super().__init__(ioctx.input_config["input_files"], ioctx)


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

    # make absolute path because relative path looks in result directory
    args.input_files = os.path.abspath(args.input_files)


    def input_creator(ioctx: IOContext) -> InputReader:
        # must return an instance of ShuffledInput to work with some offline rl
        # algorithms
        return ShuffledInput(
            JsonReader(ioctx.input_config["input_files"], ioctx))


    # we register our custom input creator with this convenient function
    register_input("custom_input", input_creator)

    # config modified from rllib/tuned_examples/cql/pendulum-cql.yaml
    config = {
        "env": "Pendulum-v0",
        # we can either set this as `custom_input` or
        # `ray.rllib.examples.custom_input_api.CustomJsonReader`
        # "input": "custom_input",
        "input": "ray.rllib.examples.custom_input_api.CustomJsonReader",
        # this gets passed to the IOContext
        "input_config": {
            "input_files": args.input_files,
        },
        "framework": args.framework,
        "actions_in_input_normalized": True,
        "clip_actions": True,
        "twin_q": True,
        "train_batch_size": 2000,
        "learning_starts": 0,
        "bc_iters": 100,
        "metrics_smoothing_episodes": 5,
        "evaluation_interval": 1,
        "evaluation_num_workers": 2,
        "evaluation_num_episodes": 10,
        "evaluation_parallel_to_training": True,
        "evaluation_config": {
            "input": "sampler",
            "explore": False,
        }
    }

    stop = {
        "training_iteration": args.stop_iters,
        "evaluation/episode_reward_mean": -600,
    }

    analysis = tune.run(args.run, config=config, stop=stop, verbose=1)
    info = analysis.results[next(iter(analysis.results))]["info"]
