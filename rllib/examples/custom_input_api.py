"""Example of creating a custom input api

Custom input apis are useful when your data source is in a custom format or
when it is necessary to use an external data loading mechanism.
In this example, we train an rl agent on user specified input data.
Instead of using the built in JsonReader, we will create our own custom input
api, and show how to pass config arguments to it.

To train CQL on the pendulum environment:
$ python custom_input_api.py --input-files=../tests/data/pendulum/enormous.zip
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.offline import JsonReader, ShuffledInput, IOContext, InputReader
from ray.tune.registry import get_trainable_cls, register_input

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="CQL", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument("--stop-iters", type=int, default=100)
parser.add_argument(
    "--input-files",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "../tests/data/pendulum/small.json"
    ),
)


class CustomJsonReader(JsonReader):
    """
    Example custom InputReader implementation (extended from JsonReader).

    This gets wrapped in ShuffledInput to comply with offline rl algorithms.
    """

    def __init__(self, ioctx: IOContext):
        """
        The constructor must take an IOContext to be used in the input config.
        Args:
            ioctx: use this to access the `input_config` arguments.
        """
        super().__init__(ioctx.input_config["input_files"], ioctx)


def input_creator(ioctx: IOContext) -> InputReader:
    """
    The input creator method can be used in the input registry or set as the
    config["input"] parameter.

    Args:
        ioctx: use this to access the `input_config` arguments.

    Returns:
        instance of ShuffledInput to work with some offline rl algorithms
    """
    return ShuffledInput(CustomJsonReader(ioctx))


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()

    # make absolute path because relative path looks in result directory
    args.input_files = os.path.abspath(args.input_files)

    # we register our custom input creator with this convenient function
    register_input("custom_input", input_creator)

    # Config modified from rllib/tuned_examples/cql/pendulum-cql.yaml
    default_config = get_trainable_cls(args.run).get_default_config()
    config = (
        default_config.environment("Pendulum-v1", clip_actions=True)
        .framework(args.framework)
        .offline_data(
            # we can either use the tune registry, class path, or direct function
            # to connect our input api.
            input_="custom_input",
            # "input": "ray.rllib.examples.custom_input_api.CustomJsonReader",
            # "input": input_creator,
            # this gets passed to the IOContext
            input_config={"input_files": args.input_files},
            actions_in_input_normalized=True,
        )
        .training(train_batch_size=2000)
        .evaluation(
            evaluation_interval=1,
            evaluation_num_workers=2,
            evaluation_duration=10,
            evaluation_parallel_to_training=True,
            evaluation_config=default_config.overrides(
                input_="sampler",
                explore=False,
            ),
        )
        .reporting(metrics_num_episodes_for_smoothing=5)
    )

    if args.run == "CQL":
        config.training(
            twin_q=True,
            num_steps_sampled_before_learning_starts=0,
            bc_iters=100,
        )

    stop = {
        "training_iteration": args.stop_iters,
        "evaluation/episode_reward_mean": -600,
    }

    tuner = tune.Tuner(
        args.run, param_space=config, run_config=air.RunConfig(stop=stop, verbose=1)
    )
    tuner.fit()
