"""
This example script demonstrates how one can define a custom logger
object for any RLlib Algorithm via the Algorithm's config's `logger_config` property.
By default (logger_config=None), RLlib will construct a tune
UnifiedLogger object, which logs JSON, CSV, and TBX output.

Below examples include:
- Disable logging entirely.
- Using only one of tune's Json, CSV, or TBX loggers.
- Defining a custom logger (by sub-classing tune.logger.py::Logger).
"""

import argparse
import os

from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import Logger, LegacyLoggerCallback
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)


class MyPrintLogger(Logger):
    """Logs results by simply printing out everything."""

    def _init(self):
        # Custom init function.
        print("Initializing ...")
        # Setting up our log-line prefix.
        self.prefix = self.config.get("logger_config").get("prefix")

    def on_result(self, result: dict):
        # Define, what should happen on receiving a `result` (dict).
        print(f"{self.prefix}: {result}")

    def close(self):
        # Releases all resources used by this logger.
        print("Closing")

    def flush(self):
        # Flushing all possible disk writes to permanent storage.
        print("Flushing ;)", flush=True)


if __name__ == "__main__":
    import ray
    from ray import air, tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(
            "CartPole-v1" if args.run not in ["DDPG", "TD3"] else "Pendulum-v1"
        )
        # Run with tracing enabled for tf2.
        .framework(args.framework)
        # Setting up a custom logger config.
        # ----------------------------------
        # The following are different examples of custom logging setups:
        # 1) Disable logging entirely.
        # "logger_config": {
        #     # Use the tune.logger.NoopLogger class for no logging.
        #     "type": "ray.tune.logger.NoopLogger",
        # },
        # 2) Use tune's JsonLogger only.
        # Alternatively, use `CSVLogger` or `TBXLogger` instead of
        # `JsonLogger` in the "type" key below.
        # "logger_config": {
        #     "type": "ray.tune.logger.JsonLogger",
        #     # Optional: Custom logdir (do not define this here
        #     # for using ~/ray_results/...).
        #     "logdir": "/tmp",
        # },
        # 3) Custom logger (see `MyPrintLogger` class above).
        .debugging(
            logger_config={
                # Provide the class directly or via fully qualified class
                # path.
                "type": MyPrintLogger,
                # `config` keys:
                "prefix": "ABC",
                # Optional: Custom logdir (do not define this here
                # for using ~/ray_results/...).
                # "logdir": "/somewhere/on/my/file/system/"
            }
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
            callbacks=[LegacyLoggerCallback(MyPrintLogger)],
        ),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
