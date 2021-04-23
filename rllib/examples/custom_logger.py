"""
This example script demonstrates how one can define a custom logger
object for any RLlib Trainer via the Trainer's config dict's
"logger_config" key.
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
from ray.tune.logger import Logger

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150.0)


class MyPrintLogger(Logger):
    """Logs results by simply printing out everything.
    """

    def _init(self):
        # Custom init function.
        print("Initializing ...")
        # Setting up our log-line prefix.
        self.prefix = self.config.get("prefix")

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
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "env": "CartPole-v0"
        if args.run not in ["DDPG", "TD3"] else "Pendulum-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2.
        "eager_tracing": args.framework in ["tfe", "tf2"],

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
        "logger_config": {
            # Provide the class directly or via fully qualified class
            # path.
            "type": MyPrintLogger,
            # `config` keys:
            "prefix": "ABC",
            # Optional: Custom logdir (do not define this here
            # for using ~/ray_results/...).
            # "logdir": "/somewhere/on/my/file/system/"
        }
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(
        args.run, config=config, stop=stop, verbose=2, loggers=[MyPrintLogger])

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
