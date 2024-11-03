"""Example showing how to define a custom Logger class for an RLlib Algorithm.

The script uses the AlgorithmConfig's `debugging` API to setup the custom Logger:

```
config.debugging(logger_config={
    "type": [some Logger subclass],
    "ctor_arg1", ...,
    "ctor_arg2", ...,
})
```

All keys other than "type" in the logger_config dict will be passed into the Logger
class's constructor.
By default (logger_config=None), RLlib will construct a Ray Tune UnifiedLogger object,
which logs results to JSON, CSV, and TBX.

NOTE that a custom Logger is different from a custom `ProgressReporter`, which defines,
how the (frequent) outputs to your console will be formatted. To see an example on how
to write your own Progress reporter, see:
https://github.com/ray-project/ray/tree/master/rllib/examples/ray_tune/custom_progress_reporter.py  # noqa

Below examples include:
- Disable logging entirely.
- Using only one of tune's Json, CSV, or TBX loggers.
- Defining a custom logger (by sub-classing tune.logger.py::Logger).


How to run this script
----------------------
`python [script file name].py`


Results to expect
-----------------
You should see log lines similar to the following in your console output. Note that
these logged lines will mix with the ones produced by Tune's default ProgressReporter.
See above link on how to setup a custom one.

ABC Avg-return: 20.609375; pi-loss: -0.02921550187703246
ABC Avg-return: 32.28688524590164; pi-loss: -0.023369029412534572
ABC Avg-return: 51.92; pi-loss: -0.017113141975661456
ABC Avg-return: 76.16; pi-loss: -0.01305474770361625
ABC Avg-return: 100.54; pi-loss: -0.007665307738129169
ABC Avg-return: 132.33; pi-loss: -0.005010405003325517
ABC Avg-return: 169.65; pi-loss: -0.008397869592997183
ABC Avg-return: 203.17; pi-loss: -0.005611495616764371
Flushing
Closing

"""

from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.tune.logger import Logger, LegacyLoggerCallback


class MyPrintLogger(Logger):
    """Logs results by simply printing out everything."""

    def _init(self):
        # Custom init function.
        print("Initializing ...")
        # Setting up our log-line prefix.
        self.prefix = self.config.get("logger_config").get("prefix")

    def on_result(self, result: dict):
        # Define, what should happen on receiving a `result` (dict).
        mean_return = result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        pi_loss = result[LEARNER_RESULTS][DEFAULT_MODULE_ID]["policy_loss"]
        print(f"{self.prefix} " f"Avg-return: {mean_return} " f"pi-loss: {pi_loss}")

    def close(self):
        # Releases all resources used by this logger.
        print("Closing")

    def flush(self):
        # Flushing all possible disk writes to permanent storage.
        print("Flushing", flush=True)


if __name__ == "__main__":
    config = (
        PPOConfig().environment("CartPole-v1")
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
    )

    stop = {f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0}

    # Run the actual experiment (using Tune).
    results = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=2,
            # Plugin our own logger.
            callbacks=[
                LegacyLoggerCallback([MyPrintLogger]),
            ],
        ),
    ).fit()
