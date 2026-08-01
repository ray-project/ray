"""Example showing how to define a custom LoggerCallback for an RLlib Algorithm.

The script uses a custom ``LoggerCallback`` passed via ``RunConfig(callbacks=[...])``.

Below examples include:
- Defining a custom logger callback (by sub-classing ``LoggerCallback``).


How to run this script
----------------------
`python [script file name].py`


Results to expect
-----------------
You should see log lines similar to the following in your console output. Note that
these logged lines will mix with the ones produced by Tune's default ProgressReporter.

ABC Avg-return: 20.609375; pi-loss: -0.02921550187703246
ABC Avg-return: 32.28688524590164; pi-loss: -0.023369029412534572
ABC Avg-return: 51.92; pi-loss: -0.017113141975661456
ABC Avg-return: 76.16; pi-loss: -0.01305474770361625
ABC Avg-return: 100.54; pi-loss: -0.007665307738129169
ABC Avg-return: 132.33; pi-loss: -0.005010405003325517
ABC Avg-return: 169.65; pi-loss: -0.008397869592997183
ABC Avg-return: 203.17; pi-loss: -0.005611495616764371

"""

from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.tune.logger import LoggerCallback


class MyPrintLoggerCallback(LoggerCallback):
    """Logs results by simply printing out a summary."""

    def __init__(self, prefix="ABC"):
        self.prefix = prefix

    def log_trial_result(self, iteration, trial, result):
        mean_return = result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        pi_loss = result[LEARNER_RESULTS][DEFAULT_MODULE_ID]["policy_loss"]
        print(f"{self.prefix} Avg-return: {mean_return} pi-loss: {pi_loss}")


if __name__ == "__main__":
    config = PPOConfig().environment("CartPole-v1")

    stop = {f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 200.0}

    # Run the actual experiment (using Tune).
    results = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=tune.RunConfig(
            stop=stop,
            verbose=2,
            # Plugin our own logger callback.
            callbacks=[
                MyPrintLoggerCallback(prefix="ABC"),
            ],
        ),
    ).fit()
