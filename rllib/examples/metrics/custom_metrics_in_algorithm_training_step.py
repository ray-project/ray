"""Example of adding custom metrics to the ResultDict returned by `Algorithm.train()`.

We use the `MetricsLogger` class, which RLlib provides inside all its components (only
when using the new API stack via `config.experimental(_enable_new_api_stack=True)`),
and which offers a unified API to log individual values per iteration, per episode
timestep, per episode (as a whole), per loss call, etc..
`MetricsLogger` objects are available in all custom API code, for example inside your
custom `Algorithm.training_step()` methods, custom loss functions, custom callbacks,
and custom EnvRunners.

In this script, we define a custom `DefaultCallbacks` class and then override its
`on_train_result()` method in order to alter the final `ResultDict` before it is sent
back to Ray Tune (and possibly a WandB logger).

For demonstration purposes only, we add the following simple metrics to this
`ResultDict`:
- The ratio of the delta-times for sampling vs. training. If this ratio is roughly 1.0,
it means we are spending roughly as much time sampling than we do updating our model.
- The number of times an episode reaches an overall return of > 100.0 and the number
of times an episode stays below an overall return of 12.0.
-
"""

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


class CustomOnTrainResultCallback(DefaultCallbacks):
    def on_train_result(
        self,
        *,
        algorithm: "Algorithm",
        result: dict,
        **kwargs,
    ) -> None:
        pass


if __name__ == "__main__":
    pass
