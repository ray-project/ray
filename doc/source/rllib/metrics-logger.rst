.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-metric-logger-doc:

MetricsLogger API
==================

Overview
--------

The RLlib team has designed the :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` API
to unify and make accessible the logging and processing of stats and metrics during your
reinforcement learning (RL) experiments. RLlib's :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
class and all its sub-components have one :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger`
instance and use it to manage their own their stats and
metrics and send the results upstream to the parent component.

You are encouraged to use this API in all your custom code, like in your
:py:class:`~ray.rllib.env.env_runner.EnvRunner`-based :ref:`callbacks <rllib-callback-docs>`,
in your `custom loss functions <>`__, or in custom `training_step <>`__ implementations.

.. figure:: images/metrics_logger_overview.svg
    :width: 500
    :align: left

    **RLlib's MetricsLogger system**: Every sub-components of an RLlib Algorithm has-a
    :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` instance
    and uses it to locally log metrics and stats. Once a component has completed a distinct task,
    for example, a sampling request to an :py:class:`~ray.rllib.env.env_runner.EnvRunner`, the local metrics of the sub-component are "reduced"
    and sent upstream to the containing parent component, for example the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.
    The parent component merges the arriving results into its own :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` and,
    at the end of its own task cycle, "reduces" as well for final reporting to the user or Ray Tune.


.. note::
    So far, RLlib components owning a :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger`
    instance are `Algorithm`, :py:class:`~ray.rllib.env.env_runner.EnvRunner`, and :py:class:`~ray.rllib.core.learner.learner.Learner`,
    but the Ray team is considering expanding access to this API on other components as well.


Features of MetricsLogger
-------------------------

The :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` API offers the following features:

- Users can `log scalar values over time`, such as losses or rewards.
- Thereby, users can configure different reduction types, in particular "mean", "min", "max", and "sum".
- Users can also specify sliding windows, over which the reductions take place, for example "mean over
  the last 100 logged values", or specify "exponential moving average" (EMA) coefficients, through
  which the weight of older values in the computed mean should decay over time.
- Users can merge ``n`` result dicts from ``n`` parallel sub-components, each of which is the result
  of a "reduce" operation on each sub-component's own :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger`
  instance.
- Logging execution times for distinct code blocks through convenient ``with ...`` blocks.
- Adding up lifetime counts and automatically computing the corresponding throughput metric along the way.


Built-in usages of MetricsLogger
--------------------------------

RLlib uses the :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` API extensively in the
existing code-base. The following is an overview of a typical information flow resulting from this:

#. Algorithm sends parallel sample requests to its ``n`` EnvRunner actors.
#. Each EnvRunner collects training data by stepping through its RL environment and logs to its MetricsLogger some standard stats, such as episode return, episode length, etc..
#. Each EnvRunner calls "reduce" on its own MetricsLogger instance and returns the resulting stats dict.
#. Algorithm merges the ``n`` received EnvRunner stats dicts into its own MetricsLogger instance under the top-level key "env_runners", thereby making sure all log-settings chosen by the EnvRunner actors are respected.
#. Algorithm sends parallel update requests to its ``m`` Learner actors.
#. Each Learner performs a model update through computing losses and gradients and logs some standard stats to its MetricsLogger, such as total loss, mean gradient, etc..
#. Each Learner calls "reduce" on its own MetricsLogger instance and returns the resulting stats dict.
#. Algorithm merges the ``m`` received Learner stats dicts into its own MetricsLogger instance under the top-level key "learners", thereby making sure all log-settings chosen by the Learner actors are respected.
#. Algorithm may add its own top-level stats to its own MetricsLogger instance, for example the average time it takes for a parallel sample request to be completed.
#. Algorithm calls "reduce" on its own MetricsLogger instance, thereby compiling a final, all-including stats dict to be returned to the user or Ray Tune.


The MetricsLogger APIs in detail
--------------------------------

Before you can use MetricsLogger in your custom code, you should familiarize yourself with how to actually use its APIs.

Logging scalar values
~~~~~~~~~~~~~~~~~~~~~

To log a scalar value under some string key in your :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger`,
use the :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.log_value` method:

.. testcode::

    from ray.rllib.utils.metrics.metrics_logger import MetricsLogger

    logger = MetricsLogger()

    # Log a scalar float value under the "loss" key. By default, all logged
    # values under that key are averaged, once `reduce()` is called.
    logger.log_value("loss", 0.01, reduce="mean", window=2)

By default, MetricsLogger reduces values through avgeraging them (``reduce="mean"``).
Other available reduce types are ``reduce="min"``, ``reduce="max"``, and ``reduce="sum"``.

Specifying a ``window`` causes the reduction to take place over the last ``n`` logged values
(``n=window``). For example, you can continue logging new values under the "loss" key:

.. testcode::
    logger.log_value("loss", 0.02)  # don't have to repeat `reduce` or `window` args,
                                    # because the key already exists.
    logger.log_value("loss", 0.03)
    logger.log_value("loss", 0.04)
    logger.log_value("loss", 0.05)

Because you specified a window of 2, only the last 2 values are used to compute the reduced result.
You can "peek" at the currently reduced result throug the :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.peek` method:

    # Peek at the current (reduced) value.
    # Note that in the underlying structure, the internal values list still
    # contains all logged values (0.01, 0.02, 0.03, 0.04, and 0.05).
    print(logger.peek("loss"))  # Expect: 0.045, which is the average over the last 2 values

The :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.peek` method allows you to
check the current underlying reduced result for some key, without actually having to call
:py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.reduce`.

.. warning::
    You **shouldn't call the :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.reduce` method ever** on any
    :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` object in your custom code.
    The only time this API should be invoked is at the end of a task cycle.
    RLlib controls all of these "hand over" points entirely, so unless you write your own subcomponent that reports to a parent component, such as
    :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`, you should refrain from calling the :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.reduce`
    method on any :py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger`.

    To get the current reduced result, use the :py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.peek` method instead,
    which doesn't alter any underlying values.


Instead of providing a flat key, you can also log a value under some nested key through passing in a tuple:

.. testcode::

    # Log a value under a deeper nested key.
    logger.log_value(("some", "nested", "key"), -1.0)
    print(logger.peek(("some", "nested", "key")))  # expect: -1.0


To use reduce methods, other than "mean", specify the ``reduce`` argument in
:py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.log_value`:

.. testcode::

    # Log a maximum value.
    logger.log_value(key="max_value", value=0.0, reduce="max")

Because you didn't specify a ``window`` and are using ``reduce="max"``, the window used is the infinite window,
meaning MetricsLogger reports the lifetime maximum value, whenever reduction takes place or you peek at the current value:

.. testcode::

    for i in range(1000, 0, -1):
        logger.log_value(key="max_value", value=float(i))

    logger.peek("max_value")  # expect: 1000.0, which is the lifetime max (infinite window)


You can also chose to not reduce at all, but to simply collect individual values, for example a set of images you receive
from your environment over time and for which it doesn't make sense to reduce them in any way.

Use the ``reduce=None`` argument for achieving this. However, it is stongly advised that you should also
set the ``clear_on_reduce=True`` flag, because this may cause memory leaks otherwise.
This flag assures that the underlying list of values is cleared out after every "reduce" handover operation, for example
from :py:class:`~ray.rllib.env.env_runner.EnvRunner` to :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`:

.. testcode::

    logger.log_value("some_items", value="a", reduce=None, clear_on_reduce=True)
    logger.log_value("some_items", value="b")
    logger.log_value("some_items", value="c")
    logger.log_value("some_items", value="d")

    logger.peek("some_items")  # expect a list: ["a", "b", "c", "d"]

    logger.reduce()
    logger.peek("some_items")  # expect an empty list: []

Logging non-scalar data
~~~~~~~~~~~~~~~~~~~~~~~

:py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` isn't limited to scalar values.
You can use it also to log images, videos, or any other complex data.

Normally, you would chose the previously described ``reduce=None`` argument. For example, to
log three consecutive image frames from a ``CartPole`` environment, do the following:

.. testcode::

    import gymnasium as gym

    env = gym.make("CartPole-v1")

    # Log three consecutive render frames from the env.
    # Make sure to set ``clear_on_reduce=True`` to avoid memory leaks.
    env.reset()
    logger.log_value("some_images", value=env.render(), reduce=None, clear_on_reduce=True)
    env.step(0)
    logger.log_value("some_images", value=env.render())
    env.step(1)
    logger.log_value("some_images", value=env.render())


Timing things
~~~~~~~~~~~~~

:py:class:`~ray.rllib.utils.metrics_logger.MetricsLogger` is context capable and offers the following
simple API to log timer values:

.. testcode::

    import time
    from ray.rllib.utils.metrics.metrics_logger import MetricsLogger

    logger = MetricsLogger()

    # First delta measurement:
    with logger.log_time("my_block_to_be_timed", reduce="mean", ema_coeff=0.1):
        time.sleep(1.0)

    # EMA should be ~1sec.
    assert 1.1 > logger.peek("my_block_to_be_timed") > 0.9

    # Second delta measurement:
    with logger.log_time("my_block_to_be_timed"):
        time.sleep(2.0)

    # EMA should be ~1.1sec.
    assert 1.15 > logger.peek("my_block_to_be_timed") > 1.05

Notice that you can now time all your code blocks of interest inside your custom code through a single ``with-`` line.
Also note that the default logging behavior through EMA, with a default EMA-coefficient of 0l01 is usually a good choice for
averaging these timings over time.

Counting things
~~~~~~~~~~~~~~~

In case you want to count things, for example the number of environment steps taken in a sample phase, and add up those
counts either over the lifetime or over some particular phase, use the ``reduce="sum"`` argument in the call to
:py:meth:`~ray.rllib.utils.metrics_logger.MetricsLogger.log_value`.

Combine this with ``clear_on_reduce=True``, if you want the count to only accumulate until the next "reduce" event:

.. testcode::

    from ray.rllib.utils.metrics.metrics_logger import MetricsLogger

    logger = MetricsLogger()

    logger.log_value("my_counter", 50, reduce="sum", window=None)
    logger.log_value("my_counter", 25)
    logger.peek("my_counter")  # expect: 75

    # Even if your logger gets "reduced" from time to time, the counter keeps increasing
    # because we set clear_on_reduce=False (default behavior):
    logger.reduce()
    logger.peek("my_counter")  # still expect: 75

    # To clear the sum after each "reduce" event, set `clear_on_reduce=True`:
    logger.log_value("my_temp_counter", 50, reduce="sum", window=None, clear_on_reduce=True)
    logger.log_value("my_temp_counter", 25)
    logger.peek("my_counter")  # expect: 75
    logger.reduce()
    logger.peek("my_counter")  # expect: 0 (upon reduction, all values are cleared)

Automatic throughput measurements
+++++++++++++++++++++++++++++++++

You can


Example 1: How to use MetricsLogger in EnvRunner callbacks
----------------------------------------------------------

To demonstrate how to use the MetricsLogger on an EnvRunner, take a look at this end-to-end example here
that makes use of the RLlibCallback API to inject custom code into the RL environment loop.

Note that this example here is identical to the one described here, but the focus of the
comments here have shifted to only explain the MetricsLogger aspects of the code:

The example computes the average "first-joint angle" of the
`Acrobot-v1 RL environment <https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/acrobot.py>`__
environment and logs the results through the MetricsLogger API. You should be able to find these
stats in the returned training iteration results as shown in the code below:

.. figure:: images/acrobot-v1.png
    :width: 150
    :align: left

    **The Acrobot-v1 environment**: The env's code described the angle you are about to
    compute and log through your custom callback as:

    .. code-block:: text
        `theta1` is the angle of the first joint, where an angle of 0.0 indicates that the first
        link is pointing directly downwards.

The example utilizes RLlib's :py:class:`~ray.rllib.utils.metrics.metrics_logger.MetricsLogger`
API to log the custom computations happening in the injected code your Algorithm's main results system.

.. todo: uncomment this once metrics-logger.rst page is online.
   Read :ref:`more about the MetricsLogger API here <>`__ or also

Also take a look at this more complex example on `how to generate and log a PacMan heatmap (image) to WandB <https://github.com/ray-project/ray/blob/master/rllib/examples/metrics/custom_metrics_in_env_runners.py>`__.

.. testcode::

    import math
    import numpy as np
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.callbacks.callbacks import RLlibCallback

    class LogAcrobotAngle(RLlibCallback):
        def on_episode_step(self, *, episode, env, **kwargs):
            # First get the angle from the env (note that `env` is a VectorEnv).
            # See https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/acrobot.py
            # for the env's source code.
            cos_theta1, sin_theta1 = env.envs[0].unwrapped.state[0], env.envs[0].unwrapped.state[1]
            # Convert cos/sin/tan into degree.
            deg_theta1 = math.degrees(math.atan2(sin_theta1, cos_theta1))

            # Log the theta1 degree value in the episode object, temporarily.
            episode.add_temporary_timestep_data("theta1", deg_theta1)

        def on_episode_end(self, *, episode, metrics_logger, **kwargs):
            # Get all the logged theta1 degree values and average them.
            theta1s = episode.get_temporary_timestep_data("theta1")
            avg_theta1 = np.mean(theta1s)

            # Log the final result - per episode - to the MetricsLogger.
            # Report with a sliding/smoothing window of 50.
            metrics_logger.log_value("theta1_mean", avg_theta1, reduce="mean", window=50)

    config = (
        PPOConfig()
        .environment("Acrobot-v1")
        .callbacks(
            callbacks_class=LogAcrobotAngle,
        )
    )
    ppo = config.build()

    # Train n times. Expect `theta1_mean` to be found in the results under:
    # `env_runners/theta1_mean`
    for i in range(10):
        results = ppo.train()
        print(
            f"iter={i} "
            f"theta1_mean={results['env_runners']['theta1_mean']} "
            f"R={results['env_runners']['episode_return_mean']}"
        )



Example 2: How to use MetricsLogger in custom loss functions
------------------------------------------------------------

Example 3: How to use MetricsLogger in a custom Algorithm
---------------------------------------------------------



### Logging Metrics in EnvRunners

`MetricsLogger` is used extensively in EnvRunners to log environment-specific metrics during episode sampling.
For example, the `MsPacmanHeatmapCallback` demonstrates how to track and log:

- Pacman's positions to create 2D heatmaps.
- Maximum and mean distance traveled by Pacman per episode.
- An EMA-smoothed number of lives at each timestep.

Example:

```python
class MsPacmanHeatmapCallback(DefaultCallbacks):
    def on_episode_step(self, *, episode, metrics_logger, env, **kwargs):
        # Log custom timestep data, such as Pacman’s position.
        yx_pos = self._get_pacman_yx_pos(env)
        episode.add_temporary_timestep_data("pacman_yx_pos", yx_pos)

    def on_episode_end(self, *, episode, metrics_logger, **kwargs):
        # Log a heatmap and summary metrics at the end of an episode.
        metrics_logger.log_value("pacman_heatmap", heatmap_data, reduce=None)
        metrics_logger.log_value("pacman_mean_distance", mean_distance, reduce="mean")
```

### Using MetricsLogger in Custom Loss Functions

You can log custom metrics during loss computation, providing insights into intermediate training states.

Example:

```python
def custom_loss(policy, model, dist_class, train_batch):
    loss = ...  # Compute custom loss
    policy.metrics_logger.log_value("loss/custom_loss", loss.item(), reduce="mean")
    return loss
```

### MetricsLogger in Algorithm.training_step()

The `training_step()` method is a key location to log training metrics. This is particularly useful for tracking overall progress and debugging.

Example:

```python
class CustomAlgorithm(Algorithm):
    def training_step(self):
        metrics_logger = self.metrics_logger
        loss = ...  # Compute loss
        metrics_logger.log_value("training/loss", loss, reduce="mean")

        return {"loss": loss}
```

Advanced Features
------------------

### Time Profiling

The `MetricsLogger` can measure and log execution times of code blocks for profiling purposes. Use the `log_time()` method within a `with` block to track durations.

Example:

```python
with logger.log_time("block_execution_time", reduce="mean", ema_coeff=0.1):
    time.sleep(1)
```

### Merging Metrics

Metrics from multiple components, such as parallel EnvRunners, can be aggregated using `merge_and_log_n_dicts()`. This is essential for distributed training.

Example:

```python
main_logger = MetricsLogger()
logger1 = MetricsLogger()
logger2 = MetricsLogger()

logger1.log_value("metric", 1, reduce="sum")
logger2.log_value("metric", 2, reduce="sum")

main_logger.merge_and_log_n_dicts([
    logger1.reduce(),
    logger2.reduce()
])

print(main_logger.peek("metric"))  # Outputs: 3
```

### Heatmap Example from MsPacman

In the `MsPacmanHeatmapCallback`, a heatmap of Pacman's positions is created and logged as an image metric.

Example:

```python
def _get_pacman_yx_pos(self, env):
    image = env.render()
    image = resize(image, 100, 100)
    mask = (image[:, :, 0] > 200) & (image[:, :, 1] < 175)
    coords = np.argwhere(mask)
    return coords.mean(axis=0).astype(int) if coords.size else (-1, -1)
```

The heatmap is then visualized and logged:

```python
heatmap = np.zeros((80, 100), dtype=np.int32)
for yx_pos in positions:
    heatmap[yx_pos[0], yx_pos[1]] += 1

metrics_logger.log_value("pacman_heatmap", heatmap, reduce=None)
```
