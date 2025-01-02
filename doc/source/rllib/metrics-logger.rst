.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _rllib-metric-logger-doc:

MetricsLogger API
==================

Overview
--------

The `MetricsLogger` API in RLlib is designed to streamline the logging and processing of
metrics during reinforcement learning (RL) training and evaluation. It serves as the
primary mechanism for collecting, reducing, and reporting metrics in RLlib's architecture, and
users are encouraged to utilize this API inside their own custom code, for example custom callbacks,
custom loss functions, or custom algorithms.

The `MetricsLogger` API supports:

- Logging scalar values, such as losses or rewards, over time or training iterations.
- Logging complex data structures like dictionaries or nested statistics.
- Applying reduction methods like mean, sum, min, or max to reduce logged values over time or across parallel components, such as EnvRunner or Learner.
- Measuring and logging time-based metrics for performance profiling and automatic throughput measurements.

Every RLlib component, including `Algorithm`, `EnvRunner`, and `Learner`, has an instance of `MetricsLogger` to collect metrics within these
processes locally. When appropriate, local metrics are reduced and then passed upstream for aggregation (along the time axis and across
parallel components) and final reporting.


Using MetricsLogger in RLlib
----------------------------

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

Summary
-------

The `MetricsLogger` API is a versatile tool for managing metrics across RLlib's hierarchical architecture. Its integration into all major components and support for flexible reduction, time profiling, and distributed aggregation makes it indispensable for RLlib users.
