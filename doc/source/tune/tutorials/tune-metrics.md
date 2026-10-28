---
myst:
  html_meta:
    description: "Work with Tune callbacks and metrics: hook into trial events, log custom metrics, and see which metrics Tune fills in automatically."
---

# A guide to callbacks and metrics in Ray Tune

(tune-callbacks)=

## Use callbacks in Tune

Ray Tune supports callbacks that it calls at various points during training. Pass your callbacks to `RunConfig` through its `callbacks` parameter, and pass the `RunConfig` to `Tuner`. Tune then automatically invokes the callback methods you implement.

This callback prints a metric each time Tune receives a result:

```python
from ray import tune
from ray.tune import Callback


class MyCallback(Callback):
    def on_trial_result(self, iteration, trials, trial, result, **info):
        print(f"Got result: {result['metric']}")


def train_fn(config):
    for i in range(10):
        tune.report({"metric": i})


tuner = tune.Tuner(
    train_fn,
    run_config=tune.RunConfig(callbacks=[MyCallback()]))
tuner.fit()
```

For more details and the available hooks, see the {ref}`API docs for Ray Tune callbacks <tune-callbacks-docs>`.

(tune-autofilled-metrics)=

## Log metrics in Tune

You can log arbitrary values and metrics with both the function API and the class API:

```python
def trainable(config):
    for i in range(num_epochs):
        ...
        tune.report({"acc": accuracy, "metric_foo": random_metric_1, "bar": metric_2})

class Trainable(tune.Trainable):
    def step(self):
        ...
        # don't call report here!
        return dict(acc=accuracy, metric_foo=random_metric_1, bar=metric_2)
```

:::{tip}
`tune.report()` isn't meant to transfer large amounts of data, such as models or datasets. Doing so can incur large overhead and significantly slow down your Tune run.
:::

### Which metrics does Tune fill in automatically?

During training, Tune automatically logs a set of auto-filled metrics in addition to any values you report. You can use any of them as a stopping condition or pass them as a parameter to trial schedulers or search algorithms. Tune fills in the following metrics:

* `config`: The hyperparameter configuration
* `date`: String-formatted date and time when the result was processed
* `done`: `True` if the trial has finished, `False` otherwise
* `episodes_total`: Total number of episodes, for RLlib trainables
* `experiment_id`: Unique experiment ID
* `experiment_tag`: Tag of the trial, which includes parameter values
* `hostname`: Hostname of the worker
* `iterations_since_restore`: The number of times `tune.report()` has been called after restoring the worker from a checkpoint
* `node_ip`: Host IP of the worker
* `pid`: Process ID (PID) of the worker process
* `time_since_restore`: Time in seconds since restoring from a checkpoint
* `time_this_iter_s`: Runtime of the current training iteration in seconds, meaning one call to the trainable function or to `step()` in the class API
* `time_total_s`: Total runtime in seconds
* `timestamp`: Timestamp when the result was processed
* `timesteps_since_restore`: Number of timesteps since restoring from a checkpoint
* `timesteps_total`: Total number of timesteps
* `training_iteration`: The number of times `tune.report()` has been called
* `trial_id`: Unique trial ID

You can find all of these metrics in the `Trial.last_result` dictionary.
