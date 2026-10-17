---
myst:
  html_meta:
    description: "Pass data into Tune trainables through search spaces and tune.with_parameters, and get metrics, checkpoints, and artifacts back out."
---

# Get data in and out of Tune

You often need to pass data into Ray Tune [trainables](tune_60_seconds_trainables) and get data out of them. Data going in includes datasets, models, and other large parameters. Data coming out includes metrics, checkpoints, and other artifacts. This guide explores different ways to do both and when to use each one.

```{contents}
    :local:
    :backlinks: none
```

Start by defining a simple function trainable. This guide extends the function step by step.

```python
import random
import time
import pandas as pd


def training_function(config):
    # For now, we have nothing here.
    data = None
    model = {"hyperparameter_a": None, "hyperparameter_b": None}
    epochs = 0

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}
```

As inputs, `training_function` requires a pandas DataFrame, a model with some hyperparameters, and the number of epochs to train the model for. The model's hyperparameters affect the returned metric, and each epoch, or iteration of training, changes the `trained_model` state.

Use the [Tuner API](tune-run-ref) to run hyperparameter optimization.

```python
from ray.tune import Tuner
from ray import tune

tuner = Tuner(training_function, tune_config=tune.TuneConfig(num_samples=4))
```

## Get data into Tune

First, provide the inputs for the trainable. Inputs fall broadly into two categories: variables and constants.

Variables are the parameters you want to tune. They're different for every [trial](tune_60_seconds_trials). For example, variables might be the learning rate and batch size for a neural network, the number of trees and the maximum depth for a random forest, or the data partition if you're using Tune as an execution engine for batch training.

Constants are the parameters that are the same for every trial. Examples include the number of epochs, model hyperparameters you want to set but not tune, and the dataset. Constants are often large, such as the dataset or the model.

```{warning}
Objects from the outer scope of `training_function` are also automatically serialized and sent to trial actors, which might lead to unintended behavior. For example, a global lock doesn't work because each actor operates on its own copy, and you might hit general serialization errors. As a best practice, don't refer to any objects from the outer scope in `training_function`.
```

### Pass data into a Tune run through search spaces

```{note}
In short, use the `param_space` argument to specify small, serializable constants and variables.
```

The first way to pass inputs into trainables is the [*search space*](tune-key-concepts-search-spaces), also called the *parameter space* or *config*. In the trainable, the search space maps to the `config` dictionary passed to the function as an argument. You define the search space with the `param_space` argument of the `Tuner`. The search space is a dictionary whose values can be constants or [*distributions*](<tune-search-space>), which sample a different value for each trial. The search space can also contain nested dictionaries, which can contain distributions as well.

```{warning}
Tune saves each value in the search space directly in the trial metadata, so every value in the search space must be serializable and take up a small amount of memory.
```

For example, passing a large pandas DataFrame or an unserializable model object as a value in the search space leads to unwanted behavior. At best, it causes large slowdowns and disk space usage, because the trial metadata saved to disk also contains this data. At worst, an exception is raised because the data can't be sent to the trial workers. For more details, see {ref}`tune-bottlenecks`.

Instead, use strings or other identifiers as your values, and initialize or load the objects directly inside your trainable based on those values.

```{note}
You can use [Ray Data datasets](data_quickstart) directly as values in the search space.
```

In this example, you tune the two model hyperparameters and set the number of epochs so you can adjust it later. Use the `tune.uniform` distribution for the hyperparameters, and modify `training_function` to read those values from the `config` dictionary.

```python
def training_function(config):
    # For now, we have nothing here.
    data = None

    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}


tuner = Tuner(
    training_function,
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
)
```

### Use `tune.with_parameters` to access data in Tune runs

```{note}
In short, use the `tune.with_parameters` utility function to specify large constant parameters.
```

If you have large objects that are constant across trials, you can use the {func}`tune.with_parameters <ray.tune.with_parameters>` utility to pass them directly into the trainable. The objects are stored in the [Ray object store](serialization-guide), so each trial worker can access them to get a local copy to use in its process.

```{note}
Objects put into the Ray object store must be serializable.
```

Large objects are serialized once and deserialized for each trial, which might add performance overhead.

In this example, pass the `data` DataFrame with `tune.with_parameters`. To do that, add `data` as an argument to the function signature.

```python
def training_function(config, data):
    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}


tuner = Tuner(
    training_function,
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
)
```

Next, wrap `training_function` with `tune.with_parameters` before passing it to the `Tuner`. Each keyword argument of the `tune.with_parameters` call maps to a keyword argument in the trainable signature.

```python
data = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

tuner = Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4),
)
```

### Load data in a Tune trainable

You can also load data directly in the trainable, for example from cloud storage, from shared file storage such as NFS, or from the local disk of the trainable worker.

```{warning}
When loading from disk, make sure that all nodes in your cluster have access to the file you're loading.
```

A common use case is loading the dataset from S3 or another cloud storage service with pandas, Arrow, or another framework.

The working directory of the trainable worker is automatically set to the corresponding trial directory. For more details, see {ref}`tune-working-dir`.

You can now run tuning, though you won't get any meaningful outputs back yet.

```python
results = tuner.fit()
```

## Get data out of Tune

You can now run tuning with the `training_function` trainable. Next, report *metrics* to Tune that it can use to guide the optimization. You also want to *checkpoint* your trained models so you can resume training after an interruption and use the models for prediction later.

Use the `ray.tune.report` API to get data out of the trainable workers. You can call it multiple times in the function trainable. Each call corresponds to one iteration of training, such as an epoch, a step, or a tree.

### Report metrics with Tune

*Metrics* are values passed through the `metrics` argument in a `tune.report` call. Tune [search algorithms](search-alg-ref) and [schedulers](schedulers-ref) can use metrics to direct the search. After the tuning run completes, you can [analyze the results](tune-analysis-guide), which include the reported metrics.

```{note}
As with search space values, Tune saves each value reported as a metric directly in the trial metadata, so every value reported as a metric must be serializable and take up a small amount of memory.
```

```{note}
Tune automatically includes some metrics, such as the training iteration and timestamp. See the [full list of automatically included metrics](tune-autofilled-metrics).
```

In this example, you want to maximize `metric`. Report it to Tune each epoch, and set the `metric` and `mode` arguments in `tune.TuneConfig` to tell Tune to use it as the optimization objective.

```python
from ray import tune


def training_function(config, data):
    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}
        tune.report(metrics={"metric": metric})


tuner = Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4, metric="metric", mode="max"),
)
```

### Log metrics with Tune callbacks

During the tuning run, Tune [callbacks](tune-logging) can access every metric logged with `tune.report`. Ray Tune provides [built-in integrations](loggers-docstring) with popular frameworks, such as MLflow, Weights & Biases, and CometML. You can also create your own callbacks with the [Callback API](tune-callbacks-docs).

Pass callbacks in the `callbacks` argument of the `RunConfig` that you pass to the `Tuner`.

In this example, use the MLflow callback to track the progress of the tuning run and the changing value of `metric`. To use this callback, install `mlflow`.

```python
import ray.tune
from ray.tune.logger.mlflow import MLflowLoggerCallback


def training_function(config, data):
    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}
        tune.report(metrics={"metric": metric})


tuner = tune.Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4, metric="metric", mode="max"),
    run_config=tune.RunConfig(
        callbacks=[MLflowLoggerCallback(experiment_name="example")]
    ),
)
```

### Get data out of Tune with checkpoints and other artifacts

Besides metrics, you might want to save the state of your trained model and other artifacts, so you can resume after a training failure and inspect or use them later. You can't save these as metrics, because they're often far too large and might be hard to serialize. Persist them on disk or in cloud storage so you can access them after the Tune run is interrupted or terminated.

Ray Train provides a {class}`Checkpoint <ray.tune.Checkpoint>` API for that purpose. You can create `Checkpoint` objects from various sources, such as dictionaries, directories, and cloud storage.

In Ray Tune, you create `Checkpoint` objects in your function trainable and report them with the optional `checkpoint` argument of `tune.report`. A checkpoint can contain arbitrary data, and you can pass it freely around the Ray cluster. After a tuning run finishes, you can [get the checkpoints from the results](tune-analysis-guide).

You can also configure how Ray Tune handles checkpoints. For example, you can configure Ray Tune to [automatically sync checkpoints to cloud storage](tune-storage-options), or use {class}`ray.tune.CheckpointConfig` to keep only a certain number of checkpoints to save space.

```{note}
Tune checkpoints the experiment state itself separately. For more details, see {ref}`tune-persisted-experiment-data`.
```

In this example, you want to resume training from the latest checkpoint and save the `trained_model` in a checkpoint every iteration. To do this, use `tune.get_checkpoint()` and the `Checkpoint` API.

```python
import os
import pickle
import tempfile

from ray import tune

def training_function(config, data):
    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Load the checkpoint, if there is any.
    checkpoint = tune.get_checkpoint()
    start_epoch = 0
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            with open(os.path.join(checkpoint_dir, "model.pkl"), "rb") as f:
                checkpoint_dict = pickle.load(f)
        start_epoch = checkpoint_dict["epoch"] + 1
        model = checkpoint_dict["state"]

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(start_epoch, epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()

        checkpoint_dict = {"state": model, "epoch": epoch}

        # Create the checkpoint.
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            with open(os.path.join(temp_checkpoint_dir, "model.pkl"), "wb") as f:
                pickle.dump(checkpoint_dict, f)
            tune.report(
                {"metric": metric},
                checkpoint=tune.Checkpoint.from_directory(temp_checkpoint_dir),
            )


tuner = tune.Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4, metric="metric", mode="max"),
    run_config=tune.RunConfig(
        callbacks=[MLflowLoggerCallback(experiment_name="example")]
    ),
)
```

With these changes in place, you can run tuning and get meaningful metrics and artifacts.

```python
results = tuner.fit()
results.get_dataframe()
```

    2022-11-30 17:40:28,839 INFO tune.py:762 -- Total run time: 15.79 seconds (15.65 seconds for the tuning loop).

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type { vertical-align: middle; }

    .dataframe tbody tr th { vertical-align: top; }

    .dataframe thead th { text-align: right; }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;"> <th></th> <th>metric</th> <th>time_this_iter_s</th> <th>should_checkpoint</th> <th>done</th> <th>timesteps_total</th> <th>episodes_total</th> <th>training_iteration</th> <th>trial_id</th> <th>experiment_id</th> <th>date</th> <th>...</th> <th>hostname</th> <th>node_ip</th> <th>time_since_restore</th> <th>timesteps_since_restore</th> <th>iterations_since_restore</th> <th>warmup_time</th> <th>config/epochs</th> <th>config/hyperparameter_a</th> <th>config/hyperparameter_b</th> <th>logdir</th> </tr>
  </thead>
  <tbody>
    <tr> <th>0</th> <td>-58.399962</td> <td>1.015951</td> <td>True</td> <td>False</td> <td>NaN</td> <td>NaN</td> <td>10</td> <td>0b239_00000</td> <td>acf38c19d59c4cf2ad7955807657b6ea</td> <td>2022-11-30_17-40-26</td> <td>...</td> <td>ip-172-31-43-110</td> <td>172.31.43.110</td> <td>10.282120</td> <td>0</td> <td>10</td> <td>0.003541</td> <td>10</td> <td>18.065981</td> <td>-98.298928</td> <td>/home/ubuntu/ray_results/training_function_202...</td> </tr> <tr> <th>1</th> <td>-24.461518</td> <td>1.030420</td> <td>True</td> <td>False</td> <td>NaN</td> <td>NaN</td> <td>10</td> <td>0b239_00001</td> <td>5ca9e03d7cca46a7852cd501bc3f7b38</td> <td>2022-11-30_17-40-28</td> <td>...</td> <td>ip-172-31-43-110</td> <td>172.31.43.110</td> <td>10.362581</td> <td>0</td> <td>10</td> <td>0.004031</td> <td>10</td> <td>1.544918</td> <td>-47.741455</td> <td>/home/ubuntu/ray_results/training_function_202...</td> </tr> <tr> <th>2</th> <td>18.510299</td> <td>1.034228</td> <td>True</td> <td>False</td> <td>NaN</td> <td>NaN</td> <td>10</td> <td>0b239_00002</td> <td>aa38dd786c714486a8d69fa5b372df48</td> <td>2022-11-30_17-40-28</td> <td>...</td> <td>ip-172-31-43-110</td> <td>172.31.43.110</td> <td>10.333781</td> <td>0</td> <td>10</td> <td>0.005286</td> <td>10</td> <td>8.129285</td> <td>28.846415</td> <td>/home/ubuntu/ray_results/training_function_202...</td> </tr> <tr> <th>3</th> <td>-16.138780</td> <td>1.020072</td> <td>True</td> <td>False</td> <td>NaN</td> <td>NaN</td> <td>10</td> <td>0b239_00003</td> <td>5b401e15ab614332b631d552603a8d77</td> <td>2022-11-30_17-40-28</td> <td>...</td> <td>ip-172-31-43-110</td> <td>172.31.43.110</td> <td>10.242707</td> <td>0</td> <td>10</td> <td>0.003809</td> <td>10</td> <td>17.982020</td> <td>-27.867871</td> <td>/home/ubuntu/ray_results/training_function_202...</td> </tr>
  </tbody>
</table>
<p>4 rows × 23 columns</p>
</div>

You can access the checkpoints, metrics, and log directory for each trial through the `ResultGrid` output of a Tune experiment. For more information on working with the returned `ResultGrid`, see {doc}`/tune/examples/tune-analyze-results`.

### Access Tune results after tuning finishes

After your Python session ends, you can still access the results and checkpoints. By default, Tune saves the experiment results to the local `~/ray_results` directory. You can also configure Tune to persist results in the cloud. For more information on configuring storage for experiment results, see {ref}`tune-storage-options`.

To restore the Tune experiment, call {meth}`Tuner.restore(path_or_cloud_uri, trainable) <ray.tune.Tuner.restore>`, where `path_or_cloud_uri` points to the filesystem or cloud location where the experiment was saved. After you restore the `Tuner`, call `Tuner.get_results()` to get the `ResultGrid` object, then access the results and checkpoints as described in the previous section.
