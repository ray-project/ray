# Getting Data in and out of Tune

Often, you find yourself needing to pass data into Tune [Trainables](tune_60_seconds_trainables) (datasets, models, other large parameters) and get data out of them (metrics, checkpoints, other artifacts). In this guide, explore different ways of doing that and see in what circumstances to use them.

```{contents}
    :local:
    :backlinks: none
```

Start by defining a simple Trainable function. This function expands with different features as needed.

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

The `training_function` requires a pandas DataFrame, a model with some hyperparameters and the number of epochs to train the model for as inputs. The hyperparameters of the model impact the metric returned, and in each epoch (iteration of training), the `trained_model` state is changed.

Run hyperparameter optimization using the [Tuner API](tune-run-ref).

```python
from ray.tune import Tuner
from ray import tune

tuner = Tuner(training_function, tune_config=tune.TuneConfig(num_samples=4))
```

## Getting data into Tune

First order of business is to provide the inputs for the Trainable. Broadly separate them into two categories - variables and constants.

Variables are the parameters to tune. They're different for every [Trial](tune_60_seconds_trials). For example, those may be the learning rate and batch size for a neural network, number of trees and the maximum depth for a random forest, or the data partition if you are using Tune as an execution engine for batch training.

Constants are the parameters that are the same for every Trial. Those can be the number of epochs, model hyperparameters to set but not tune, the dataset and so on. Often, the constants are quite large (for example, the dataset or the model).

```{warning}
Objects from the outer scope of the `training_function` will also be automatically serialized and sent to Trial Actors, which may lead to unintended behavior. Examples include global locks not working (as each Actor operates on a copy) or general errors related to serialization. Best practice is to not refer to any objects from outer scope in the `training_function`.
```

### Passing data into a Tune run through search spaces

```{note}
TL;DR - use the `param_space` argument to specify small, serializable constants and variables.
```

The first way of passing inputs into Trainables is the [*search space*](tune-key-concepts-search-spaces) (it may also be called *parameter space* or *config*). In the Trainable itself, it maps to the `config` dict passed in as an argument to the function. You define the search space using the `param_space` argument of the `Tuner`. The search space is a dict and may be composed of [*distributions*](<tune-search-space>), which sample a different value for each Trial, or of constant values. The search space may be composed of nested dictionaries, and those in turn can have distributions as well.

```{warning}
Each value in the search space will be saved directly in the Trial metadata. This means that every value in the search space **must** be serializable and take up a small amount of memory.
```

For example, passing in a large pandas DataFrame or a non-serializable model object as a value in the search space leads to unwanted behavior. At best it causes large slowdowns and disk space usage as Trial metadata saved to disk also contains this data. At worst, an exception is raised, as the data can't be sent over to the Trial workers. For more details, see {ref}`tune-bottlenecks`.

Instead, use strings or other identifiers as your values, and initialize/load the objects inside your Trainable directly depending on those.

```{note}
[Datasets](data_quickstart) can be used as values in the search space directly.
```

In this example, tune the two model hyperparameters. Also set the number of epochs, so that it can be easily tweaked later. For the hyperparameters, use the `tune.uniform` distribution. Also modify the `training_function` to obtain those values from the `config` dictionary.

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

### Using `tune.with_parameters` access data in Tune runs

```{note}
TL;DR - use the `tune.with_parameters` util function to specify large constant parameters.
```

If there are large objects that are constant across Trials, use the {func}`tune.with_parameters <ray.tune.with_parameters>` utility to pass them into the Trainable directly. The objects are stored in the [Ray object store](serialization-guide) so that each Trial worker may access them to obtain a local copy to use in its process.

```{tip}
Objects put into the Ray object store must be serializable.
```

Note that the serialization (once) and deserialization (for each Trial) of large objects may incur a performance overhead.

In this example, pass the `data` DataFrame using `tune.with_parameters`. To do that, modify the function signature to include `data` as an argument.

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

Next step is to wrap the `training_function` using `tune.with_parameters` before passing it into the `Tuner`. Every keyword argument of the `tune.with_parameters` call is mapped to the keyword arguments in the Trainable signature.

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

### Loading data in a Tune Trainable

You can also load data directly in Trainable from, for example, cloud storage, shared file storage such as NFS, or from the local disk of the Trainable worker.

```{warning}
When loading from disk, ensure that all nodes in your cluster have access to the file you are trying to load.
```

A common use-case is to load the dataset from S3 or any other cloud storage with pandas, arrow, or any other framework.

The working directory of the Trainable worker is automatically changed to the corresponding Trial directory. For more details, see {ref}`tune-working-dir`.

The tuning run can now be run, though it won't yet obtain any meaningful outputs back.

```python
results = tuner.fit()
```

## Getting data out of Ray Tune

The tuning run can now run using the `training_function` Trainable. The next step is to report *metrics* to Tune that can be used to guide the optimization. Also *checkpoint* the trained models so that training can resume after an interruption, and to use them for prediction later.

The `ray.tune.report` API is used to get data out of the Trainable workers. It can be called multiple times in the Trainable function. Each call corresponds to one iteration (epoch, step, tree) of training.

### Reporting metrics with Tune

*Metrics* are values passed through the `metrics` argument in a `tune.report` call. Metrics can be used by Tune [Search Algorithms](search-alg-ref) and [Schedulers](schedulers-ref) to direct the search. After the tuning run is complete, you can [analyze the results](tune-analysis-guide), which include the reported metrics.

```{note}
Similarly to search space values, each value reported as a metric will be saved directly in the Trial metadata. This means that every value reported as a metric **must** be serializable and take up a small amount of memory.
```

```{note}
Tune will automatically include some metrics, such as the training iteration, timestamp and more. See [here](tune-autofilled-metrics) for the entire list.
```

In this example, maximize the `metric`. Report it each epoch to Tune, and set the `metric` and `mode` arguments in `tune.TuneConfig` to let Tune know to use it as the optimization objective.

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

### Logging metrics with Tune callbacks

Every metric logged using `tune.report` can be accessed during the tuning run through Tune [Callbacks](tune-logging). Ray Tune provides [several built-in integrations](loggers-docstring) with popular frameworks, such as MLFlow, Weights & Biases, CometML and more. You can also use the [Callback API](tune-callbacks-docs) to create your own callbacks.

Callbacks are passed in the `callback` argument of the `Tuner`'s `RunConfig`.

In this example, use the MLflow callback to track the progress of the tuning run and the changing value of the `metric` (requires `mlflow` to be installed).

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

### Getting data out of Tune using checkpoints & other artifacts

Aside from metrics, you may want to save the state of your trained model and any other artifacts to allow resumption from training failure and further inspection and usage. Those can't be saved as metrics, as they're often far too large and may not be easily serializable. Finally, they should be persisted on disk or cloud storage to allow access after the Tune run is interrupted or terminated.

Ray Train provides a {class}`Checkpoint <ray.tune.Checkpoint>` API for that purpose. `Checkpoint` objects can be created from various sources (dictionaries, directories, cloud storage).

In Ray Tune, `Checkpoints` are created by the user in their Trainable functions and reported using the optional `checkpoint` argument of `tune.report`. `Checkpoints` can contain arbitrary data and can be freely passed around the Ray cluster. After a tuning run is over, `Checkpoints` can be [obtained from the results](tune-analysis-guide).

Ray Tune can be configured to [automatically sync checkpoints to cloud storage](tune-storage-options), keep only a certain number of checkpoints to save space (with {class}`ray.tune.CheckpointConfig`) and more.

```{note}
The experiment state itself is checkpointed separately. See {ref}`tune-persisted-experiment-data` for more details.
```

In this example, be able to resume the training from the latest checkpoint, and to save the `trained_model` in a checkpoint every iteration. To accomplish this, use the `session` and `Checkpoint` APIs.

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

With all of those changes implemented, run the tuning and obtain meaningful metrics and artifacts.

```python
results = tuner.fit()
results.get_dataframe()
```

    2022-11-30 17:40:28,839 INFO tune.py:762 -- Total run time: 15.79 seconds (15.65 seconds for the tuning loop).

<!-- vale Google.Ellipses = NO -->

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>metric</th>
      <th>time_this_iter_s</th>
      <th>should_checkpoint</th>
      <th>done</th>
      <th>timesteps_total</th>
      <th>episodes_total</th>
      <th>training_iteration</th>
      <th>trial_id</th>
      <th>experiment_id</th>
      <th>date</th>
      <th>...</th>
      <th>hostname</th>
      <th>node_ip</th>
      <th>time_since_restore</th>
      <th>timesteps_since_restore</th>
      <th>iterations_since_restore</th>
      <th>warmup_time</th>
      <th>config/epochs</th>
      <th>config/hyperparameter_a</th>
      <th>config/hyperparameter_b</th>
      <th>logdir</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>-58.399962</td>
      <td>1.015951</td>
      <td>True</td>
      <td>False</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10</td>
      <td>0b239_00000</td>
      <td>acf38c19d59c4cf2ad7955807657b6ea</td>
      <td>2022-11-30_17-40-26</td>
      <td>...</td>
      <td>ip-172-31-43-110</td>
      <td>172.31.43.110</td>
      <td>10.282120</td>
      <td>0</td>
      <td>10</td>
      <td>0.003541</td>
      <td>10</td>
      <td>18.065981</td>
      <td>-98.298928</td>
      <td>/home/ubuntu/ray_results/training_function_202...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>-24.461518</td>
      <td>1.030420</td>
      <td>True</td>
      <td>False</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10</td>
      <td>0b239_00001</td>
      <td>5ca9e03d7cca46a7852cd501bc3f7b38</td>
      <td>2022-11-30_17-40-28</td>
      <td>...</td>
      <td>ip-172-31-43-110</td>
      <td>172.31.43.110</td>
      <td>10.362581</td>
      <td>0</td>
      <td>10</td>
      <td>0.004031</td>
      <td>10</td>
      <td>1.544918</td>
      <td>-47.741455</td>
      <td>/home/ubuntu/ray_results/training_function_202...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>18.510299</td>
      <td>1.034228</td>
      <td>True</td>
      <td>False</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10</td>
      <td>0b239_00002</td>
      <td>aa38dd786c714486a8d69fa5b372df48</td>
      <td>2022-11-30_17-40-28</td>
      <td>...</td>
      <td>ip-172-31-43-110</td>
      <td>172.31.43.110</td>
      <td>10.333781</td>
      <td>0</td>
      <td>10</td>
      <td>0.005286</td>
      <td>10</td>
      <td>8.129285</td>
      <td>28.846415</td>
      <td>/home/ubuntu/ray_results/training_function_202...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>-16.138780</td>
      <td>1.020072</td>
      <td>True</td>
      <td>False</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10</td>
      <td>0b239_00003</td>
      <td>5b401e15ab614332b631d552603a8d77</td>
      <td>2022-11-30_17-40-28</td>
      <td>...</td>
      <td>ip-172-31-43-110</td>
      <td>172.31.43.110</td>
      <td>10.242707</td>
      <td>0</td>
      <td>10</td>
      <td>0.003809</td>
      <td>10</td>
      <td>17.982020</td>
      <td>-27.867871</td>
      <td>/home/ubuntu/ray_results/training_function_202...</td>
    </tr>
  </tbody>
</table>
<p>4 rows by 23 columns</p>
</div>

<!-- vale Google.Ellipses = YES -->

Checkpoints, metrics, and the log directory for each trial can be accessed through the `ResultGrid` output of a Tune experiment. For more information on how to interact with the returned `ResultGrid`, see {doc}`/tune/examples/tune_analyze_results`.

### How to access Tune results after finishing

After finishing running the Python session, you can still access the results and checkpoints. By default, Tune saves the experiment results to the `~/ray_results` local directory. You can configure Tune to persist results in the cloud as well. See {ref}`tune-storage-options` for more information on how to configure storage options for persisting experiment results.

You can restore the Tune experiment by calling {meth}`Tuner.restore(path_or_cloud_uri, trainable) <ray.tune.Tuner.restore>`, where `path_or_cloud_uri` points to a location either on the filesystem or cloud where the experiment was saved to. After the `Tuner` has been restored, you can access the results and checkpoints by calling `Tuner.get_results()` to receive the `ResultGrid` object, and then proceeding as outlined in the previous section.
