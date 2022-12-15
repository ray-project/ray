# Getting Data in and out of Tune

Often, you will find yourself needing to pass data into Tune [Trainables](tune_60_seconds_trainables) (datasets, models, other large parameters) and get data out of them (metrics, checkpoints, other artifacts). In this guide, we'll explore different ways of doing that and see in what circumstances they should be used.

```{contents}
    :local:
    :backlinks: none
```

Let's start by defining a simple Trainable function. We'll be expanding this function with different functionality as we go.


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

Our `training_function` function requires a pandas DataFrame, a model with some hyperparameters and the number of epochs to train the model for as inputs. The hyperparameters of the model impact the metric returned, and in each epoch (iteration of training), the `trained_model` state is changed.

We will run hyperparameter optimization using the [Tuner API](tune-run-ref).


```python
from ray.tune import Tuner
from ray import tune

tuner = Tuner(training_function, tune_config=tune.TuneConfig(num_samples=4))
```

## Getting data in

First order of business is to provide the inputs for the Trainable. We can broadly separate them into two categories - variables and constants.

Variables are the parameters we want to tune. They will be different for every [Trial](tune_60_seconds_trials). For example, those may be the learning rate and batch size for a neural network, number of trees and the maximum depth for a random forest, or the data partition if you are using Tune as an execution engine for batch training.

Constants are the parameters that are the same for every Trial. Those can be the number of epochs, model hyperparameters we want to set but not tune, the dataset and so on. Often, the constants will be quite large (e.g. the dataset or the model).

```{warning}
Objects from the outer scope of the `training_function` will also be automatically copiedm serialized and sent to Trial Actors, which may lead to unintended behavior. Examples include global locks not working (as each Actor operates on a copy) or general errors related to serialization. Best practice is to not refer to any objects from outer scope in the `training_function`.
```

### Search space

```{note}
TL;DR - use the `param_space` argument to specify small, serializable constants and variables.
```

The first way of passing inputs into Trainables is the [*search space*](tune-key-concepts-search-spaces) (it may also be called *parameter space* or *config*). In the Trainable itself, it maps to the `config` dict passed in as an argument to the function. You define the search space using the `param_space` argument of the `Tuner`. The search space is a dict and may be composed of [*distributions*](<tune-sample-docs>), which will sample a different value for each Trial, or of constant values. The search space may be composed of nested dictionaries, and those in turn can have distributions as well.

```{warning}
Each value in the search space will be saved directly in the Trial metadata. This means that every value in the search space **must** be serializable and take up a small amount of memory.
```

For example, passing in a large pandas DataFrame or an unserializable model object as a value in the search space will lead to unwanted behavior. At best it will cause large slowdowns and disk space usage as Trial metadata saved to disk will also contain this data. At worst, an exception will be raised, as the data cannot be sent over to the Trial workers. For more details, see {ref}`tune-bottlenecks`.

Instead, use strings or other identifiers as your values, and initialize/load the objects inside your Trainable directly depending on those.

```{note}
[Ray Datasets](datasets_getting_started) can be used as values in the search space directly.
```

In our example, we want to tune the two model hyperparameters. We also want to set the number of epochs, so that we can easily tweak it later. For the hyperparameters, we will use the `tune.uniform` distribution. We will also modify the `training_function` to obtain those values from the `config` dictionary.


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

### Parameters

```{note}
TL;DR - use the `tune.with_parameters` util function to specify large constant parameters.
```

If we have large objects that are constant across Trials, we can use the [`tune.with_parameters`](tune-with-parameters) utility to pass them into the Trainable directly. The objects will be stored in the [Ray object store](serialization-guide) so that each Trial worker may access them to obtain a local copy to use in its process.

```{tip}
Objects put into the Ray object store must be serializable.
```

Note that the serialization (once) and deserialization (for each Trial) of large objects may incur a performance overhead.

In our example, we will pass the `data` DataFrame using `tune.with_parameters`. In order to do that, we need to modify our function signature to include `data` as an argument.


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

Next step is to wrap the `training_function` using `tune.with_parameters` before passing it into the `Tuner`. Every keyword argument of the `tune.with_parameters` call will be mapped to the keyword arguments in the Trainable signature.


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

### Loading data in Trainable

You can also load data directly in Trainable from e.g. cloud storage, shared file storage such as NFS, or from the local disk of the Trainable worker.

```{warning}
When loading from disk, ensure that all nodes in your cluster have access to the file you are trying to load.
```

A common use-case is to load the dataset from S3 or any other cloud storage with pandas, arrow or any other framework.

The working directory of the Trainable worker will be automatically changed to the corresponding Trial directory. For more details, see {ref}`tune-working-dir`.


Our tuning run can now be run, though we will not yet obtain any meaningful outputs back.


```python
results = tuner.fit()
```


## Getting data out

We can now run our tuning run using the `training_function` Trainable. The next step is to report *metrics* to Tune that can be used to guide the optimization. We will also want to *checkpoint* our trained models so that we can resume the training after an interruption, and to use them for prediction later.

The [`ray.air.session`](air-session-ref) API is used to get data out of the Trainable workers. `session.report` can be called multiple times in the Trainable function. Each call corresponds to one iteration (epoch, step, tree) of training.

### Reporting metrics

*Metrics* are values passed through the `metrics` argument in a `session.report` call. Metrics can be used by Tune [Search Algorithms](search-alg-ref) and [Schedulers](schedulers-ref) to direct the search. After the tuning run is complete, you can [analyze the results](/tune/examples/tune_analyze_results), which include the reported metrics.

```{note}
Similarly to search space values, each value reported as a metric will be saved directly in the Trial metadata. This means that every value reported as a metric **must** be serializable and take up a small amount of memory.
```

```{note}
Tune will automatically include some metrics, such as the training iteration, timestamp and more. See [here](tune-autofilled-metrics) for the entire list.
```

In our example, we want to maximize the `metric`. We will report it each epoch to Tune, and set the `metric` and `mode` arguments in `tune.TuneConfig` to let Tune know that it should use it as the optimization objective.


```python
from ray.air import session


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
        session.report(metrics={"metric": metric})


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

### Logging metrics with callbacks

Every metric logged using `session.report` can be accessed during the tuning run through Tune [Callbacks](tune-logging). Ray AIR provides [several built-in integrations](air-builtin-callbacks) with popular frameworks, such as MLFlow, Weights & Biases, CometML and more. You can also use the [Callback API](tune-callbacks-docs) to create your own callbacks.

Callbacks are passed in the `callback` argument of the `Tuner`'s `RunConfig`.

In our example, we'll use the MLFlow callback to track the progress of our tuning run and the changing value of the `metric` (requires `mlflow` to be installed).


```python
from ray.air import RunConfig
from ray.air.integrations.mlflow import MLflowLoggerCallback


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
        session.report(metrics={"metric": metric})


tuner = Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4, metric="metric", mode="max"),
    run_config=RunConfig(
        callbacks=[MLflowLoggerCallback(experiment_name="example")]
    ),
)
```

### Checkpoints & other artifacts

Aside from metrics, you may want to save the state of your trained model and any other artifacts to allow resumption from training failure and further inspection and usage. Those cannot be saved as metrics, as they often are far too large and may not be easily serializable. Finally, they should be persisted on disk or cloud storage to allow access after the Tune run is interrupted or terminated.

Ray AIR (which contains Ray Tune) provides a [`Checkpoint`](air-checkpoints-doc) API for that purpose. `Checkpoint` objects can be created from various sources (dictionaries, directories, cloud storage) and used between different AIR components.

In Tune, `Checkpoints` are created by the user in their Trainable functions and reported using the optional `checkpoint` argument of `session.report`. `Checkpoints` can contain arbitrary data and can be freely passed around the Ray cluster. After a tuning run is over, `Checkpoints` can be [obtained from the results](/tune/examples/tune_analyze_results).

Ray Tune can be configured to [automatically sync checkpoints to cloud storage](tune-checkpoint-syncing), keep only a certain number of checkpoints to save space (with {class}`ray.air.config.CheckpointConfig`) and more.

```{note}
The experiment state itself is checkpointed separately. See {ref}`tune-two-types-of-ckpt` for more details.
```

In our example, we want to be able to resume the training from the latest checkpoint, and to save the `trained_model` in a checkpoint every iteration. To accomplish this, we will use the `session` and `Checkpoint` APIs.


```python
from ray.air import Checkpoint


def training_function(config, data):
    model = {
        "hyperparameter_a": config["hyperparameter_a"],
        "hyperparameter_b": config["hyperparameter_b"],
    }
    epochs = config["epochs"]

    # Load the checkpoint, if there is any.
    loaded_checkpoint = session.get_checkpoint()
    if loaded_checkpoint is not None:
        last_epoch = loaded_checkpoint.to_dict()["epoch"] + 1
    else:
        last_epoch = 0

    # Simulate training & evaluation - we obtain back a "metric" and a "trained_model".
    for epoch in range(last_epoch, epochs):
        # Simulate doing something expensive.
        time.sleep(1)
        metric = (0.1 + model["hyperparameter_a"] * epoch / 100) ** (
            -1
        ) + model["hyperparameter_b"] * 0.1 * data["A"].sum()
        trained_model = {"state": model, "epoch": epoch}

        # Create the checkpoint.
        checkpoint = Checkpoint.from_dict({"model": trained_model})
        session.report(metrics={"metric": metric}, checkpoint=checkpoint)


tuner = Tuner(
    tune.with_parameters(training_function, data=data),
    param_space={
        "hyperparameter_a": tune.uniform(0, 20),
        "hyperparameter_b": tune.uniform(-100, 100),
        "epochs": 10,
    },
    tune_config=tune.TuneConfig(num_samples=4, metric="metric", mode="max"),
    run_config=RunConfig(
        callbacks=[MLflowLoggerCallback(experiment_name="example")]
    ),
)
```

With all of those changes implemented, we can now run our tuning and obtain meaningful metrics and artifacts.


```python
results = tuner.fit()
results.get_dataframe()
```



    2022-11-30 17:40:28,839	INFO tune.py:762 -- Total run time: 15.79 seconds (15.65 seconds for the tuning loop).





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
<p>4 rows × 23 columns</p>
</div>



Checkpoints, metrics, and the log directory for each trial can be accessed through the `ResultGrid` output of a Tune experiment. For more information on how to interact with the returned `ResultGrid`, see {doc}`/tune/examples/tune_analyze_results`.
