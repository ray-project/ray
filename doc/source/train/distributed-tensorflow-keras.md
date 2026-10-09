---
myst:
  html_meta:
    description: "Distribute TensorFlow and Keras training with TensorflowTrainer: adapt the training function, load data, report results, and checkpoint."
---

(train-tensorflow-overview)=

# Get started with distributed training using TensorFlow and Keras

Use Ray Train's [TensorFlow](https://www.tensorflow.org/) integration to scale your TensorFlow and Keras training functions to many machines and GPUs.

Ray Train schedules your training workers and configures `TF_CONFIG` for you, so you can run your `MultiWorkerMirroredStrategy` training script. For more information, see [Distributed training with TensorFlow](https://www.tensorflow.org/guide/distributed_training).

Most of the examples in this guide use TensorFlow with Keras, but Ray Train also works with TensorFlow without Keras.


## Quickstart

```{literalinclude} ./doc_code/tf_starter.py
:language: python
:start-after: __tf_train_start__
:end-before: __tf_train_end__
```


## Update your training function

First, update your {ref}`training function <train-overview-training-function>` to support distributed training.


:::{note}
The TensorFlow implementation currently supports `MultiWorkerMirroredStrategy` and `MirroredStrategy`. To request support for another strategy, submit a [feature request on GitHub](https://github.com/ray-project/ray/issues).
:::

These instructions closely follow TensorFlow's [Multi-worker training with Keras](https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras) tutorial. One key difference is that Ray Train handles the environment variable setup for you.

:::{warning}
Ray doesn't automatically set any environment variables or configuration related to local parallelism or threading {ref}`aside from "OMP_NUM_THREADS" <omp-num-thread-note>`. For more control over TensorFlow threading, call functions from the `tf.config.threading` module, such as `tf.config.threading.set_inter_op_parallelism_threads(num_cpus)`, at the beginning of your `train_loop_per_worker` function.
:::

### Step 1: Wrap your model in `MultiWorkerMirroredStrategy`

[`MultiWorkerMirroredStrategy`](https://www.tensorflow.org/api_docs/python/tf/distribute/experimental/MultiWorkerMirroredStrategy) provides synchronous distributed training. You must build and compile the `Model` inside the strategy's scope.

```{testcode}
:skipif: True

with tf.distribute.MultiWorkerMirroredStrategy().scope():
    model = ... # build model
    model.compile()
```

### Step 2: Update your `Dataset` batch size to the global batch size

Because the [batch](https://www.tensorflow.org/api_docs/python/tf/data/Dataset#batch) splits evenly across worker processes, set `batch_size` to the per-worker batch size times the number of workers.

```diff
-batch_size = worker_batch_size
+batch_size = worker_batch_size * train.get_context().get_world_size()
```

## Create a `TensorflowTrainer`

Trainers are the primary Ray Train classes for managing state and executing training. For distributed TensorFlow, set up a {class}`~ray.train.tensorflow.TensorflowTrainer` as follows:

```{testcode}
:hide:

train_func = lambda: None
```

```{testcode}
from ray.train import ScalingConfig
from ray.train.tensorflow import TensorflowTrainer
# For GPU Training, set `use_gpu` to True.
use_gpu = False
trainer = TensorflowTrainer(
    train_func,
    scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
)
```

To customize the backend setup, pass a {class}`~ray.train.tensorflow.TensorflowConfig`:

```{testcode}
:skipif: True

from ray.train import ScalingConfig
from ray.train.tensorflow import TensorflowTrainer, TensorflowConfig

trainer = TensorflowTrainer(
    train_func,
    tensorflow_backend=TensorflowConfig(...),
    scaling_config=ScalingConfig(num_workers=2),
)
```


For more configuration options, see the {py:class}`~ray.train.data_parallel_trainer.DataParallelTrainer` API.


## Run a training function

With a distributed training function and a Ray Train trainer in place, call `fit()` to start training:

```{testcode}
:skipif: True

trainer.fit()
```

## Load and preprocess data

By default, TensorFlow uses its own internal dataset sharding policy, as the [dataset sharding section](https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras#dataset_sharding) of TensorFlow's multi-worker tutorial describes. If your TensorFlow dataset is compatible with distributed loading, you don't need to change anything.

For more advanced preprocessing, consider using Ray Data for distributed data ingest. See {ref}`Ray Data with Ray Train <data-ingest-torch>`.

The main difference is that, to use the Keras API for model training, you might want to convert your Ray Data dataset shard to a TensorFlow dataset in your training function.

For distributed data loading, see the [TensorFlow autoencoder example](https://github.com/ray-project/ray/blob/master/python/ray/train/examples/tf/tensorflow_autoencoder_example.py). The following code shows the relevant parts:

```{testcode}
import tensorflow as tf
from ray import train
from ray.train.tensorflow import prepare_dataset_shard

def train_func(config: dict):
    # ...

    # Get dataset shard from Ray Train
    dataset_shard = train.get_context().get_dataset_shard("train")

    # Define a helper function to build a TensorFlow dataset
    def to_tf_dataset(dataset, batch_size):
        def to_tensor_iterator():
            for batch in dataset.iter_tf_batches(
                batch_size=batch_size, dtypes=tf.float32
            ):
                yield batch["image"], batch["label"]

        output_signature = (
            tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
            tf.TensorSpec(shape=(None, 784), dtype=tf.float32),
        )
        tf_dataset = tf.data.Dataset.from_generator(
            to_tensor_iterator, output_signature=output_signature
        )
        # Call prepare_dataset_shard to disable automatic sharding
        # (since the dataset is already sharded)
        return prepare_dataset_shard(tf_dataset)

    for epoch in range(epochs):
        # Call our helper function to build the dataset
        tf_dataset = to_tf_dataset(
            dataset=dataset_shard,
            batch_size=64,
        )
        history = multi_worker_model.fit(tf_dataset)
```



## Report results

During training, report intermediate results and checkpoints to Ray Train from the training loop. Reporting logs the results to the console output, appends them to local log files, and triggers {ref}`checkpoint bookkeeping <train-dl-configure-checkpoints>`.

The easiest way to report results with Keras is the {class}`~ray.train.tensorflow.keras.ReportCheckpointCallback`:

```{testcode}
from ray.train.tensorflow.keras import ReportCheckpointCallback

def train_func(config: dict):
    # ...
    for epoch in range(epochs):
        model.fit(dataset, callbacks=[ReportCheckpointCallback()])
```


This callback automatically forwards all results and checkpoints from the Keras training function to Ray Train.


### Aggregate results

TensorFlow Keras automatically aggregates metrics from all workers. For more control over aggregation, consider implementing a [custom training loop](https://www.tensorflow.org/tutorials/distribute/custom_training).


## Save and load checkpoints

Save {class}`checkpoints <ray.train.Checkpoint>` by calling `train.report(metrics, checkpoint=Checkpoint(...))` in the training function. This call saves the checkpoint state from the distributed workers on the trainer, where you ran your Python script.

Access the latest saved checkpoint through the `checkpoint` attribute of the {py:class}`~ray.train.Result`, and the best saved checkpoints through the `best_checkpoints` attribute.

The following examples show how Ray Train saves checkpoints in distributed training, saving model weights but not models.


```{testcode}
import json
import os
import tempfile

from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer

import numpy as np

def train_func(config):
    os.environ["TF_USE_LEGACY_KERAS"] = "1"
    import tensorflow as tf
    n = 100
    # create a toy dataset
    # data   : X - dim = (n, 4)
    # target : Y - dim = (n, 1)
    X = np.random.normal(0, 1, size=(n, 4))
    Y = np.random.uniform(0, 1, size=(n, 1))

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # toy neural network : 1-layer
        model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))])
        model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

    dataset = tf.data.Dataset.from_tensor_slices((X, Y)).batch(20)

    for epoch in range(config["num_epochs"]):
        history = model.fit(dataset)

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            model.save(os.path.join(temp_checkpoint_dir, "model.keras"))
            checkpoint_dict = os.path.join(temp_checkpoint_dir, "checkpoint.json")
            with open(checkpoint_dict, "w") as f:
                json.dump({"epoch": epoch}, f)
            checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

            train.report({"loss": history.history["loss"][0]}, checkpoint=checkpoint)

trainer = TensorflowTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
)
result = trainer.fit()
print(result.checkpoint)
```

By default, checkpoints persist to local disk in the {ref}`log directory <train-log-dir>` of each run.

### Load checkpoints

To load a checkpoint, call `train.get_checkpoint()` in the training function. The following example loads the model from the checkpoint if one exists, and otherwise builds a new model:

```{testcode}
import os
import tempfile

from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.tensorflow import TensorflowTrainer

import numpy as np

def train_func(config):
    os.environ["TF_USE_LEGACY_KERAS"] = "1"
    import tensorflow as tf
    n = 100
    # create a toy dataset
    # data   : X - dim = (n, 4)
    # target : Y - dim = (n, 1)
    X = np.random.normal(0, 1, size=(n, 4))
    Y = np.random.uniform(0, 1, size=(n, 1))

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # toy neural network : 1-layer
        checkpoint = train.get_checkpoint()
        if checkpoint:
            with checkpoint.as_directory() as checkpoint_dir:
                model = tf.keras.models.load_model(
                    os.path.join(checkpoint_dir, "model.keras")
                )
        else:
            model = tf.keras.Sequential(
                [tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))]
            )
        model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

    dataset = tf.data.Dataset.from_tensor_slices((X, Y)).batch(20)

    for epoch in range(config["num_epochs"]):
        history = model.fit(dataset)

        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            model.save(os.path.join(temp_checkpoint_dir, "model.keras"))
            extra_json = os.path.join(temp_checkpoint_dir, "checkpoint.json")
            with open(extra_json, "w") as f:
                json.dump({"epoch": epoch}, f)
            checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

            train.report({"loss": history.history["loss"][0]}, checkpoint=checkpoint)

trainer = TensorflowTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
)
result = trainer.fit()
print(result.checkpoint)
```


## Further reading

For more topics, see {ref}`User guides <train-user-guides>`, including the following:

- {ref}`Experiment tracking <train-experiment-tracking-native>`
- {ref}`Fault tolerance and training on spot instances <train-fault-tolerance>`
- {ref}`Hyperparameter optimization <train-tune>`
