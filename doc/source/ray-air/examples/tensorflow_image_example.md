---
jupytext:
    text_representation:
        extension: .md
        format_name: myst
kernelspec:
    display_name: Python 3
    language: python
    name: python3
---


# Training a TensorFlow Classifier

This tutorial demonstrates how to train an image classifier using the [Ray AI Runtime](air) (AIR).

You should be familiar with TensorFlow before starting this tutorial. If you need a refresher, read TensorFlow's [Convolutional Neural Network](https://www.tensorflow.org/tutorials/images/cnn) tutorial.

## Before you begin

* Install the [Ray AI Runtime](air). You'll need Ray 1.13 or later to run this example.

```{code-cell} python3
!pip install 'ray[air]'
```

* Install `tensorflow` and `tensorflow-datasets`

```{code-cell} python3
!pip install tensorflow tensorflow-datasets
```

## Load and normalize CIFAR-10

We'll train our classifier on a popular image dataset called [CIFAR-10](https://www.cs.toronto.edu/~kriz/cifar.html).

First, let's load CIFAR-10 into a Ray Dataset.

```{code-cell} ipython3
import ray
from ray.data.datasource import SimpleTensorFlowDatasource
import tensorflow as tf

from tensorflow.keras import layers, models
import tensorflow_datasets as tfds


def train_dataset_factory():
    return tfds.load("cifar10", split=["train"], as_supervised=True)[0]

def test_dataset_factory():
    return tfds.load("cifar10", split=["test"], as_supervised=True)[0]

train_dataset = ray.data.read_datasource(
    SimpleTensorFlowDatasource(), dataset_factory=train_dataset_factory
)
test_dataset = ray.data.read_datasource(
    SimpleTensorFlowDatasource(), dataset_factory=test_dataset_factory
)

train_dataset
```

Note that {py:class}`SimpleTensorFlowDatasource <ray.data.datasource.SimpleTensorFlowDatasource>` loads all data into memory, so you shouldn't use it with larger datasets.

Our model will expect float arrays, so let's normalize pixel values to be between 0 and 1.

```{code-cell} ipython3
def normalize_images(batch):
    return [(tf.cast(image, tf.float32) / 255.0, label) for image, label in batch]

train_dataset = train_dataset.map_batches(normalize_images)
test_dataset = test_dataset.map_batches(normalize_images)
```

Next, let's represent our data using Pandas DataFrames instead of tuples. This lets us call methods like {py:meth}`Dataset.to_tf <ray.data.Dataset.to_tf>` later in the tutorial.

```{code-cell} python3
import pandas as pd
from ray.data.extensions import TensorArray


def convert_batch_to_pandas(batch):
    images = TensorArray([image.numpy() for image, _ in batch])
    labels = [label.numpy() for _, label in batch]

    df = pd.DataFrame({"image": images, "label": labels})

    return df


train_dataset = train_dataset.map_batches(convert_batch_to_pandas)
test_dataset = test_dataset.map_batches(convert_batch_to_pandas)

test_dataset
```

## Train a convolutional neural network

Now that we've created our datasets, let's define the training logic.

```{code-cell} python3
def build_model():
    model = models.Sequential()
    model.add(layers.Conv2D(6, (5, 5), activation='relu', input_shape=(32, 32, 3)))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(16, (5, 5), activation='relu'))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Flatten())
    model.add(layers.Dense(120, activation='relu'))
    model.add(layers.Dense(84, activation='relu'))
    model.add(layers.Dense(10))
    return model
```

We define our training logic in a function called `train_loop_per_worker`.

`train_loop_per_worker` contains regular TensorFlow code with a few notable exceptions:
* We build and compile our model in the [`MultiWorkerMirrioredStrategy`](https://www.tensorflow.org/api_docs/python/tf/distribute/experimental/MultiWorkerMirroredStrategy) context.
* We call {py:func}`train.get_dataset_shard <ray.train.get_dataset_shard>` to get a subset of our training data, and call {py:meth}`Dataset.to_tf <ray.data.Dataset.to_tf>` with {py:func}`prepare_dataset_shard <ray.train.tensorflow.prepare_dataset_shard>` to convert the subset to a TensorFlow dataset.
* We save model state using {py:func}`train.save_checkpoint <ray.train.save_checkpoint>`.

```{code-cell} python3
from ray import train
from ray.train.tensorflow import prepare_dataset_shard


def train_loop_per_worker(config):
    dataset_shard = train.get_dataset_shard("train")
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    with strategy.scope():
        model = build_model()
        model.compile(optimizer='adam',
                    loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                    metrics=['categorical_accuracy'])

    for epoch in range(2):
        tf_dataset = prepare_dataset_shard(
            dataset_shard.to_tf(
                feature_columns=["image"],
                label_column="label",
                output_signature=(
                    tf.TensorSpec(shape=(None, 32, 32, 3), dtype=tf.float32),
                    tf.TensorSpec(shape=(None,), dtype=tf.uint8),
                ),
                batch_size=config["batch_size"],
            )
        )
        model.fit(tf_dataset)
        train.save_checkpoint(epoch=epoch, model=model.get_weights())
```

Finally, we can train our model. This should take a few minutes to run.

```{code-cell} python3
from ray.ml.train.integrations.tensorflow import TensorflowTrainer

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"batch_size": 2},
    datasets={"train": train_dataset},
    scaling_config={"num_workers": 2}
)
result = trainer.fit()
latest_checkpoint = result.checkpoint
```

To scale your training script, create a [Ray Cluster](deployment-guide) and increase the number of workers. If your cluster contains GPUs, add `"use_gpu": True` to your scaling config.

```{code-block} python
scaling_config={"num_workers": 8, "use_gpu": True}
```

## Test the network on the test data

Let's see how our model performs.

To classify images in the test dataset, we'll need to create a {py:class}`Predictor <ray.ml.predictor.Predictor>`.

{py:class}`Predictors <ray.ml.predictor.Predictor>` load data from checkpoints and efficiently perform inference. In contrast to {py:class}`TensorflowPredictor <ray.ml.predictors.integrations.tensorflow.TensorflowPredictor>`, which performs inference on a single batch, {py:class}`BatchPredictor <ray.ml.batch_predictor.BatchPredictor>` performs inference on an entire dataset. Because we want to classify all of the images in the test dataset, we'll use a {py:class}`BatchPredictor <ray.ml.batch_predictor.BatchPredictor>`.

```{code-cell} python3
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.batch_predictor import BatchPredictor
batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint=latest_checkpoint,
    predictor_cls=TensorflowPredictor,
    model_definition=build_model,
)


outputs: ray.data.Dataset = batch_predictor.predict(
    data=test_dataset, feature_columns=["image"])
```

Our models outputs a list of energies for each class. To classify an image, we
choose the class that has the highest energy.

```{code-cell} python3
import numpy as np

def convert_logits_to_classes(df):
    best_class = df["predictions"].map(lambda x: x.argmax())
    df["prediction"] = best_class
    return df[["prediction"]]

predictions = outputs.map_batches(
    convert_logits_to_classes, batch_format="pandas"
)

predictions.show(1)
```

Now that we've classified all of the images, let's figure out which images were
classified correctly. The ``predictions`` dataset contains predicted labels and 
the ``test_dataset`` contains the true labels. To determine whether an image 
was classified correctly, we join the two datasets and check if the predicted 
labels are the same as the actual labels.

```{code-cell} python3
def calculate_prediction_scores(df):
    df["correct"] = df["prediction"] == df["label"]
    return df[["prediction", "label", "correct"]]

scores = test_dataset.zip(predictions).map_batches(calculate_prediction_scores)

scores.show(1)
```

To compute our test accuracy, we'll count how many images the model classified 
correctly and divide that number by the total number of test images. 

```{code-cell} python3
scores.sum(on="correct") / scores.count()
```

## Deploy the network and make a prediction

Our model seems to perform decently, so let's deploy the model to an 
endpoint. This'll allow us to make predictions over the Internet.

```{code-cell} python3
from ray import serve
from ray.serve.model_wrappers import ModelWrapperDeployment

serve.start(detached=True)
deployment = ModelWrapperDeployment.options(name="my-deployment")
deployment.deploy(TensorflowPredictor, latest_checkpoint, batching_params=False, model_definition=build_model)
```

Let's classify a test image.

```{code-cell} python3
batch = test_dataset.take(1)
array = np.expand_dims(np.array(batch[0]["image"]), axis=0)
```

You can perform inference against a deployed model by posting a dictionary with an `"array"` key. To learn more about the default input schema, read the {py:class}`NdArray <ray.serve.http_adapters.NdArray>` documentation.

```{code-cell} python3
import requests
payload = {"array": array.tolist()}
response = requests.post(deployment.url, json=payload)
response.json()
```
