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


# Training a Torch Classifier

This tutorial demonstrates how to train an image classifier using the [Ray AI Runtime](air) (AIR).

You should be familiar with [PyTorch](https://pytorch.org/) before starting the tutorial. If you need a refresher, read PyTorch's [training a classifier](https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html) tutorial.

## Before you begin

* Install the [Ray AI Runtime](air). You'll need Ray 1.13 later to run this example.

```{code-cell} python3
!pip install 'ray[air]'
```

* Install `requests`, `torch`, and `torchvision`

```{code-cell} python3
!pip install requests torch torchvision
```

## Load and normalize CIFAR-10

We'll train our classifier on a popular image dataset called [CIFAR-10](https://www.cs.toronto.edu/~kriz/cifar.html).

First, let's load CIFAR-10 into a Ray Dataset.

```{code-cell} ipython3
import ray
from ray.data.datasource import SimpleTorchDatasource
import torchvision
import torchvision.transforms as transforms

transform = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
)

def train_dataset_factory():
    return torchvision.datasets.CIFAR10(root="./data", download=True, train=True, transform=transform)

def test_dataset_factory():
    return torchvision.datasets.CIFAR10(root="./data", download=True, train=False, transform=transform)

train_dataset: ray.data.Dataset = ray.data.read_datasource(SimpleTorchDatasource(), dataset_factory=train_dataset_factory)
test_dataset: ray.data.Dataset = ray.data.read_datasource(SimpleTorchDatasource(), dataset_factory=test_dataset_factory)

train_dataset
```

Note that {py:class}`SimpleTorchDatasource <ray.data.datasource.SimpleTorchDatasource>` loads all data into memory, so you shouldn't use it with larger datasets.

Next, let's represent our data using pandas dataframes instead of tuples. This lets us call methods like {py:meth}`Dataset.to_torch <ray.data.Dataset.to_torch>` later in the tutorial.

```{code-cell} python3
from typing import Tuple
import pandas as pd
from ray.data.extensions import TensorArray


def convert_batch_to_pandas(batch: Tuple[torch.Tensor, int]) -> pd.DataFrame:
    images = [TensorArray(image.numpy()) for image, _ in batch]
    labels = [label for _, label in batch]

    df = pd.DataFrame({"image": images, "label": labels})

    return df


train_dataset = train_dataset.map_batches(convert_batch_to_pandas)
test_dataset = test_dataset.map_batches(convert_batch_to_pandas)

train_dataset
```

## Train a convolutional neural network

Now that we've created our datasets, let's define the training logic.

```{code-cell} python3
import torch
import torch.nn as nn
import torch.nn.functional as F


class Net(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = torch.flatten(x, 1)  # flatten all dimensions except batch
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x
```

We define our training logic in a function called `train_loop_per_worker`.

`train_loop_per_worker` contains regular PyTorch code with a few notable exceptions:
* We wrap our model with {py:func}`train.torch.prepare_model <ray.train.torch.prepare_model>`.
* We call {py:func}`train.get_dataset_shard <ray.train.get_dataset_shard>` and {py:meth}`Dataset.to_torch <ray.data.Dataset.to_torch>` to convert a subset of our training data to a Torch dataset.
* We save model state using {py:func}`train.save_checkpoint <ray.train.save_checkpoint>`.

```{code-cell} python3
from ray import train
import torch.optim as optim


def train_loop_per_worker(config):
    model = train.torch.prepare_model(Net())

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(model.parameters(), lr=0.001, momentum=0.9)

    train_dataset_shard: torch.utils.data.Dataset = train.get_dataset_shard("train").to_torch(
        feature_columns=["image"],
        label_column="label",
        batch_size=config["batch_size"],
        unsqueeze_feature_tensors=False,
        unsqueeze_label_tensor=False
    )

    for epoch in range(2):
        running_loss = 0.0
        for i, data in enumerate(train_dataset_shard):
            # get the inputs; data is a list of [inputs, labels]
            inputs, labels = data

            # zero the parameter gradients
            optimizer.zero_grad()

            # forward + backward + optimize
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            # print statistics
            running_loss += loss.item()
            if i % 2000 == 1999:  # print every 2000 mini-batches
                print(f"[{epoch + 1}, {i + 1:5d}] loss: {running_loss / 2000:.3f}")
                running_loss = 0.0

        train.save_checkpoint(model=model.module.state_dict())
```

Finally, we can train our model. This should take a few minutes to run.

```{code-cell} python3
from ray.ml.train.integrations.torch import TorchTrainer

trainer = TorchTrainer(
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

{py:class}`Predictors <ray.ml.predictor.Predictor>` load data from checkpoints and efficiently perform inference. In contrast to {py:class}`TorchPredictor <ray.ml.predictors.integrations.torch.TorchPredictor>`, which performs inference on a single batch, {py:class}`BatchPredictor <ray.ml.batch_predictor.BatchPredictor>` performs inference on an entire dataset. Because we want to classify all of the images in the test dataset, we'll use a {py:class}`BatchPredictor <ray.ml.batch_predictor.BatchPredictor>`.

```{code-cell} python3
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.batch_predictor import BatchPredictor

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint=latest_checkpoint,
    predictor_cls=TorchPredictor,
    model=Net(),
)

outputs: ray.data.Dataset = batch_predictor.predict(
    data=test_dataset, feature_columns=["image"], unsqueeze=False
)

outputs.show(1)
```

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
deployment.deploy(TorchPredictor, latest_checkpoint, batching_params=False, model=Net())
```

Let's classify a test image.

```{code-cell} python3
batch = test_dataset.take(1)
array = np.array(batch[0]["image"])
```

You can perform inference against a deployed model by posting a dictionary with an `"array"` key. To learn more about the default input schema, read the {py:class}`NdArray <ray.serve.http_adapters.NdArray>` documentation.

```{code-cell} python3
import requests

payload = {"array": array.tolist()}
response = requests.post(deployment.url, json=payload)
response.json()
```
