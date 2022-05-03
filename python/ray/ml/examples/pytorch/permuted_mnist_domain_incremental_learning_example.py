"""Example of domain incremental learning with Permuted MNIST.

We train a simple neural network N times, with each task/experience containing a
different pixel permutation of MNIST.

After training on N tasks, the neural network can make predictions on any permutation
of MNIST digits that it was trained on.

This example uses just a naive fine-tuning strategy.
"""
from typing import Iterator

import pandas as pd
import torch.nn as nn
from torch.nn import CrossEntropyLoss
from torch.optim import SGD
import torchvision
from torchvision import transforms
from torchvision.transforms import ToTensor, RandomCrop
import numpy as np

import ray
from ray import train
from ray.data.datasource.torch_datasource import SimpleTorchDatasource
from ray.data import ActorPoolStrategy
from ray.data.extensions import TensorArray
from ray.ml import Checkpoint
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.preprocessors import BatchMapper
from ray.ml.train.integrations.torch import TorchTrainer


class SimpleMLP(nn.Module):
    def __init__(self, num_classes=10, input_size=28 * 28):
        super(SimpleMLP, self).__init__()

        self.features = nn.Sequential(
            nn.Linear(input_size, 512),
            nn.ReLU(inplace=True),
            nn.Dropout(),
        )
        self.classifier = nn.Linear(512, num_classes)
        self._input_size = input_size

    def forward(self, x):
        x = x.contiguous()
        x = x.view(-1, self._input_size)
        x = self.features(x)
        x = self.classifier(x)
        return x


def get_mnist_dataset(train: bool = True) -> ray.data.Dataset:
    """Returns MNIST Dataset as a ray.data.Dataset.

    Args:
        train: Whether to return the train dataset or test dataset.
    """

    def mnist_dataset_factory():
        torchvision.datasets.MNIST("./data", download=True, train=train)

    mnist_dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=mnist_dataset_factory
    )

    # TODO: provide a better way to apply torchvision transforms.
    # Do some initial preprocessing on the PIL Image directly, then convert to numpy.
    if train:
        mnist_dataset = mnist_dataset.map_batches(
            lambda batch: [
                (RandomCrop(28, padding=4)(image), label) for (image, label) in batch
            ]
        )
    mnist_dataset = mnist_dataset.map_batches(
        lambda batch: [(np.asarray(image), label) for (image, label) in batch]
    )

    return mnist_dataset


class PermutedMNISTStream:
    """Generates streams of permuted MNIST Datasets.

    Args:
        n_tasks: The number of tasks to generate.

    """

    def __init__(self, n_tasks: int = 3):
        self.n_tasks = n_tasks
        self.permutations = [
            np.random.permutation(28 * 28) for _ in range(self.n_tasks)
        ]

    def random_permute_dataset(
        self, dataset: ray.data.Dataset, permutation: np.ndarray
    ):
        """Randomly permutes the pixels for each image in the dataset."""

        # permutation transformation
        class PixelsPermutation(object):
            def __call__(self, batch):
                permuted_batch = [
                    (image.reshape(-1)[permutation].reshape(28, 28), label)
                    for (image, label) in batch
                ]
                return permuted_batch

        return dataset.map_batches(PixelsPermutation, compute=ActorPoolStrategy())

    def generate_train_stream(self) -> Iterator[ray.data.Dataset]:
        for permutation in self.permutations:
            mnist_dataset = get_mnist_dataset(train=True)
            permuted_mnist_dataset = self.random_permute_dataset(
                mnist_dataset, permutation
            )
            yield convert_dataset_to_pandas_format(permuted_mnist_dataset)

    def generate_test_stream(self) -> Iterator[ray.data.Dataset]:
        for permutation in self.permutations:
            mnist_dataset = get_mnist_dataset(train=False)
            permuted_mnist_dataset = self.random_permute_dataset(
                mnist_dataset, permutation
            )
            yield convert_dataset_to_pandas_format(permuted_mnist_dataset)


# TODO: Handle this automatically in TorchVisionDatasource
def convert_dataset_to_pandas_format(image_dataset: ray.data.Dataset):
    """Converts the image dataset consisting of tuples to Pandas format.

    Converting to Pandas format is necessary so that Dataset can be used with
    Preprocessors and Predictors.
    """

    def convert_batch_to_pandas(batch):
        images = [item[0] for item in batch]
        labels = [item[1] for item in batch]

        df = pd.DataFrame({"image": TensorArray(images), "label": labels})

        return df

    return image_dataset.map_batches(convert_batch_to_pandas)


def train_loop_per_worker(config: dict):
    num_epochs = config["num_epochs"]
    learning_rate = config["learning_rate"]
    momentum = config["momentum"]
    batch_size = config["batch_size"]

    model = SimpleMLP(num_classes=10)

    # Load model from checkpoint if there is a checkpoint to load from.
    checkpoint_to_load = train.load_checkpoint()
    if checkpoint_to_load:
        state_dict_to_resume_from = checkpoint_to_load["model"]
        model.load_state_dict(state_dict=state_dict_to_resume_from)

    model = train.torch.prepare_model(model)

    optimizer = SGD(model.parameters(), lr=learning_rate, momentum=momentum)
    criterion = CrossEntropyLoss()

    dataset_shard = train.get_dataset_shard("train").to_torch(
        label_column="label",
        batch_size=batch_size,
        unsqueeze_feature_tensors=False,
        unsqueeze_label_tensor=False,
    )

    for epoch_idx in range(num_epochs):
        for iteration, (train_mb_x, train_mb_y) in enumerate(dataset_shard):
            optimizer.zero_grad()
            train_mb_x = train_mb_x.to(train.torch.get_device())
            train_mb_y = train_mb_y.to(train.torch.get_device())

            # Forward
            logits = model(train_mb_x)
            # Loss
            loss = criterion(logits, train_mb_y)
            # Backward
            loss.backward()
            # Update
            optimizer.step()

            if train.world_rank() == 0 and iteration % 100 == 0:
                print(f"loss: {loss.item():>7f}, iteration: {iteration}")

        # Checkpoint model after every epoch.
        train.save_checkpoint(model=model.module.state_dict())


def preprocess_images(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess images by scaling each channel in the image."""

    torchvision_transforms = transforms.Compose(
        [ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    new_df = df.copy()
    new_df["image"] = df["image"].map(torchvision_transforms)
    return new_df


def incremental_train(dataset_stream: Iterator[ray.data.Dataset]) -> Checkpoint:
    """Incrementally trains MLP model on each task in a distributed fashion.

    Args:
        dataset_stream: The stream of datasets to train on.
    """

    latest_checkpoint = None

    for dataset in dataset_stream:
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config={
                "num_epochs": 2,
                "learning_rate": 0.001,
                "momentum": 0.9,
                "batch_size": 32,
            },
            scaling_config={"num_workers": 2, "use_gpu": False},
            datasets={"train": dataset},
            preprocessor=BatchMapper(fn=preprocess_images),
            resume_from_checkpoint=latest_checkpoint,
        )
        result = trainer.fit()
        latest_checkpoint = result.checkpoint

    return latest_checkpoint


def predict(checkpoint: Checkpoint, test_dataset_stream: Iterator[ray.data.Dataset]):
    """Runs prediction of the incrementally trained model on stream of test datasets.

    Args:
        checkpoint: The checkpoint containing the trained model.
        test_dataset_stream: A stream of Ray Datasets to test on.
    """

    acc_results = []
    batch_predictor = BatchPredictor.from_checkpoint(
        checkpoint=checkpoint,
        predictor_cls=TorchPredictor,
        model=SimpleMLP(num_classes=10),
    )

    for test_dataset in test_dataset_stream:
        model_output = batch_predictor.predict(
            data=test_dataset, feature_columns=["image"], unsqueeze=False
        )
        # Postprocess model outputs.
        # Convert logits outputted from model into actual class predictions.
        # TODO: Find a better way to do this. This is pretty ugly right now.
        # TODO: See if Predictors can accept "postprocessing" transforms to be applied
        #  directly after model output.

        def convert_logits_to_classes(df):
            best_class = df["predictions"].map(lambda x: np.array(x).argmax())
            df["predictions"] = best_class
            return df

        prediction_results = model_output.map_batches(
            convert_logits_to_classes, batch_format="pandas"
        )

        # Then, for each prediction output, see if it matches with the ground truth
        # label.
        # TODO: Find a better way to do this. This is pretty ugly right now.
        zipped_dataset = test_dataset.zip(prediction_results)

        def calculate_prediction_scores(df):
            df["correct"] = df["predictions"] == df["label"]
            return df

        correct_dataset = zipped_dataset.map_batches(
            calculate_prediction_scores, batch_format="pandas"
        )

        acc_results.append(correct_dataset.sum(on="correct") / test_dataset.count())

    print(acc_results)
    return acc_results


if __name__ == "__main__":
    dataset_stream = PermutedMNISTStream(n_tasks=3)
    checkpoint = incremental_train(dataset_stream.generate_train_stream())
    checkpoint.to_directory("./checkpoint")
    predict(
        Checkpoint.from_directory("./checkpoint"), dataset_stream.generate_test_stream()
    )
