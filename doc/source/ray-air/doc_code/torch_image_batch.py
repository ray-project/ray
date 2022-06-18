import os

import numpy as np
import pandas as pd
import torch.nn as nn
import torchvision
from torch.nn import CrossEntropyLoss
from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present
from torch.optim import SGD
from torchvision import transforms
from torchvision.transforms import RandomCrop

import ray
from ray import train
from ray.data.datasource.torch_datasource import SimpleTorchDatasource
from ray.data.preprocessors import BatchMapper
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor, TorchTrainer

DATA_PATH = os.path.expanduser("~/data")

# Number of epochs to train each task for.
num_epochs = 4
# Batch size.
batch_size = 32
# Optimizer args.
learning_rate = 0.001
momentum = 0.9

# Number of data parallel workers to use for training.
num_workers = 1
# Whether to use GPU or not.
use_gpu = True


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
        if train:
            # Only perform random cropping on the Train dataset.
            transform = RandomCrop(28, padding=4)
        else:
            transform = None
        return torchvision.datasets.MNIST(
            DATA_PATH, download=True, train=train, transform=transform)

    def convert_batch_to_pandas(batch):
        images = [np.array(item[0]) for item in batch]
        labels = [item[1] for item in batch]

        df = pd.DataFrame({"image": images, "label": labels})

        return df

    mnist_dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=mnist_dataset_factory)
    mnist_dataset = mnist_dataset.map_batches(convert_batch_to_pandas)
    return mnist_dataset


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

    # Get the Ray Dataset shard for this data parallel worker, and convert it to a PyTorch Dataset.
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

            if train.world_rank() == 0 and iteration % 500 == 0:
                print(
                    f"loss: {loss.item():>7f}, epoch: {epoch_idx}, iteration: {iteration}"
                )

        # Checkpoint model after every epoch.
        state_dict = model.state_dict()
        consume_prefix_in_state_dict_if_present(state_dict, "module.")
        train.save_checkpoint(model=state_dict)


def preprocess_images(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess images by scaling each channel in the image."""

    torchvision_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    df["image"] = df["image"].map(torchvision_transforms)
    return df


def train_on_data(train_dataset):
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={
            "num_epochs": num_epochs,
            "learning_rate": learning_rate,
            "momentum": momentum,
            "batch_size": batch_size,
        },
        # Have to specify trainer_resources as 0 so that the example works on Colab.
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
            "trainer_resources": {
                "CPU": 0
            }
        },
        datasets={"train": train_dataset},
        preprocessor=BatchMapper(fn=preprocess_images),
    )
    result = trainer.fit()
    return result.checkpoint


def batch_predict(checkpoint: ray.air.Checkpoint,
                  test_dataset: ray.data.Dataset) -> float:
    """Perform batch prediction on the provided test dataset, and return accuracy results."""

    batch_predictor = BatchPredictor.from_checkpoint(
        checkpoint,
        predictor_cls=TorchPredictor,
        model=SimpleMLP(num_classes=10))
    model_output = batch_predictor.predict(
        data=test_dataset, feature_columns=["image"], unsqueeze=False)

    # Postprocess model outputs.
    # Convert logits outputted from model into actual class predictions.
    def convert_logits_to_classes(df):
        best_class = df["predictions"].map(lambda x: np.array(x).argmax())
        df["predictions"] = best_class
        return df

    prediction_results = model_output.map_batches(
        convert_logits_to_classes, batch_format="pandas")

    # Then, for each prediction output, see if it matches with the ground truth
    # label.
    zipped_dataset = test_dataset.zip(prediction_results)

    def calculate_prediction_scores(df):
        return pd.DataFrame({"correct": df["predictions"] == df["label"]})

    correct_dataset = zipped_dataset.map_batches(
        calculate_prediction_scores, batch_format="pandas")

    result = correct_dataset.sum(on="correct") / correct_dataset.count()
    print(result)
    return result


if __name__ == "__main__":
    ray.init()
    # If runnning on a cluster, use the below line instead.
    # ray.init("auto")

    train_dataset = get_mnist_dataset(train=True)
    test_mnist_dataset = get_mnist_dataset(train=False)
    checkpoint = train_on_data(train_dataset)
    result = batch_predict(checkpoint, test_mnist_dataset)
