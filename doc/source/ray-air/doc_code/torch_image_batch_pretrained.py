"""Batch prediction using a pretrained pytorch model on CIFAR."""

from typing import Tuple

import numpy as np
import pandas as pd
import torch
import torchvision
from torchvision import models, transforms

import ray
from ray.data.datasource import SimpleTorchDatasource
from ray.data.extensions import TensorArray
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor, to_air_checkpoint
from ray.util.ml_utils.resnet import ResNet18


ray.init()

transform = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
)

def preprocess_images(batch: Tuple[torch.Tensor, int]) -> pd.DataFrame:
    images = [TensorArray(image.numpy()) for image, _ in batch]
    labels = [label for _, label in batch]

    df = pd.DataFrame({"image": images, "label": labels})

    return df

def create_unprocessed_test_dataset():
    def test_dataset_factory():
        return torchvision.datasets.CIFAR10(root="./data", download=True, train=False, transform=transform)
    
    test_dataset: ray.data.Dataset = ray.data.read_datasource(SimpleTorchDatasource(), dataset_factory=test_dataset_factory)
    test_dataset = test_dataset.map_batches(preprocess_images)
    return test_dataset

def get_accuracy(batch_predictor, dataset) -> float:


# __batch_prediction_start__
test_mnist_dataset = create_unprocessed_test_dataset()
model = models.resnet18(pretrained=True)
# mdoel =  ResNet18()

checkpoint = to_air_checkpoint(model)
# Perform batch prediction on the provided test dataset, and return accuracy results."""

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint,
    predictor_cls=TorchPredictor)
    
model_output = batch_predictor.predict(
    data=test_mnist_dataset, feature_columns=["image"], unsqueeze=False)

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
zipped_dataset = test_mnist_dataset.zip(prediction_results)

def calculate_prediction_scores(df):
    return pd.DataFrame({"correct": df["predictions"] == df["label"]})

correct_dataset = zipped_dataset.map_batches(
    calculate_prediction_scores, batch_format="pandas")

result = correct_dataset.sum(on="correct") / correct_dataset.count()
print(result)
# __batch_prediction_end__
