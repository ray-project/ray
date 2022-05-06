import numpy as np

from torch import nn, optim
from torchvision import datasets, transforms

import ray
from ray.ml.train.integrations.torch.torch_ots import TorchOffTheShelfTrainer


def get_dataset(train: bool = False) -> ray.data.Dataset:
    """Download FashionMNIST dataset and return as Ray dataset."""
    training_data = datasets.FashionMNIST(
        root="~/data",
        train=train,
        download=True,
        transform=transforms.Lambda(lambda img: np.array(img).tolist()),
    )
    ds = ray.data.from_items([{"x": x, "y": y} for x, y in training_data])
    return ds


class NeuralNetwork(nn.Module):
    """Simple MLP with three layers."""

    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10),
            nn.ReLU(),
        )

    def forward(self, x):
        x = self.flatten(x)
        return self.linear_relu_stack(x)


# Initialize the off-the-shelf trainer and fit to the dataset

trainer = TorchOffTheShelfTrainer(
    model_cls=NeuralNetwork,
    optimizer_cls=optim.SGD,
    optimizer_args={"lr": 0.05},
    scaling_config={"num_workers": 2},
    feature_columns=["x"],
    label_column="y",
    datasets={
        "train": lambda: get_dataset(train=True),
        "validate": lambda: get_dataset(train=False),
    },
)
trainer.fit()
