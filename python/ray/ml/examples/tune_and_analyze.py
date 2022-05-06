import numpy as np
import pandas as pd
from torch import nn, optim
from torchvision import datasets, transforms


import ray
from ray.ml.train.integrations.torch.torch_ots import TorchOffTheShelfTrainer


def get_train_dataset() -> ray.data.Dataset:
    training_data = datasets.FashionMNIST(
        root="~/data",
        train=True,
        download=True,
        transform=transforms.Lambda(lambda img: np.array(img)),
        target_transform=transforms.Lambda(lambda target: np.array(target)),
    )
    print(list(training_data))
    raise RuntimeError
    df = pd.DataFrame({"x": training_data, "y": training_targets})
    ds = ray.data.from_pandas(df)
    return ds


class NeuralNetwork(nn.Module):
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
        logits = self.linear_relu_stack(x)
        return logits


train_dataset = get_train_dataset()

trainer = TorchOffTheShelfTrainer(
    model_cls=NeuralNetwork,
    optimizer_cls=optim.SGD,
    optimizer_args={"lr": 0.05},
    scaling_config={"num_workers": 2},
    datasets={"train": train_dataset},
)
trainer.fit()
