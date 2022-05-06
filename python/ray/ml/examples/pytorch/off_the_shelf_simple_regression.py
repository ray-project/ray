from sklearn.datasets import make_regression
from torch import nn, optim

import ray
from ray.ml.train.integrations.torch.torch_ots import TorchOffTheShelfTrainer


def get_dataset() -> ray.data.Dataset:
    features, labels = make_regression(
        n_samples=10_000, n_features=100, n_informative=2
    )
    return ray.data.from_items([{"x": x, "y": y} for x, y in zip(features, labels)])


trainer = TorchOffTheShelfTrainer(
    model_cls=nn.Linear,
    model_args={"in_features": 100, "out_features": 1},
    loss_fn_cls=nn.MSELoss,
    optimizer_cls=optim.SGD,
    optimizer_args={"lr": 0.05},
    scaling_config={"num_workers": 2},
    datasets={"train": lambda: get_dataset()},
)
trainer.fit()
