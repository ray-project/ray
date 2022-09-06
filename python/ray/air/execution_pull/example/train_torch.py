import numpy as np
import torch
import torch.nn as nn

import ray.train as train
from ray.air import session
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.impl.train.train_controller import TrainController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.train._internal.utils import construct_train_func
from ray.train.torch import TorchConfig


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def train_epoch(dataloader, model, loss_fn, optimizer):
    for X, y in dataloader:
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(dataloader, model, loss_fn):
    num_batches = len(dataloader)
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    import copy

    model_copy = copy.deepcopy(model)
    result = {"model": model_copy.cpu().state_dict(), "loss": loss}
    return result


def train_func(config):
    print("START TRAINING")
    data_size = config.get("data_size", 1000)
    val_size = config.get("val_size", 400)
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset = LinearDataset(2, 5, size=data_size)
    val_dataset = LinearDataset(2, 5, size=val_size)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size)
    validation_loader = torch.utils.data.DataLoader(val_dataset, batch_size=batch_size)

    train_loader = train.torch.prepare_data_loader(train_loader)
    validation_loader = train.torch.prepare_data_loader(validation_loader)

    model = nn.Linear(1, hidden_size)
    model = train.torch.prepare_model(model)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []
    for _ in range(epochs):
        train_epoch(train_loader, model, loss_fn, optimizer)
        result = validate_epoch(validation_loader, model, loss_fn)
        results.append(result)
        session.report(result)

    # return required for backwards compatibility with the old API
    # TODO(team-ml) clean up and remove return
    return results


def train_linear(num_workers=2, use_gpu=False, epochs=3):
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}

    wrapped_train_func = construct_train_func(train_func, config)

    train_controller = TrainController(
        train_fn=wrapped_train_func,
        backend_config=TorchConfig(),
    )
    fixed_resource_manager = FixedResourceManager(total_resources={"CPU": 4})
    manager = ActorManager(
        controller=train_controller, resource_manager=fixed_resource_manager
    )
    manager.step_until_finished()


if __name__ == "__main__":
    train_linear(num_workers=2)
