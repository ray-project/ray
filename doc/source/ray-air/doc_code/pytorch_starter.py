# flake8: noqa
# isort: skip_file

# __air_pytorch_preprocess_start__

from torchvision import datasets
from torchvision.transforms import ToTensor

# Download training data from open datasets.
training_data = datasets.FashionMNIST(
    root="~/data",
    train=True,
    download=True,
    transform=ToTensor(),
)

# Download test data from open datasets.
test_data = datasets.FashionMNIST(
    root="~/data",
    train=False,
    download=True,
    transform=ToTensor(),
)

# __air_pytorch_preprocess_end__

# __air_pytorch_train_start__
import torch
from torch import nn
from torch.utils.data import DataLoader
import ray.train as train
from ray.air import session
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

# Define model
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


def train_epoch(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset) // session.get_world_size()
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def validate_epoch(dataloader, model, loss_fn):
    size = len(dataloader.dataset) // session.get_world_size()
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(
        f"Test Error: \n "
        f"Accuracy: {(100 * correct):>0.1f}%, "
        f"Avg loss: {test_loss:>8f} \n"
    )
    return test_loss


def train_func(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]

    worker_batch_size = batch_size // session.get_world_size()

    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=worker_batch_size)
    test_dataloader = DataLoader(test_data, batch_size=worker_batch_size)

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    # Create model.
    model = NeuralNetwork()
    model = train.torch.prepare_model(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for _ in range(epochs):
        train_epoch(train_dataloader, model, loss_fn, optimizer)
        loss = validate_epoch(test_dataloader, model, loss_fn)
        session.report(dict(loss=loss))


num_workers = 2
use_gpu = False

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 4},
    scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
)
result = trainer.fit()
print(f"Last result: {result.metrics}")
# __air_pytorch_train_end__


# # __air_pytorch_batchpred_start__
# import random
# from ray.train.batch_predictor import BatchPredictor
# from ray.train.torch import TorchPredictor

# batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TorchPredictor)

# items = [{"x": random.uniform(0, 1) for _ in range(10)}]
# prediction_dataset = ray.data.from_items(items)

# predictions = batch_predictor.predict(prediction_dataset, dtype=torch.float)
# # __air_pytorch_batchpred_end__
