# flake8: noqa
# fmt: off
# isort: skip_file

# __torch_setup_begin__
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor

def get_dataset():
    return datasets.FashionMNIST(
        root="/tmp/data",
        train=True,
        download=True,
        transform=ToTensor(),
    )

class NeuralNetwork(nn.Module):
    def __init__(self):
        super().__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10),
        )

    def forward(self, inputs):
        inputs = self.flatten(inputs)
        logits = self.linear_relu_stack(inputs)
        return logits
# __torch_setup_end__

# __torch_single_begin__
def train_func():
    num_epochs = 3
    batch_size = 64

    dataset = get_dataset()
    dataloader = DataLoader(dataset, batch_size=batch_size)

    model = NeuralNetwork()

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        for inputs, labels in dataloader:
            optimizer.zero_grad()
            pred = model(inputs)
            loss = criterion(pred, labels)
            loss.backward()
            optimizer.step()
        print(f"epoch: {epoch}, loss: {loss.item()}")
# __torch_single_end__

# __torch_distributed_begin__
from ray import train

def train_func_distributed():
    num_epochs = 3
    batch_size = 64

    dataset = get_dataset()
    dataloader = DataLoader(dataset, batch_size=batch_size)
    dataloader = train.torch.prepare_data_loader(dataloader)

    model = NeuralNetwork()
    model = train.torch.prepare_model(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        for inputs, labels in dataloader:
            optimizer.zero_grad()
            pred = model(inputs)
            loss = criterion(pred, labels)
            loss.backward()
            optimizer.step()
        print(f"epoch: {epoch}, loss: {loss.item()}")
# __torch_distributed_end__


if __name__ == "__main__":
    # __torch_single_run_begin__
    train_func()
    # __torch_single_run_end__

    # __torch_trainer_begin__
    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    # For GPU Training, set `use_gpu` to True.
    use_gpu = False

    trainer = TorchTrainer(
        train_func_distributed,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=use_gpu)
    )

    results = trainer.fit()
    # __torch_trainer_end__
