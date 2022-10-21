# flake8: noqa
# fmt: off
# isort: skip_file

# __torch_setup_begin__
import torch
import torch.nn as nn

num_samples = 20
input_size = 10
layer_size = 15
output_size = 5

class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.layer1 = nn.Linear(input_size, layer_size)
        self.relu = nn.ReLU()
        self.layer2 = nn.Linear(layer_size, output_size)

    def forward(self, input):
        return self.layer2(self.relu(self.layer1(input)))

# In this example we use a randomly generated dataset.
input = torch.randn(num_samples, input_size)
labels = torch.randn(num_samples, output_size)

# __torch_setup_end__

# __torch_single_begin__

import torch.optim as optim

def train_func():
    num_epochs = 3
    model = NeuralNetwork()
    loss_fn = nn.MSELoss()
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    for epoch in range(num_epochs):
        output = model(input)
        loss = loss_fn(output, labels)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        print(f"epoch: {epoch}, loss: {loss.item()}")

# __torch_single_end__

# __torch_distributed_begin__

from ray import train

def train_func_distributed():
    num_epochs = 3
    model = NeuralNetwork()
    model = train.torch.prepare_model(model)
    loss_fn = nn.MSELoss()
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    for epoch in range(num_epochs):
        output = model(input)
        loss = loss_fn(output, labels)
        optimizer.zero_grad()
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
    from ray.air.config import ScalingConfig

    # For GPU Training, set `use_gpu` to True.
    use_gpu = False

    trainer = TorchTrainer(
        train_func_distributed,
        scaling_config=ScalingConfig(
            num_workers=4, use_gpu=use_gpu)
    )

    results = trainer.fit()

    # __torch_trainer_end__
