import os
import tempfile

import torch
import torch.nn as nn

from accelerate import Accelerator

import ray
from ray import train
from ray.train import Checkpoint, ScalingConfig
from ray.train.huggingface import AccelerateTrainer


# If using GPUs, set this to True.
use_gpu = False


input_size = 1
layer_size = 15
output_size = 1
num_epochs = 3


class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.layer1 = nn.Linear(input_size, layer_size)
        self.relu = nn.ReLU()
        self.layer2 = nn.Linear(layer_size, output_size)

    def forward(self, input):
        return self.layer2(self.relu(self.layer1(input)))


def train_loop_per_worker():
    accelerator = Accelerator()
    dataset_shard = train.get_dataset_shard("train")
    model = NeuralNetwork()
    loss_fn = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)

    model, optimizer = accelerator.prepare(model, optimizer)

    for epoch in range(num_epochs):
        for batches in dataset_shard.iter_torch_batches(
            batch_size=32, dtypes=torch.float
        ):
            inputs, labels = torch.unsqueeze(batches["x"], 1), batches["y"]
            output = model(inputs)
            loss = loss_fn(output, labels)
            optimizer.zero_grad()
            accelerator.backward(loss)
            optimizer.step()
            print(f"epoch: {epoch}, loss: {loss.item()}")

        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(
                accelerator.unwrap_model(model).state_dict(),
                os.path.join(tmpdir, "model.pt"),
            )
            train.report(
                metrics={"epoch": epoch, "loss": loss.item()},
                checkpoint=Checkpoint.from_directory(tmpdir),
            )


train_dataset = ray.data.from_items([{"x": x, "y": 2 * x + 1} for x in range(200)])
scaling_config = ScalingConfig(num_workers=3, use_gpu=use_gpu)
trainer = AccelerateTrainer(
    train_loop_per_worker=train_loop_per_worker,
    # Instead of using a dict, you can run ``accelerate config``.
    # The default value of None will then load that configuration
    # file.
    accelerate_config={},
    scaling_config=scaling_config,
    datasets={"train": train_dataset},
)
result = trainer.fit()
