import os
import tempfile

import horovod.torch as hvd
import ray
from ray import train
from ray.train import Checkpoint, ScalingConfig
import ray.train.torch  # Need this to use `train.torch.get_device()`
from ray.train.horovod import HorovodTrainer
import torch
import torch.nn as nn

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
    hvd.init()
    dataset_shard = train.get_dataset_shard("train")
    model = NeuralNetwork()
    device = train.torch.get_device()
    model.to(device)
    loss_fn = nn.MSELoss()
    lr_scaler = 1
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1 * lr_scaler)
    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(
        optimizer,
        named_parameters=model.named_parameters(),
        op=hvd.Average,
    )
    for epoch in range(num_epochs):
        model.train()
        for batch in dataset_shard.iter_torch_batches(
            batch_size=32, dtypes=torch.float
        ):
            inputs, labels = torch.unsqueeze(batch["x"], 1), batch["y"]
            outputs = model(inputs)
            loss = loss_fn(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"epoch: {epoch}, loss: {loss.item()}")

        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))
            train.report(
                {"loss": loss.item()}, checkpoint=Checkpoint.from_directory(tmpdir)
            )


train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
scaling_config = ScalingConfig(num_workers=3, use_gpu=use_gpu)
trainer = HorovodTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config,
    datasets={"train": train_dataset},
)
result = trainer.fit()
