import torch
import torch.nn as nn

import ray
from ray import train
from ray.air import session, Checkpoint
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig

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
    dataset_shard = session.get_dataset_shard("train")
    model = NeuralNetwork()
    loss_fn = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)

    model = train.torch.prepare_model(model)

    for epoch in range(num_epochs):
        for batches in dataset_shard.iter_torch_batches(
            batch_size=32, dtypes=torch.float
        ):
            inputs, labels = torch.unsqueeze(batches["x"], 1), batches["y"]
            output = model(inputs)
            loss = loss_fn(output, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"epoch: {epoch}, loss: {loss.item()}")

        session.report(
            {},
            checkpoint=Checkpoint.from_dict(
                dict(epoch=epoch, model=model.state_dict())
            ),
        )


train_dataset = ray.data.from_items([{"x": x, "y": 2 * x + 1} for x in range(200)])
scaling_config = ScalingConfig(num_workers=3)
# If using GPUs, use the below scaling config instead.
# scaling_config = ScalingConfig(num_workers=3, use_gpu=True)
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=scaling_config,
    datasets={"train": train_dataset},
)
result = trainer.fit()
