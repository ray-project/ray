import ray

import horovod.torch as hvd
import torch
import torch.nn as nn

from ray import train
from ray.ml.train.integrations.torch import TorchTrainer


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
    use_adasum = False
    hvd.init()
    dataset_shard = train.get_dataset_shard("train")
    model = NeuralNetwork()
    device = train.torch.get_device()
    model.to(device)
    loss_fn = nn.MSELoss()

    # By default, Adasum doesn't need scaling up learning rate.
    lr_scaler = hvd.size() if not use_adasum else 1
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1 * lr_scaler)

    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(
        optimizer,
        named_parameters=model.named_parameters(),
        op=hvd.Adasum if use_adasum else hvd.Average,
    )

    for epoch in range(num_epochs):
        model.train()
        for inputs, labels in iter(dataset_shard.to_torch(label_column="y", label_column_dtype=torch.float,
            feature_column_dtypes=torch.float, batch_size=32)):
            inputs.to(device)
            labels.to(device)
            outputs = model(inputs)
            loss = loss_fn(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print(f"epoch: {epoch}, loss: {loss.item()}")

        train.save_checkpoint(model=model.state_dict())


train_dataset = ray.data.from_items(
    [{"x": x, "y": x + 1} for x in range(32)])
scaling_config = {"num_workers": 3}
# If using GPUs, use the below scaling config instead.
# scaling_config = {"num_workers": 3, "use_gpu": True}
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config={"num_workers": 3},
    datasets={"train": train_dataset})
result = trainer.fit()
