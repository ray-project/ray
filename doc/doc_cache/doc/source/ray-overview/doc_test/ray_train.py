import os
import tempfile

import torch

import ray.train as train
from ray.train import Checkpoint, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func():
    # Setup model.
    model = torch.nn.Linear(1, 1)
    model = train.torch.prepare_model(model)
    loss_fn = torch.nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)

    # Setup data.
    input = torch.randn(1000, 1)
    labels = input * 2
    dataset = torch.utils.data.TensorDataset(input, labels)
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=32)
    dataloader = train.torch.prepare_data_loader(dataloader)

    # Train.
    for epoch in range(5):
        if train.get_context().get_world_size() > 1:
            dataloader.sampler.set_epoch(epoch)

        epoch_loss = []
        for X, y in dataloader:
            pred = model(X)
            loss = loss_fn(pred, y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            epoch_loss.append(loss.item())
        train.report({"loss": sum(epoch_loss) / len(epoch_loss)})

    with tempfile.TemporaryDirectory() as tmpdir:
        torch.save(model.module.state_dict(), os.path.join(tmpdir, "model.pt"))
        train.report(
            {"loss": loss.item()}, checkpoint=Checkpoint.from_directory(tmpdir)
        )


trainer = TorchTrainer(train_func, scaling_config=ScalingConfig(num_workers=4))
results = trainer.fit()

print(results.metrics)
print(results.checkpoint)
