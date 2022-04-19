import numpy as np

import torch
import torch.nn as nn
import torch.multiprocessing as mp
from torch.utils.data import DataLoader
from torch.utils.data.distributed import DistributedSampler

import ray.train as train
from ray.train import Trainer
from ray.train.bagua import BaguaBackend

import bagua


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
    result = {"loss": loss}
    return result


def train_func(config):
    def train_local_per_worker(local_rank, config):
        import os

        nproc_per_worker = config.get("nproc_per_worker")
        world_rank = config.get("world_rank")

        # set env variables for each gpu process
        # to override Ray Train's default setting
        dist_rank = nproc_per_worker * world_rank + local_rank
        os.environ["RANK"] = str(dist_rank)
        os.environ["LOCAL_RANK"] = str(local_rank)
        os.environ["NODE_RANK"] = str(world_rank)
        os.environ["LOCAL_WORLD_SIZE"] = str(nproc_per_worker)

        data_size = config.get("data_size", 1000)
        val_size = config.get("val_size", 400)
        batch_size = config.get("batch_size", 32)
        hidden_size = config.get("hidden_size", 1)
        lr = config.get("lr", 1e-2)
        epochs = config.get("epochs", 3)

        train_dataset = LinearDataset(2, 5, size=data_size)
        val_dataset = LinearDataset(2, 5, size=val_size)

        model = nn.Linear(1, hidden_size)
        optimizer = torch.optim.SGD(model.parameters(), lr=lr)

        # Bagua process group has been initialized
        # when creating a Trainer using a Bagua backend
        # prepare data loaders for Bagua training
        train_sampler = DistributedSampler(
            train_dataset, num_replicas=bagua.get_world_size(), rank=bagua.get_rank()
        )
        train_loader = DataLoader(
            train_dataset,
            batch_size=batch_size,
            shuffle=(train_sampler is None),
            sampler=train_sampler,
        )

        validation_sampler = DistributedSampler(
            val_dataset, num_replicas=bagua.get_world_size(), rank=bagua.get_rank()
        )
        validation_loader = DataLoader(
            val_dataset,
            batch_size=batch_size,
            shuffle=(validation_sampler is None),
            sampler=validation_sampler,
        )

        # prepare torch model for bagua training
        from bagua.torch_api.algorithms import gradient_allreduce

        model = model.with_bagua(
            [optimizer], gradient_allreduce.GradientAllReduceAlgorithm()
        )

        loss_fn = nn.MSELoss()

        results = []
        for _ in range(epochs):
            train_epoch(train_loader, model, loss_fn, optimizer)
            result = validate_epoch(validation_loader, model, loss_fn)
            results.append(result.get("loss"))

        print(f"Worker {world_rank}, process {local_rank}: loss = {np.mean(results)}")

    # get the worker's (node) world rank
    config["world_rank"] = train.world_rank()
    mp.spawn(train_local_per_worker, args=(config,), nprocs=config["nproc_per_worker"])


def train_linear(num_workers=2, nproc_per_worker=1, epochs=3):
    bagua_backend = BaguaBackend(
        nnodes=num_workers,
        nproc_per_node=nproc_per_worker,
    )

    trainer = Trainer(
        backend=bagua_backend,
        num_workers=num_workers,
        use_gpu=True,
        resources_per_worker={"GPU": nproc_per_worker},
    )

    batch_size_per_gpu = 16 // (num_workers * nproc_per_worker)

    config = {"nproc_per_worker": nproc_per_worker, "batch_size": batch_size_per_gpu}
    trainer.start()
    trainer.run(train_func, config)
    trainer.shutdown()


if __name__ == "__main__":
    import ray

    ray.init()
    train_linear(num_workers=1, nproc_per_worker=1, epochs=10)
    ray.shutdown()
