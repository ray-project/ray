# The PyTorch data transfer benchmark script.
import argparse
import warnings

import numpy as np
import torch
import torch.nn as nn

import ray.train as train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig


class Net(nn.Module):
    def __init__(self, in_d, hidden):
        # output dim = 1
        super(Net, self).__init__()
        dims = [in_d] + hidden + [1]
        self.layers = nn.ModuleList(
            [nn.Linear(dims[i - 1], dims[i]) for i in range(len(dims))]
        )

    def forward(self, x):
        for layer in self.layers:
            x = layer(x)
        return x


class BenchmarkDataset(torch.utils.data.Dataset):
    """Create a naive dataset for the benchmark"""

    def __init__(self, dim, size=1000):
        self.x = torch.from_numpy(np.random.normal(size=(size, dim))).float()
        self.y = torch.from_numpy(np.random.normal(size=(size, 1))).float()
        self.size = size

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return self.size


def train_epoch(dataloader, model, loss_fn, optimizer):
    for X, y in dataloader:
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def train_func(config):
    data_size = config.get("data_size", 4096 * 50)
    batch_size = config.get("batch_size", 4096)
    hidden_size = config.get("hidden_size", 1)
    use_auto_transfer = config.get("use_auto_transfer", False)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 10)

    train_dataset = BenchmarkDataset(4096, size=data_size)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size)

    train_loader = train.torch.prepare_data_loader(
        data_loader=train_loader, move_to_device=True, auto_transfer=use_auto_transfer
    )

    model = Net(in_d=4096, hidden=[4096] * hidden_size)
    model = train.torch.prepare_model(model)

    loss_fn = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)

    choice = "with" if use_auto_transfer else "without"
    print(f"Starting the torch data prefetch benchmark {choice} auto pipeline...")

    torch.cuda.synchronize()
    start.record()
    for _ in range(epochs):
        train_epoch(train_loader, model, loss_fn, optimizer)
    end.record()
    torch.cuda.synchronize()

    print(
        f"Finished the torch data prefetch benchmark {choice} "
        f"auto pipeline: {start.elapsed_time(end)} ms."
    )

    return "Experiment done."


def train_linear(num_workers=1, num_hidden_layers=1, use_auto_transfer=True, epochs=3):
    config = {
        "lr": 1e-2,
        "hidden_size": num_hidden_layers,
        "batch_size": 4096,
        "epochs": epochs,
        "use_auto_transfer": use_auto_transfer,
    }
    trainer = TorchTrainer(
        train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(use_gpu=True, num_workers=num_workers),
    )
    results = trainer.fit()

    print(results.metrics)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--epochs", type=int, default=1, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--num_hidden_layers",
        type=int,
        default=1,
        help="Number of epochs to train for.",
    )

    args, _ = parser.parse_known_args()

    import ray

    ray.init(address=args.address)

    if not torch.cuda.is_available():
        warnings.warn("GPU is not available. Skip the test using auto pipeline.")
    else:
        train_linear(
            num_workers=1,
            num_hidden_layers=args.num_hidden_layers,
            use_auto_transfer=True,
            epochs=args.epochs,
        )

    torch.cuda.empty_cache()
    train_linear(
        num_workers=1,
        num_hidden_layers=args.num_hidden_layers,
        use_auto_transfer=False,
        epochs=args.epochs,
    )

    ray.shutdown()
