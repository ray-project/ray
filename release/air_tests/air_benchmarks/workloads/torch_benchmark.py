import json
import os
import socket
import time
from typing import Dict

import click
import torch
from torch import nn, distributed
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor


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


def train_epoch(dataloader, model, loss_fn, optimizer, world_size: int):
    size = len(dataloader.dataset) // world_size
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


def validate_epoch(dataloader, model, loss_fn, world_size: int):
    size = len(dataloader.dataset) // world_size
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


def train_func(use_ray: bool, config: Dict):
    if use_ray:
        from ray.air import session
        import ray.train as train

    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]

    if use_ray:
        world_size = session.get_world_size()
    else:
        world_size = distributed.get_world_size()

    worker_batch_size = batch_size // world_size

    # Load datasets. Use download=False to catch errors in preparation, as the
    # data should have already been downloaded.
    training_data = datasets.FashionMNIST(
        root="/tmp/data_fashion_mnist",
        train=True,
        download=False,
        transform=ToTensor(),
    )

    test_data = datasets.FashionMNIST(
        root="/tmp/data_fashion_mnist",
        train=False,
        download=False,
        transform=ToTensor(),
    )

    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=worker_batch_size)
    test_dataloader = DataLoader(test_data, batch_size=worker_batch_size)

    # Create model.
    model = NeuralNetwork()

    if use_ray:
        model = train.torch.prepare_model(model)
    else:
        model = nn.parallel.DistributedDataParallel(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for _ in range(epochs):
        train_epoch(train_dataloader, model, loss_fn, optimizer, world_size=world_size)
        loss = validate_epoch(test_dataloader, model, loss_fn, world_size=world_size)
        if use_ray:
            session.report(dict(loss=loss))
        else:
            print(f"Reporting loss: {loss:.4f}")


def train_torch_ray_air(*, config: dict, num_workers: int = 4, use_gpu: bool = False):
    # This function is kicked off by the main() function and runs a full training
    # run using Ray AIR.
    from ray.train.torch import TorchTrainer

    def train_loop(config):
        train_func(use_ray=True, config=config)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=config,
        scaling_config={"num_workers": num_workers, "use_gpu": use_gpu},
    )
    result = trainer.fit()
    print(f"Last result: {result.metrics}")


def train_torch_vanilla_worker(
    *, config: dict, rank: int, world_size: int, master_addr: str, master_port: int
):
    # This function is kicked off by the main() function and runs the vanilla
    # training script on a single worker.
    os.environ["MASTER_ADDR"] = master_addr
    os.environ["MASTER_PORT"] = str(master_port)
    distributed.init_process_group("gloo", rank=rank, world_size=world_size)

    train_func(use_ray=False, config=config)

    distributed.destroy_process_group()


def train_torch_vanilla(*, config: dict, num_workers: int = 4, use_gpu: bool = False):
    # This function is kicked off by the main() function and subsequently kicks
    # off tasks that run train_torch_vanilla_worker() on the worker nodes.
    import ray
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    path = os.path.abspath(__file__)
    upload_file_to_all_nodes(path)
    master_addr = ray.util.get_node_ip_address()
    master_port = 12355  # hardcoded

    # Each worker needs to know which rank it is. To pass this, we construct a
    # map of IPs to ranks, which is passed as a string to all workers. The workers
    # then parse this string to determine their local rank.
    node_ip_to_ranks = {
        ip: rank
        for rank, ip in enumerate(
            [
                n["NodeManagerAddress"]
                for n in ray.nodes()
                if n["Alive"] and n["NodeManagerAddress"] != master_addr
            ],
            start=1,
        )
    }
    node_ip_to_ranks[master_addr] = 0
    node_to_rank_str = ",".join(
        {f"{ip}:{rank}" for ip, rank in node_ip_to_ranks.items()}
    )

    num_epochs = config["epochs"]

    cmd = [
        "python",
        path,
        "--vanilla-worker",
        "--num-epochs",
        num_epochs,
        "--node-to-rank-str",
        node_to_rank_str,
        "--num-workers",
        str(num_workers),
        "--master-addr",
        master_addr,
        "--master-port",
        str(master_port),
    ]

    if use_gpu:
        cmd += ["--use-gpu"]

    run_command_on_all_nodes(cmd)


@click.command()
@click.option("--num-workers", type=int, default=4)
@click.option("--num-epochs", type=int, default=4)
@click.option("--use-gpu", is_flag=True, default=False)
@click.option("--vanilla-worker", is_flag=True, default=False)
@click.option("--node-to-rank-str", type=str, default="")
@click.option("--world-size", type=int, default=4)
@click.option("--master-addr", type=str, default="")
@click.option("--master-port", type=int, default=0)
def main(
    num_workers: int = 4,
    num_epochs: int = 4,
    use_gpu: bool = False,
    vanilla_worker: bool = False,
    node_to_rank_str: str = "",
    master_addr: str = "",
    master_port: int = 0,
):
    config = {"lr": 1e-3, "batch_size": 64, "epochs": num_epochs}

    if vanilla_worker:
        # This path is invoked on every worker when kicking off vanilla training.
        # First we parse the node to rank string into a map and then find out
        # our local rank.
        node_ip_to_ranks = {
            ip: rank
            for ip, rank in [
                ip_rank.split(":", maxsplit=1)
                for ip_rank in node_to_rank_str.split(",")
            ]
        }
        node_ip = socket.gethostbyname(socket.gethostname())
        rank = int(node_ip_to_ranks[node_ip])

        # Then we kick off the training function on every worker.
        return train_torch_vanilla_worker(
            config=config,
            rank=rank,
            world_size=num_workers,
            master_addr=master_addr,
            master_port=master_port,
        )

    import ray
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    ray.init("auto")
    print("Preparing Torch benchmark: Downloading MNIST")

    path = os.path.abspath("workloads/_torch_prepare.py")
    upload_file_to_all_nodes(path)
    run_command_on_all_nodes(["python", path])

    print("Running Torch Ray benchmark")

    start_time = time.monotonic()
    train_torch_ray_air(num_workers=4, use_gpu=use_gpu, config=config)
    time_taken = time.monotonic() - start_time

    time_ray = time_taken

    print(f"Finished Ray training ({num_epochs} epochs) in {time_taken:.2f} seconds.")

    print("Running Torch vanilla benchmark")

    start_time = time.monotonic()
    train_torch_vanilla(num_workers=4, use_gpu=use_gpu, config=config)
    time_taken = time.monotonic() - start_time

    time_vanilla = time_taken

    print(
        f"Finished vanilla training ({num_epochs} epochs) in {time_taken:.2f} seconds."
    )

    result = {
        "torch_mnist_ray": time_ray,
        "torch_mnist_vanilla": time_vanilla,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()
