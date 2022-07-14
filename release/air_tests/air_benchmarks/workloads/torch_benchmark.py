import json
import os
import time
from typing import Dict, Tuple

import click
import numpy as np
import torch
from torch import nn, distributed
from torch.utils.data import DataLoader, DistributedSampler
from torchvision import datasets
from torchvision.transforms import ToTensor


CONFIG = {"lr": 1e-3, "batch_size": 64}
VANILLA_RESULT_JSON = "/tmp/vanilla_out.json"


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


def train_epoch(
    dataloader, model, loss_fn, optimizer, world_size: int, local_rank: int
):
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
            print(f"[rank={local_rank}] loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def validate_epoch(dataloader, model, loss_fn, world_size: int, local_rank: int):
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
        f"[rank={local_rank}] Test Error: \n "
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
    shuffle = config.get("shuffle", False)

    if use_ray:
        world_size = session.get_world_size()
        local_rank = distributed.get_rank()
    else:
        world_size = distributed.get_world_size()
        local_rank = distributed.get_rank()

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

    if use_ray:
        # Ray adds DistributedSampler in train.torch.prepare_data_loader below
        training_sampler = None
        test_sampler = None
    else:
        # In vanilla PyTorch we create the distributed sampler here
        training_sampler = DistributedSampler(training_data, shuffle=shuffle)
        test_sampler = DistributedSampler(test_data, shuffle=shuffle)

    # Create data loaders and potentially pass distributed sampler
    train_dataloader = DataLoader(
        training_data,
        shuffle=shuffle,
        batch_size=worker_batch_size,
        sampler=training_sampler,
    )
    test_dataloader = DataLoader(
        test_data, shuffle=shuffle, batch_size=worker_batch_size, sampler=test_sampler
    )

    if use_ray:
        # In Ray, we now retrofit the DistributedSampler
        train_dataloader = train.torch.prepare_data_loader(train_dataloader)
        test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    # Create model.
    model = NeuralNetwork()

    # Prepare model
    if use_ray:
        model = train.torch.prepare_model(model)
    else:
        model = nn.parallel.DistributedDataParallel(model)
        device = torch.device("cpu")
        model = model.to(device)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for _ in range(epochs):
        train_epoch(
            train_dataloader,
            model,
            loss_fn,
            optimizer,
            world_size=world_size,
            local_rank=local_rank,
        )
        loss = validate_epoch(
            test_dataloader,
            model,
            loss_fn,
            world_size=world_size,
            local_rank=local_rank,
        )
        if use_ray:
            session.report(dict(loss=loss))
        else:
            print(f"Reporting loss: {loss:.4f}")
            if local_rank == 0:
                with open(VANILLA_RESULT_JSON, "w") as f:
                    json.dump({"loss": loss}, f)


def train_torch_ray_air(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
) -> Tuple[float, float]:
    # This function is kicked off by the main() function and runs a full training
    # run using Ray AIR.
    from ray.train.torch import TorchTrainer

    def train_loop(config):
        train_func(use_ray=True, config=config)

    start_time = time.monotonic()
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=config,
        scaling_config={
            "trainer_resources": {"CPU": 0},
            "num_workers": num_workers,
            "resources_per_worker": {"CPU": cpus_per_worker},
            "use_gpu": use_gpu,
        },
    )
    result = trainer.fit()
    time_taken = time.monotonic() - start_time

    print(f"Last result: {result.metrics}")
    return time_taken, result.metrics["loss"]


def train_torch_vanilla_worker(
    *,
    config: dict,
    rank: int,
    world_size: int,
    master_addr: str,
    master_port: int,
    use_gpu: bool = False,
):
    # This function is kicked off by the main() function and runs the vanilla
    # training script on a single worker.
    backend = "nccl" if use_gpu else "gloo"

    os.environ["MASTER_ADDR"] = master_addr
    os.environ["MASTER_PORT"] = str(master_port)
    distributed.init_process_group(backend=backend, rank=rank, world_size=world_size)

    train_func(use_ray=False, config=config)

    distributed.destroy_process_group()


def train_torch_vanilla(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
    master_port: int = 12355,
) -> Tuple[float, float]:
    # This function is kicked off by the main() function and subsequently kicks
    # off tasks that run train_torch_vanilla_worker() on the worker nodes.
    import ray
    from benchmark_util import upload_file_to_all_nodes, run_commands_with_resources

    path = os.path.abspath(__file__)
    upload_file_to_all_nodes(path)
    master_addr = ray.util.get_node_ip_address()
    master_port = master_port

    num_epochs = config["epochs"]

    cmds = [
        [
            "python",
            path,
            "worker",
            "--num-epochs",
            str(num_epochs),
            "--num-workers",
            str(num_workers),
            "--rank",
            str(rank),
            "--master-addr",
            master_addr,
            "--master-port",
            str(master_port),
        ]
        + (["--use-gpu"] if use_gpu else [])
        for rank in range(num_workers)
    ]

    start_time = time.monotonic()
    run_commands_with_resources(
        cmds,
        resources={
            "CPU": cpus_per_worker,
            "GPU": int(use_gpu),
        },
    )
    time_taken = time.monotonic() - start_time

    loss = 0.0
    if os.path.exists(VANILLA_RESULT_JSON):
        with open(VANILLA_RESULT_JSON, "r") as f:
            result = json.load(f)
        loss = result["loss"]

    return time_taken, loss


@click.group(help="Run Torch benchmarks")
def cli():
    pass


@cli.command(help="Kick off Ray and vanilla benchmarks")
@click.option("--num-runs", type=int, default=1)
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--cpus-per-worker", type=int, default=8)
@click.option("--use-gpu", is_flag=True, default=False)
@click.option("--master-port", type=int, default=12355)
def run(
    num_runs: int = 1,
    num_epochs: int = 4,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
    master_port: int = 12355,
):
    import ray
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    config = CONFIG.copy()
    config["epochs"] = num_epochs

    ray.init("auto")
    print("Preparing Torch benchmark: Downloading MNIST")

    path = os.path.abspath("workloads/_torch_prepare.py")
    upload_file_to_all_nodes(path)
    run_command_on_all_nodes(["python", path])

    times_ray = []
    losses_ray = []
    times_vanilla = []
    losses_vanilla = []
    for run in range(1, num_runs + 1):
        print(f"[Run {run}/{num_runs}] Running Torch Ray benchmark")

        time_ray, loss_ray = train_torch_ray_air(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
        )

        print(
            f"[Run {run}/{num_runs}] Finished Ray training ({num_epochs} epochs) in "
            f"{time_ray:.2f} seconds. Observed loss = {loss_ray:.4f}"
        )

        time.sleep(5)

        print(f"[Run {run}/{num_runs}] Running Torch vanilla benchmark")

        time_vanilla, loss_vanilla = train_torch_vanilla(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
            master_port=master_port + run,
        )

        print(
            f"[Run {run}/{num_runs}] Finished vanilla training ({num_epochs} epochs) "
            f"in {time_vanilla:.2f} seconds. Observed loss = {loss_vanilla:.4f}"
        )

        print(
            f"[Run {run}/{num_runs}] Observed results: ",
            {
                "torch_mnist_ray_time_s": time_ray,
                "torch_mnist_ray_loss": loss_ray,
                "torch_mnist_vanilla_time_s": time_vanilla,
                "torch_mnist_vanilla_loss": loss_vanilla,
            },
        )

        times_ray.append(time_ray)
        losses_ray.append(loss_ray)
        times_vanilla.append(time_vanilla)
        losses_vanilla.append(loss_vanilla)

    times_ray_mean = np.mean(times_ray)
    times_ray_sd = np.std(times_ray)

    times_vanilla_mean = np.mean(times_vanilla)
    times_vanilla_sd = np.std(times_vanilla)

    result = {
        "torch_mnist_ray_num_runs": num_runs,
        "torch_mnist_ray_time_s_all": times_ray,
        "torch_mnist_ray_time_s_mean": times_ray_mean,
        "torch_mnist_ray_time_s_sd": times_ray_sd,
        "torch_mnist_ray_loss_mean": np.mean(losses_ray),
        "torch_mnist_ray_loss_sd": np.std(losses_ray),
        "torch_mnist_vanilla_time_s_all": times_vanilla,
        "torch_mnist_vanilla_time_s_mean": times_vanilla_mean,
        "torch_mnist_vanilla_time_s_sd": times_vanilla_sd,
        "torch_mnist_vanilla_loss_mean": np.mean(losses_vanilla),
        "torch_mnist_vanilla_loss_std": np.std(losses_vanilla),
    }

    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    target_ratio = 1.15
    ratio = (times_ray_mean / times_vanilla_mean) if times_vanilla_mean != 0.0 else 1.0
    if ratio > 1.15:
        raise RuntimeError(
            f"Training on Ray took an average of {times_ray_mean:.2f} seconds, "
            f"which is more than {target_ratio:.2f}x of the average vanilla training "
            f"time of {times_vanilla_mean:.2f} seconds ({ratio:.2f}x). FAILED"
        )

    print(
        f"Training on Ray took an average of {times_ray_mean:.2f} seconds, "
        f"which is less than {target_ratio:.2f}x of the average vanilla training "
        f"time of {times_vanilla_mean:.2f} seconds ({ratio:.2f}x). PASSED"
    )


@cli.command(help="Run PyTorch vanilla worker")
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--rank", type=int, default=0)
@click.option("--master-addr", type=str, default="")
@click.option("--master-port", type=int, default=0)
@click.option("--use-gpu", is_flag=True, default=False)
def worker(
    num_epochs: int = 4,
    num_workers: int = 4,
    rank: int = 0,
    master_addr: str = "",
    master_port: int = 0,
    use_gpu: bool = False,
):
    config = CONFIG.copy()
    config["epochs"] = num_epochs

    # Then we kick off the training function on every worker.
    return train_torch_vanilla_worker(
        config=config,
        rank=rank,
        world_size=num_workers,
        master_addr=master_addr,
        master_port=master_port,
        use_gpu=use_gpu,
    )


def main():
    return cli()


if __name__ == "__main__":
    main()
