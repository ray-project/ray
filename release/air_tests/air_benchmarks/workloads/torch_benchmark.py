import json
import os
import time
from pathlib import Path
from typing import Dict, Tuple

import click
import numpy as np
import torch
from torch import nn, distributed
from torch.utils.data import DataLoader, DistributedSampler
from torch.utils.data.dataloader import default_collate
from torchvision import datasets
from torchvision.transforms import ToTensor


CONFIG = {"lr": 1e-3, "batch_size": 64}
VANILLA_RESULT_JSON = "/tmp/vanilla_out.json"


def find_network_interface():
    for iface in os.listdir("/sys/class/net"):
        if iface.startswith("ens"):
            network_interface = iface
            break
    else:
        network_interface = "^lo,docker"

    return network_interface


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
    local_start_time = time.monotonic()

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

    if not use_ray and config.get("use_gpu", False):
        assert torch.cuda.is_available(), "No GPUs available"
        gpu_id = config.get("gpu_id", 0)
        vanilla_device = torch.device(f"cuda:{gpu_id}")
        torch.cuda.set_device(vanilla_device)

        print(
            "Setting GPU ID to",
            gpu_id,
            "with visible devices",
            os.environ.get("CUDA_VISIBLE_DEVICES"),
        )

        def collate_fn(x):
            return tuple(x_.to(vanilla_device) for x_ in default_collate(x))

    else:
        vanilla_device = torch.device("cpu")
        collate_fn = None

    # Create data loaders and potentially pass distributed sampler
    train_dataloader = DataLoader(
        training_data,
        shuffle=shuffle,
        batch_size=worker_batch_size,
        sampler=training_sampler,
        collate_fn=collate_fn,
    )
    test_dataloader = DataLoader(
        test_data,
        shuffle=shuffle,
        batch_size=worker_batch_size,
        sampler=test_sampler,
        collate_fn=collate_fn,
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
        model = model.to(vanilla_device)

        if config.get("use_gpu", False):
            model = nn.parallel.DistributedDataParallel(
                model, device_ids=[gpu_id], output_device=gpu_id
            )
        else:
            model = nn.parallel.DistributedDataParallel(model)

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

        local_time_taken = time.monotonic() - local_start_time

        if use_ray:
            session.report(dict(loss=loss, local_time_taken=local_time_taken))
        else:
            print(f"Reporting loss: {loss:.4f}")
            if local_rank == 0:
                with open(VANILLA_RESULT_JSON, "w") as f:
                    json.dump({"loss": loss, "local_time_taken": local_time_taken}, f)


def train_torch_ray_air(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
) -> Tuple[float, float, float]:
    # This function is kicked off by the main() function and runs a full training
    # run using Ray AIR.
    from ray.train.torch import TorchTrainer
    from ray.air.config import ScalingConfig

    def train_loop(config):
        train_func(use_ray=True, config=config)

    start_time = time.monotonic()
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=config,
        scaling_config=ScalingConfig(
            trainer_resources={"CPU": 0},
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker},
            use_gpu=use_gpu,
        ),
    )
    result = trainer.fit()
    time_taken = time.monotonic() - start_time

    print(f"Last result: {result.metrics}")
    return time_taken, result.metrics["local_time_taken"], result.metrics["loss"]


def train_torch_vanilla_worker(
    *,
    config: dict,
    rank: int,
    world_size: int,
    master_addr: str,
    master_port: int,
    use_gpu: bool = False,
    gpu_id: int = 0,
):
    # This function is kicked off by the main() function and runs the vanilla
    # training script on a single worker.
    backend = "nccl" if use_gpu else "gloo"

    os.environ["MASTER_ADDR"] = master_addr
    os.environ["MASTER_PORT"] = str(master_port)
    os.environ["NCCL_BLOCKING_WAIT"] = "1"
    distributed.init_process_group(
        backend=backend, rank=rank, world_size=world_size, init_method="env://"
    )

    config["use_gpu"] = use_gpu
    config["gpu_id"] = gpu_id
    train_func(use_ray=False, config=config)

    distributed.destroy_process_group()


def train_torch_vanilla(
    *,
    config: dict,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
) -> Tuple[float, float, float]:
    # This function is kicked off by the main() function and subsequently kicks
    # off tasks that run train_torch_vanilla_worker() on the worker nodes.
    from benchmark_util import (
        upload_file_to_all_nodes,
        create_actors_with_options,
        run_commands_on_actors,
        run_fn_on_actors,
        get_ip_port_actors,
        get_gpu_ids_actors,
        map_ips_to_gpus,
        set_cuda_visible_devices,
    )

    path = os.path.abspath(__file__)
    upload_file_to_all_nodes(path)

    num_epochs = config["epochs"]

    try:
        nccl_network_interface = find_network_interface()
        runtime_env = {"env_vars": {"NCCL_SOCKET_IFNAME": nccl_network_interface}}
    except Exception:
        runtime_env = {}

    actors = create_actors_with_options(
        num_actors=num_workers,
        resources={
            "CPU": cpus_per_worker,
            "GPU": int(use_gpu),
        },
        runtime_env=runtime_env,
    )

    run_fn_on_actors(actors=actors, fn=lambda: os.environ.pop("OMP_NUM_THREADS", None))

    # Get IPs and ports for all actors
    ip_ports = get_ip_port_actors(actors=actors)

    # Rank 0 is the master addr/port
    master_addr, master_port = ip_ports[0]

    if use_gpu:
        # Extract IPs
        actor_ips = [ipp[0] for ipp in ip_ports]

        # Get allocated GPU IDs for all actors
        gpu_ids = get_gpu_ids_actors(actors=actors)

        # Build a map of IP to all allocated GPUs on that machine
        ip_to_gpu_map = map_ips_to_gpus(ips=actor_ips, gpus=gpu_ids)

        # Set the environment variables on the workers
        set_cuda_visible_devices(
            actors=actors, actor_ips=actor_ips, ip_to_gpus=ip_to_gpu_map
        )

        use_gpu_ids = [gi[0] for gi in gpu_ids]
    else:
        use_gpu_ids = [0] * num_workers

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
            "--batch-size",
            str(config["batch_size"]),
        ]
        + (["--use-gpu"] if use_gpu else [])
        + (["--gpu-id", str(use_gpu_ids[rank])] if use_gpu else [])
        for rank in range(num_workers)
    ]

    run_fn_on_actors(
        actors=actors, fn=lambda: os.environ.setdefault("OMP_NUM_THREADS", "1")
    )

    start_time = time.monotonic()
    run_commands_on_actors(actors=actors, cmds=cmds)
    time_taken = time.monotonic() - start_time

    loss = 0.0
    if os.path.exists(VANILLA_RESULT_JSON):
        with open(VANILLA_RESULT_JSON, "r") as f:
            result = json.load(f)
        loss = result["loss"]
        local_time_taken = result["local_time_taken"]

    return time_taken, local_time_taken, loss


@click.group(help="Run Torch benchmarks")
def cli():
    pass


@cli.command(help="Kick off Ray and vanilla benchmarks")
@click.option("--num-runs", type=int, default=1)
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--cpus-per-worker", type=int, default=8)
@click.option("--use-gpu", is_flag=True, default=False)
@click.option("--batch-size", type=int, default=64)
@click.option("--smoke-test", is_flag=True, default=False)
@click.option("--local", is_flag=True, default=False)
def run(
    num_runs: int = 1,
    num_epochs: int = 4,
    num_workers: int = 4,
    cpus_per_worker: int = 8,
    use_gpu: bool = False,
    batch_size: int = 64,
    smoke_test: bool = False,
    local: bool = False,
):
    # Note: smoke_test is ignored as we just adjust the batch size.
    # The parameter is passed by the release test pipeline.
    import ray
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    config = CONFIG.copy()
    config["epochs"] = num_epochs
    config["batch_size"] = batch_size

    if local:
        ray.init(num_cpus=4)
    else:
        ray.init("auto")

    print("Preparing Torch benchmark: Downloading MNIST")

    path = str((Path(__file__).parent / "_torch_prepare.py").absolute())
    upload_file_to_all_nodes(path)
    run_command_on_all_nodes(["python", path])

    times_ray = []
    times_local_ray = []
    losses_ray = []
    times_vanilla = []
    times_local_vanilla = []
    losses_vanilla = []
    for run in range(1, num_runs + 1):
        time.sleep(2)

        print(f"[Run {run}/{num_runs}] Running Torch Ray benchmark")

        time_ray, time_local_ray, loss_ray = train_torch_ray_air(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
        )

        print(
            f"[Run {run}/{num_runs}] Finished Ray training ({num_epochs} epochs) in "
            f"{time_ray:.2f} seconds (local training time: {time_local_ray:.2f}s). "
            f"Observed loss = {loss_ray:.4f}"
        )

        time.sleep(2)

        print(f"[Run {run}/{num_runs}] Running Torch vanilla benchmark")

        time_vanilla, time_local_vanilla, loss_vanilla = train_torch_vanilla(
            num_workers=num_workers,
            cpus_per_worker=cpus_per_worker,
            use_gpu=use_gpu,
            config=config,
        )

        print(
            f"[Run {run}/{num_runs}] Finished vanilla training ({num_epochs} epochs) "
            f"in {time_vanilla:.2f} seconds "
            f"(local training time: {time_local_vanilla:.2f}s). "
            f"Observed loss = {loss_vanilla:.4f}"
        )

        print(
            f"[Run {run}/{num_runs}] Observed results: ",
            {
                "tensorflow_mnist_ray_time_s": time_ray,
                "tensorflow_mnist_ray_local_time_s": time_local_ray,
                "tensorflow_mnist_ray_loss": loss_ray,
                "tensorflow_mnist_vanilla_time_s": time_vanilla,
                "tensorflow_mnist_vanilla_local_time_s": time_local_vanilla,
                "tensorflow_mnist_vanilla_loss": loss_vanilla,
            },
        )

        times_ray.append(time_ray)
        times_local_ray.append(time_local_ray)
        losses_ray.append(loss_ray)
        times_vanilla.append(time_vanilla)
        times_local_vanilla.append(time_local_vanilla)
        losses_vanilla.append(loss_vanilla)

    times_ray_mean = np.mean(times_ray)
    times_ray_sd = np.std(times_ray)

    times_local_ray_mean = np.mean(times_local_ray)
    times_local_ray_sd = np.std(times_local_ray)

    times_vanilla_mean = np.mean(times_vanilla)
    times_vanilla_sd = np.std(times_vanilla)

    times_local_vanilla_mean = np.mean(times_local_vanilla)
    times_local_vanilla_sd = np.std(times_local_vanilla)

    result = {
        "torch_mnist_ray_num_runs": num_runs,
        "torch_mnist_ray_time_s_all": times_ray,
        "torch_mnist_ray_time_s_mean": times_ray_mean,
        "torch_mnist_ray_time_s_sd": times_ray_sd,
        "torch_mnist_ray_time_local_s_all": times_local_ray,
        "torch_mnist_ray_time_local_s_mean": times_local_ray_mean,
        "torch_mnist_ray_time_local_s_sd": times_local_ray_sd,
        "torch_mnist_ray_loss_mean": np.mean(losses_ray),
        "torch_mnist_ray_loss_sd": np.std(losses_ray),
        "torch_mnist_vanilla_time_s_all": times_vanilla,
        "torch_mnist_vanilla_time_s_mean": times_vanilla_mean,
        "torch_mnist_vanilla_time_s_sd": times_vanilla_sd,
        "torch_mnist_vanilla_local_time_s_all": times_local_vanilla,
        "torch_mnist_vanilla_local_time_s_mean": times_local_vanilla_mean,
        "torch_mnist_vanilla_local_time_s_sd": times_local_vanilla_sd,
        "torch_mnist_vanilla_loss_mean": np.mean(losses_vanilla),
        "torch_mnist_vanilla_loss_std": np.std(losses_vanilla),
    }

    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    target_ratio = 1.15
    ratio = (
        (times_local_ray_mean / times_local_vanilla_mean)
        if times_local_vanilla_mean != 0.0
        else 1.0
    )
    if ratio > target_ratio:
        raise RuntimeError(
            f"Training on Ray took an average of {times_local_ray_mean:.2f} seconds, "
            f"which is more than {target_ratio:.2f}x of the average vanilla training "
            f"time of {times_local_vanilla_mean:.2f} seconds ({ratio:.2f}x). FAILED"
        )

    print(
        f"Training on Ray took an average of {times_local_ray_mean:.2f} seconds, "
        f"which is less than {target_ratio:.2f}x of the average vanilla training "
        f"time of {times_local_vanilla_mean:.2f} seconds ({ratio:.2f}x). PASSED"
    )


@cli.command(help="Run PyTorch vanilla worker")
@click.option("--num-epochs", type=int, default=4)
@click.option("--num-workers", type=int, default=4)
@click.option("--rank", type=int, default=0)
@click.option("--master-addr", type=str, default="")
@click.option("--master-port", type=int, default=0)
@click.option("--batch-size", type=int, default=64)
@click.option("--use-gpu", is_flag=True, default=False)
@click.option("--gpu-id", type=int, default=0)
def worker(
    num_epochs: int = 4,
    num_workers: int = 4,
    rank: int = 0,
    master_addr: str = "",
    master_port: int = 0,
    batch_size: int = 64,
    use_gpu: bool = False,
    gpu_id: int = 0,
):
    config = CONFIG.copy()
    config["epochs"] = num_epochs
    config["batch_size"] = batch_size

    # Then we kick off the training function on every worker.
    return train_torch_vanilla_worker(
        config=config,
        rank=rank,
        world_size=num_workers,
        master_addr=master_addr,
        master_port=master_port,
        use_gpu=use_gpu,
        gpu_id=gpu_id,
    )


def main():
    return cli()


if __name__ == "__main__":
    main()
