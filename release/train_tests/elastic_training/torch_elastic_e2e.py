import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple

import click
from elastic_util import NeuralNetwork, terminate_node
from filelock import FileLock
import ray
import ray.train as train
from ray.tune.utils import date_str
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor


logger = logging.getLogger(__name__)


CONFIG = {"lr": 1e-3, "batch_size": 64}
LOG_FILE = "/tmp/driver.log"
DATA_DIR = "/tmp/fashion_mnist"


def get_default_storage_path():
    remote_default_artifact_storage_prefix = os.environ.get(
        "ANYSCALE_ARTIFACT_STORAGE", "artifact_storage"
    )
    return f"{remote_default_artifact_storage_prefix}/train_release_tests/elastic_e2e"


STORAGE_PATH = get_default_storage_path()


def load_data(data_dir):
    with FileLock(f"{DATA_DIR}.data.lock"):
        trainset = datasets.FashionMNIST(
            root=data_dir, train=True, download=True, transform=ToTensor()
        )
        testset = datasets.FashionMNIST(
            root=data_dir, train=False, download=True, transform=ToTensor()
        )
    return trainset, testset


def train_epoch(
    dataloader, model, loss_fn, optimizer, world_size: int, world_rank: int
):
    size = len(dataloader.dataset) // world_size
    model.train()
    for batch_index, (inputs, labels) in enumerate(dataloader):
        predictions = model(inputs)
        loss = loss_fn(predictions, labels)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch_index % 100 == 0:
            current = batch_index * len(inputs)
            print(
                f"[rank={world_rank}] loss: {loss.item():>7f}  [{current:>5d}/{size:>5d}]"
            )


def validate_epoch(dataloader, model, loss_fn, world_size: int, world_rank: int):
    size = len(dataloader.dataset) // world_size
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for inputs, labels in dataloader:
            predictions = model(inputs)
            test_loss += loss_fn(predictions, labels).item()
            correct += (predictions.argmax(1) == labels).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(
        f"[rank={world_rank}] Test Error: \n "
        f"Accuracy: {(100 * correct):>0.1f}%, "
        f"Avg loss: {test_loss:>8f} \n"
    )
    return test_loss


def save_checkpoint(local_dir, model, optimizer, epoch):
    checkpoint = {
        "model": model.state_dict(),
        "optimizer": optimizer.state_dict(),
        "epoch": epoch,
    }
    torch.save(checkpoint, os.path.join(local_dir, "checkpoint.pt"))


def load_checkpoint(local_ckpt_path, model, optimizer) -> int:
    checkpoint = torch.load(os.path.join(local_ckpt_path, "checkpoint.pt"))
    model.load_state_dict(checkpoint["model"])
    optimizer.load_state_dict(checkpoint["optimizer"])
    return checkpoint["epoch"] + 1


def train_func(config: Dict):
    local_start_time = time.monotonic()

    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]
    shuffle = config.get("shuffle", False)

    world_size = train.get_context().get_world_size()
    world_rank = train.get_context().get_world_rank()

    worker_batch_size = batch_size // world_size
    if world_rank == 0:
        print(f"global batch size is {worker_batch_size * world_size}")

    training_data, test_data = load_data(DATA_DIR)

    train_dataloader = DataLoader(
        training_data, shuffle=shuffle, batch_size=worker_batch_size
    )
    test_dataloader = DataLoader(
        test_data, shuffle=shuffle, batch_size=worker_batch_size
    )

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    model = train.torch.prepare_model(NeuralNetwork())
    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    start_epoch = 1
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as temp_ckpt_dir:
            print("Found checkpoint: ", checkpoint)
            start_epoch = load_checkpoint(temp_ckpt_dir, model, optimizer)
            print(f"Restoration done! Resuming training from {start_epoch=}")

    for epoch in range(start_epoch, epochs + 1):
        if world_size > 1:
            train_dataloader.sampler.set_epoch(epoch)

        train_epoch(
            train_dataloader,
            model,
            loss_fn,
            optimizer,
            world_size=world_size,
            world_rank=world_rank,
        )
        loss = validate_epoch(
            test_dataloader,
            model,
            loss_fn,
            world_size=world_size,
            world_rank=world_rank,
        )

        local_time_taken = time.monotonic() - local_start_time
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            checkpoint = None
            if world_rank == 0:
                print("Saving checkpoint...")
                save_checkpoint(temp_checkpoint_dir, model, optimizer, epoch)
                checkpoint = train.Checkpoint.from_directory(temp_checkpoint_dir)

            train.report(
                metrics={"loss": loss, "local_time_taken": local_time_taken},
                checkpoint=checkpoint,
                checkpoint_dir_name=f"checkpoint-epoch={epoch}",
            )


def train_torch_ray_train(
    config: dict,
    num_workers: Tuple[int, int] = (4, 12),
    use_gpu: bool = True,
) -> train.Result:
    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(
        train_loop_per_worker=lambda c: train_func(config=c),
        train_loop_config=config,
        scaling_config=ray.train.ScalingConfig(
            num_workers=num_workers, use_gpu=use_gpu
        ),
        run_config=ray.train.RunConfig(
            name=f"elastic_train_experiment-{date_str()}",
            storage_path=STORAGE_PATH,
            checkpoint_config=ray.train.CheckpointConfig(num_to_keep=2),
            failure_config=ray.train.FailureConfig(max_failures=3),
        ),
    )
    return trainer.fit()


@ray.remote(num_cpus=0)
def run_cluster_node_killing_events(target_gpu_count: int):
    logging.basicConfig(level=logging.INFO)
    terminator_logger = logging.getLogger(__name__)
    terminator_logger.addHandler(get_file_handler())

    start = time.time()
    head_node_id = ray.get_runtime_context().get_node_id()

    def get_cluster_resources() -> Dict[str, float]:
        return {
            resource: value
            for resource, value in ray.cluster_resources().items()
            if resource in ("CPU", "GPU")
        }

    def get_worker_nodes() -> List[Dict]:
        return [
            node
            for node in ray.nodes()
            if node["Alive"] and node["NodeID"] != head_node_id
        ]

    def kill_nodes(nodes_to_kill):
        terminator_logger.info(
            "Nodes to kill: %s", [n["NodeID"] for n in nodes_to_kill]
        )
        for node in nodes_to_kill:
            terminator_logger.info(
                "Killing node: %s (alive=%s)", node["NodeID"], node["Alive"]
            )
            terminate_node(node["NodeID"])

    def all_nodes_dead(dying_nodes) -> bool:
        dying_node_ids = [n["NodeID"] for n in dying_nodes]
        return all(
            not node["Alive"]
            for node in ray.nodes()
            if node["NodeID"] in dying_node_ids
        )

    def log_status(message):
        elapsed = time.time() - start
        status_str = "\n"
        status_str += "-" * 80 + "\n"
        status_str += (
            f"[elapsed={elapsed:.1f}s] cluster_resources={get_cluster_resources()}\n"
        )
        status_str += message + "\n"
        status_str += "-" * 80 + "\n\n"
        terminator_logger.info(status_str)

    log_status(f"Waiting to upscale back to {target_gpu_count} GPUs...")
    while get_cluster_resources().get("GPU", 0) < target_gpu_count:
        time.sleep(1)

    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    log_status("Killing all nodes in the current cluster...")
    nodes_to_kill = get_worker_nodes()
    kill_nodes(nodes_to_kill)
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    log_status(f"Waiting to upscale back to {target_gpu_count} GPUs...")
    while get_cluster_resources().get("GPU", 0) < target_gpu_count:
        time.sleep(1)

    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    log_status("Killing two worker nodes...")
    nodes_to_kill = get_worker_nodes()[-2:]
    kill_nodes(nodes_to_kill)
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    log_status(f"Waiting to upscale back to {target_gpu_count} GPUs...")
    while get_cluster_resources().get("GPU", 0) < target_gpu_count:
        time.sleep(1)

    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    log_status("Killing 1 worker node...")
    nodes_to_kill = [get_worker_nodes()[-1]]
    kill_nodes(nodes_to_kill)
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    log_status("All node killing events generated, waiting for training finish...")


@click.group(help="Run Torch benchmarks")
def cli():
    pass


@cli.command(help="Kick off Ray Train elastic benchmark")
@click.option("--num-epochs", type=int, default=50)
@click.option("--num-workers", type=tuple, default=(4, 12))
@click.option("--use-gpu", is_flag=True, default=True)
@click.option("--batch-size", type=int, default=64)
def run(
    num_epochs: int = 50,
    num_workers: Tuple[int, int] = (4, 12),
    use_gpu: bool = True,
    batch_size: int = 64,
):
    config = CONFIG.copy()
    config["epochs"] = num_epochs
    config["batch_size"] = batch_size

    ray.init(log_to_driver=True, runtime_env={"working_dir": os.path.dirname(__file__)})

    head_node_id = ray.get_runtime_context().get_node_id()
    event_future = run_cluster_node_killing_events.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        runtime_env={"env_vars": {"RAY_TRAIN_V2_ENABLED": "1"}},
    ).remote(target_gpu_count=num_workers[1])

    result = train_torch_ray_train(
        config=config,
        num_workers=num_workers,
        use_gpu=use_gpu,
    )
    ray.get(event_future)

    logger.info(
        "`trainer.fit` finished with (error, checkpoint):\nerror = %s\ncheckpoint = %s",
        result.error,
        result.checkpoint,
    )
    assert not result.error, result.error
    assert result.checkpoint

    checkpoint_dir_name = Path(result.checkpoint.path).name
    expected_checkpoint_dir_name = f"checkpoint-epoch={num_epochs}"
    assert (
        checkpoint_dir_name == expected_checkpoint_dir_name
    ), f"{checkpoint_dir_name=} != {expected_checkpoint_dir_name=}"

    with open(LOG_FILE, "r") as f:
        print(f.read())


def get_file_handler() -> logging.FileHandler:
    handler = logging.FileHandler(LOG_FILE)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] :: %(message)s")
    handler.setFormatter(formatter)
    return handler


def setup_logging():
    file_handler = get_file_handler()
    logger.addHandler(file_handler)
    logging.getLogger("ray.train").addHandler(file_handler)


def main():
    setup_logging()
    cli()


if __name__ == "__main__":
    main()
