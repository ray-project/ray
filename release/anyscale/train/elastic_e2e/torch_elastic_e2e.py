import os
import time
from pathlib import Path
import tempfile
from typing import Dict, List, Tuple

import click
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor
import logging
from filelock import FileLock

import ray
import ray.train as train
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.tune.utils import date_str

from elastic_util import terminate_node, NeuralNetwork

logger = logging.getLogger(__name__)


CONFIG = {"lr": 1e-3, "batch_size": 64}
VANILLA_RESULT_JSON = "/tmp/vanilla_out.json"
LOG_FILE = "/tmp/driver.log"


def get_default_storage_path():
    remote_default_artifact_storage_prefix = os.environ.get(
        "ANYSCALE_ARTIFACT_STORAGE", "artifact_storage"
    )
    storage_path = (
        f"{remote_default_artifact_storage_prefix}/train_release_tests/elastic_e2e"
    )
    return storage_path


STORAGE_PATH = get_default_storage_path()
DATA_DIR = "/tmp/fashion_mnist"


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
    for batch_index, (X, y) in enumerate(dataloader):
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch_index % 100 == 0:
            loss, current = loss.item(), batch_index * len(X)
            print(f"[rank={world_rank}] loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def validate_epoch(dataloader, model, loss_fn, world_size: int, world_rank: int):
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

    # Print the real global batch size from rank 0
    if world_rank == 0:
        print(f"global batch size is {worker_batch_size * world_size}")

    # Load datasets. Use download=False to catch errors in preparation, as the
    # data should have already been downloaded.
    training_data, test_data = load_data(DATA_DIR)

    # Create data loaders and potentially pass distributed sampler
    train_dataloader = DataLoader(
        training_data,
        shuffle=shuffle,
        batch_size=worker_batch_size,
    )
    test_dataloader = DataLoader(
        test_data,
        shuffle=shuffle,
        batch_size=worker_batch_size,
    )

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = train.torch.prepare_data_loader(test_dataloader)

    # Create model.
    model = NeuralNetwork()

    # Prepare model
    model = train.torch.prepare_model(model)

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
                metrics=dict(loss=loss, local_time_taken=local_time_taken),
                checkpoint=checkpoint,
                checkpoint_dir_name=f"checkpoint-epoch={epoch}",
            )


def train_torch_ray_train(
    config: dict,
    num_workers: Tuple[int, int] = (4, 12),
    use_gpu: bool = True,
) -> train.Result:
    from ray.train.torch import TorchTrainer

    def train_loop(config):
        train_func(config=config)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=config,
        scaling_config=ray.train.ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
        ),
        run_config=ray.train.RunConfig(
            name=f"elastic_train_experiment-{date_str()}",
            storage_path=STORAGE_PATH,
            checkpoint_config=ray.train.CheckpointConfig(num_to_keep=2),
            failure_config=ray.train.FailureConfig(max_failures=3),
        ),
    )
    result = trainer.fit()
    return result


@ray.remote(num_cpus=0)
def run_cluster_node_killing_events():
    logging.basicConfig(level=logging.INFO)
    terminator_logger = logging.getLogger(__name__)
    terminator_logger.addHandler(get_file_handler())

    start = time.time()
    head_node_id = ray.get_runtime_context().get_node_id()

    def get_cluster_resources() -> Dict[str, float]:
        cluster_resources = {
            resource: value
            for resource, value in ray.cluster_resources().items()
            if resource in ("CPU", "GPU")
        }
        return cluster_resources

    def get_worker_nodes() -> List[Dict]:
        return [
            node
            for node in ray.nodes()
            if node["Alive"] and node["NodeID"] != head_node_id
        ]

    def kill_nodes(nodes_to_kill):
        terminator_logger.info(f'Nodes to kill: {[n["NodeID"] for n in nodes_to_kill]}')
        for node in nodes_to_kill:
            terminator_logger.info(
                f'Killing node: {node["NodeID"]} (alive={node["Alive"]})'
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
        cluster_resources = get_cluster_resources()
        status_str += f"[elapsed={elapsed:.1f}s] {cluster_resources=}\n"
        status_str += message + "\n"
        status_str += "-" * 80 + "\n\n"
        terminator_logger.info(status_str)

    # Wait a bit before adding resource
    log_status("Waiting to upscale back to 12 GPUs...")
    while get_cluster_resources().get("GPU", 0) < 12:
        time.sleep(1)

    # Run training for 30s before changing any cluster resources
    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    # Kill all nodes in the current cluster
    log_status("Killing all nodes in the current cluster...")
    nodes_to_kill = get_worker_nodes()
    kill_nodes(nodes_to_kill)

    # Wait for the killed nodes detected by ray as dead nodes
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    # Wait a bit before adding resources.
    log_status("Waiting to upscale back to 12 GPUs...")
    while get_cluster_resources().get("GPU", 0) < 12:
        time.sleep(1)

    # Run training for 30s before changing any cluster resources
    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    # Kill the last two node in current cluster node table
    log_status("Killing two worker nodes...")
    nodes_to_kill = get_worker_nodes()[-2:]
    kill_nodes(nodes_to_kill)

    # Wait for the killed nodes detected by ray as dead nodes
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    # Wait a bit before adding resources.
    log_status("Waiting to upscale back to 12 GPUs...")
    while get_cluster_resources().get("GPU", 0) < 12:
        time.sleep(1)

    # Run training for 30s before changing any cluster resources
    log_status("Waiting for 30s before modifying cluster resources...")
    time.sleep(30)

    # Kill the last node in current cluster node table
    log_status("Killing 1 worker node...")
    nodes_to_kill = [get_worker_nodes()[-1]]
    kill_nodes(nodes_to_kill)

    # Wait for the killed nodes detected by ray as dead nodes
    while not all_nodes_dead(nodes_to_kill):
        time.sleep(1)

    # The node killer is done changing the cluster status
    # Now, just wait for the cluster to recover and finish training.
    log_status("All node killing events generated, waiting for training finish...")


@click.group(help="Run Torch benchmarks")
def cli():
    pass


@cli.command(help="Kick off Ray and vanilla benchmarks")
@click.option("--num-epochs", type=int, default=50)
@click.option("--num-workers", type=tuple, default=(4, 12))
@click.option("--use-gpu", is_flag=True, default=True)
@click.option("--batch-size", type=int, default=64)
def run(
    num_epochs: int = 4,
    num_workers: Tuple[int, int] = (4, 12),
    use_gpu: bool = False,
    batch_size: int = 64,
):
    config = CONFIG.copy()
    config["epochs"] = num_epochs
    config["batch_size"] = batch_size

    ray.init(log_to_driver=True, runtime_env={"working_dir": os.path.dirname(__file__)})

    # Run the node killing events function as the main thread.
    head_node_id = ray.get_runtime_context().get_node_id()
    event_future = run_cluster_node_killing_events.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(
            node_id=head_node_id, soft=False
        ),
        runtime_env={"env_vars": {"RAY_TRAIN_V2_ENABLED": "1"}},
    ).remote()

    # Run the trainer driver thread in another thread as a ray task.
    result = train_torch_ray_train(
        config=config,
        num_workers=num_workers,
        use_gpu=use_gpu,
    )

    ray.get(event_future)

    logger.info(
        "`trainer.fit` finished with (error, checkpoint):\n"
        f"error = {result.error}\ncheckpoint = {result.checkpoint}"
    )

    assert not result.error, result.error
    assert result.checkpoint

    checkpoint_dir_name = Path(result.checkpoint.path).name
    expected_checkpoint_dir_name = f"checkpoint-epoch={num_epochs}"
    assert (
        checkpoint_dir_name == expected_checkpoint_dir_name
    ), f"{checkpoint_dir_name=} != {expected_checkpoint_dir_name=}"

    # print out the driver log of the experiment.
    with open(LOG_FILE, "r") as f:
        log_contents = f.read()
        print(log_contents)


def main():
    return cli()


def get_file_handler() -> logging.FileHandler:
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] :: %(message)s")
    fh.setFormatter(formatter)
    return fh


def setup_logging():
    fh = get_file_handler()
    logger.addHandler(fh)
    logging.getLogger("ray.train").addHandler(fh)
    logging.getLogger("ray.anyscale.train").addHandler(fh)


if __name__ == "__main__":
    setup_logging()
    main()
