import os
import tempfile
from datetime import timedelta

import torch
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torchdata.stateful_dataloader import StatefulDataLoader
from torchft import (
    DistributedDataParallel,
    DistributedSampler,
    Manager,
    Optimizer,
    ProcessGroupNCCL,
)
from torchft.checkpointing.pg_transport import PGTransport
from torchvision.datasets import FashionMNIST
from torchvision.models import resnet18
from torchvision.transforms import Compose, Normalize, ToTensor

import ray.train.torch


TP_SIZE = 2  # Tensor Parallel dimension


def train_func():
    world_rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()

    # Derived values for TP
    group_rank = world_rank % TP_SIZE  # Rank within replica group (0 or 1 for TP=2)
    replica_group_id = world_rank // TP_SIZE  # Which replica group this worker belongs to
    num_replica_groups = world_size // TP_SIZE  # Number of replica groups

    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.to("cuda")
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Wrap model and optimizer with torchft primitives
    def load_state_dict(state_dict):
        model.load_state_dict(state_dict["model"])
        optimizer.load_state_dict(state_dict["optim"])

    def state_dict():
        return {
            "model": model.state_dict(),
            "optim": optimizer.state_dict(),
        }

    pg = ProcessGroupNCCL(timeout=timedelta(seconds=30))
    transport = PGTransport(
        pg,
        timeout=timedelta(seconds=10),
        device="cuda",
    )
    manager = Manager(
        pg=pg,
        min_replica_size=1,
        load_state_dict=load_state_dict,
        state_dict=state_dict,
        # Replica group world size (TP dimension)
        world_size=TP_SIZE,
        # Rank within replica group
        rank=group_rank,
        # Replica group identifier (workers in same group share the same replica_id)
        replica_id=f"train_ddp_{replica_group_id}",
        timeout=timedelta(seconds=30),
        checkpoint_transport=transport,
    )
    model = DistributedDataParallel(manager, model)
    optimizer = Optimizer(manager, optimizer)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(
        root=data_dir, train=True, download=True, transform=transform
    )

    # Wrap dataloader with torchft primitives
    # This shards the training set across all ranks and replica groups. We manage
    # the dataloaders on a per replica group basis with the assumption that the
    # majority of groups will be available so few batches will be dropped.
    sampler = DistributedSampler(
        train_data,
        replica_rank=replica_group_id,
        num_replica_groups=num_replica_groups,
        group_rank=group_rank,
        # TP dimension - number of workers per replica group
        num_replicas=TP_SIZE,
        shuffle=True,
    )
    # This uses the torchdata StatefulDataLoader to be able to checkpoint and
    # restore the per worker dataloader position.
    train_loader = StatefulDataLoader(
        train_data, batch_size=64, num_workers=2, sampler=sampler
    )

    # Training
    for images, labels in train_loader:
        images, labels = images.to("cuda"), labels.to("cuda")
        # Do zero_grad first to start quorum early as done in torchft train_ddp example
        optimizer.zero_grad()
        outputs = model(images)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        if manager.current_step() >= 200:
            # complete training
            break

    # [3] Print metrics.
    metrics = {"loss": loss.item()}
    print(f"Rank {ray.train.get_context().get_world_rank()} metrics: {metrics}")


# [4] Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=4, use_gpu=True)

# [5] Launch distributed training job.
trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    # [5a] If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    run_config=ray.train.RunConfig(
        storage_path="/mnt/cluster_storage",
        name="my_run_name",
        failure_config=ray.train.FailureConfig(max_failures=3),
    ),
)
result = trainer.fit()
