import os
import tempfile
from datetime import timedelta
from pathlib import Path

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
from ray.train.torch.config import TorchftConfig


def train_func():
    group_rank = ray.train.get_context().get_replica_group_rank()
    replica_group_id = ray.train.get_context().get_replica_group_id()

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
        # This is replica group world size. torchft example doesn't set this.
        world_size=1,
        # Always rank 0 per replica group.
        rank=0,
        # example does REPLICA_GROUP_ID, but we will do N replica groups and 1 worker per replica group
        replica_id=f"train_ddp_{ray.train.get_context().get_world_rank()}",
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
        replica_rank=ray.train.get_context().get_world_rank(),
        num_replica_groups=ray.train.get_context().get_world_size(),
        group_rank=0,
        # for DDP we can use replica groups of size 1, FSDP/PP/CP would need more.
        num_replicas=1,
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

        current_step = manager.current_step()

        checkpoint_path = Path(ray.train.get_context().get_storage().build_checkpoint_path_from_name("marker"))
        if ray.train.get_context().get_world_rank() == 1 and current_step == 123 and not checkpoint_path.exists():
            checkpoint_path.touch()
            raise ValueError("inject fake error")

        if current_step % 50 == 0 or current_step >= 195:
            print(
                f"[group={replica_group_id} rank={group_rank}] "
                f"step={current_step} loss={loss.item():.4f}"
            )

        if current_step >= 200:
            # complete training
            break

    # [3] Print metrics.
    metrics = {"loss": loss.item()}
    print(f"Rank {ray.train.get_context().get_world_rank()} metrics: {metrics}")


# [4] Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

# [5] Launch distributed training job.
trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    # TODO: fix misleading naming. Right now dp_workers is actually the size of each replica group.
    torch_config=TorchftConfig(dp_workers=1, min_replicas=2),
    # [5a] If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    run_config=ray.train.RunConfig(
        storage_path="/mnt/cluster_storage",
        #name="my_run_name",
        failure_config=ray.train.FailureConfig(max_failures=3),
    ),
)
result = trainer.fit()
