import os

# Standalone TorchftConfig snippet shown in the user guide. Wrapped in a
# function so importing this module doesn't try to actually start a
# lighthouse — only the lines between the markers are pulled into the doc.
def _torchft_config_example():
# __torchft_config_start__
    from ray.train.v2.torch.torchft_config import TorchftConfig
    torch_config = TorchftConfig(
        backend="gloo", lighthouse_kwargs={"min_replicas": 2}
    )
# __torchft_config_end__
    return torch_config


# fmt: off
# __torchft_ddp_torch_dataloader_start__
import os
import tempfile
import time
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
from ray.train.v2.torch.torchft_config import TorchftConfig


def train_func():
    group_rank = 0
    replica_group_id = ray.train.get_context().get_world_rank()

    # Model, loss, optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.to("cuda")
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Callbacks used by the torchft Manager to recover state from peers.
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
        # Replica-group world size. One worker per replica group for DDP.
        world_size=1,
        # Always rank 0 within the replica group.
        rank=0,
        # One replica group per Ray Train worker.
        replica_id=f"train_ddp_{ray.train.get_context().get_world_rank()}",
        timeout=timedelta(seconds=30),
        checkpoint_transport=transport,
    )
    # Wrap model and optimizer with torchft primitives.
    model = DistributedDataParallel(manager, model)
    optimizer = Optimizer(manager, optimizer)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(
        root=data_dir, train=True, download=True, transform=transform
    )

    # Shard the dataset across fault-tolerant replica groups.
    sampler = DistributedSampler(
        train_data,
        replica_rank=ray.train.get_context().get_world_rank(),
        num_replica_groups=ray.train.get_context().get_world_size(),
        group_rank=0,
        # For DDP each replica group has size 1; FSDP/PP/CP would set this higher.
        num_replicas=1,
        shuffle=True,
    )
    # StatefulDataLoader lets each worker checkpoint and resume its position.
    train_loader = StatefulDataLoader(
        train_data, batch_size=64, num_workers=3, sampler=sampler
    )

    # Training loop
    for images, labels in train_loader:
        images, labels = images.to("cuda"), labels.to("cuda")
        # Call zero_grad() first so quorum negotiation can start early
        # (matches the torchft train_ddp example).
        optimizer.zero_grad()
        outputs = model(images)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        current_step = manager.current_step()

        if current_step % 50 == 0 or current_step >= 195:
            print(
                f"[group={replica_group_id} rank={group_rank}] "
                f"step={current_step} loss={loss.item():.4f}"
            )
            time.sleep(5)

        if current_step >= 200:
            break

    metrics = {"loss": loss.item()}
    print(f"Rank {ray.train.get_context().get_world_rank()} metrics: {metrics}")


scaling_config = ray.train.ScalingConfig(num_workers=3, use_gpu=True)

trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    torch_config=TorchftConfig(lighthouse_kwargs={"min_replicas": 3}),
    run_config=ray.train.RunConfig(
        # In a multi-node cluster set storage_path to a path accessible
        # from every worker node (e.g. NFS or cloud storage).
        storage_path="/mnt/cluster_storage",
        failure_config=ray.train.FailureConfig(max_failures=3),
    ),
)
result = trainer.fit()
# __torchft_ddp_torch_dataloader_end__
# fmt: on
