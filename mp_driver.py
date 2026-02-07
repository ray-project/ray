import os
import tempfile
from datetime import timedelta

import torch.nn as nn
from torch.distributed.device_mesh import init_device_mesh
from torch.distributed.tensor.parallel import (
    ColwiseParallel,
    RowwiseParallel,
    parallelize_module,
)
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torchdata.stateful_dataloader import StatefulDataLoader
from torchft import (
    DistributedSampler,
    Manager,
    Optimizer,
    ProcessGroupNCCL,
)
from torchft.checkpointing.pg_transport import PGTransport
from torchvision.datasets import FashionMNIST
from torchvision.transforms import Compose, Normalize, ToTensor

import ray.train
import ray.train.torch
from ray.train.torch.config import TorchftConfig

TP_SIZE = 2  # Tensor Parallel dimension


class SimpleMLP(nn.Module):
    """Simple MLP model suitable for tensor parallelism demo."""

    def __init__(self, input_dim=784, hidden_dim=1024, output_dim=10):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        # Flatten input: (batch, 1, 28, 28) -> (batch, 784)
        x = x.view(x.size(0), -1)
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x


def train_func():
    world_rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()

    # Use context getters instead of manual computation
    group_rank = ray.train.get_context().get_replica_group_rank()
    replica_group_id = ray.train.get_context().get_replica_group_id()
    num_replica_groups = world_size // TP_SIZE

    print(
        f"Worker {world_rank}: group_rank={group_rank}, "
        f"replica_group_id={replica_group_id}, "
        f"num_replica_groups={num_replica_groups}"
    )

    # Create 1D TP DeviceMesh within the replica group's process group
    # Each replica group has its own init_process_group with dp_workers workers,
    # so init_device_mesh sees only the group-local world.
    tp_mesh = init_device_mesh("cuda", (TP_SIZE,), mesh_dim_names=("tp",))
    print(f"Worker {world_rank}: TP mesh created: {tp_mesh}")

    # Model - using simple MLP that's easier to apply TP to
    model = SimpleMLP(input_dim=784, hidden_dim=1024, output_dim=10)
    model.to("cuda")

    # Apply tensor parallelism using PyTorch's native TP
    # ColwiseParallel: splits output features across TP ranks
    # RowwiseParallel: splits input features across TP ranks
    model = parallelize_module(
        model,
        tp_mesh,
        {
            "fc1": ColwiseParallel(),  # First layer: split columns
            "fc2": RowwiseParallel(),  # Second layer: split rows
        },
    )
    print(f"Worker {world_rank}: Model parallelized with TP")
    # Debug: verify sharding
    for name, param in model.named_parameters():
        global_shape = tuple(param.shape)  # DTensor.shape is the global/logical shape
        local_shape = tuple(param._local_tensor.shape)  # Actual local tensor shape
        placements = getattr(param, "placements", None)
        print(
            f"  Worker {world_rank}: {name}: global={global_shape}, "
            f"local={local_shape}, placements={placements}"
        )

    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Wrap model and optimizer with torchft primitives for fault tolerance
    def load_state_dict(state_dict):
        model.load_state_dict(state_dict["model"])
        optimizer.load_state_dict(state_dict["optim"])

    def state_dict():
        return {
            "model": model.state_dict(),
            "optim": optimizer.state_dict(),
        }

    # torchft process group for fault tolerance coordination
    ft_pg = ProcessGroupNCCL(timeout=timedelta(seconds=30))
    transport = PGTransport(
        ft_pg,
        timeout=timedelta(seconds=10),
        device="cuda",
    )
    manager = Manager(
        pg=ft_pg,
        # With TP, all workers in a replica group must be up
        min_replica_size=TP_SIZE,
        load_state_dict=load_state_dict,
        state_dict=state_dict,
        # Replica group world size (TP dimension)
        world_size=TP_SIZE,
        # Rank within replica group
        rank=group_rank,
        # Replica group identifier
        replica_id=f"train_tp_{replica_group_id}",
        timeout=timedelta(seconds=30),
        checkpoint_transport=transport,
    )
    # Note: We don't wrap model with torchft's DistributedDataParallel since
    # we're using PyTorch's native TP. torchft provides fault tolerance via Manager.
    optimizer = Optimizer(manager, optimizer)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(
        root=data_dir, train=True, download=True, transform=transform
    )

    # Wrap dataloader with torchft primitives
    # This shards the training set across replica groups. Within a TP group,
    # all workers see the same data (they process it with sharded weights).
    sampler = DistributedSampler(
        train_data,
        replica_rank=replica_group_id,
        num_replica_groups=num_replica_groups,
        group_rank=group_rank,
        # TP dimension - number of workers per replica group
        num_replicas=TP_SIZE,
        shuffle=True,
    )
    train_loader = StatefulDataLoader(
        train_data, batch_size=64, num_workers=2, sampler=sampler
    )

    # Training
    for images, labels in train_loader:
        images, labels = images.to("cuda"), labels.to("cuda")
        # Do zero_grad first to start quorum early as done in torchft examples
        optimizer.zero_grad()
        outputs = model(images)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

        if manager.current_step() >= 200:
            # complete training
            break

    # Print metrics
    metrics = {"loss": loss.item()}
    print(
        f"Rank {world_rank} (replica_group={replica_group_id}, "
        f"group_rank={group_rank}) metrics: {metrics}"
    )


# Configure scaling and resource requirements.
# With TP_SIZE=2, num_workers=4 gives us 2 replica groups with 2 workers each
scaling_config = ray.train.ScalingConfig(num_workers=4, use_gpu=True)

# Launch distributed training job.
trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    backend_config=TorchftConfig(dp_workers=TP_SIZE, min_replicas=1),
    run_config=ray.train.RunConfig(
        storage_path="/mnt/cluster_storage",
        name="my_run_name",
        failure_config=ray.train.FailureConfig(max_failures=3),
    ),
)
result = trainer.fit()
