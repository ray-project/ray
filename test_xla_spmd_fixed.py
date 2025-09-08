import os
import tempfile
import torch
from torch import nn
import torch.distributed as dist
import ray
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.torch.config import TorchConfig

# Define your network structure.
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.layer1 = nn.Linear(1, 32)
        self.relu = nn.ReLU()
        self.layer2 = nn.Linear(32, 1)

    def forward(self, input):
        return self.layer2(self.relu(self.layer1(input)))


import copy


def verify_sync_by_checksum(
    model, optimizer, device, *, restore=True, tag="sync-check"
):
    """
    Make grads deliberately different on each rank, take one XLA step,
    then assert parameters are identical across ranks.

    If `restore=True`, model/optimizer state is restored afterwards.
    """
    import torch
    import torch.distributed as dist
    import torch_xla.core.xla_model as xm

    rank = dist.get_rank()
    world = dist.get_world_size()

    # Snapshot state so we can restore after the check
    if restore:
        model_state = {
            k: v.detach().cpu().clone() for k, v in model.state_dict().items()
        }
        opt_state = copy.deepcopy(optimizer.state_dict())

    # Start from identical params
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()

    # Inject rank-skewed grads (different on each rank)
    for p in model.parameters():
        p.grad = torch.ones_like(p, device=device) * (rank + 1)

    # One synchronized optimizer step
    xm.optimizer_step(optimizer, barrier=True)
    xm.mark_step()

    # Compute a scalar checksum of params
    checksum = torch.tensor(0.0, device=device)
    for p in model.parameters():
        checksum = checksum + p.detach().float().sum().view(1)

    # Gather checksums from all ranks -> shape [world, 1] -> flatten to [world]
    gathered = xm.all_gather(checksum).view(-1)  # shape: [world]
    print(
        f">>> [{tag}] rank {rank}: param checksums from all ranks -> {gathered.cpu().tolist()}"
    )

    # If synced, all checksums must be identical
    if world > 1 and not torch.allclose(gathered, gathered[:1].expand_as(gathered)):
        raise RuntimeError(
            "XLA optimizer step did not sync — parameter checksums differ across ranks"
        )

    # Restore state so training proceeds unaffected
    if restore:
        model.load_state_dict(model_state)
        optimizer.load_state_dict(opt_state)
        xm.mark_step()


def sanity_check_xla_collectives():
    import torch
    import torch_xla.core.xla_model as xm
    import torch_xla.runtime as xr

    dev = xm.xla_device()
    # Each rank contributes its 1-based index
    t = torch.tensor([float(xr.process_index() + 1)], device=dev)
    xm.all_reduce("sum", [t])  # in-place
    xm.mark_step()
    val = float(t.item())
    print(
        f">>> xla all_reduce sum at proc {xr.process_index()} -> {val} (expected {sum(range(1, xr.process_count()+1))})"
    )


def verify_xla_sync(model, atol=1e-6):
    import torch_xla.core.xla_model as xm

    dev = xm.xla_device()
    with torch.no_grad():
        chk = torch.tensor([0.0], device=dev)
        for p in model.parameters():
            chk += p.detach().to(dev, dtype=torch.float32).sum()

    # dist.* is routed through xla_backend, so tensors must be XLA:
    ws = dist.get_world_size()
    gathered = [torch.empty_like(chk) for _ in range(ws)]
    dist.all_gather(gathered, chk)  # OK because chk is XLA
    xm.mark_step()

    vals = [float(g.cpu().item()) for g in gathered]
    print(
        f">>> [sync-check] rank {dist.get_rank()}: param checksums from all ranks -> {vals}"
    )
    if any(abs(v - vals[0]) > atol for v in vals):
        raise RuntimeError(
            f"XLA optimizer step did not synchronize parameters across ranks (got {vals})"
        )


# Training loop.
def train_loop_per_worker(config):
    import torch_xla
    import torch_xla.core.xla_model as xm
    import torch_xla.runtime as xr

    device = torch_xla.device()
    xm_device = xm.xla_device()

    # Read configurations.
    print(
        f">>> Starting XLA SPMD training on worker {ray.train.get_context().get_world_rank()} on device = {device}, xm_device = {xm_device}"
    )

    print(f">>> PG backend: {dist.get_backend()}")
    print(
        f">>> is_spmd={xr.is_spmd()} proc_index={xr.process_index()}, proc_count={xr.process_count()}, local_device_count={xr.local_device_count()} global_device_count={xr.global_device_count()}"
    )
    print(
        f">>> xla global_ordinal = [{xr.global_ordinal()}], xr_world_size = [{xr.world_size()}]"
    )
    print(f">>> dist world_size={dist.get_world_size()}  dist_rank={dist.get_rank()}")
    device_ids = xm.get_xla_supported_devices()
    print(f">>> device ids = {device_ids}")

    print(f">>> master port is {os.environ['MASTER_PORT']}")
    print(f">>> pjrt port is {os.environ['PJRT_COORDINATOR_ADDRESS']}")
    print(f">>> XLA_USE_SPMD = {os.environ.get('XLA_USE_SPMD', 'NOT SET')}")

    lr = config["lr"]
    batch_size = config["batch_size"]
    num_epochs = config["num_epochs"]

    # Fetch training dataset.
    train_dataset_shard = ray.train.get_dataset_shard("train")
    # Create data loader.
    dataloader = train_dataset_shard.iter_torch_batches(
        batch_size=batch_size, dtypes=torch.float
    )

    # Instantiate and prepare model for training.
    model = NeuralNetwork()
    model = model.to(device)

    # For XLA SPMD, we don't use DDP - XLA handles distribution automatically
    # Just broadcast master parameters to ensure all processes start with same weights
    xm.broadcast_master_param(model)

    # Define loss and optimizer.
    loss_fn = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    if dist.get_world_size() > 1:
        print(">>> doing the verification...")
        sanity_check_xla_collectives()
        # Test parameter synchronization
        verify_sync_by_checksum(
            model, optimizer, device, restore=True, tag="initial-sync"
        )

    # Train multiple epochs.
    for epoch in range(num_epochs):
        # Train epoch.
        for batch in dataloader:
            x = batch["input"].to("xla")
            y = batch["label"].to("xla")
            output = model(x)
            loss = loss_fn(output, y)
            optimizer.zero_grad()

            loss.backward()

            # Use XLA's optimizer step with barrier for synchronization
            xm.optimizer_step(optimizer, barrier=True)
            xm.mark_step()

        # Create checkpoint.
        base_model = model  # No DDP wrapper in XLA SPMD
        checkpoint_dir = tempfile.mkdtemp()
        torch.save(
            {"model_state_dict": base_model.state_dict()},
            os.path.join(checkpoint_dir, "model.pt"),
        )
        checkpoint = Checkpoint.from_directory(checkpoint_dir)

        # Report metrics and checkpoint.
        ray.train.report({"loss": loss.item()}, checkpoint=checkpoint)


def main():
    train_loop_config = {"num_epochs": 10, "lr": 0.01, "batch_size": 32}
    # Define configurations.
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    run_config = RunConfig(
        name="xla_trainer_spmd",
        checkpoint_config=CheckpointConfig(num_to_keep=1),
        storage_path="/mnt/cluster_storage/xla",
        worker_runtime_env={
            "env_vars": {
                "LD_LIBRARY_PATH": "/home/ray/anaconda3/lib:$LD_LIBRARY_PATH",
                "PJRT_DEVICE": "CUDA",
                # Enable XLA SPMD mode
                "XLA_USE_SPMD": "1",
                "XLA_DISTRIBUTED": "1",
            },
        },
    )

    # Define datasets.
    train_dataset = ray.data.from_items(
        [{"input": [x], "label": [2 * x + 1]} for x in range(2000)]
    )
    datasets = {"train": train_dataset}

    xla_torch_config = TorchConfig(
        backend="xla",
        # SPMD configuration will be handled by environment variables
    )

    # Initialize the Trainer.
    trainer = TorchTrainer(
        torch_config=xla_torch_config,
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        run_config=run_config,
        datasets=datasets,
    )

    # Train the model.
    trainer.fit()


if __name__ == "__main__":
    main()
