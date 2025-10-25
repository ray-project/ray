"""Example: Using Just-In-Time (JIT) Checkpointing in Ray Train.

This example demonstrates how to enable JIT checkpointing to minimize
data loss when training jobs are preempted.

JIT checkpointing automatically saves a checkpoint when a SIGTERM signal
is received, which is common in preemptible environments such as:
- Kubernetes pod preemption
- AWS Spot instances
- GCP Preemptible VMs
- Kubernetes Kueue job management
"""

import tempfile

import ray
from ray import train
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    """Simple training function that demonstrates checkpointing."""
    import torch
    import torch.nn as nn

    # Create a simple model
    model = nn.Linear(10, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    # Load from checkpoint if resuming
    checkpoint = train.get_checkpoint()
    start_epoch = 0

    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            import os

            # Load model state
            model_path = os.path.join(checkpoint_dir, "model.pt")
            if os.path.exists(model_path):
                model.load_state_dict(torch.load(model_path))

            # Load optimizer state
            optimizer_path = os.path.join(checkpoint_dir, "optimizer.pt")
            if os.path.exists(optimizer_path):
                optimizer.load_state_dict(torch.load(optimizer_path))

            # Load epoch
            epoch_path = os.path.join(checkpoint_dir, "epoch.pt")
            if os.path.exists(epoch_path):
                start_epoch = torch.load(epoch_path) + 1

            print(f"Resumed from epoch {start_epoch}")

    # Training loop
    for epoch in range(start_epoch, config.get("num_epochs", 10)):
        # Simulate training
        for _ in range(100):
            x = torch.randn(32, 10)
            y = torch.randn(32, 1)

            optimizer.zero_grad()
            output = model(x)
            loss = nn.functional.mse_loss(output, y)
            loss.backward()
            optimizer.step()

        # Save checkpoint periodically
        # Note: JIT checkpointing will ALSO save on SIGTERM
        if epoch % 5 == 0:
            with tempfile.TemporaryDirectory() as temp_dir:
                import os

                torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
                torch.save(
                    optimizer.state_dict(), os.path.join(temp_dir, "optimizer.pt")
                )
                torch.save(epoch, os.path.join(temp_dir, "epoch.pt"))

                checkpoint = Checkpoint.from_directory(temp_dir)

                # Report checkpoint
                # Rank 0 reports the checkpoint
                if train.get_context().get_world_rank() == 0:
                    train.report({"epoch": epoch, "loss": loss.item()}, checkpoint=checkpoint)
                else:
                    train.report({"epoch": epoch, "loss": loss.item()})

        else:
            # Report metrics without checkpoint
            train.report({"epoch": epoch, "loss": loss.item()})


def main():
    """Main function demonstrating JIT checkpoint configuration."""

    # Initialize Ray
    ray.init()

    # Configure JIT checkpointing
    jit_checkpoint_config = JITCheckpointConfig(
        enabled=True,  # Enable JIT checkpointing
        kill_wait=3.0,  # Wait 3 seconds after SIGTERM before checkpointing
        # This avoids wasting time if SIGKILL follows immediately
    )

    # Create trainer with JIT checkpointing enabled
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 100},
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,  # Set to True if GPUs available
        ),
        run_config=RunConfig(
            name="jit_checkpoint_example",
            storage_path="/tmp/ray_results",
            jit_checkpoint_config=jit_checkpoint_config,
        ),
    )

    # Run training
    # If the job is preempted (receives SIGTERM), a checkpoint will be
    # automatically saved before termination
    result = trainer.fit()

    print("Training completed!")
    print(f"Final metrics: {result.metrics}")

    # Cleanup
    ray.shutdown()


if __name__ == "__main__":
    main()

