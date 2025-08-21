"""Example demonstrating async checkpoint functionality for training.

This example shows how to use async_report for non-blocking checkpoint uploads,
particularly useful for training that checkpoints asynchronously.
"""

import tempfile
import time
import torch
import torch.nn as nn
from pathlib import Path

from ray import train
from ray.train import Checkpoint
from ray.train.torch import TorchTrainer


class SimpleModel(nn.Module):
    """Simple model for demonstration."""

    def __init__(self, input_size=10, hidden_size=20, output_size=1):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_size),
        )

    def forward(self, x):
        return self.network(x)


def train_func(config):
    """Training function demonstrating async checkpointing."""
    print("Starting training with async checkpointing...")

    model = SimpleModel()
    optimizer = torch.optim.Adam(model.parameters(), lr=config.get("lr", 0.001))

    num_episodes = config.get("num_episodes", 50)
    checkpoint_freq = config.get("checkpoint_freq", 5)

    for episode in range(num_episodes):
        start_time = time.time()
        # dummy fwd/bwd pass
        x = torch.randn(32, 10)
        y_pred = model(x)
        loss = torch.mean(y_pred**2)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        step_time = time.time() - start_time

        metrics = {
            "episode": episode,
            "loss": loss.item(),
            "step_time": step_time,
            "lr": optimizer.param_groups[0]["lr"],
        }

        if episode % checkpoint_freq == 0:
            print(f"Episode {episode}: Creating checkpoint...")

            checkpoint_start = time.time()

            with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                # Save model and optimizer state
                model_path = Path(temp_checkpoint_dir) / "model.pt"
                optimizer_path = Path(temp_checkpoint_dir) / "optimizer.pt"
                config_path = Path(temp_checkpoint_dir) / "config.json"

                torch.save(model.state_dict(), model_path)
                torch.save(optimizer.state_dict(), optimizer_path)

                # Save training config
                import json

                with open(config_path, "w") as f:
                    json.dump(
                        {
                            "episode": episode,
                            "lr": config.get("lr", 0.001),
                            "model_config": {
                                "input_size": 10,
                                "hidden_size": 20,
                                "output_size": 1,
                            },
                        },
                        f,
                    )

                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

                train.async_report(metrics, checkpoint=checkpoint)

                checkpoint_time = time.time() - checkpoint_start
                print(
                    f"Episode {episode}: Checkpoint created in {checkpoint_time:.3f}s (non-blocking)"
                )
        else:
            train.async_report(metrics)

        if episode % 10 == 0 and episode > 0:
            print(f"Episode {episode}: Checking for async upload errors...")
            train.async_report({}, flush=True)

            # Print upload status
            train.print_async_checkpoint_status()

        time.sleep(0.01)

    print("Training complete! Checking for any remaining upload errors...")
    train.async_report({}, flush=True)
    print("All async uploads completed successfully!")


def main():
    """Run the example."""
    import tempfile

    with tempfile.TemporaryDirectory() as temp_storage:
        storage_path = Path(temp_storage) / "checkpoints"
        storage_path.mkdir()

        print(f"Using storage path: {storage_path}")

        trainer = TorchTrainer(
            train_func,
            train_loop_config={
                "lr": 0.001,
                "num_episodes": 30,
                "checkpoint_freq": 3,
            },
            scaling_config=train.ScalingConfig(num_workers=1),
            run_config=train.RunConfig(
                name="async_checkpoint_example",
                storage_path=str(storage_path),
            ),
        )

        print("Starting trainer...")
        result = trainer.fit()

        print(f"\nTraining completed!")
        print(f"Final metrics: {result.metrics}")

        checkpoint_dirs = list(storage_path.glob("**/checkpoint_*"))
        print(f"\nCheckpoints created: {len(checkpoint_dirs)}")
        for checkpoint_dir in checkpoint_dirs:
            files = list(checkpoint_dir.rglob("*"))
            print(f"  {checkpoint_dir.name}: {len(files)} files")

        latest_files = list(storage_path.rglob("LATEST.json"))
        if latest_files:
            print(f"\nFound LATEST.json manifest files: {len(latest_files)}")
            for latest_file in latest_files:
                print(f"  {latest_file}")
                with open(latest_file) as f:
                    import json

                    manifest = json.load(f)
                    print(f"    Last checkpoint metrics: {manifest.get('metrics', {})}")


if __name__ == "__main__":
    main()
