#!/usr/bin/env python3
"""
Test script demonstrating the XLA mesh integration with Ray Train TorchTrainer.

This script shows how users can access the XLA SPMD mesh in their training loops
when using the XLA backend with Ray Train.
"""

import os
import tempfile
from typing import Dict

import torch
import torch.nn as nn
import torch.optim as optim

import ray
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer, TorchConfig


class SimpleModel(nn.Module):
    """A simple neural network for testing."""
    
    def __init__(self, input_size: int = 10, hidden_size: int = 64, output_size: int = 1):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, output_size)
        )
    
    def forward(self, x):
        return self.layers(x)


def train_loop_per_worker(config: Dict):
    """
    Training loop that demonstrates XLA mesh usage with Train V2.
    
    This function shows how to:
    1. Check if XLA backend is being used
    2. Get the XLA mesh for SPMD operations
    3. Use the mesh for model sharding and data parallelism
    """
    import ray.train as train
    context = train.get_context()
    print(f"Starting training on worker {context.get_world_rank()}")
    
    # Check if we're using XLA backend
    try:
        from ray.train.torch.xla import is_xla_backend, get_xla_mesh
        is_xla = is_xla_backend()
        print(f"XLA backend active: {is_xla}")
        
        if is_xla:
            # Get the XLA mesh from train context
            mesh = context.get_xla_mesh()
            if mesh is not None:
                print(f"XLA mesh shape: {mesh.shape}")
                print(f"XLA mesh axes: {mesh.axis_names}")
                print(f"XLA mesh devices: {mesh.devices}")
                
                # Example: Use the mesh for model sharding
                # This is where users would implement their SPMD logic
                try:
                    import torch_xla.distributed.spmd as xs
                    
                    # Create model
                    model = SimpleModel()
                    
                    # Example of sharding the model across the mesh
                    # For a 4x4 TPU slice, this would shard across the "data" axis
                    if len(mesh.shape) > 1:
                        # Multi-dimensional mesh: use data parallelism
                        sharding_spec = xs.Shard(0)  # Shard along first axis
                    else:
                        # 1D mesh: simple data parallelism
                        sharding_spec = xs.Shard(0)
                    
                    # Mark sharding on the model (this is a simplified example)
                    print(f"Would apply sharding spec: {sharding_spec}")
                    
                    # In a real implementation, you would do:
                    # model = xs.mark_sharding(model, mesh, sharding_spec)
                    
                except ImportError:
                    print("torch_xla.distributed.spmd not available")
            else:
                print("XLA mesh not available")
        else:
            print("Not using XLA backend, using standard PyTorch")
            
    except ImportError:
        print("XLA utilities not available")
    
    # Standard training setup (works for both XLA and non-XLA)
    model = SimpleModel()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.MSELoss()
    
    # Create some dummy data
    batch_size = 32
    input_data = torch.randn(batch_size, 10)
    target_data = torch.randn(batch_size, 1)
    
    # Training step
    optimizer.zero_grad()
    output = model(input_data)
    loss = criterion(output, target_data)
    loss.backward()
    optimizer.step()
    
    print(f"Training step completed, loss: {loss.item():.4f}")
    
    # Report metrics
    ray.train.report({"loss": loss.item()})


def main():
    """Main function to run the test."""
    print("Testing XLA mesh integration with Ray Train TorchTrainer")
    
    # Configuration for XLA backend
    torch_config = TorchConfig(backend="xla")
    
    # Scaling configuration
    # For a 4x4 TPU slice: 4 workers, 4 hosts, each with 4 TPU chips
    scaling_config = ScalingConfig(
        num_workers=4,  # 4 workers (one per host)
        use_gpu=False,  # Using TPU, not GPU
    )
    
    # Run configuration
    run_config = RunConfig(
        checkpoint_config=CheckpointConfig(num_to_keep=1),
        name="xla_mesh_test"
    )
    
    # Training configuration
    train_loop_config = {
        "epochs": 1,
        "batch_size": 32,
    }
    
    # Create trainer
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        torch_config=torch_config,
        scaling_config=scaling_config,
        run_config=run_config,
    )
    
    print("Starting training...")
    result = trainer.fit()
    
    print("Training completed!")
    print(f"Final metrics: {result.metrics}")


if __name__ == "__main__":
    # Initialize Ray
    ray.init()
    
    try:
        main()
    finally:
        ray.shutdown()
