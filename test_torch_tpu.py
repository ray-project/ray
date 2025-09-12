#!/usr/bin/env python3
"""
Test script to demonstrate TPU support with XLA SPMD in TorchTrainer.
This script shows how to use ScalingConfig(use_tpu=True) to enable torch_xla backend
with automatic XLA SPMD configuration for efficient distributed TPU training.
"""

import ray
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer
import torch
import torch.nn as nn


def train_loop_per_worker(config):
    """Training loop that will run on each TPU worker with XLA SPMD execution."""
    print(f"Starting XLA SPMD training on worker {ray.train.get_context().get_world_rank()}")
    
    # Create a simple model
    model = nn.Linear(10, 1)
    
    # Move model to TPU device if available
    try:
        import torch_xla.core.xla_model as xm
        import torch_xla.runtime as xr
        
        # Get TPU device - XLA SPMD will handle device placement automatically
        device = xm.xla_device()
        model = model.to(device)
        print(f"Model moved to TPU device: {device}")
        
        # XLA SPMD will automatically handle data parallelism across TPU cores
        print(f"XLA SPMD enabled - training will use data parallelism")
        
    except ImportError:
        print("torch_xla not available, using CPU")
        device = torch.device("cpu")
        model = model.to(device)
    
    # Create dummy data
    x = torch.randn(100, 10).to(device)
    y = torch.randn(100, 1).to(device)
    
    # Training loop
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    
    for epoch in range(5):
        optimizer.zero_grad()
        output = model(x)
        loss = criterion(output, y)
        loss.backward()
        optimizer.step()
        
        if epoch % 2 == 0:
            print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    print(f"XLA SPMD training completed on worker {ray.train.get_context().get_world_rank()}")


def main():
    """Main function to demonstrate TPU training with XLA SPMD."""
    # Initialize Ray
    ray.init()
    
    print("Starting TPU training with XLA SPMD demonstration...")
    
    # Create scaling config with TPU enabled
    scaling_config = ScalingConfig(
        num_workers=4,
        use_tpu=True,  # This enables torch_xla backend with XLA SPMD
        resources_per_worker={"TPU": 1},
        # For multi-worker TPU training, these are required:
        topology="2x2",  # TPU topology - will auto-configure XLA SPMD mesh
        accelerator_type="TPU-V4",  # TPU accelerator type
        placement_strategy="SPREAD"  # Ensures workers are on separate TPU VMs
    )
    
    # Create run config
    run_config = RunConfig(name="tpu_spmd_training_demo")
    
    # Create trainer - XLA SPMD will be auto-configured
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"epochs": 5},
        scaling_config=scaling_config,
        run_config=run_config
    )
    
    print("Starting XLA SPMD training...")
    result = trainer.fit()
    print("XLA SPMD training completed!")
    
    ray.shutdown()


def demonstrate_manual_spmd_config():
    """Demonstrate manual XLA SPMD configuration."""
    print("\nDemonstrating manual XLA SPMD configuration...")
    
    from ray.train.torch import TorchConfig
    
    # Manual XLA SPMD configuration
    torch_config = TorchConfig(
        backend="xla_tpu",
        xla_spmd_config={
            "mesh_shape": [2, 2],  # 2x2 TPU mesh
            "data_parallel_size": 2,  # 2 data parallel shards
            "model_parallel_size": 2,  # 2 model parallel shards
        }
    )
    
    scaling_config = ScalingConfig(
        num_workers=4,
        use_tpu=True,
        resources_per_worker={"TPU": 1},
        topology="2x2",
        accelerator_type="TPU-V4"
    )
    
    print(f"Manual SPMD config: {torch_config.xla_spmd_config}")
    print("This configuration enables:")
    print("- 2D mesh partitioning across TPU cores")
    print("- Data parallelism for efficient batch processing")
    print("- Model parallelism for large model training")


if __name__ == "__main__":
    main()
    demonstrate_manual_spmd_config() 