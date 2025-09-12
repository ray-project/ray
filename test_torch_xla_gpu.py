#!/usr/bin/env python3
"""
Test script to demonstrate torch_xla backend with multiple L4 GPUs using TorchTrainer.
This script shows how to use the fixed TorchTPUConfig to properly assign different
XLA devices to different workers when training on multiple GPUs.
"""

import ray
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer, TorchConfig
import torch
import torch.nn as nn


def train_loop_per_worker(config):
    """Training loop that will run on each GPU worker with torch_xla backend."""
    import torch_xla.core.xla_model as xm
    
    worker_rank = ray.train.get_context().get_world_rank()
    print(f"Starting torch_xla training on worker {worker_rank}")
    
    # Create a simple model
    model = nn.Linear(10, 1)
    
    # Get XLA device - should now be different for each worker
    device = xm.xla_device()
    model = model.to(device)
    print(f"Worker {worker_rank}: Model moved to XLA device: {device}")
    
    # Verify device assignment
    expected_device = f"xla:{worker_rank}"
    if str(device) != expected_device:
        print(f"WARNING: Worker {worker_rank} got device {device}, expected {expected_device}")
    else:
        print(f"✓ Worker {worker_rank} correctly assigned to {device}")
    
    # Create dummy data
    x = torch.randn(100, 10).to(device)
    y = torch.randn(100, 1).to(device)
    
    # Training loop
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    criterion = nn.MSELoss()
    
    for epoch in range(3):
        optimizer.zero_grad()
        output = model(x)
        loss = criterion(output, y)
        loss.backward()
        optimizer.step()
        
        if epoch % 2 == 0:
            print(f"Worker {worker_rank}, Epoch {epoch}, Loss: {loss.item():.4f}")
    
    print(f"torch_xla training completed on worker {worker_rank}")


def main():
    """Main function to demonstrate torch_xla training with multiple GPUs."""
    # Initialize Ray
    ray.init()
    
    print("Starting torch_xla training with multiple L4 GPUs...")
    
    # Create scaling config for 2 L4 GPUs
    scaling_config = ScalingConfig(
        num_workers=2,  # 2 workers for 2 L4 GPUs
        use_gpu=True,   # Enable GPU training
        resources_per_worker={"GPU": 1}  # 1 GPU per worker
    )
    
    # Create torch config with xla_tpu backend for torch_xla
    torch_config = TorchConfig(
        backend="xla_tpu",  # Use torch_xla backend
        xla_spmd_config={
            "data_parallel_size": 2,  # 2 data parallel shards
        }
    )
    
    # Create run config
    run_config = RunConfig(name="torch_xla_gpu_training_demo")
    
    # Create trainer with torch_xla backend
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"epochs": 3},
        scaling_config=scaling_config,
        torch_config=torch_config,
        run_config=run_config
    )
    
    print("Starting torch_xla training...")
    result = trainer.fit()
    print("torch_xla training completed!")
    
    ray.shutdown()


if __name__ == "__main__":
    main()
