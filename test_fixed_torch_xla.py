#!/usr/bin/env python3
"""
Test script to verify the fixed torch_xla backend works correctly.
This script tests that the backend can initialize without CUDA_VISIBLE_DEVICES conflicts.
"""

import ray
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer, TorchConfig
import torch
import torch.nn as nn


def train_loop_per_worker(config):
    """Simple training loop to test torch_xla backend initialization."""
    import torch_xla.core.xla_model as xm
    
    worker_rank = ray.train.get_context().get_world_rank()
    print(f"Worker {worker_rank}: Starting torch_xla training")
    
    try:
        # Get XLA device - should work without CUDA_VISIBLE_DEVICES conflicts
        device = xm.xla_device()
        print(f"Worker {worker_rank}: Successfully got XLA device: {device}")
        
        # Create a simple model
        model = nn.Linear(10, 1).to(device)
        print(f"Worker {worker_rank}: Model created and moved to device")
        
        # Simple forward pass to verify everything works
        x = torch.randn(5, 10).to(device)
        output = model(x)
        print(f"Worker {worker_rank}: Forward pass successful, output shape: {output.shape}")
        
        print(f"Worker {worker_rank}: All torch_xla operations completed successfully!")
        
    except Exception as e:
        print(f"Worker {worker_rank}: Error occurred: {e}")
        raise


def main():
    """Test the fixed torch_xla backend."""
    print("Testing fixed torch_xla backend...")
    
    # Initialize Ray
    ray.init()
    
    # Create scaling config for 2 workers
    scaling_config = ScalingConfig(
        num_workers=2,
        use_gpu=True,
        resources_per_worker={"GPU": 1}
    )
    
    # Create torch config with xla_tpu backend
    torch_config = TorchConfig(
        backend="xla_tpu",
        xla_spmd_config={
            "data_parallel_size": 2,
        }
    )
    
    # Create trainer
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        scaling_config=scaling_config,
        torch_config=torch_config
    )
    
    print("Starting torch_xla backend test...")
    try:
        result = trainer.fit()
        print("✓ torch_xla backend test completed successfully!")
    except Exception as e:
        print(f"✗ torch_xla backend test failed: {e}")
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
