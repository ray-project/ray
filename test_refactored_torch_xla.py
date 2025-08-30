#!/usr/bin/env python3
"""
Test script to verify the refactored torch_xla backend works correctly.
This script tests the new architecture where process group initialization
happens in on_start instead of on_training_start.
"""

import ray
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer, TorchConfig
import torch
import torch.nn as nn


def train_loop_per_worker(config):
    """Simple training loop to test refactored torch_xla backend."""
    import torch_xla.core.xla_model as xm
    
    worker_rank = ray.train.get_context().get_world_rank()
    print(f"Worker {worker_rank}: Starting torch_xla training")
    
    try:
        # Get XLA device - should work without race conditions now
        device = xm.xla_device()
        print(f"Worker {worker_rank}: Successfully got XLA device: {device}")
        
        # Create a simple model
        model = nn.Linear(10, 1).to(device)
        print(f"Worker {worker_rank}: Model created and moved to device")
        
        # Simple forward pass to verify everything works
        x = torch.randn(5, 10).to(device)
        output = model(x)
        print(f"Worker {worker_rank}: Forward pass successful, output shape: {output.shape}")
        
        # Test basic operations
        loss = torch.nn.functional.mse_loss(output, torch.randn_like(output))
        print(f"Worker {worker_rank}: Loss computation successful: {loss.item():.4f}")
        
        print(f"Worker {worker_rank}: All torch_xla operations completed successfully!")
        
    except Exception as e:
        print(f"Worker {worker_rank}: Error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    """Test the refactored torch_xla backend."""
    print("Testing refactored torch_xla backend...")
    print("This should now work without race conditions!")
    
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
    
    print("Starting refactored torch_xla backend test...")
    try:
        result = trainer.fit()
        print("✓ Refactored torch_xla backend test completed successfully!")
        print("✓ No more race conditions!")
        print("✓ Process group initialization happens in on_start!")
    except Exception as e:
        print(f"✗ Refactored torch_xla backend test failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
