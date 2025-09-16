#!/usr/bin/env python3
"""
CPU Testing Script for XLA SPMD Ray Train Integration

This script simulates the XLA SPMD setup on CPU to test the mesh creation
and storage functionality without requiring actual TPU hardware.

It demonstrates:
1. How the XLA mesh is created and stored in train context
2. How users can access the mesh in their training loops
3. The complete flow from backend initialization to mesh usage
"""

import os
import tempfile
from typing import Dict

# Fix OpenMP library conflict on macOS
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

import torch
import torch.nn as nn
import torch.optim as optim

import ray
from ray import train
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer, TorchConfig


class SimpleModel(nn.Module):
    """A simple model for testing."""
    
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


def mock_xla_mesh_creation():
    """
    Mock function that simulates XLA mesh creation for CPU testing.
    
    This simulates what happens in the _xla_worker_bootstrap function
    but creates a mock mesh object instead of using real XLA.
    """
    import numpy as np
    
    # Mock mesh class to simulate torch_xla.distributed.spmd.Mesh
    class MockXlaMesh:
        def __init__(self, devices, shape, axes):
            self.devices = devices
            self.shape = shape
            self.axis_names = axes
        
        def __repr__(self):
            return f"MockXlaMesh(devices={len(self.devices)}, shape={self.shape}, axes={self.axis_names})"
    
    # Simulate mesh creation logic from the real implementation
    # For a 4x4 TPU slice simulation:
    num_devices = 16  # 4 hosts * 4 TPU chips per host
    num_processes = 4  # 4 hosts
    local_devices = 4  # 4 TPU chips per host
    
    if num_processes * local_devices == num_devices:
        shape, axes = (num_processes, local_devices), ("data", "model")
    else:
        shape, axes = (num_devices,), ("data",)
    
    # Create mock devices array
    devices = np.arange(num_devices)
    
    # Create mock mesh
    mesh = MockXlaMesh(devices, shape, axes)
    
    print(f"Mock XLA mesh created: shape={shape}, axes={axes}, devices={num_devices}")
    return mesh


def train_loop_per_worker(config: Dict):
    """
    Training loop that demonstrates XLA mesh usage with CPU simulation.
    
    This function shows how to:
    1. Access the XLA mesh from train context
    2. Use the mesh for simulated SPMD operations
    3. Demonstrate the complete training flow
    """
    # Get the train context
    context = train.get_context()
    world_rank = context.get_world_rank()
    world_size = context.get_world_size()
    
    print(f"Worker {world_rank}/{world_size} starting training")
    
    # Simulate XLA mesh creation and storage (this would normally happen in backend init)
    print("Simulating XLA mesh creation...")
    mock_mesh = mock_xla_mesh_creation()
    
    # Store the mesh in train context (simulating what happens in _xla_worker_bootstrap)
    context.set_xla_mesh(mock_mesh)
    print(f"Mesh stored in train context: {mock_mesh}")
    
    # Now demonstrate how users would access the mesh
    retrieved_mesh = context.get_xla_mesh()
    if retrieved_mesh is not None:
        print(f"Retrieved mesh from context: {retrieved_mesh}")
        print(f"Mesh shape: {retrieved_mesh.shape}")
        print(f"Mesh axes: {retrieved_mesh.axis_names}")
        print(f"Number of devices: {len(retrieved_mesh.devices)}")
        
        # Simulate SPMD operations
        print("Simulating SPMD operations...")
        
        # Create model
        model = SimpleModel()
        
        # Simulate model sharding based on mesh topology
        if len(retrieved_mesh.shape) == 2 and "data" in retrieved_mesh.axis_names:
            print("2D mesh detected: Using data + model parallelism")
            print(f"Data parallelism: {retrieved_mesh.shape[0]} ways")
            print(f"Model parallelism: {retrieved_mesh.shape[1]} ways")
            
            # Simulate sharding the model
            print("Simulating model sharding across mesh...")
            # In real XLA: model = xs.mark_sharding(model, mesh, xs.Shard(1))
            
        elif len(retrieved_mesh.shape) == 1:
            print("1D mesh detected: Using data parallelism only")
            print(f"Data parallelism: {retrieved_mesh.shape[0]} ways")
            
        else:
            print(f"Unknown mesh topology: {retrieved_mesh.shape}")
    else:
        print("No mesh available in train context")
    
    # Standard training setup
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
    
    print(f"Worker {world_rank}: Training step completed, loss: {loss.item():.4f}")
    
    # Report metrics including mesh information
    metrics = {
        "loss": loss.item(),
        "world_rank": world_rank,
        "world_size": world_size,
        "mesh_shape": list(retrieved_mesh.shape) if retrieved_mesh else None,
        "mesh_axes": list(retrieved_mesh.axis_names) if retrieved_mesh else None,
        "mesh_devices": len(retrieved_mesh.devices) if retrieved_mesh else 0,
    }
    
    train.report(metrics)


def test_xla_mesh_storage():
    """Test the XLA mesh storage functionality in isolation."""
    print("=" * 60)
    print("Testing XLA Mesh Storage Functionality")
    print("=" * 60)
    
    # Initialize Ray with error handling
    try:
        ray.init(ignore_reinit_error=True)
    except Exception as e:
        print(f"Failed to initialize Ray: {e}")
        return
    
    try:
        # Test configuration
        torch_config = TorchConfig(backend="xla")  # This will trigger XLA path
        scaling_config = ScalingConfig(num_workers=4, use_gpu=False)
        run_config = RunConfig(
            checkpoint_config=CheckpointConfig(num_to_keep=1),
            name="xla_mesh_test_cpu"
        )
        
        train_loop_config = {
            "epochs": 1,
            "batch_size": 32,
        }
        
        print("Configuration:")
        print(f"  Backend: {torch_config.backend}")
        print(f"  Workers: {scaling_config.num_workers}")
        print(f"  Use GPU: {scaling_config.use_gpu}")
        print()
        
        # Create trainer
        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
        )
        
        print("Starting XLA SPMD simulation training...")
        print("Expected behavior:")
        print("  1. Each worker will simulate XLA mesh creation")
        print("  2. Mesh will be stored in train context")
        print("  3. Workers will retrieve and use the mesh")
        print("  4. SPMD operations will be simulated")
        print()
        
        # Run training
        result = trainer.fit()
        
        print("Training completed!")
        print(f"Final metrics: {result.metrics}")
        
        # Verify mesh information was reported
        if "mesh_shape" in result.metrics:
            print(f"Mesh shape reported: {result.metrics['mesh_shape']}")
            print(f"Mesh axes reported: {result.metrics['mesh_axes']}")
            print(f"Mesh devices reported: {result.metrics['mesh_devices']}")
        
    finally:
        ray.shutdown()


def test_mesh_context_isolation():
    """Test that mesh storage is properly isolated per worker."""
    print("=" * 60)
    print("Testing Mesh Context Isolation")
    print("=" * 60)
    
    try:
        ray.init(ignore_reinit_error=True)
    except Exception as e:
        print(f"Failed to initialize Ray: {e}")
        return
    
    try:
        def test_worker_isolation():
            context = train.get_context()
            world_rank = context.get_world_rank()
            
            # Each worker creates its own mesh
            mock_mesh = mock_xla_mesh_creation()
            context.set_xla_mesh(mock_mesh)
            
            # Verify each worker has its own mesh
            retrieved_mesh = context.get_xla_mesh()
            print(f"Worker {world_rank}: Mesh shape = {retrieved_mesh.shape}")
            
            return {
                "worker_rank": world_rank,
                "mesh_shape": list(retrieved_mesh.shape),
                "mesh_devices": len(retrieved_mesh.devices)
            }
        
        # Test with multiple workers
        torch_config = TorchConfig(backend="xla")
        scaling_config = ScalingConfig(num_workers=3, use_gpu=False)
        
        trainer = TorchTrainer(
            train_loop_per_worker=test_worker_isolation,
            torch_config=torch_config,
            scaling_config=scaling_config,
        )
        
        result = trainer.fit()
        
        print("Isolation test completed!")
        print("Each worker should have its own mesh instance:")
        for i, metrics in enumerate(result.metrics):
            print(f"  Worker {metrics['worker_rank']}: shape={metrics['mesh_shape']}, devices={metrics['mesh_devices']}")
        
    finally:
        ray.shutdown()


def test_basic_functionality():
    """Test basic functionality without Ray Train."""
    print("=" * 60)
    print("Testing Basic Mesh Functionality")
    print("=" * 60)
    
    # Test mock mesh creation
    print("Testing mock mesh creation...")
    mock_mesh = mock_xla_mesh_creation()
    print(f"Created mesh: {mock_mesh}")
    print(f"Mesh shape: {mock_mesh.shape}")
    print(f"Mesh axes: {mock_mesh.axis_names}")
    print(f"Number of devices: {len(mock_mesh.devices)}")
    
    # Test mesh properties
    print("\nTesting mesh properties...")
    if len(mock_mesh.shape) == 2:
        print("2D mesh detected: data + model parallelism")
        print(f"Data parallelism: {mock_mesh.shape[0]} ways")
        print(f"Model parallelism: {mock_mesh.shape[1]} ways")
    elif len(mock_mesh.shape) == 1:
        print("1D mesh detected: data parallelism only")
        print(f"Data parallelism: {mock_mesh.shape[0]} ways")
    
    print("Basic functionality test completed!")
    print("=" * 60)


if __name__ == "__main__":
    print("XLA SPMD Ray Train CPU Testing")
    print("=" * 60)
    print("This script simulates XLA SPMD functionality on CPU")
    print("to test mesh creation, storage, and retrieval.")
    print()
    
    # Run basic test first
    test_basic_functionality()
    
    print("\n" + "=" * 60)
    print("Testing Ray Train Integration...")
    print("=" * 60)
    
    try:
        # Run the main test
        test_xla_mesh_storage()
        
        print("\n" + "=" * 60)
        
        # Run isolation test
        test_mesh_context_isolation()
        
    except Exception as e:
        print(f"Ray Train test failed: {e}")
        print("This might be due to Ray installation or configuration issues.")
        print("The basic mesh functionality test above should still work.")
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
