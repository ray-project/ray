#!/usr/bin/env python3
"""
Simple Mesh Testing Script (No Ray Train Required)

This script tests just the mesh creation and storage logic
without requiring Ray Train or any distributed setup.
"""

import os
import numpy as np

# Fix OpenMP library conflict on macOS
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"


class MockXlaMesh:
    """Mock XLA mesh class for testing."""
    
    def __init__(self, devices, shape, axes):
        self.devices = devices
        self.shape = shape
        self.axis_names = axes
    
    def __repr__(self):
        return f"MockXlaMesh(devices={len(self.devices)}, shape={self.shape}, axes={self.axis_names})"


class MockTrainContext:
    """Mock train context for testing mesh storage."""
    
    def __init__(self):
        self._xla_mesh = None
    
    def set_xla_mesh(self, mesh):
        """Set the XLA mesh."""
        self._xla_mesh = mesh
        print(f"Mesh stored in context: {mesh}")
    
    def get_xla_mesh(self):
        """Get the XLA mesh."""
        return self._xla_mesh


def mock_xla_mesh_creation():
    """Simulate XLA mesh creation logic."""
    print("Simulating XLA mesh creation...")
    
    # Simulate mesh creation logic from the real implementation
    # For a 4x4 TPU slice simulation:
    num_devices = 16  # 4 hosts * 4 TPU chips per host
    num_processes = 4  # 4 hosts
    local_devices = 4  # 4 TPU chips per host
    
    print(f"Device counts: total={num_devices}, processes={num_processes}, local={local_devices}")
    
    if num_processes * local_devices == num_devices:
        shape, axes = (num_processes, local_devices), ("data", "model")
        print("Creating 2D mesh: data + model parallelism")
    else:
        shape, axes = (num_devices,), ("data",)
        print("Creating 1D mesh: data parallelism only")
    
    # Create mock devices array
    devices = np.arange(num_devices)
    
    # Create mock mesh
    mesh = MockXlaMesh(devices, shape, axes)
    
    print(f"Mock XLA mesh created: shape={shape}, axes={axes}, devices={num_devices}")
    return mesh


def test_mesh_creation():
    """Test mesh creation functionality."""
    print("=" * 60)
    print("Testing Mesh Creation")
    print("=" * 60)
    
    # Test different scenarios
    scenarios = [
        {"total": 16, "processes": 4, "local": 4, "name": "4x4 TPU slice"},
        {"total": 8, "processes": 2, "local": 4, "name": "2x4 TPU slice"},
        {"total": 8, "processes": 1, "local": 8, "name": "Single host 8 TPUs"},
        {"total": 4, "processes": 1, "local": 4, "name": "Single host 4 TPUs"},
    ]
    
    for scenario in scenarios:
        print(f"\nTesting scenario: {scenario['name']}")
        print(f"Total devices: {scenario['total']}")
        print(f"Processes: {scenario['processes']}")
        print(f"Local devices: {scenario['local']}")
        
        # Simulate the mesh creation logic
        num_devices = scenario['total']
        num_processes = scenario['processes']
        local_devices = scenario['local']
        
        if num_processes * local_devices == num_devices:
            shape, axes = (num_processes, local_devices), ("data", "model")
            mesh_type = "2D mesh (data + model parallelism)"
        else:
            shape, axes = (num_devices,), ("data",)
            mesh_type = "1D mesh (data parallelism only)"
        
        devices = np.arange(num_devices)
        mesh = MockXlaMesh(devices, shape, axes)
        
        print(f"Result: {mesh_type}")
        print(f"Mesh: {mesh}")
        
        if len(shape) == 2:
            print(f"  Data parallelism: {shape[0]} ways")
            print(f"  Model parallelism: {shape[1]} ways")
        else:
            print(f"  Data parallelism: {shape[0]} ways")


def test_mesh_storage():
    """Test mesh storage and retrieval."""
    print("\n" + "=" * 60)
    print("Testing Mesh Storage and Retrieval")
    print("=" * 60)
    
    # Create mock context
    context = MockTrainContext()
    
    # Create and store mesh
    mesh = mock_xla_mesh_creation()
    context.set_xla_mesh(mesh)
    
    # Retrieve mesh
    retrieved_mesh = context.get_xla_mesh()
    
    if retrieved_mesh is not None:
        print(f"Successfully retrieved mesh: {retrieved_mesh}")
        print(f"Mesh shape: {retrieved_mesh.shape}")
        print(f"Mesh axes: {retrieved_mesh.axis_names}")
        print(f"Number of devices: {len(retrieved_mesh.devices)}")
    else:
        print("Failed to retrieve mesh!")


def test_spmd_simulation():
    """Test simulated SPMD operations."""
    print("\n" + "=" * 60)
    print("Testing Simulated SPMD Operations")
    print("=" * 60)
    
    # Create mesh
    mesh = mock_xla_mesh_creation()
    
    print(f"Using mesh: {mesh}")
    
    # Simulate different SPMD operations based on mesh topology
    if len(mesh.shape) == 2 and "data" in mesh.axis_names and "model" in mesh.axis_names:
        print("\n2D mesh detected: data + model parallelism")
        print("Simulating operations:")
        print("  1. Data parallelism across hosts")
        print("  2. Model parallelism across TPU chips")
        print("  3. Sharding strategy: xs.Shard(1) for model axis")
        
        # Simulate sharding
        print("\nSimulating model sharding:")
        print("  - Embedding layer: shard across model axis")
        print("  - Transformer blocks: shard across model axis")
        print("  - Output projection: shard across model axis")
        
    elif len(mesh.shape) == 1 and "data" in mesh.axis_names:
        print("\n1D mesh detected: data parallelism only")
        print("Simulating operations:")
        print("  1. Data parallelism across all devices")
        print("  2. Model replication (no sharding)")
        print("  3. Use DistributedDataParallel")
        
    else:
        print(f"\nUnknown mesh topology: {mesh.shape}, {mesh.axis_names}")
    
    print("\nSPMD simulation completed!")


if __name__ == "__main__":
    print("XLA SPMD Mesh Testing (No Ray Train Required)")
    print("=" * 60)
    print("This script tests mesh creation, storage, and SPMD simulation")
    print("without requiring Ray Train or distributed setup.")
    print()
    
    try:
        # Run all tests
        test_mesh_creation()
        test_mesh_storage()
        test_spmd_simulation()
        
        print("\n" + "=" * 60)
        print("All tests completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
