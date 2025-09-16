# XLA SPMD Mesh Integration with Ray Train TorchTrainer

This document describes the integration of XLA SPMD (Single Program, Multiple Data) mesh functionality with Ray Train TorchTrainer, enabling seamless TPU training with automatic mesh creation and exposure to user training loops.

## Overview

The XLA SPMD mesh integration provides:

1. **Automatic Mesh Creation**: Automatically creates an XLA SPMD mesh based on your TPU topology
2. **Mesh Exposure**: Provides easy access to the mesh through `ray.train.torch.xla.get_xla_mesh()`
3. **Seamless Integration**: Works with existing Ray Train workflows without code changes
4. **TPU Optimization**: Optimized for TPU training with proper sharding strategies

## Features

### Automatic Mesh Configuration

For a 4x4 TPU slice configuration:
- **4 workers** (4 hosts)
- **4 TPU chips per host**
- **Total: 16 TPU devices**

The system automatically creates a mesh with:
- **Shape**: `(4, 4)`
- **Axes**: `("data", "model")`
- **Data Parallelism**: 4-way across hosts
- **Model Parallelism**: 4-way across TPU chips

### Easy Mesh Access

```python
import ray.train as train

def train_loop_per_worker(config):
    # Get the train context
    context = train.get_context()
    
    # Check if XLA backend is active
    from ray.train.torch.xla import is_xla_backend
    if is_xla_backend():
        # Get the automatically created mesh from train context
        mesh = context.get_xla_mesh()
        
        if mesh is not None:
            print(f"Mesh shape: {mesh.shape}")
            print(f"Mesh axes: {mesh.axis_names}")
            print(f"Number of devices: {len(mesh.devices)}")
            
            # Use the mesh for SPMD operations
            # ... your training code here
```

## API Reference

### `train.get_context().get_xla_mesh()`

Returns the XLA SPMD mesh created during worker bootstrap.

**Returns:**
- `torch_xla.distributed.spmd.Mesh` if XLA backend is active
- `None` if XLA backend is not being used

**Raises:**
- `RuntimeError` if called outside of a Ray Train training context

### `ray.train.torch.xla.is_xla_backend()`

Checks if the current training is using XLA backend.

**Returns:**
- `True` if XLA backend is active
- `False` otherwise

## Usage Examples

### Basic Usage

```python
import ray
from ray import train
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer, TorchConfig

def train_loop_per_worker(config):
    # Get the train context
    context = train.get_context()
    
    # Get the XLA mesh from train context
    mesh = context.get_xla_mesh()
    if mesh is not None:
        print(f"Using XLA mesh: {mesh.shape}, {mesh.axis_names}")
        
        # Your training code here
        # Use mesh for sharding operations
        pass

# Configure for TPU training
torch_config = TorchConfig(backend="xla")
scaling_config = ScalingConfig(num_workers=4, use_gpu=False)

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    torch_config=torch_config,
    scaling_config=scaling_config,
)

result = trainer.fit()
```

### Advanced SPMD Usage

```python
def train_loop_per_worker(config):
    import ray.train as train
    import torch_xla.distributed.spmd as xs
    
    # Get the train context
    context = train.get_context()
    
    # Get the XLA mesh from train context
    mesh = context.get_xla_mesh()
    if mesh is None:
        raise RuntimeError("XLA mesh not available")
    
    # Create your model
    model = MyModel()
    
    # Shard the model across the mesh
    if len(mesh.shape) == 2 and "data" in mesh.axis_names:
        # 2D mesh: data + model parallelism
        model = xs.mark_sharding(model, mesh, xs.Shard(1))
    else:
        # 1D mesh: data parallelism only
        pass
    
    # Use the mesh for training
    with xs.Mesh(mesh.devices, mesh.shape, mesh.axis_names):
        # Your training loop here
        pass
```

## Configuration

### TorchConfig for XLA

```python
torch_config = TorchConfig(
    backend="xla",           # Use XLA backend
    timeout_s=1800,          # 30 minutes timeout
)
```

### ScalingConfig for TPU

```python
scaling_config = ScalingConfig(
    num_workers=4,          # Number of hosts
    use_gpu=False,          # Using TPU, not GPU
)
```

## Mesh Topology Examples

### 4x4 TPU Slice
- **Workers**: 4 (one per host)
- **TPU chips per host**: 4
- **Total devices**: 16
- **Mesh shape**: `(4, 4)`
- **Mesh axes**: `("data", "model")`
- **Data parallelism**: 4-way
- **Model parallelism**: 4-way

### 2x8 TPU Slice
- **Workers**: 2 (one per host)
- **TPU chips per host**: 8
- **Total devices**: 16
- **Mesh shape**: `(2, 8)`
- **Mesh axes**: `("data", "model")`
- **Data parallelism**: 2-way
- **Model parallelism**: 8-way

### Single Host (8 TPU chips)
- **Workers**: 1
- **TPU chips**: 8
- **Total devices**: 8
- **Mesh shape**: `(8,)`
- **Mesh axes**: `("data",)`
- **Data parallelism**: 8-way

## Best Practices

1. **Check XLA Backend**: Always check if XLA backend is active before using mesh functions
2. **Handle Mesh Availability**: Check if mesh is available before using it
3. **Use Appropriate Sharding**: Choose sharding strategies based on your mesh topology
4. **Error Handling**: Implement proper error handling for TPU-specific operations
5. **Performance**: Use the mesh for both data and model parallelism when possible

## Troubleshooting

### Common Issues

1. **Mesh Not Available**: Ensure you're using `TorchConfig(backend="xla")`
2. **Import Errors**: Make sure `torch_xla` is installed
3. **TPU Configuration**: Verify your TPU slice configuration matches your scaling config

### Debug Information

```python
def train_loop_per_worker(config):
    import ray.train.torch.xla as xla_utils
    
    print(f"XLA backend active: {xla_utils.is_xla_backend()}")
    
    mesh = xla_utils.get_xla_mesh()
    if mesh is not None:
        print(f"Mesh details:")
        print(f"  Shape: {mesh.shape}")
        print(f"  Axes: {mesh.axis_names}")
        print(f"  Devices: {len(mesh.devices)}")
        print(f"  Device IDs: {mesh.devices}")
```

## Implementation Details

The mesh integration works by:

1. **Worker Bootstrap**: During XLA backend initialization, a mesh is created based on the TPU topology
2. **Global Storage**: The mesh is stored in a global variable accessible to all workers
3. **API Exposure**: The `get_xla_mesh()` function provides easy access to the stored mesh
4. **Automatic Configuration**: Mesh shape and axes are automatically determined based on available devices

This design ensures that users can easily access the mesh without needing to understand the underlying TPU topology or mesh creation logic.
