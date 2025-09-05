# TPU Support with XLA SPMD in Ray TorchTrainer

This document describes the new TPU support added to Ray's TorchTrainer, enabling **efficient distributed training on Google Cloud TPUs using the torch_xla backend with XLA SPMD (Single Program Multiple Data) execution**.

## Overview

Ray TorchTrainer now supports TPU training through the `ScalingConfig(use_tpu=True)` parameter. When enabled, the trainer automatically configures the torch_xla backend with **XLA SPMD execution**, which is essential for efficient data distributed parallel training on TPU devices.

**Key Innovation**: Unlike traditional distributed training approaches, XLA SPMD enables a single program to run across multiple TPU cores with automatic data parallelism, model parallelism, and mesh partitioning optimizations.

## Features

- **Automatic Backend Selection**: When `use_tpu=True` is set, TorchTrainer automatically configures the torch_xla backend
- **XLA SPMD Execution**: Enables Single Program Multiple Data execution for optimal TPU utilization
- **Automatic SPMD Configuration**: Automatically configures mesh partitioning, data parallelism, and model parallelism based on TPU topology
- **TPU Resource Management**: Proper allocation and management of TPU resources per worker
- **Distributed Training**: Support for multi-worker TPU training with SPMD execution
- **Environment Setup**: Automatic configuration of TPU-specific environment variables and XLA SPMD settings

## Why XLA SPMD for Data Distributed Training?

XLA SPMD is the recommended approach for TPU training because it:

1. **Eliminates Communication Overhead**: Single program runs across all TPU cores, reducing inter-process communication
2. **Automatic Optimization**: XLA automatically optimizes data flow, memory usage, and computation distribution
3. **Scalability**: Scales efficiently across TPU pods with minimal code changes
4. **Performance**: Achieves near-linear scaling for data parallel workloads
5. **Simplicity**: Single training script runs on all workers with automatic data sharding

## Requirements

- `torch_xla` package installed: `pip install torch_xla`
- Access to Google Cloud TPU resources
- Ray cluster with TPU nodes configured

## Seamless TPU Integration

**The key innovation of this implementation is that users can write training loops that work seamlessly on both GPU and TPU without any code changes!**

### How It Works

1. **Automatic Device Detection**: The system automatically detects whether you're running on TPU, GPU, or CPU
2. **Seamless API**: Use the same training code for all devices
3. **Automatic Optimization**: TPU-specific optimizations are applied automatically when available
4. **Fallback Support**: Gracefully falls back to standard PyTorch operations when TPU is not available

### Writing Device-Agnostic Training Loops

#### Option 1: Using SeamlessTPUTrainer Class

```python
from ray.train.torch.seamless_tpu import SeamlessTPUTrainer

def train_loop_per_worker(config):
    # Create model and optimizer
    model = YourModel()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    # Create seamless trainer - automatically handles device placement
    trainer = SeamlessTPUTrainer(model)
    
    # Create data
    x = torch.randn(100, 10)
    y = torch.randn(100, 1)
    
    # Move data to device automatically
    x, y = trainer.to_device(x, y)
    
    # Training loop - works the same on GPU and TPU!
    for epoch in range(config["epochs"]):
        optimizer.zero_grad()
        output = model(x)
        loss = criterion(output, y)
        loss.backward()
        
        # Regular PyTorch calls - automatically optimized for TPU when available
        optimizer.step()
        
        # No manual TPU calls needed!
```

#### Option 2: Using Functional Approach

```python
from ray.train.torch.seamless_tpu import to_device

def train_loop_per_worker(config):
    # Create model and optimizer
    model = YourModel()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    # Create data
    x = torch.randn(100, 10)
    y = torch.randn(100, 1)
    
    # Move data to device automatically
    x, y = to_device(x, y)
    model = model.to(x.device)
    
    # Training loop - works the same on GPU and TPU!
    for epoch in range(config["epochs"]):
        optimizer.zero_grad()
        output = model(x)
        loss = criterion(output, y)
        loss.backward()
        
        # Regular PyTorch calls - automatically optimized for TPU when available
        optimizer.step()
        
        # No manual TPU calls needed!
```

### What Happens Automatically

When you use `ScalingConfig(use_tpu=True)`:

1. **Device Placement**: Model and data are automatically moved to TPU
2. **XLA SPMD**: XLA SPMD environment is automatically configured
3. **Automatic Optimization**: Regular PyTorch calls are automatically optimized for TPU
4. **XLA Compilation**: XLA graph optimization is enabled automatically on TPU
5. **No Manual Calls**: Users don't need to call `mark_step()`, `xm.optimizer_step()`, etc.

### Benefits of Seamless Integration

- **Zero Code Changes**: Same training loop works on GPU and TPU
- **No Manual Calls**: Users don't need to learn TPU-specific APIs
- **Automatic Optimization**: TPU-specific optimizations are applied automatically
- **Easy Migration**: Switch from GPU to TPU by changing only the ScalingConfig
- **Maintainability**: Single codebase for all device types
- **Performance**: Optimal performance on each device type without manual tuning

### Example: GPU vs TPU Training

```python
# GPU Training
scaling_config_gpu = ScalingConfig(
    num_workers=4,
    use_gpu=True
)

# TPU Training - SAME training loop!
scaling_config_tpu = ScalingConfig(
    num_workers=4,
    use_tpu=True,
    topology="2x2",
    accelerator_type="TPU-V4"
)

# Use the exact same training function
trainer_gpu = TorchTrainer(
    train_loop_per_worker=your_training_function,  # No changes needed!
    scaling_config=scaling_config_gpu
)

trainer_tpu = TorchTrainer(
    train_loop_per_worker=your_training_function,  # No changes needed!
    scaling_config=scaling_config_tpu
)
```

## Usage

### Basic TPU Training with XLA SPMD

```python
from ray.train import ScalingConfig, RunConfig
from ray.train.torch import TorchTrainer

# Enable TPU training with automatic XLA SPMD configuration
scaling_config = ScalingConfig(
    num_workers=2,
    use_tpu=True,  # This enables torch_xla backend with XLA SPMD
    resources_per_worker={"TPU": 1}
)

trainer = TorchTrainer(
    train_loop_per_worker=your_training_function,
    scaling_config=scaling_config,
    run_config=RunConfig(name="tpu_training")
)

result = trainer.fit()
```

**What happens automatically:**
- TorchTrainer sets `backend="xla_tpu"`
- XLA SPMD environment variables are configured
- Mesh partitioning is automatically determined
- Data parallelism is enabled across TPU cores

### Multi-Worker TPU Training with Advanced SPMD

For multi-worker TPU training, XLA SPMD automatically optimizes the configuration:

```python
scaling_config = ScalingConfig(
    num_workers=4,
    use_tpu=True,
    resources_per_worker={"TPU": 1},
    topology="2x2",  # TPU topology - auto-configures 2x2 mesh
    accelerator_type="TPU-V4",  # TPU accelerator type
    placement_strategy="SPREAD"  # Ensures workers are on separate TPU VMs
)
```

**Automatic XLA SPMD Configuration:**
- **Mesh Shape**: `[2, 2]` (derived from topology "2x2")
- **Data Parallel Size**: `4` (number of workers)
- **Model Parallel Size**: `2` (second dimension of mesh)
- **Environment Variables**: All XLA SPMD optimizations enabled

### Manual XLA SPMD Configuration

For advanced users, you can manually configure XLA SPMD:

```python
from ray.train.torch import TorchConfig

torch_config = TorchConfig(
    backend="xla_tpu",
    xla_spmd_config={
        "mesh_shape": [2, 2],  # 2x2 TPU mesh
        "data_parallel_size": 2,  # 2 data parallel shards
        "model_parallel_size": 2,  # 2 model parallel shards
    }
)

trainer = TorchTrainer(
    train_loop_per_worker=your_training_function,
    torch_config=torch_config,
    scaling_config=scaling_config
)
```

### Training Function with XLA SPMD

Your training function automatically benefits from XLA SPMD:

```python
def train_loop_per_worker(config):
    import torch_xla.core.xla_model as xm
    
    # Get TPU device - XLA SPMD handles placement automatically
    device = xm.xla_device()
    
    # Create model and move to TPU
    model = YourModel().to(device)
    
    # XLA SPMD automatically handles:
    # - Data parallelism across TPU cores
    # - Gradient synchronization
    # - Memory optimization
    # - Computation distribution
    
    # Training loop
    for epoch in range(config["epochs"]):
        # Your training code here - XLA SPMD optimizes everything
        pass
```

## Configuration Details

### ScalingConfig Parameters

- `use_tpu`: Set to `True` to enable TPU training with XLA SPMD
- `num_workers`: Number of TPU workers (should match TPU slice configuration)
- `resources_per_worker`: Must include `{"TPU": N}` where N is the number of TPU chips per worker
- `topology`: TPU topology (e.g., "2x2", "4x4") - automatically configures XLA SPMD mesh
- `accelerator_type`: TPU accelerator type (e.g., "TPU-V4", "TPU-V5") - required for multi-worker training
- `placement_strategy`: Should be "SPREAD" for TPU training to ensure workers are on separate VMs

### TorchConfig with XLA SPMD

The `torch_config` parameter is automatically configured when `use_tpu=True`:

```python
# This is automatically set when use_tpu=True
torch_config = TorchConfig(
    backend="xla_tpu",
    xla_spmd_config={
        "mesh_shape": [2, 2],  # Auto-configured from topology
        "data_parallel_size": 4,  # Auto-configured from num_workers
        "model_parallel_size": 2,  # Auto-configured from mesh shape
    }
)
```

### XLA SPMD Configuration Options

The `xla_spmd_config` parameter supports the following options:

- **mesh_shape**: List defining the TPU mesh dimensions (e.g., `[2, 2]` for 2x2 mesh)
- **data_parallel_size**: Number of data parallel shards
- **model_parallel_size**: Number of model parallel shards
- **partitioning_mode**: SPMD partitioning strategy (defaults to "auto")

### Automatic SPMD Configuration

When `use_tpu=True` is set, TorchTrainer automatically:

1. **Parses TPU Topology**: Converts topology string (e.g., "2x2") to mesh shape `[2, 2]`
2. **Configures Data Parallelism**: Sets data parallel size based on number of workers
3. **Configures Model Parallelism**: Sets model parallel size based on mesh dimensions
4. **Enables SPMD Optimizations**: Sets all necessary XLA environment variables
5. **Optimizes Mesh Partitioning**: Automatically determines optimal data and model parallel configurations

## Backend Implementation

The TPU backend (`_TorchTPUBackend`) with XLA SPMD handles:

1. **Environment Setup**: Configures TPU-specific environment variables
2. **XLA SPMD Configuration**: Sets up XLA SPMD environment variables and mesh partitioning
3. **Process Group Initialization**: Initializes torch_xla distributed process group with SPMD support
4. **Automatic SPMD Setup**: Configures mesh shape, data parallelism, and model parallelism
5. **Resource Cleanup**: Proper cleanup of TPU resources

### XLA SPMD Backend Features

- **Automatic Mesh Partitioning**: Converts TPU topology to optimal mesh configuration
- **Data Parallelism**: Automatically configures data parallel execution across TPU cores
- **Model Parallelism**: Sets up model parallel execution when mesh dimensions allow
- **Environment Optimization**: Enables all XLA SPMD optimizations and TPU-specific settings
- **Error Handling**: Provides clear error messages for missing dependencies or misconfiguration

### How XLA SPMD Works

1. **Single Program**: One training script runs across all TPU cores
2. **Automatic Data Sharding**: XLA automatically distributes data across cores
3. **Gradient Synchronization**: Gradients are automatically synchronized across the mesh
4. **Memory Optimization**: XLA optimizes memory usage and computation distribution
5. **Performance Scaling**: Achieves near-linear scaling for data parallel workloads

## Error Handling

- If `torch_xla` is not installed, a clear error message is provided
- Validation ensures TPU resources are properly configured
- Multi-worker TPU training requires topology and accelerator_type specification
- XLA SPMD configuration errors are caught and logged with helpful messages

## Examples

See `test_torch_tpu.py` for a complete working example of TPU training with XLA SPMD in TorchTrainer.

## Limitations

- Currently supports Google Cloud TPUs via torch_xla
- Requires torch_xla package installation
- Multi-worker training requires specific TPU slice configuration
- TPU training is experimental and may have limitations compared to GPU training
- XLA SPMD is optimized for TPU architectures and may not provide the same benefits on other accelerators

## Troubleshooting

### Common Issues

1. **torch_xla not found**: Install with `pip install torch_xla`
2. **TPU resources not available**: Ensure your Ray cluster has TPU nodes configured
3. **Multi-worker configuration errors**: Verify topology and accelerator_type are set correctly
4. **XLA SPMD configuration issues**: Check that topology string follows the "NxM" format

### Debug Information

Enable debug logging to see detailed TPU and XLA SPMD setup information:

```python
import logging
logging.getLogger("ray.train.torch").setLevel(logging.DEBUG)
```

### XLA SPMD Debugging

To debug XLA SPMD configuration:

```python
# Check environment variables
import os
print(f"XLA_USE_SPMD: {os.environ.get('XLA_USE_SPMD')}")
print(f"XLA_MESH_SHAPE: {os.environ.get('XLA_MESH_SHAPE')}")
print(f"XLA_DATA_PARALLEL_SIZE: {os.environ.get('XLA_DATA_PARALLEL_SIZE')}")
```

## Future Enhancements

- Support for additional TPU backends
- Enhanced XLA SPMD optimizations and mesh partitioning strategies
- Integration with TPU profiling tools
- Support for TPU-specific data loading optimizations
- Advanced SPMD configurations for complex model parallelism scenarios
- Integration with Ray's distributed data processing for optimal TPU utilization