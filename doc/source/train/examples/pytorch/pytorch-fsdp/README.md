# Get started with PyTorch Fully Sharded Data Parallel (FSDP2) and Ray Train

**Time to complete:** 30 min

This template shows how to get memory and performance improvements of integrating PyTorch's Fully Sharded Data Parallel with Ray Train. 

PyTorch's FSDP2 enables model sharding across nodes, allowing distributed training of large models with a significantly smaller memory footprint compared to standard Distributed Data Parallel (DDP). For a more detailed overview of FSDP2, see [PyTorch's official documentation](https://docs.pytorch.org/tutorials/intermediate/FSDP_tutorial.html#getting-started-with-fully-sharded-data-parallel-fsdp2). 

This tutorial provides a comprehensive, step-by-step guide on integrating PyTorch FSDP2 with Ray Train. Specifically, this guide covers the following: 

- A hands-on example of training an image classification model
- Configuring FSDP2 to mitigate out-of-memory (OOM) errors using mixed precision, CPU offloading, sharding granularity, and more
- Model checkpoint saving and loading with PyTorch Distributed Checkpoint (DCP)
- GPU memory profiling with PyTorch Profiler
- Loading a distributed model for inference

**Note:** This notebook uses FSDP2's `fully_sharded` API. If you're using FSDP1's `FullyShardedDataParallel`, consider migrating to FSDP2 for improved performance and features such as lower memory usage and `DTensor` integration. 

<div id="anyscale-note" class="alert alert-block alert-warning">

  <strong>Anyscale Specific Configuration</strong>

  <p><strong>Note:</strong> This tutorial is optimized for the Anyscale platform. When running on open source Ray, additional configuration is required. For example, you would need to manually:</p>

  <ul>
    <li><strong>Configure your Ray Cluster</strong>: Set up your multi-node environment and manage resource allocation without Anyscale's automation.</li>
    <li><strong>Manage Dependencies</strong>: Manually install and manage dependencies on each node.</li>
    <li><strong>Set Up Storage</strong>: Configure your own distributed or shared storage system for model checkpointing.</li>
  </ul>
</div>

<style>
  div#anyscale-note > p,
  div#anyscale-note > ul,
  div#anyscale-note > ul li {
    color: black;
  }

  div#anyscale-note {
    background-color: rgb(255, 243, 205);
  }

  div#anyscale-note {
    border: 1px solid #ccc; 
    border-radius: 8px;
    padding: 15px;
  }

</style>

## Example overview

For demonstration purposes, this tutorial integrates Ray Train with FSDP2 using a **Vision Transformer (ViT)** trained on the FashionMNIST dataset. ViT was chosen because it has clear, repeatable block structures (transformer blocks) that are ideal for demonstrating FSDP2's sharding capabilities. 

While this example is relatively simple, FSDP's complexity can lead to common challenges during training, such as out-of-memory (OOM) errors. This guide addresses common issues by providing practical tips for improving performance and reducing memory utilization based on your specific use case. 

## 1. Package and model setup

Install the required dependencies for this tutorial:


```bash
%%bash
pip install torch
pip install torchvision
pip install matplotlib
```


```python
# Enable Ray Train V2 for the latest train APIs
import os
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Profiling and utilities
import torch.profiler
import tempfile
import uuid
import logging

# Set up logging
logger = logging.getLogger(__name__)
```

### Model definition
The following function initializes a Vision Transformer (ViT) model configured for the FashionMNIST dataset:


```python
# Computer vision components
from torchvision.models import VisionTransformer
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose

def init_model() -> torch.nn.Module:
    """Initialize a Vision Transformer model for FashionMNIST classification.
    
    Returns:
        torch.nn.Module: Configured ViT model
    """
    logger.info("Initializing Vision Transformer model...")

    # Create a ViT model with architecture suitable for 28x28 images
    model = VisionTransformer(
        image_size=28,        # FashionMNIST image size
        patch_size=7,         # Divide 28x28 into 4x4 patches of 7x7 pixels each
        num_layers=10,        # Number of transformer encoder layers
        num_heads=2,          # Number of attention heads per layer
        hidden_dim=128,       # Hidden dimension size
        mlp_dim=128,          # MLP dimension in transformer blocks
        num_classes=10,       # FashionMNIST has 10 classes
    )

    # Modify the patch embedding layer for grayscale images (1 channel instead of 3)
    model.conv_proj = torch.nn.Conv2d(
        in_channels=1,        # FashionMNIST is grayscale (1 channel)
        out_channels=128,     # Match the hidden_dim
        kernel_size=7,        # Match patch_size
        stride=7,             # Non-overlapping patches
    )

    return model
```

## 2. Define the training function

Below is the main training function that orchestrates the FSDP2 training process. The following sections implement each of the helper functions used in this training loop. First, make the necessary imports for the training function:


```python
# Ray Train imports
import ray
import ray.train
import ray.train.torch

# PyTorch Core import
import torch

# PyTorch training components
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torch.utils.data import DataLoader
```


```python
def train_func(config):
    """Main training function that integrates FSDP2 with Ray Train.
    
    Args:
        config: Training configuration dictionary containing hyperparameters
    """
    # Initialize the model
    model = init_model()

    # Configure device and move model to GPU
    device = ray.train.torch.get_device()
    torch.cuda.set_device(device)
    model.to(device)

    # Apply FSDP2 sharding to the model
    shard_model(model)

    # Initialize loss function and optimizer
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=config.get('learning_rate', 0.001))

    # Load from checkpoint if available (for resuming training)
    loaded_checkpoint = ray.train.get_checkpoint()
    if loaded_checkpoint:
        load_fsdp_checkpoint(model, optimizer, loaded_checkpoint)

    # Prepare training data
    transform = Compose([
        ToTensor(), 
        Normalize((0.5,), (0.5,))
    ])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(
        root=data_dir, train=True, download=True, transform=transform
    )
    train_loader = DataLoader(
        train_data, 
        batch_size=config.get('batch_size', 64), 
        shuffle=True
    )
    # Prepare data loader for distributed training
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    world_rank = ray.train.get_context().get_world_rank()

    # Set up PyTorch Profiler for memory monitoring
    with torch.profiler.profile(
       activities=[
           torch.profiler.ProfilerActivity.CPU,
           torch.profiler.ProfilerActivity.CUDA,
       ],
       schedule=torch.profiler.schedule(wait=0, warmup=0, active=6, repeat=1),
       record_shapes=True,
       profile_memory=True,
       with_stack=True,
   ) as prof:

        # Main training loop
        running_loss = 0.0
        num_batches = 0
        epochs = config.get('epochs', 5)
        
        for epoch in range(epochs):
            # Set epoch for distributed sampler to ensure proper shuffling
            if ray.train.get_context().get_world_size() > 1:
                train_loader.sampler.set_epoch(epoch)

            for images, labels in train_loader:
                # Note: prepare_data_loader automatically moves data to the correct device
                outputs = model(images)
                loss = criterion(outputs, labels)
                
                # Standard training step
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                
                # Update profiler
                prof.step()
                
                # Track metrics
                running_loss += loss.item()
                num_batches += 1

            # Report metrics and save checkpoint after each epoch
            avg_loss = running_loss / num_batches
            metrics = {"loss": avg_loss, "epoch": epoch}
            report_metrics_and_save_fsdp_checkpoint(model, optimizer, metrics)

            # Log metrics from rank 0 only to avoid duplicate outputs
            if world_rank == 0:
                logger.info(metrics)
    
    # Export memory profiling results to cluster storage
    run_name = ray.train.get_context().get_experiment_name()
    prof.export_memory_timeline(
        f"/mnt/cluster_storage/{run_name}/rank{world_rank}_memory_profile.html"
    )

    # Save the final model for inference
    save_model_for_inference(model, world_rank)
```

### Storage Configuration

This demo uses cluster storage to allow for quick iteration and development, but this may not be suitable in production environments or at high scale. In those cases, you should use object storage instead. For more information about how to select your storage type, see the [Anyscale storage configuration docs](https://docs.anyscale.com/configuration/storage).

## 3. Model sharding with FSDP2

PyTorch's `fully_shard` enables sharding at various granularities. At the most granular level, you can shard every layer to minimize peak memory utilization, but this also increases communication costs between Ray Train workers. Experiment with different sharding granularities to find the optimal balance for your use case. This example only shards the encoder blocks—the largest layers in the Vision Transformer.

Beyond sharding granularity, FSDP2 offers several configuration options to optimize performance and mitigate OOM errors:

### Device mesh configuration

`init_device_mesh` configures a `DeviceMesh` that describes the training run's device topology. This example uses a simple 1D mesh for data parallelism, but `DeviceMesh` also supports multi-dimensional parallelism approaches including tensor parallelism and pipeline parallelism. In many cases, integrating several types of parallelism can further help to improve training performance.

For more information about advanced multi-dimensional parallelism configurations, see the [PyTorch device mesh documentation](https://docs.pytorch.org/tutorials/recipes/distributed_device_mesh.html).

### CPU offloading 

CPU offloading reduces GPU memory footprint by storing model components in the CPU. However, this comes with the trade-off of increased data transfer overhead between CPU and GPU during computation.

**CPU offloading does the following:**
- Stores sharded parameters, gradients, and optimizer states on CPU
- Copies sharded parameters to GPU during forward/backward computation and frees them after use
- Copies computed gradients to the CPU where PyTorch computes the optimizer step

**When to use CPU offloading:**
- When GPU memory is constrained
- For very large models that don't fit in GPU memory

**Don't use CPU offloading in the following cases:**
- When CPU memory is limited (can cause CPU crashes due to out-of-memory error)
- When training speed is more important than memory usage

<div style="display: flex; gap: 40px; align-items: flex-start;">
  <div style="text-align: center;">
    <h3>Without CPU offloading</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/gpu_memory_profile.png" width="600"/>
  </div>
  <div style="text-align: center;">
    <h3>With CPU offloading</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/cpu_offload_profile.png" width="600"/>
  </div>
</div>
Note: The above images are generated using PyTorch's Memory Profiler, which this tutorial covers later.

It can be seen that CPU offloading significantly reduces the amount of GPU memory occupied by model parameters. 

Learn more about CPU offloading in the [PyTorch documentation](https://docs.pytorch.org/docs/stable/distributed.fsdp.fully_shard.html#torch.distributed.fsdp.CPUOffloadPolicy).


### `reshard_after_forward` flag 
`fully_shard` has a `reshard_after_forward` flag that enables all-gathered model weights to be freed immediately after the forward pass. This reduces peak GPU memory usage but increases the communication overhead between workers during the backward pass as parameters need to be all-gathered again. If unsharded model parameters are able to completely fit on each worker and don't pose a memory bottleneck, there's no need to enable `reshard_after_forward`.

<div style="display: flex; gap: 40px; align-items: flex-start;">
  <div style="text-align: center;"> 
    <h3><code>reshard_after_forward=False</code></h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/gpu_memory_profile.png" width="600"/>
  </div>
  <div style="text-align: center;">
    <h3><code>reshard_after_forward=True</code></h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/reshard_after_forward_memory_profile.png" width="600"/>
  </div>
</div>

With `reshard_after_forward=True`, the memory allocated to model parameters drops after the forward step whereas it peaks when `reshard_after_forward=False`.

### Mixed precision

Enabling mixed precision accelerates training and reduces GPU memory usage with minimal accuracy impact.

**Benefits of mixed precision with FSDP2**
- Reduced memory usage for activations and intermediate computations
- Faster computation on modern GPUs
- Maintained numerical stability through selective precision

<div style="display: flex; gap: 40px; align-items: flex-start;">
  <div style="text-align: center;">
    <h3>Without mixed precision</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/gpu_memory_profile.png" width="600"/>
  </div>
  <div style="text-align: center;">
    <h3>With mixed precision</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/mixed_precision_profile.png" width="600"/>
  </div>
</div>

With mixed precision enabled, the peak memory allocated to activations is halved.

Learn more about mixed precision configuration on the [PyTorch documentation](https://docs.pytorch.org/docs/stable/distributed.fsdp.fully_shard.html#torch.distributed.fsdp.MixedPrecisionPolicy).

### Combining Memory Strategies

The below diagram compares the GPU memory profile of default sharding to when all of the above strategies are enabled (CPU Offloading, Mixed Precision, `reshard_after_forward=True`).

<div style="display: flex; gap: 40px; align-items: flex-start;">
  <div style="text-align: center;">
    <h3>Default Sharding</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/gpu_memory_profile.png" width="600"/>
  </div>
  <div style="text-align: center;">
    <h4>Combined CPU Offloading, Mixed Precision, and Resharding</h4>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/all_strategies_profile.png" width="600"/>
  </div>
</div>



```python
# FSDP2 sharding imports 
from torch.distributed.fsdp import (
    fully_shard,
    FSDPModule,
    CPUOffloadPolicy,
    MixedPrecisionPolicy,
)
from torch.distributed.device_mesh import init_device_mesh 
```


```python
def shard_model(model: torch.nn.Module): 
    """Apply FSDP2 sharding to the model with optimized configuration.
    
    Args:
        model: The PyTorch model to shard
    """
    logger.info("Applying FSDP2 sharding to model...")

    # Step 1: Create 1D device mesh for data parallel sharding
    world_size = ray.train.get_context().get_world_size()
    mesh = init_device_mesh(
        device_type="cuda", 
        mesh_shape=(world_size,), 
        mesh_dim_names=("data_parallel",)
    )

    # Step 2: Configure CPU offloading policy (optional)
    offload_policy = CPUOffloadPolicy()

    # Step 3: Configure mixed precision policy (optional)
    mp_policy = MixedPrecisionPolicy(
        param_dtype=torch.float16,    # Store parameters in half precision
        reduce_dtype=torch.float16,   # Use half precision for gradient reduction
    )

    # Step 4: Apply sharding to each transformer encoder block
    for encoder_block in model.encoder.layers.children():
        fully_shard(
            encoder_block, 
            mesh=mesh, 
            reshard_after_forward=True,   # Free memory after forward pass
            offload_policy=offload_policy, 
            mp_policy=mp_policy
        )

    # Step 5: Apply sharding to the root model
    # This wraps the entire model and enables top-level FSDP2 functionality
    fully_shard(
        model, 
        mesh=mesh, 
        reshard_after_forward=True,   # Free memory after forward pass
        offload_policy=offload_policy, 
        mp_policy=mp_policy
    )
    
```

## 4. Distributed Checkpointing
This section sets up distributed checkpointing, loads a distributed model from a checkpoint, saves distributed model checkpoints, and saves a model for inference. 

### Distributed checkpoint wrapper setup

This section creates a checkpointing wrapper using PyTorch's `Stateful` API to simplify distributed checkpoint management. From the PyTorch docs, this basic wrapper handles the complexities of saving and loading FSDP2 model states across multiple workers.


```python
# PyTorch Distributed Checkpoint (DCP) imports
from torch.distributed.checkpoint.state_dict import (
    get_state_dict,
    set_state_dict,
    get_model_state_dict,
    StateDictOptions
)
from torch.distributed.checkpoint.stateful import Stateful
```


```python
class AppState(Stateful):
    """This is a useful wrapper for checkpointing the Application State. Because this object is compliant
    with the Stateful protocol, PyTorch DCP automatically calls state_dict/load_state_dict as needed in the
    dcp.save/load APIs.

    Note: This wrapper is used to handle calling distributed state dict methods on the model
    and optimizer.
    """

    def __init__(self, model, optimizer=None):
        self.model = model
        self.optimizer = optimizer

    def state_dict(self):
        # this line automatically manages FSDP2 FQN's (Fully Qualified Name), as well as sets the default state dict type to FSDP.SHARDED_STATE_DICT
        model_state_dict, optimizer_state_dict = get_state_dict(self.model, self.optimizer)
        return {
            "model": model_state_dict,
            "optim": optimizer_state_dict
        }

    def load_state_dict(self, state_dict):
        # sets our state dicts on the model and optimizer, now that loading is complete
        set_state_dict(
            self.model,
            self.optimizer,
            model_state_dict=state_dict["model"],
            optim_state_dict=state_dict["optim"],
        )
```

### Load distributed model from checkpoint

Load distributed checkpoints using `dcp.load`, which automatically handles resharding when the number of workers changes between training runs. This flexibility allows you to resume training with different resource configurations. 


```python
# PyTorch Distributed Checkpoint (DCP) Core import
import torch.distributed.checkpoint as dcp
```


```python
def load_fsdp_checkpoint(model: FSDPModule, optimizer: torch.optim.Optimizer, ckpt: ray.train.Checkpoint):
    """Load an FSDP checkpoint into the model and optimizer.
    
    This function handles distributed checkpoint loading with automatic resharding
    support. It can restore checkpoints even when the number of workers differs
    from the original training run.
    
    Args:
        model: The FSDP-wrapped model to load state into
        optimizer: The optimizer to load state into
        ckpt: Ray Train checkpoint containing the saved state
    """
    logger.info("Loading distributed checkpoint for resuming training...")
    
    try:
        with ckpt.as_directory() as checkpoint_dir:
            # Create state wrapper for DCP loading
            state_dict = {"app": AppState(model, optimizer)}
            
            # Load the distributed checkpoint
            dcp.load(
                state_dict=state_dict,
                checkpoint_id=checkpoint_dir
            )
            
        logger.info("Successfully loaded distributed checkpoint")
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        raise RuntimeError(f"Checkpoint loading failed: {e}") from e
```

### Save model checkpoints

The following function handles periodic checkpoint saving during training, combining metrics reporting with distributed checkpoint storage:


```python
def report_metrics_and_save_fsdp_checkpoint(
    model: FSDPModule, optimizer: torch.optim.Optimizer, metrics: dict
) -> None:
    """Report training metrics and save an FSDP checkpoint.
    
    This function performs two critical operations:
    1. Saves the current model and optimizer state using distributed checkpointing
    2. Reports metrics to Ray Train for tracking
    
    Args:
        model: The FSDP-wrapped model to checkpoint
        optimizer: The optimizer to checkpoint
        metrics: Dictionary of metrics to report (e.g., loss, accuracy)
    """
    logger.info("Saving checkpoint and reporting metrics...")
    
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        # Perform a distributed checkpoint with DCP
        state_dict = {"app": AppState(model, optimizer)}
        dcp.save(state_dict=state_dict, checkpoint_id=temp_checkpoint_dir)

        # Report each checkpoint shard from all workers
        # This saves the checkpoint to shared cluster storage for persistence
        checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)
        ray.train.report(metrics, checkpoint=checkpoint)
        
    logger.info(f"Checkpoint saved successfully. Metrics: {metrics}")
```

### Save the model for inference

After training, it is often useful to consolidate sharded checkpoints into a single file for convenient sharing or inference. Unlike regular distributed checkpointing, this process produces a large artifact compatible with torch.load. To do so, the `get_model_state_dict` function all-gathers parameter shards to rank 0, reconstructs the full state dict, and then saves the consolidated checkpoint to cluster storage.

Note that a key limitation of this approach is that the entire model must be materialized in memory on rank 0. For large models, this can exceed the available CPU RAM and result in out-of-memory errors. In such cases, it is advised to keep the model in its sharded format and rely on distributed model loading for inference.


```python
def save_model_for_inference(model: FSDPModule, world_rank: int) -> None:
    """Save the complete unsharded model for inference.
    
    This function consolidates the distributed model weights into a single
    checkpoint file that can be used for inference without FSDP.
    
    Args:
        model: The FSDP2-wrapped model to save
        world_rank: The rank of the current worker
    """
    logger.info("Preparing model for inference...")
    
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        save_file = os.path.join(temp_checkpoint_dir, "full-model.pt")

        # Step 1: All-gather the model state across all ranks
        # This reconstructs the complete model from distributed shards
        model_state_dict = get_model_state_dict(
            model=model,
            options=StateDictOptions(
                full_state_dict=True,    # Reconstruct full model
                cpu_offload=True,        # Move to CPU to save GPU memory
            )
        )

        logger.info("Successfully retrieved complete model state dict")
        checkpoint = None

        # Step 2: Save the complete model (rank 0 only)
        if world_rank == 0: 
            torch.save(model_state_dict, save_file)
            logger.info(f"Saved complete model to {save_file}")

            # Create checkpoint for shared storage
            checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # Step 3: Report the final checkpoint to Ray Train
        ray.train.report(
            {}, 
            checkpoint=checkpoint, 
            checkpoint_dir_name="full_model"
        )
```

## Launching the distributed training job

This section configures and launches the distributed training job using Ray Train's TorchTrainer:


```python
# Configure distributed training resources
scaling_config = ray.train.ScalingConfig(
    num_workers=2,      # Number of distributed workers
    use_gpu=True        # Enable GPU training
)

# Configure training parameters
train_loop_config = {
    "epochs": 5,
    "learning_rate": 0.001,
    "batch_size": 64,
}

# Create experiment name
experiment_name=f"fsdp_mnist_{uuid.uuid4().hex[:8]}"

# Configure run settings and storage
run_config = ray.train.RunConfig(
    # Persistent storage path accessible across all worker nodes
    storage_path="/mnt/cluster_storage/",
    # Unique experiment name (use consistent name to resume from checkpoints)
    name=experiment_name,
    # Fault tolerance configuration
    failure_config=ray.train.FailureConfig(max_failures=1),
)

# Initialize and launch the distributed training job
trainer = ray.train.torch.TorchTrainer(
    train_loop_per_worker=train_func,
    scaling_config=scaling_config,
    train_loop_config=train_loop_config,
    run_config=run_config,
)

print("Starting FSDP2 training job...")
result = trainer.fit()
print("Training completed successfully!")

```

## GPU memory profiling

GPU memory profiling is a useful tool for monitoring and analyzing memory usage during model training. It helps identify bottlenecks, optimize resource allocation, and prevent OOM errors. PyTorch's GPU Memory Profiler is configured within the training function.

In this demo, the profiler is configured to generate a profiling file for each worker accessible from cluster storage under the Anyscale Files tab. To inspect a worker's memory profile, download the corresponding HTML file and open it in your browser. The profiler configuration and export path can be customized within the training function.  For more details on PyTorch's memory profiler, see the [PyTorch blog](https://pytorch.org/blog/understanding-gpu-memory-1/).

<div style="display: flex; gap: 40px; align-items: flex-start;">
  <div style="text-align: center;">
    <h3>Example memory profile</h3>
    <img src="https://raw.githubusercontent.com/ray-project/ray/master/doc/source/train/examples/pytorch/pytorch-fsdp/images/gpu_memory_profile.png" width="600"/>
  </div>
</div>


### Post training directory view
The Anyscale platform saves the checkpoint shards, full model, and memory profiling reports in cluster storage with the following layout:

```
/mnt/cluster_storage/fsdp_mnist_1/
├── checkpoint_1/
│ ├── __0_0.distcp                  # Shard file for rank 0
│ └── __1_0.distcp                  # Shard file for rank 1
├── checkpoint_2/
│ └── ... (similar structure)
├── checkpoint_3/
│ └── ... (similar structure)
├── ... # Additional checkpoints
├── full_model/
│ └── full_model.pt                 # Full model checkpoint (for inference/deployment)
├── checkpoint_manager_snapshot.json
├── rank0_memory_profile.html       # Memory profiling for rank 0
└── rank1_memory_profile.html       # Memory profiling for rank 1
```

## Loading the trained model for inference

After training completes, you can load the saved model for inference on new data. Ray Train loads the model in its unsharded form, ready for standard PyTorch inference.


```python
# Update this path to match your trained model location
# The path follows the pattern: /mnt/cluster_storage/{experiment_name}/full_model/full-model.pt
PATH_TO_FULL_MODEL = f"/mnt/cluster_storage/{experiment_name}/full_model/full-model.pt"
```


```python
# Initialize the same model architecture for inference
model = init_model()

# Load the trained weights 
state_dict = torch.load(PATH_TO_FULL_MODEL, map_location='cpu')
model.load_state_dict(state_dict)
model.eval()
```


```python
# Load the test data
transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
test_data = FashionMNIST(
    root=".", train=False, download=True, transform=transform
)
test_data
```


```python
# Test model inference
with torch.no_grad():
    out = model(test_data.data[0].reshape(1, 1, 28, 28).float())
    predicted_label = out.argmax().item()
    test_label = test_data.targets[0].item()
    print(f"{predicted_label=} {test_label=}")
```

    predicted_label=8 test_label=9


## Summary

In this tutorial, you did the following: 

- Trained an image classification model using FSDP2 and Ray Train
- Learned how to load and save distributed checkpoints with PyTorch DCP
- Gained insight on configuring FSDP2 to balance training performance and memory usage
- Unlocked multi-node GPU memory observability with PyTorch Memory Profiler
