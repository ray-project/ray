# Fine-tune an LLM with Ray Train and DeepSpeed

**Time to complete:** 20 min

This notebook combines **Ray Train** with **DeepSpeed** to efficiently scale PyTorch training across GPUs and nodes while minimizing GPU memory usage.

This hands-on example includes the following:
-  Fine-tuning an LLM
- Checkpoint saving and resuming with Ray Train
- Configuring ZeRO for memory and performance (stages, mixed precision, CPU offload)
- Launching a distributed training job

<div id="anyscale-note" class="alert alert-block alert-warning">

  <strong>Anyscale specific configuration</strong>

  <p><strong>Note:</strong> This template is optimized for the Anyscale platform. On Anyscale, most configuration is automated. When running on open-source Ray, manually complete the following steps:</p>

  <ul>
    <li><strong>Configure your Ray cluster</strong>: Multi-node setup and resource allocation.</li>
    <li><strong>Manage dependencies</strong>: Install prerequisites on each node.</li>
    <li><strong>Set up storage</strong>: Provide shared or distributed checkpoint storage.</li>
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


## Install dependencies (if needed)

Run the cell below only if your environment still needs these packages installed.



```bash
%%bash
pip install torch torchvision
pip install transformers datasets==3.6.0 trl==0.23.1
pip install deepspeed ray[train]
```


## Configuration constants

This notebook uses simple constants instead of `argparse` to simplify execution. Adjust these as needed.


```python
# ---- Training constants (edit these) ----
MODEL_NAME = "gpt2"
DATASET_NAME = "ag_news"
BATCH_SIZE = 1
NUM_EPOCHS = 1
SEQ_LENGTH = 512
LEARNING_RATE = 1e-6
ZERO_STAGE = 3
TUTORIAL_STEPS = 30

# Ray scaling settings
NUM_WORKERS = 2
USE_GPU = True

# Storage
STORAGE_PATH = "/mnt/cluster_storage/"
EXPERIMENT_PREFIX = "deepspeed_sample"
```

## 1. Define the training function

First, define the training loop function for each worker to execute. Note that Ray Train allocates a unique GPU to each worker.
Ray Train runs this training function on every worker to orchestrate the overall training process. The training function outlines the high-level structure common to most deep learning workflows, showing how setup, data ingestion, optimization, and reporting stages come together on each worker.

The training function does the following:

1. Initializes the model and optimizer with DeepSpeed (`setup_model_and_optimizer`).
1. Restores training from a checkpoint if one is available (`load_checkpoint`).
1. Sets up the dataloader (`setup_dataloader`).
1. Accesses the device that Ray Train assigns to this worker.
1. Iterates through the specified number of epochs.
1. For multi-GPU training, ensures each worker sees a unique data shard each epoch.
1. For each batch:
   - Moves inputs to the device.
   - Runs the forward pass to compute loss.
   - Logs the loss.
1. Performs the backward pass and optimizer step with DeepSpeed.
1. Aggregates average loss and reports metrics, saving a checkpoint at the end of each epoch. (`report_metrics_and_save_checkpoint`)

Later steps define the above helper functions (`setup_model_and_optimizer`, `load_checkpoint`, `setup_dataloader`, `report_metrics_and_save_checkpoint`).


```python
from typing import Dict, Any

import os
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"  # Ensure Ray Train v2 APIs
import ray

def train_loop(config: Dict[str, Any]) -> None:
    # (1) Initialize model and optimizer with DeepSpeed
    ds_engine = setup_model_and_optimizer(config["model_name"], config["learning_rate"], config["ds_config"])

    # (2) Load checkpoint if it exists
    ckpt = ray.train.get_checkpoint()
    start_epoch = 0
    if ckpt:
        start_epoch = load_checkpoint(ds_engine, ckpt)

    # (3) Set up dataloader
    train_loader = setup_dataloader(config["model_name"], config["dataset_name"], config["seq_length"], config["batch_size"])
    steps_per_epoch = len(train_loader)

    # (4) Access the device for this worker
    device = ray.train.torch.get_device()

    # Set model to training mode
    ds_engine.train()

    for epoch in range(start_epoch, config["epochs"]):
        # (6) Ensure unique shard per worker when using multiple GPUs
        if ray.train.get_context().get_world_size() > 1 and hasattr(train_loader, "sampler"):
            sampler = getattr(train_loader, "sampler", None)
            if sampler and hasattr(sampler, "set_epoch"):
                sampler.set_epoch(epoch)

        running_loss = 0.0
        num_batches = 0

        # (7) Iterate over batches
        for step, batch in enumerate(train_loader):
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)

            # Forward pass
            outputs = ds_engine(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=input_ids,
                use_cache=False
            )
            loss = outputs.loss
            print(f"Epoch: {epoch} Step: {step + 1}/{steps_per_epoch} Loss: {loss.item()}")

            # Backward pass and optimizer step
            ds_engine.backward(loss)
            ds_engine.step()

            running_loss += loss.item()
            num_batches += 1

            # Stop early in the tutorial so runs finish quickly
            if step + 1 >= config["tutorial_steps"]:
                print(f"Stopping early at {config['tutorial_steps']} steps for the tutorial")
                break

        # (8) Report metrics and save checkpoint
        report_metrics_and_save_checkpoint(ds_engine, {"loss": running_loss / num_batches, "epoch": epoch})

```

Ray Train runs the `train_loop` on each worker, which naturally supports **data parallelism**. In this setup, each worker processes a unique shard of data, computes gradients locally, and participates in synchronization to keep model parameters consistent. On top of this, DeepSpeed partitions model and optimizer states across GPUs to reduce memory usage and communication overhead.


## 2. Set Up the dataloader

The code below demonstrates how to prepare text data so that each worker can efficiently feed batches during training.

1. Downloads a tokenizer from the Hugging Face Hub (`AutoTokenizer`).  
2. Loads the `ag_news` dataset using Hugging Face's `load_dataset`.  
3. Applies tokenization with padding and truncation by calling `map`.  
4. Converts the dataset into a PyTorch `DataLoader`, which handles batching and shuffling.  
5. Finally, call `ray.train.torch.prepare_data_loader` to make the dataloader distributed-ready.

When you use **data parallelism**, each GPU worker trains on a unique shard of the dataset while holding its own copy of the model; gradients are synchronized after each step.
Ray Train's `prepare_data_loader` wraps PyTorch’s `DataLoader` and ensures workers see disjoint data, balances splits, and correctly handle epoch boundaries.


```python
import ray.train
import ray.train.torch
from torch.utils.data import DataLoader
from transformers import AutoTokenizer
from datasets import load_dataset, DownloadConfig

def setup_dataloader(model_name: str, dataset_name: str, seq_length: int, batch_size: int) -> DataLoader:
    # (1) Get tokenizer
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    # Set pad token if not already set
    if tokenizer.pad_token is None:
        if tokenizer.eos_token is not None:
            tokenizer.pad_token = tokenizer.eos_token
        else:
            # Fallback for models without eos_token
            tokenizer.pad_token = tokenizer.unk_token

    # (2) Load dataset
    # This example uses only 1% of the dataset for quick testing. Adjust as needed.
    dataset = load_dataset(dataset_name, split="train[:1%]", download_config=DownloadConfig(disable_tqdm=True))

    # (3) Tokenize
    def tokenize_function(examples):
        return tokenizer(examples['text'], padding='max_length', max_length=seq_length, truncation=True)
    tokenized_dataset = dataset.map(tokenize_function, batched=True, num_proc=1, keep_in_memory=True)
    tokenized_dataset.set_format(type='torch', columns=['input_ids', 'attention_mask'])

    # (4) Create DataLoader
    data_loader = DataLoader(tokenized_dataset, batch_size=batch_size, shuffle=True)

    # (5) Use prepare_data_loader for distributed training
    return ray.train.torch.prepare_data_loader(data_loader)
```

The following code demonstrates how to use the tokenizer to encode a sample string. 
- `AutoTokenizer.from_pretrained` downloads and configures the tokenizer for your model.
- You can encode any text string and inspect the resulting token IDs and attention mask.


```python
# Example usage of get_tokenizer
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
sample_text = "Ray Train and DeepSpeed make distributed training easy!"
encoded = tokenizer(sample_text)
print(encoded)
```


## 3. Initialize model and optimizer

After preparing and distributing the dataset, the next step is to set up the model and optimizer for training. This function does the following:

1. Loads a pretrained model from the Hugging Face Hub (`AutoModelForCausalLM`).  
2. Defines the optimizer (`AdamW`).  
3. Initializes DeepSpeed with ZeRO options and returns a `DeepSpeedEngine`.

DeepSpeed’s `initialize` always partitions **optimizer states** (ZeRO Stage 1) across the GPU memory of all workers participating in training. Depending on the chosen stage, it can also partition **gradients** (Stage 2) and **model parameters/weights** (Stage 3). This staged approach balances memory savings and communication overhead, and the tutorial covers these stages in more detail [in later steps](#deepspeed-zero-stages).


```python
from typing import Dict, Any
import torch
from transformers import AutoModelForCausalLM
import deepspeed

def setup_model_and_optimizer(model_name: str, learning_rate: float, ds_config: Dict[str, Any]) -> deepspeed.runtime.engine.DeepSpeedEngine:
    # (1) Load pretrained model
    model = AutoModelForCausalLM.from_pretrained(model_name)

    # (2) Define optimizer
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)

    # (3) Initialize with DeepSpeed (distributed and memory optimizations)
    ds_engine, _, _, _ = deepspeed.initialize(model=model, optimizer=optimizer, config=ds_config)
    return ds_engine

```


## 4. Checkpoint saving and loading

Checkpointing is crucial for fault tolerance and for resuming training after interruptions. This section saves and restores model and optimizer states in a distributed Ray Train with DeepSpeed setup. It demonstrates how each worker saves its own checkpoint shard, how Ray bundles them into a unified checkpoint, and how this enables seamless recovery or further fine-tuning from the saved state.

### Saving checkpoints

First define how Ray Train should save checkpoints during training. The code below shows how to create temporary directories, store model states, and report checkpoint information and metrics back to Ray Train for tracking and coordination. Note that DeepSpeed saves model and optimizer states in a **partitioned format**, where each worker stores only its shard.

1. Create a temporary directory for storing checkpoints.
1. Save the partitioned model and optimizer states with DeepSpeed's `save_checkpoint`.
1. Report metrics and checkpoint location to Ray Train with `ray.train.report`.


```python
import tempfile
import ray.train
from ray.train import Checkpoint

def report_metrics_and_save_checkpoint(
    ds_engine: deepspeed.runtime.engine.DeepSpeedEngine,
    metrics: Dict[str, Any]
) -> None:
    """Save worker checkpoints and report metrics to Ray Train.
    Each rank writes its shard to a temp directory so Ray Train bundles all of them.
    """
    ctx = ray.train.get_context()
    epoch_value = metrics["epoch"]

    with tempfile.TemporaryDirectory() as tmp_dir:
        checkpoint_dir = os.path.join(tmp_dir, "checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)

        ds_engine.save_checkpoint(checkpoint_dir)

        epoch_file = os.path.join(checkpoint_dir, "epoch.txt")
        with open(epoch_file, "w", encoding="utf-8") as f:
            f.write(str(epoch_value))

        checkpoint = Checkpoint.from_directory(tmp_dir)
        ray.train.report(metrics, checkpoint=checkpoint)

        if ctx.get_world_rank() == 0:
            experiment_name = ctx.get_experiment_name()
            print(
                f"Checkpoint saved successfully for experiment {experiment_name} at {checkpoint_dir}. Metrics: {metrics}"
            )
```

### Loading checkpoints

After saving checkpoints, the next step is being able to resume training or evaluation from a saved state.
This ensures that progress isn’t lost due to interruptions and allows long-running jobs to continue seamlessly across sessions.
When restarting, Ray Train provides each worker with the latest checkpoint so that DeepSpeed can rebuild the model, optimizer, and training progress from where it left off.

Restore a previously saved checkpoint into the DeepSpeed engine using `load_checkpoint`.


```python
def load_checkpoint(ds_engine: deepspeed.runtime.engine.DeepSpeedEngine, ckpt: ray.train.Checkpoint) -> int:
    """Restore DeepSpeed state and determine next epoch."""
    next_epoch = 0
    try:
        with ckpt.as_directory() as checkpoint_dir:
            print(f"Loading checkpoint from {checkpoint_dir}")
            epoch_dir = os.path.join(checkpoint_dir, "checkpoint")
            if not os.path.isdir(epoch_dir):
                epoch_dir = checkpoint_dir

            ds_engine.load_checkpoint(epoch_dir)

            epoch_file = os.path.join(epoch_dir, "epoch.txt")
            if os.path.isfile(epoch_file):
                with open(epoch_file, "r", encoding="utf-8") as f:
                    last_epoch = int(f.read().strip())
                next_epoch = last_epoch + 1

    except Exception as e:
        raise RuntimeError(f"Checkpoint loading failed: {e}") from e
    return next_epoch
```


## 5. Configure DeepSpeed

Before launching distributed training, you need to define a DeepSpeed configuration dictionary (`ds_config`) that controls data type settings, batch sizes, optimizations including ZeRO (model state partitioning strategies), etc. This configuration determines how DeepSpeed manages memory, communication, and performance across GPUs.

The example below shows a minimal setup that enables bfloat16 precision, gradient clipping, and ZeRO optimization. You can further customize this configuration based on your model size, hardware, and performance goals. See [Advanced Configurations](#advanced-configurations) for more details.


```python
# DeepSpeed configuration
ds_config = {
    "train_micro_batch_size_per_gpu": BATCH_SIZE,
    "bf16": {"enabled": True},
    "grad_accum_dtype": "bf16",
    "zero_optimization": {
        "stage": ZERO_STAGE,
        "overlap_comm": True,
        "contiguous_gradients": True,
    },
    "gradient_clipping": 1.0,
}
```

## 6. Launch distributed training

The final step is to configure parameters and launch the distributed training job.
Ray Train’s `TorchTrainer` automatically starts multiple workers—one per GPU—and executes the `train_loop` on each instance. The **scaling configuration** determines how many workers to launch and what resources they use, while the **run configuration** manages storage and experiment tracking.

This function does the following:
1. Parses command-line arguments for training and model settings.
1. Defines the Ray Train `ScalingConfig`—for example, the number of workers and GPU usage.
1. Prepares the training loop configuration with hyperparameters and model details.
1. Sets up the Ray Train `RunConfig` to manage storage and experiment metadata. This example sets a random experiment name, but you can specify the name of a previous experiment to load the checkpoint.
1. Creates a `TorchTrainer` that launches the training function on multiple GPU workers.
1. Starts training with `trainer.fit()` and prints the result.


```python
import uuid
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, RunConfig

# Ray Train scaling configuration
scaling_config = ScalingConfig(num_workers=NUM_WORKERS, use_gpu=USE_GPU)

# Training loop configuration
train_loop_config = {
    "epochs": NUM_EPOCHS,
    "learning_rate": LEARNING_RATE,
    "batch_size": BATCH_SIZE,
    "ds_config": ds_config,
    "model_name": MODEL_NAME,
    "dataset_name": DATASET_NAME,
    "seq_length": SEQ_LENGTH,
    "tutorial_steps": TUTORIAL_STEPS,
}

# Ray Train run configuration
run_config = RunConfig(
    storage_path=STORAGE_PATH,
    # Set the name of the previous experiment when resuming from a checkpoint
    name=f"{EXPERIMENT_PREFIX}_{uuid.uuid4().hex[:8]}",
)

# Create and launch the trainer
trainer = TorchTrainer(
    train_loop_per_worker=train_loop,
    scaling_config=scaling_config,
    train_loop_config=train_loop_config,
    run_config=run_config,
)

# To actually run training, execute the following:
result = trainer.fit()
print(f"Training finished. Result: {result}")
```

## Running as a standalone script

While this tutorial is designed to run interactively in a Jupyter notebook, you can also launch the same training workflow as a standalone Python script.
This is useful for running longer experiments, automating jobs, or deploying training on a cluster.

The [full code](https://github.com/ray-project/ray/blob/master/doc/source/train/examples/pytorch/deepspeed_finetune/train.py) is also available.
To start training from the command line, run:

```bash
python train.py
```

## Advanced configurations

DeepSpeed has many other configuration options to tune performance and memory usage.
This section introduces some of the most commonly used options.
See the [DeepSpeed documentation](https://www.deepspeed.ai/docs/config-json/) for more details.

### DeepSpeed ZeRO stages
- **Stage 1**: Partitions optimizer states (always on when using ZeRO).  
- **Stage 2**: Additionally partitions gradients.  
- **Stage 3**: Additionally partitions model parameters or weights.

The higher the stage, the more memory savings you get, but it may also introduce more communication overhead and complexity in training.
You can select the stage through `ds_config["zero_optimization"]["stage"]`. See the DeepSpeed docs for more details.

```python
ds_config = {
    "zero_optimization": {
        "stage": 2,  # or 1 or 3
    },
}
```

### Mixed precision
Enable BF16 or FP16:
```python
ds_config = {
    "bf16": {"enabled": True},  # or "fp16": {"enabled": True}
}
```

### CPU offloading
Reduce GPU memory pressure by offloading to CPU at the cost of PCIe traffic:
```python
ds_config = {
    "offload_param": {"device": "cpu", "pin_memory": True},
    # or
    "offload_optimizer": {"device": "cpu", "pin_memory": True},
}
```


## Summary

In this tutorial, you did the following:

- Fine-tuned an LLM using Ray Train and DeepSpeed ZeRO
- Set up distributed data loading with Ray Train's `prepare_data_loader`
- Saved and managed checkpoints with Ray Train's storage configuration
- Configured and launched multi-GPU training with `TorchTrainer` and scaling configurations
- Explored advanced DeepSpeed configurations (ZeRO stages, mixed precision, and CPU offloading)
