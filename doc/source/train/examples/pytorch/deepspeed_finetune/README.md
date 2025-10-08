# A Step-by-step Guide: Fine-tuning LLMs with Ray Train and DeepSpeed

**Time to complete:** 20 min

This notebook walks through how to combine **DeepSpeed** with **Ray Train** to efficiently scale PyTorch training across GPUs and nodes while minimizing memory usage.

It includes:
- A hands-on example of fine-tuning an LLM
- Checkpoint saving and resuming with Ray Train
- Configuring ZeRO for memory and performance (stages, mixed precision, CPU offload)
- Launching a distributed training job

<div id="anyscale-note" class="alert alert-block alert-warning">

  <strong>Anyscale Specific Configuration</strong>

  <p><strong>Note:</strong> This template is optimized for the Anyscale platform. On Anyscale, most configuration is automated. When running on open-source Ray, manually complete the following steps:</p>

  <ul>
    <li><strong>Configure your Ray Cluster</strong>: Multi-node setup and resource allocation.</li>
    <li><strong>Manage Dependencies</strong>: Install prerequisites on each node.</li>
    <li><strong>Set Up Storage</strong>: Provide shared or distributed checkpoint storage.</li>
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


## Install Dependencies (if needed)

Run the cell below only if your environment still needs these packages installed.



```bash
%%bash
pip install torch torchvision
pip install transformers datasets==3.6.0 trl==0.23.1
pip install deepspeed ray[train]
```


## Configuration Constants

This notebook uses simple constants instead of `argparse` to simplify execution. Adjust these as needed.



```python
import os
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"  # Ensure Ray Train v2 APIs

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


## 1. Set Up the Dataloader

The code below:

1. Downloads a tokenizer from the Hugging Face Hub (`AutoTokenizer`).  
2. Loads the `ag_news` dataset using Hugging Face's `load_dataset`.  
3. Applies tokenization with padding and truncation by calling `map`.  
4. Converts the dataset into a PyTorch `DataLoader`, which handles batching and shuffling.  
5. Finally, call `ray.train.torch.prepare_data_loader` to make the dataloader distributed-ready.

This example uses only 1% of the dataset for quick testing. Adjust as needed.


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
encoded = tokenizer(sample_text, padding='max_length', max_length=32, truncation=True)
print(encoded)
```


**Making the dataloader distributed-ready with Ray**  
In **data parallelism**, each GPU worker trains on a unique shard of the dataset while holding its own copy of the model; gradients are synchronized after each step.  
Ray’s `prepare_data_loader` wraps PyTorch’s `DataLoader` and automatically applies a `DistributedSampler`, ensuring workers see disjoint data, splits are balanced, and epoch boundaries are handled correctly.


## 2. Initialize Model and Optimizer

The function below:

1. Loads a pretrained model from the Hugging Face Hub (`AutoModelForCausalLM`).  
2. Defines the optimizer (`AdamW`).  
3. Initializes DeepSpeed with ZeRO options and returns a `DeepSpeedEngine`.



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

    # (3) Initialize with DeepSpeed (distributed + memory optimizations)
    ds_engine, _, _, _ = deepspeed.initialize(model=model, optimizer=optimizer, config=ds_config)
    return ds_engine

```


**Making the model distributed-ready with Ray and DeepSpeed**  
DeepSpeed’s `initialize` always partitions **optimizer states** (ZeRO Stage 1). Depending on the chosen stage, it can also partition **gradients** (Stage 2) and **model parameters/weights** (Stage 3). This staged approach balances memory savings and communication overhead, and the tutorial covers these stages in more detail [later in the tutorial](#deepspeed-zero-stages).


## 3. Checkpointing and Loading


Checkpointing is crucial for fault tolerance and for resuming training after interruptions. The functions below:

1. Create a temporary directory for storing checkpoints.
1. Save the partitioned model and optimizer states with DeepSpeed's `save_checkpoint`.
1. Synchronize all workers with `torch.distributed.barrier` to ensure every process finishes saving.
1. Report metrics and checkpoint location to Ray with `ray.train.report`.
1. Restore a previously saved checkpoint into the DeepSpeed engine using `load_checkpoint`.


```python
import tempfile
import ray.train
from ray.train import Checkpoint

def report_metrics_and_save_checkpoint(
    ds_engine: deepspeed.runtime.engine.DeepSpeedEngine,
    metrics: Dict[str, Any]
) -> None:
    """Save worker checkpoints and report metrics to Ray.
    Each rank writes its shard to a temp directory so Ray bundles all of them.
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

            if torch.distributed.is_available() and torch.distributed.is_initialized():
                torch.distributed.barrier()
    except Exception as e:
        raise RuntimeError(f"Checkpoint loading failed: {e}") from e
    return next_epoch
```


**Making checkpoints distributed-ready with Ray and DeepSpeed**  
DeepSpeed saves model and optimizer states in a **partitioned format**, where each worker stores only its shard. This requires synchronization across processes, so all workers must reach the same checkpointing point before proceeding. The call to `torch.distributed.barrier()` ensures that every worker finishes saving before moving on.  
Finally, `ray.train.report` both reports training metrics and saves the checkpoint to persistent storage, making it accessible for resuming training later.



## 4. Training Iteration

Ray Train runs a training loop function on each GPU worker to orchestrate the entire process. The function below:

1. Initializes the model and optimizer with DeepSpeed.
1. Restores training from a checkpoint if one is available.
1. Sets up the dataloader with `setup_dataloader`.
1. Gets the device assigned to this worker.
1. Iterates through the specified number of epochs.
1. For multi-GPU training, ensures each worker sees a unique data shard each epoch.
1. For each batch:
   - Moves inputs to the device.
   - Runs the forward pass to compute loss.
   - Logs the loss.
1. Performs backward pass and optimizer step with DeepSpeed.
1. Aggregates average loss and reports metrics, saving a checkpoint at the end of each epoch.


```python

def train_loop(config: Dict[str, Any]) -> None:
    # (1) Initialize model + optimizer with DeepSpeed
    ds_engine = setup_model_and_optimizer(config["model_name"], config["learning_rate"], config["ds_config"])

    # (2) Load checkpoint if exists
    ckpt = ray.train.get_checkpoint()
    start_epoch = 0
    if ckpt:
        start_epoch = load_checkpoint(ds_engine, ckpt)

    # (3) Set up dataloader
    train_loader = setup_dataloader(config["model_name"], config["dataset_name"], config["seq_length"], config["batch_size"])
    steps_per_epoch = len(train_loader)

    # (4) Get device for this worker
    device = ray.train.torch.get_device()

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

            # Backward pass + optimizer step
            ds_engine.backward(loss)
            ds_engine.step()

            running_loss += loss.item()
            num_batches += 1

            # Stop early in the tutorial so runs finish quickly
            if step + 1 >= config["tutorial_steps"]:
                print(f"Stopping early at {config['tutorial_steps']} steps for the tutorial")
                break

        # (8) Report metrics + save checkpoint
        report_metrics_and_save_checkpoint(ds_engine, {"loss": running_loss / num_batches, "epoch": epoch})


```


**Coordinating distributed training with Ray and DeepSpeed**  
Ray launches this `train_loop` on each worker, while DeepSpeed manages partitioning and memory optimizations. With **data parallelism**, each worker processes a unique shard of data, computes gradients locally, and participates in synchronization so parameters stay in sync.



## 5. Configure DeepSpeed and Launch Trainer

The final step is to configure parameters and launch the distributed training job with Ray's `TorchTrainer`. The function below:
1. Parses command-line arguments for training and model settings.
1. Defines the Ray scaling configuration—for example, the number of workers and GPU usage.
1. Builds the DeepSpeed configuration dictionary (`ds_config`).
1. Prepares the training loop configuration with hyperparameters and model details.
1. Sets up the Ray `RunConfig` to manage storage and experiment metadata. This example sets a random experiment name, but you can specify the name of a previous experiment to load the checkpoint.
1. Creates a `TorchTrainer` that launches the training loop on multiple GPU workers.
1. Starts training with `trainer.fit()` and prints the result.


```python
import uuid
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig, RunConfig

# Ray scaling configuration
scaling_config = ScalingConfig(num_workers=NUM_WORKERS, use_gpu=USE_GPU)

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

# Ray run configuration
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

**Launching distributed training with Ray and DeepSpeed**

Ray’s `TorchTrainer` automatically launches multiple workers—one per GPU—and runs the `train_loop` on each instance. The scaling configuration controls how many workers to start, while the run configuration handles logging, storage, and experiment tracking.

DeepSpeed’s `ds_config` ensures that the right ZeRO stage and optimizations apply inside each worker. Together, this setup makes it easy to scale from a single GPU to a multi-node cluster without changing your training loop code.

`train.py` in this directory contains the full code.

## Advanced Configurations

DeepSpeed has many other configuration options to tune performance and memory usage.
This section introduces some of the most commonly used options.
Refer to the [DeepSpeed documentation](https://www.deepspeed.ai/docs/config-json/) for more details.

### DeepSpeed ZeRO Stages
- **Stage 1**: Partitions optimizer states (always on when using ZeRO).  
- **Stage 2**: Additionally partitions gradients.  
- **Stage 3**: Additionally partitions model parameters/weights.

The higher the stage, the more memory savings you get, but it may also introduce more communication overhead and complexity in training.
You can select the stage through `ds_config["zero_optimization"]["stage"]`. See the DeepSpeed docs for more details.

```python
ds_config = {
    "zero_optimization": {
        "stage": 2,  # or 1 or 3
    },
}
```

### Mixed Precision
Enable BF16 or FP16:
```python
ds_config = {
    "bf16": {"enabled": True},  # or "fp16": {"enabled": True}
}
```

### CPU Offloading
Reduce GPU memory pressure by offloading to CPU (at the cost of PCIe traffic):
```python
ds_config = {
    "offload_param": {"device": "cpu", "pin_memory": True},
    # or
    "offload_optimizer": {"device": "cpu", "pin_memory": True},
}
```


## Summary

In this tutorial, you did the following:

- Fine-tuned a large language model using DeepSpeed ZeRO and Ray Train
- Set up distributed data loading with Ray Train's `prepare_data_loader`
- Saved and managed checkpoints with Ray Train's storage configuration
- Configured and launched multi-GPU training with `TorchTrainer` and scaling configurations
- Explored advanced DeepSpeed configurations (ZeRO stages, mixed precision, and CPU offloading)
