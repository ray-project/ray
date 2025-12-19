# Two-Stage Flux Transformer + LoRA Training with Ray

This directory contains example scripts demonstrating how to implement a two-stage training pipeline for the FLUX.1-Fill-dev inpainting transformer using Ray Data and Ray Train.

## Overview

The training process alternates between two stages:

| Stage | Model Configuration | What's Trained |
|-------|-------------------|----------------|
| **Stage 1** | Full Transformer | All transformer parameters (BF16, DeepSpeed ZeRO-2) |
| **Stage 2** | Transformer + LoRA | Only LoRA adapter parameters (rank 64) |

The process cycles through Stage 1 → Stage 2 repeatedly until desired checkpoints are obtained.

## Files

- `flux_lora_simple_example.py` - Minimal example demonstrating core Ray patterns
- `flux_lora_two_stage_training.py` - Full production-ready implementation

## Quick Start

### 1. Install Dependencies

```bash
pip install "ray[train]" torch
# For full example:
pip install diffusers transformers peft accelerate deepspeed pillow
```

### 2. Prepare Your Dataset (JSONL Format)

```jsonl
{"user_input": {"image": "/path/to/image.png", "mask": "/path/to/mask.png"}, "output": "/path/to/target.png"}
{"user_input": {"image": "/path/to/image2.png", "mask": "/path/to/mask2.png"}, "output": "/path/to/target2.png"}
```

### 3. Run the Simple Example

```bash
python flux_lora_simple_example.py
```

### 4. Run the Full Pipeline

```bash
# Run both stages with cycling
python flux_lora_two_stage_training.py \
    --jsonl-path /path/to/data.jsonl \
    --num-workers 4 \
    --num-cycles 2

# Run only Stage 1 (transformer fine-tuning)
python flux_lora_two_stage_training.py \
    --jsonl-path /path/to/data.jsonl \
    --stage 1 \
    --num-workers 4

# Run only Stage 2 (LoRA training), resuming from Stage 1
python flux_lora_two_stage_training.py \
    --jsonl-path /path/to/data.jsonl \
    --stage 2 \
    --resume-from /path/to/stage1/checkpoint \
    --num-workers 4
```

## Key Ray Concepts

### 1. Loading Data with Ray Data

```python
import ray

# Read JSONL file - automatically handles distributed loading
train_dataset = ray.data.read_json("data.jsonl", lines=True)

# Pass to TorchTrainer
trainer = TorchTrainer(
    train_fn,
    datasets={"train": train_dataset},
    dataset_config=DataConfig(datasets_to_split=["train"]),
    ...
)
```

### 2. Accessing Data Shards in Workers

```python
def train_fn(config):
    # Each worker gets its own partition of the data
    train_ds = ray.train.get_dataset_shard("train")
    
    # Iterate with automatic batching
    for batch in train_ds.iter_torch_batches(
        batch_size=32,
        collate_fn=my_collate_fn,
    ):
        images = batch["images"]
        # ... training step
```

### 3. Distributed Model Training

```python
def train_fn(config):
    model = MyModel()
    
    # Wrap for distributed training (handles DDP/FSDP)
    model = ray.train.torch.prepare_model(model)
    
    # Get the device assigned to this worker
    device = ray.train.torch.get_device()
```

### 4. Checkpointing Between Stages

```python
# Save checkpoint
with tempfile.TemporaryDirectory() as tmpdir:
    torch.save(model.state_dict(), f"{tmpdir}/model.pt")
    checkpoint = Checkpoint.from_directory(tmpdir)
    ray.train.report(metrics={"loss": loss}, checkpoint=checkpoint)

# Load checkpoint in next stage
checkpoint = ray.train.get_checkpoint()
if checkpoint:
    with checkpoint.as_directory() as checkpoint_dir:
        state_dict = torch.load(f"{checkpoint_dir}/model.pt")
        model.load_state_dict(state_dict)
```

### 5. Passing Checkpoints Between Stages

```python
# Stage 1
stage1_trainer = TorchTrainer(train_stage1_fn, ...)
stage1_result = stage1_trainer.fit()

# Stage 2 - pass Stage 1 checkpoint
stage2_trainer = TorchTrainer(
    train_stage2_fn,
    resume_from_checkpoint=stage1_result.checkpoint,  # <-- Key!
    ...
)
stage2_result = stage2_trainer.fit()
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ray Cluster                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Worker 0   │    │   Worker 1   │    │   Worker 2   │  ...  │
│  │   (GPU 0)    │    │   (GPU 1)    │    │   (GPU 2)    │       │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘       │
│         │                   │                   │                │
│         └───────────────────┴───────────────────┘                │
│                             │                                    │
│                     ┌───────▼───────┐                           │
│                     │   Ray Data    │                           │
│                     │  (Sharded)    │                           │
│                     └───────────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Data Flow:
┌────────┐      ┌─────────────┐      ┌───────────────┐
│ JSONL  │ ──▶  │  Ray Data   │ ──▶  │ iter_torch_   │
│  File  │      │  Dataset    │      │ batches()     │
└────────┘      └─────────────┘      └───────────────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ Shard 0  │ │ Shard 1  │ │ Shard 2  │
    │(Worker 0)│ │(Worker 1)│ │(Worker 2)│
    └──────────┘ └──────────┘ └──────────┘
```

## Stage 1 vs Stage 2 Configuration

### Stage 1: Full Transformer Training

```python
# All parameters trainable
model = FluxTransformer.from_pretrained("black-forest-labs/FLUX.1-Fill-dev")
for param in model.parameters():
    param.requires_grad = True

# DeepSpeed ZeRO-2 config
deepspeed_config = {
    "bf16": {"enabled": True},
    "zero_optimization": {"stage": 2},
    "train_micro_batch_size_per_gpu": 1,
}
```

### Stage 2: LoRA-Only Training

```python
from peft import LoraConfig, get_peft_model

# Freeze base model
for param in model.parameters():
    param.requires_grad = False

# Add LoRA adapters
lora_config = LoraConfig(
    r=64,                    # Rank
    lora_alpha=96,           # Scaling factor
    lora_dropout=0.05,
    target_modules=["proj_in", "proj_out", "to_q", "to_k", "to_v", ...],
)
model = get_peft_model(model, lora_config)
```

## Scaling to Multiple GPUs/Nodes

```python
# Single node, 4 GPUs
scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

# Multi-node (4 nodes × 8 GPUs each)
scaling_config = ScalingConfig(
    num_workers=32,
    use_gpu=True,
    resources_per_worker={"GPU": 1},
)

# With persistent storage for multi-node
run_config = RunConfig(
    name="flux_training",
    storage_path="s3://my-bucket/training-runs",
)
```

## Troubleshooting

### Out of Memory

1. Reduce `batch_size`
2. Increase `gradient_accumulation_steps`
3. Enable CPU offloading in DeepSpeed config
4. Use ZeRO-3 instead of ZeRO-2

### Slow Data Loading

1. Pre-process images to tensors and save as Parquet
2. Use `ray.data.read_parquet()` instead of JSONL
3. Increase number of Ray Data read parallelism

### Checkpoint Not Found

Ensure `resume_from_checkpoint` path is accessible from all workers (use shared storage like S3/GCS for multi-node).
