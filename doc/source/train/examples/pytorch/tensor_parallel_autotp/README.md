# Get started with Tensor Parallelism (DeepSpeed AutoTP) and Ray Train

**Time to complete:** 20 min

This template shows how to train large language models using tensor parallelism with DeepSpeed's AutoTP and Ray Train for distributed execution.

**Tensor Parallelism (TP)** shards model weights across multiple GPUs, enabling training of models that are too large to fit on a single GPU. Combined with **Data Parallelism (DP)**, this creates a powerful **2D parallelism** strategy that scales efficiently to many GPUs.

This tutorial provides a step-by-step guide covering:

- Understanding 2D parallelism (Tensor Parallelism + Data Parallelism)
- Setting up DeepSpeed AutoTP for automatic model sharding
- Combining with ZeRO optimization for data parallelism
- TP-aware data loading to ensure correct gradient computation
- Distributed checkpointing with Ray Train

**Note:** This tutorial uses DeepSpeed's AutoTP API. DeepSpeed automatically identifies and shards linear layers for tensor parallelism.

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

## Understanding 2D Parallelism

2D parallelism combines two complementary strategies:

- **Tensor Parallelism (TP)**: Shards model weights across GPUs within a TP group. All GPUs in a TP group process the same input data but hold different parts of the model.
- **Data Parallelism (DP)**: Replicates the model across DP groups. Each DP group processes different data and synchronizes gradients.

With `tp_size=2` and `dp_size=2` on 4 GPUs, the device mesh looks like:

```
Device Mesh (2x2):
        TP Dim
      [0]  [1]
 DP   +---+---+
 Dim  | 0 | 1 |  <- TP Group 0 (same data, sharded model)
      +---+---+
      | 2 | 3 |  <- TP Group 1 (same data, sharded model)
      +---+---+
        ^   ^
       DP Groups (different data, gradient sync)
```

- **TP Groups** (rows): GPUs 0,1 and GPUs 2,3 share the same input data but have sharded model weights
- **DP Groups** (columns): GPUs 0,2 and GPUs 1,3 see different data and synchronize gradients

## 1. Package and environment setup

Install the required dependencies:

```bash
pip install torch transformers datasets deepspeed
```

## 2. Data loading with TP-aware sharding

A critical aspect of tensor parallelism is ensuring all GPUs in a TP group receive identical input data. Standard data loaders shard by `world_rank`, giving each GPU different data. With TP, you must shard by `dp_rank` instead:

```python
# All TP ranks in same DP group get identical batches
sampler = DistributedSampler(
    dataset,
    num_replicas=dp_size,  # NOT world_size
    rank=dp_rank,          # NOT world_rank
)
```

## 3. Model Parallelization with DeepSpeed AutoTP

DeepSpeed AutoTP provides automatic tensor parallelism through:
- `set_autotp_mode(training=True)`: Enables AutoTP instrumentation before model creation
- `deepspeed.tp_model_init()`: Automatically shards linear layers across TP ranks
- `deepspeed.initialize()`: Creates the DeepSpeed engine for training

The Model Parallel Unit (MPU) interface tells DeepSpeed about the parallelism topology, enabling correct gradient synchronization across data parallel ranks.

## 4. Checkpointing

With tensor parallelism, each worker holds a shard of the model. Checkpointing saves each shard independently, and Ray Train aggregates them into a single checkpoint.

## 5. Training loop

The main training function brings together all components to:
1. Set up TP and DP process groups
2. Create and shard the model with DeepSpeed AutoTP
3. Run the training loop with checkpointing

## 6. Launch the distributed training job

Configure and launch the training job using Ray Train's TorchTrainer. This example uses:
- 4 workers (GPUs)
- 2-way tensor parallelism
- 2-way data parallelism
- A small model (Qwen2.5-0.5B) for demonstration

## Scaling to larger models

To train larger models like Qwen2-7B or Llama-3-8B, adjust the configuration:

```python
# For 8 GPUs: 4-way TP, 2-way DP
train_loop_config = {
    "model_name": "Qwen/Qwen2-7B",
    "tp_size": 4,
    "dp_size": 2,
    "batch_size": 1,
    "seq_length": 2048,
    ...
}

scaling_config = ScalingConfig(
    num_workers=8,
    use_gpu=True,
)
```

**Tips for scaling:**
- Increase `tp_size` to fit larger models (TP shards model weights)
- Increase `dp_size` to improve throughput (DP processes more data in parallel)
- Ensure `tp_size` divides the model's `num_key_value_heads`
- Use NVLink-connected GPUs for efficient TP communication
- DeepSpeed AutoTP works with ZeRO optimization for additional memory efficiency

## Summary

In this tutorial, you learned:

- How 2D parallelism combines tensor parallelism and data parallelism
- How to set up DeepSpeed AutoTP for automatic model sharding
- The importance of TP-aware data loading for correct gradient computation
- How to combine AutoTP with ZeRO optimization for 2D parallelism
- How to save distributed checkpoints with Ray Train

For production training of large models, consider:
- Using larger `tp_size` for models that don't fit on a single GPU
- Enabling activation checkpointing for memory efficiency (`model.gradient_checkpointing_enable()`)
- Using ZeRO Stage 2 or 3 for additional memory savings with large models
