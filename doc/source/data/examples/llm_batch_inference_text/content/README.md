<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify README.ipynb instead, then regenerate this file with:
jupyter nbconvert "$nb_filename" --to markdown --output "README.md"
-->

# LLM batch inference with Ray Data LLM

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/llm_batch_inference_text"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/data/examples/llm_batch_inference_text/content" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

**⏱️ Time to complete**: 15 minutes

This example shows you how to run batch inference for large language models (LLMs) using [Ray Data LLM APIs](https://docs.ray.io/en/latest/data/api/llm.html). In this use case, the batch inference job infers company industries from company names across a large customer dataset.


## When to use LLM batch inference

Offline (batch) inference optimizes for throughput over latency. Unlike online inference, which processes requests one at a time in real-time, batch inference processes thousands or millions of inputs together, maximizing GPU utilization and reducing per-inference costs.

Choose batch inference when:
- You have a fixed dataset to process (such as daily reports or data migrations).
- Throughput matters more than immediate results.
- You want to take advantage of fault tolerance for long-running jobs.

On the contrary, if you are more interested in optimizing for latency, consider [deploying your LLM with Ray Serve LLM for online inference](https://docs.ray.io/en/latest/serve/llm/index.html).

## Prepare a Ray Data dataset

Ray Data LLM runs batch inference for LLMs on Ray Data datasets. In this tutorial, you perform batch inference with an LLM to infer company industries from company names. The source is a 2-million-row CSV file containing sample customer data.

First, load the data from a remote URL then repartition the dataset to ensure the workload can be distributed across multiple GPUs.


```python
import ray

# Define the path to the sample CSV file hosted on S3.
# This dataset contains 2 million rows of synthetic customer data.
path = "https://llm-guide.s3.us-west-2.amazonaws.com/data/ray-data-llm/customers-2000000.csv"

# Load the CSV file into a Ray Dataset.
print("Loading dataset from remote URL...")
ds = ray.data.read_csv(path)

# Inspect the dataset schema and a few rows to verify it loaded correctly.
print("\nDataset schema:")
print(ds.schema())
print("\nSample rows:")
ds.show(limit=2)
```

For this initial example, limit the dataset to 10,000 rows so you can process and test faster. Later, you can scale up to the full dataset.

If you don't repartition, the system might read a large file into only a few blocks, which limits parallelism in later steps. For example, you might see that only 4 out of 8 GPUs in your cluster are being used. To address this, you can repartition the data into a specific number of blocks so the system can better parallelize work across all available GPUs in the pipeline.


```python
# Limit the dataset to 10,000 rows for this example.
print("Limiting dataset to 10,000 rows for initial processing.")
ds_small = ds.limit(10_000)


# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, streaming datasets might not be optimally partitioned. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
# Tip: Repartition count should typically be 2-4x your worker (GPU) count.
# Example: 4 GPUs → 8-16 partitions, 10 GPUs → 20-40 partitions.
# This ensures enough parallelism while avoiding excessive overhead.
num_partitions = 128
print(f"Repartitioning dataset into {num_partitions} blocks for parallelism...")
ds_small = ds_small.repartition(num_blocks=num_partitions)
```

## Configure Ray Data LLM

Ray Data LLM provides a unified interface to run batch inference with different LLM engines. Configure the vLLM engine, define preprocessing and postprocessing functions, and build the processor.

### Configure the processor engine

Configure the model and compute resources needed for inference using `vLLMEngineProcessorConfig`.

This example uses the `unsloth/Llama-3.1-8B-Instruct` model. The configuration specifies:
- `model_source`: The Hugging Face model identifier.
- `engine_kwargs`: vLLM engine parameters such as tensor parallelism and memory settings.
- `batch_size`: Number of requests to batch together (set to 256 for small prompts and outputs).
- `accelerator_type`: GPU type to use (L4 in this case).
- `concurrency`: Number of parallel workers (4 in this case).

**Note:** Because the input prompts and expected output token lengths are small, `batch_size=256` is appropriate. However, depending on your workload, a large batch size can lead to increased idle GPU time when decoding long sequences. Adjust this value to find the optimal trade-off between throughput and latency.


```python
from ray.data.llm import vLLMEngineProcessorConfig

processor_config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len=256,  # Hard cap: system prompt + user prompt + output tokens must fit within this limit
    ),
    batch_size=256,
    accelerator_type="L4",
    concurrency=4,
)
```

For more details on the configuration options you can pass to the vLLM engine, see the [vLLM Engine Arguments documentation](https://docs.vllm.ai/en/stable/configuration/engine_args.html).

### Define the preprocess and postprocess functions

The task is to infer the industry category from the `Company` field using an LLM.

Define a preprocess function to prepare `messages` and `sampling_params` for the vLLM engine, and a postprocess function to extract the `generated_text`.

Ray Data LLM sends the output of your preprocessing function directly to a vLLM engine, so you can take advantage of vLLM features. 

In this example, you use vLLM's structured output feature to restrict the LLM's responses to a predefined list of industry categories. This increases predictability and helps you reduce the cost of unnecessary output tokens.


```python
from typing import Any

# For better output token control, restrain generation to these choices
CHOICES = [
    "Law Firm",
    "Healthcare",
    "Technology",
    "Retail",
    "Consulting",
    "Manufacturing",
    "Finance",
    "Real Estate",
    "Other",
]

# Preprocess function prepares `messages` and `sampling_params` for vLLM engine.
# All other fields are ignored by the engine.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that infers company industries. "
                           "Based on the company name provided, output only the industry category. "
                           f"Choose from: {', '.join(CHOICES)}."
            },
            {
                "role": "user",
                "content": f"What industry is this company in: {row['Company']}"
            },
        ],
        sampling_params=dict(
            temperature=0,  # Use 0 for deterministic output
            max_tokens=16,  # Max output tokens. Industry names are short
            structured_outputs=dict(choice=CHOICES), # Constraint generation
        ),
    )

# Postprocess function extracts the generated text from the engine output.
# The **row syntax returns all original columns in the input dataset.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "inferred_industry": row["generated_text"],
        **row,  # Include all original columns.
    }
```

### Build the processor

With the configuration and functions defined, build the processor.


```python
from ray.data.llm import build_llm_processor

# Build the LLM processor with the configuration and functions.
processor = build_llm_processor(
    processor_config,
    preprocess=preprocess,
    postprocess=postprocess,
)
```

## Process the dataset

Run the processor on your small dataset to perform batch inference. Ray Data automatically distributes the workload across available GPUs and handles batching, retries, and resource management.


```python
from pprint import pprint

# Run the processor on the small dataset.
processed_small = processor(ds_small)

# Materialize the dataset to memory.
processed_small = processed_small.materialize()

print(f"\nProcessed {processed_small.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
```

## Launch to production with Anyscale Jobs

For production workloads, deploy your batch inference processor as an [Anyscale Job](https://docs.anyscale.com/platform/jobs). Anyscale takes care of the infrastructure layer and runs your jobs on your dedicated clusters with automatic retries, monitoring, and scheduling.

### Anyscale Runtime

Anyscale Jobs run on [Anyscale Runtime](https://docs.anyscale.com/runtime/data), which includes performance optimizations over open-source Ray Data. Key improvements include faster shuffles, optimized memory management, improved autoscaling, and enhanced fault tolerance for large-scale data processing.

These optimizations are automatic and require no code changes. Your Ray Data pipelines benefit from them simply by running on Anyscale. For batch inference workloads specifically, Anyscale Runtime provides better GPU utilization and reduced overhead when scaling across many nodes.

### Configure an Anyscale Job

Save your batch inference code as `batch_inference_text.py`, then create a job configuration file:

```yaml
# job.yaml
name: my-llm-batch-inference-text
entrypoint: python batch_inference_text.py
image_uri: anyscale/ray-llm:2.51.1-py311-cu128
compute_config:
  head_node:
    instance_type: m5.2xlarge
  worker_nodes:
    - instance_type: g6.2xlarge
      min_nodes: 0
      max_nodes: 10
working_dir: .
max_retries: 2
```

### Submit

Submit your job using the Anyscale CLI:

```bash
anyscale job submit --config-file job.yaml
```

### Monitoring

Track your job's progress in the Anyscale Console or through the CLI:

```bash
# Check job status
anyscale job status --name my-llm-batch-inference-text

# View logs
anyscale job logs --name my-llm-batch-inference-text
```

The Ray Dashboard remains available for detailed monitoring. To access it, go to your Anyscale Job in your console.  
For cluster-level information, click the **Metrics** tab then **Data** tab, and for task-level information, click the **Ray Workloads** tab then **Data** tab.

## Monitor the execution

Use the Ray Dashboard to monitor the execution. See [Monitoring your Workload](https://docs.ray.io/en/latest/data/monitoring-your-workload.html) for more information on visualizing your Ray Data jobs.

The dashboard shows:
- Operator-level metrics (throughput, task execution times).
- Resource utilization (CPU, GPU, memory).
- Progress and remaining time estimates.
- Task status breakdown.

**Tip**: If you encounter CUDA out of memory errors, reduce your batch size, use a smaller model, or switch to a larger GPU. For more troubleshooting tips, see [GPU Memory Management](https://docs.ray.io/en/latest/data/working-with-llms.html#gpu-memory-management-and-cuda-oom-prevention).

## Scale up to larger datasets

Your Ray Data processing pipeline can easily scale up to process more data. By default, this section processes 1M rows.


```python
# The dataset has ~2M rows
# Configure how many images to process (default: 1M for demonstration).
print(f"Processing 1M rows... (or the whole dataset if you picked >2M)")
ds_large = ds.limit(1_000_000)

# As we increase our compute, we can increase the number of partitions for more parallelism
num_partitions_large = 256
print(f"Repartitioning dataset into {num_partitions_large} blocks for parallelism...")
ds_large = ds_large.repartition(num_blocks=num_partitions_large)
```

You can scale the number of concurrent workers based on the compute available in your cluster. In this case, each replica is a copy of your Llama model and fits in a single L4 GPU.


```python
processor_config_large = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len=256,  # Hard cap: system prompt + user prompt + output tokens must fit within this limit
    ),
    batch_size=256,
    accelerator_type="L4",  # Or upgrade to larger GPU
    concurrency=10,  # Deploy 10 workers across 10 GPUs to maximize throughput
)

# Build the LLM processor with the configuration and functions.
processor_large = build_llm_processor(
    processor_config_large,
    preprocess=preprocess,
    postprocess=postprocess,
)
```

Execute the new pipeline


```python
# Run the same processor on the larger dataset.
processed_large = processor_large(ds_large)
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_large.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
```

## Performance optimization tips

When scaling to larger datasets, consider these optimizations tips. For comprehensive guidance, see the [Ray Data performance guide](https://docs.ray.io/en/latest/data/performance-tips.html) and the [throughput optimization guide with Anyscale](https://docs.anyscale.com/llm/batch-inference/throughput-optimization).

**Analyze your pipeline**  
Use *stats()* to analyze each steps in your pipeline and identify any bottlenecks.
```python
processed = processor(ds).materialize()
print(processed.stats())
```
The outputs contains detailed description of each step in your pipeline.
```text
Operator 0 ...

...

Operator 8 MapBatches(vLLMEngineStageUDF): 3908 tasks executed, 3908 blocks produced in 340.21s
    * Remote wall time: ...
    ...

...

Dataset throughput:
	* Ray Data throughput: ...
	* Estimated single node throughput: ...
```

**Adjust concurrency**  
The `concurrency` parameter controls how many model replicas run in parallel. To determine the right value:
- *Available GPU count:* Start with the number of GPUs in your cluster. Each replica needs at least one GPU (more if using tensor parallelism).
- *Model memory footprint:* Ensure your model fits in GPU memory. For example, an 8&nbsp;B parameter model in FP16 requires ~16&nbsp;GB, fitting on a single L4 (2&nbsp;GB) or A10G (24&nbsp;GB).
- *CPU-bound preprocessing:* If preprocessing is slower than inference, adding more GPU replicas won't help. Check `stats()` output to identify if preprocessing is the bottleneck.

**Tune batch size**  
The `batch_size` parameter controls how many requests Ray Data sends to vLLM at once. vLLM uses continuous batching internally, controlled by `max_num_seqs` in `engine_kwargs`. This directly impacts GPU memory allocation since vLLM pre-allocates KV cache for up to `max_num_seqs` concurrent sequences.

- *Too small `batch_size`:* vLLM scheduler is under-saturated, risking GPU idle time.
- *Too large `batch_size`:* vLLM scheduler is over-saturated, causing overhead latency. Also increases retry cost on failure since the entire batch is retried.

You can try the following suggestions:
1. Start with `batch_size` equal to `max_num_seqs` in your vLLM engine parameters. See [vLLM engine arguments](https://docs.vllm.ai/en/stable/serving/engine_args.html) for defaults.
2. Monitor GPU utilization in the Ray Dashboard (see [Monitor the execution](#monitor-the-execution) section).
3. Adjust `max_num_seqs` in `engine_kwargs` to optimize GPU utilization, and re-adapt `batch_size` accordingly.

**Tune preprocessing and inference stage parallelism**  
Use `repartition()` to control parallelism during your preprocessing stage. On the other hand, the number of inference tasks is determined by `dataset_size / batch_size`, where `batch_size` controls how many rows are grouped for each vLLM engine call. Ensure you have enough tasks to keep all workers busy and enable efficient load balancing.

See [Configure parallelism for Ray Data LLM](https://docs.anyscale.com/llm/batch-inference/resource-allocation/concurrency-and-batching.md) for detailed guidance.

**Use quantization to reduce memory footprint**  
Quantization reduces model precision to save GPU memory and improve throughput; vLLM supports this with the `quantization` field in `engine_kwargs`. Note that lower precision may impact output quality, and not all models or GPUs support all quantization types, see [Quantization for LLM batch inference](https://docs.anyscale.com/llm/batch-inference/throughput-optimization/quantization.md) for more guidance.

**Fault tolerance and checkpointing**  
Ray Data automatically handles fault tolerance - if a worker fails, only that worker's current batch is retried. For long-running Anyscale Jobs, you can enable job-level checkpointing to resume from failures. See [Anyscale Runtime checkpointing documentation](https://docs.anyscale.com/runtime/data#enable-job-level-checkpointing) for more information.

**Scale to larger models with model parallelism**  
Model parallelism distributes large models across multiple GPUs when they don't fit on a single GPU. Use tensor parallelism to split model layers horizontally across multiple GPUs within a single node and use pipeline parallelism to split model layers vertically across multiple nodes, with each node processing different layers of the model.

Forward model parallelism parameters to your inference engine using the `engine_kwargs` argument of your `vLLMEngineProcessorConfig` object. If your GPUs span multiple nodes, set `ray` as the distributed executor backend to enable cross-node parallelism. This example snippet uses DeepSeek-R1, a large reasoning model requiring multiple GPUs over multiple nodes:

```python
processor_config = vLLMEngineProcessorConfig(
    model_source="deepseek-ai/DeepSeek-R1",
    accelerator_type="H100",
    engine_kwargs={
        "tensor_parallel_size": 8,  # 8 GPUs per node
        "pipeline_parallel_size": 2,  # Split across 2 nodes
        "distributed_executor_backend": "ray", # Required to enable cross-node parallelism

    },
    concurrency=1,
)
# Each worker uses: 8 GPUs × 2 nodes = 16 GPUs total
```

Each inference worker allocates GPUs based on `tensor_parallel_size × pipeline_parallel_size`. For detailed guidance on parallelism strategies, see the [vLLM parallelism and scaling documentation](https://docs.vllm.ai/en/stable/serving/distributed_serving.html).

## Summary

In this example, you built an end-to-end batch pipeline: loading a customer dataset from S3 into a Ray Dataset, configuring a vLLM processor for Llama 3.1 8&nbsp;B, and adding simple pre/post-processing to infer company industries. You validated the flow on 10,000 rows, scaled to 1M+ records, monitored progress in the Ray Dashboard, and saved the results to persistent storage.

See [Anyscale batch inference optimization](https://docs.anyscale.com/llm/batch-inference) for more information on using Ray Data with Anyscale and for more advanced use cases, see [Working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).
