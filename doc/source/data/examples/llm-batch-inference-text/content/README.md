<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify llm_batch_inference_text.ipynb instead, then regenerate this file with:
jupyter nbconvert "$nb_filename" --to markdown --output "README.md"
-->

# LLM batch inference with Ray Data

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/llm-batch-inference-text"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/data/examples/llm-batch-inference-text" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

**⏱️ Time to complete**: 15 minutes

This example shows you how to run batch inference for large language models (LLMs) using [Ray Data LLM APIs](https://docs.ray.io/en/latest/data/api/llm.html). In this use case, the batch inference job reformats dates across a large customer dataset.


## When to use LLM batch inference

Offline (batch) inference optimizes for throughput over latency. Unlike online inference, which processes requests one at a time in real-time, batch inference processes thousands or millions of inputs together, maximizing GPU utilization and reducing per-inference costs.

Choose batch inference when:
- You have a fixed dataset to process (such as daily reports or data migrations)
- Throughput matters more than immediate results
- You want to take advantage of fault tolerance and checkpointing for long-running jobs

On contrary, if you are more interested in optimizing for latency, consider [deploying your LLM with Ray Serve LLM for online inference](https://docs.ray.io/en/latest/serve/llm/index.html).

## Prepare a Ray Data dataset

Ray Data LLM runs batch inference for LLMs on Ray Data datasets. In this tutorial, you perform batch inference with an LLM to reformat dates and the source is a 2-million-row CSV file containing sample customer data.

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

For this initial example, limit the dataset to 10,000 rows for faster processing and testing. Later, you can scale up to process the full dataset.


```python
# Limit the dataset to 10,000 rows for this example.
print("Limiting dataset to 10,000 rows for initial processing.")
ds_small = ds.limit(10_000)

# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, a large remote file might be read into a single block. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
num_partitions = 64
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
- `concurrency`: Number of parallel workers (4 replicas).

**Note:** Because the input prompts and expected output token lengths are small, `batch_size=256` is appropriate. However, depending on your workload, a large batch size can lead to increased idle GPU time when decoding long sequences. Adjust this value to find the optimal trade-off between throughput and latency.


```python
from ray.data.llm import vLLMEngineProcessorConfig

processor_config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len= 256, # estimate system prompt + user prompt + output tokens (+ reasoning tokens if any)
        max_num_batched_tokens=65536, # so we can batch many rows together
        max_num_seqs=1024, # so we can batch many rows together
    ),
    batch_size=256,
    accelerator_type="L4",
    compute=4,
)
```

For more details on the configuration options you can pass to the vLLM engine, see the [vLLM Engine Arguments documentation](https://docs.vllm.ai/en/stable/configuration/engine_args.html).

### Define the preprocess and postprocess functions

The task is to format the `Subscription Date` field as `MM-DD-YYYY` using an LLM.

Define a preprocess function to prepare `messages` and `sampling_params` for the vLLM engine, and a postprocess function to extract the `generated_text`.


```python
from typing import Any

# Preprocess function prepares `messages` and `sampling_params` for vLLM engine.
# All other fields are ignored by the engine.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that reformats dates to MM-DD-YYYY."
                            "Be concise and output only the formatted date and nothing else."
                            "For example, if we ask to reformat 'Subscription Date': datetime.date(2020, 11, 29)' then your answer should only be '11-29-2020'"
            },
            {
                "role": "user",
                "content": f"Convert this date:\n{row['Subscription Date']}."
            },
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=32, # low max tokens because we are simply formatting a date
            detokenize=False,
        ),
    )

# Postprocess function extracts the generated text from the engine output.
# The **row syntax returns all original columns in the input dataset.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "formatted_date": row["generated_text"],
        **row,  # Include all original columns.
    }
```

### Build the processor

With the configuration and functions defined, build the processor.


```python
from ray.data.llm import build_llm_processor
from pprint import pprint

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
# Run the processor on the small dataset.
processed_small = processor(ds_small)

# Materialize the dataset to memory.
# You can also use writing APIs such as write_parquet() or write_csv() to persist the dataset.
processed_small = processed_small.materialize()

# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
```

## Launch to production with Anyscale Jobs

For production workloads, deploy your batch inference processor as an [Anyscale Job](https://docs.anyscale.com/platform/jobs). Anyscale takes care of the infrastructure layer and runs your jobs on your dedicated clusters with automatic retries, monitoring, and scheduling.

### Configure an Anyscale Job

Save your batch inference code as `batch_inference.py`, then create a job configuration file:

```yaml
# job.yaml
name: llm-batch-inference-text
entrypoint: python batch_inference_text.py
image_uri: anyscale/ray:2.49.0-py312-cu128
compute_config:
  head_node:
    instance_type: m5.2xlarge
  worker_nodes:
    - instance_type: g6.2xlarge
      min_nodes: 1
      max_nodes: 4
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
anyscale job status --name llm-batch-inference-text

# View logs
anyscale job logs --name llm-batch-inference-text
```

The Ray Dashboard remains available for detailed monitoring. To access it, go over your Anyscale Job in your console.  
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

Your Ray Data processing pipeline can easily scale up to process more data. By default, this section processes the full 2-million-row dataset.  

You can control the dataset size through the `LARGE_DATASET_LIMIT` environment variable.


```python
import os

# Configure how many images to process (default: 1M for demonstration).
dataset_limit = int(os.environ.get("LARGE_DATASET_LIMIT", 1_000_000))
print(f"Scaling dataset to: {dataset_limit:,} rows...")

# Apply the limit to the dataset.
ds_large = ds.limit(dataset_limit)

# Repartition for better parallelism.
num_partitions_large = 128
print(f"Repartitioning dataset into {num_partitions_large} blocks...")
ds_large = ds_large.repartition(num_blocks=num_partitions_large)

# Run the same processor on the larger dataset.
processed_large = processor(ds_large)
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} rows successfully.")
print("\nSample outputs:")
pprint(processed_large.take(3))
```

### Performance optimization tips

When scaling to larger datasets, consider these optimizations:

**Adjust concurrency**  
Increase the `concurrency` parameter to add more parallel workers.

**Tune batch size**  
Larger batch sizes improve throughput but increase memory usage.

**Repartition strategically**  
Use more partitions (blocks) than the number of workers to enable better load balancing.

**Enable checkpointing**  
For very large datasets, configure checkpointing to recover from failures.

```python
processed = processor(ds_large).materialize(
    checkpoint_path="s3://my-bucket/checkpoints/"
)
```

**Monitor GPU utilization**  
Use the Ray Dashboard to identify bottlenecks and adjust parameters.

For performance tuning, see the [Ray Data performance guide](https://docs.ray.io/en/latest/data/performance-tips.html). To configure your inference engine, see the [vLLM configuration options](https://docs.vllm.ai/en/latest/serving/engine_args.html).


## Save results

After processing, save the results to a persistent storage location such as S3 or local disk.


```python
# Save the processed dataset to Parquet format.
# Replace this path with your desired output location.
output_path_small = "local:///tmp/small_processed_customers"
output_path = "local:///tmp/processed_customers"

print(f"Saving small processed dataset to {output_path_small}...")
processed_small.write_parquet(output_path_small)
print("Saved successfully.")

print(f"Saving large processed dataset to {output_path}...")
processed_large.write_parquet(output_path)
print("Saved successfully.")

# Alternatively, save as CSV:
# processed_small.write_csv(output_path_small)
# processed_large.write_csv(output_path)
```

For more information, see [Saving Data](https://docs.ray.io/en/latest/data/saving-data.html)

## Summary

In this notebook, you built an end-to-end batch pipeline: loading a customer dataset from S3 into a Ray Dataset, configuring a vLLM processor for Llama 3.1 8 B, and adding simple pre/post-processing to normalize dates. You validated the flow on 10,000 rows, scaled to 2M+ records, monitored progress in the Ray Dashboard, and saved the results to persistent storage.

See [Anyscale batch inference optimization](https://docs.anyscale.com/llm/batch-inference) for more information on using Ray Data with Anyscale and for more advanced use cases, see [Working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).
