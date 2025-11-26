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

By default, a large remote file might be read into few blocks, limiting parallelism in the next steps. Instead, you can repartition the data into a specified number of blocks to ensure good enough parallelization in rest of the pipeline.


```python
# Limit the dataset to 10,000 rows for this example.
print("Limiting dataset to 10,000 rows for initial processing.")
ds_small = ds.limit(10_000)


# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, streaming datasets might not be optimally partitioned. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
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
        max_model_len= 256, # estimate system prompt + user prompt + output tokens (+ reasoning tokens if any)
    ),
    batch_size=256,
    accelerator_type="L4",
    concurrency=4,
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

Your Ray Data processing pipeline can easily scale up to process more data. By default, this section processes 1M rows.


```python
import os

# The dataset has ~2M rows
# Configure how many images to process (default: 1M for demonstration).
print(f"Processing 1M rows... (or the whole dataset if you picked >2M)")
ds_large = ds.limit(1_000_000)
```

You can scale the number of concurrent workers based on the compute available in your cluster. In this case, each replica is a copy of your Llama model and fits in a single L4 GPU.


```python
processor_config_large = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len= 256, # estimate system prompt + user prompt + output tokens (+ reasoning tokens if any)
    ),
    batch_size=256,
    accelerator_type="L4", # Or upgrade to larger GPU
    concurrency=10, # Deploy 10 workers across 10 GPUs to maximize throughput
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
print("\nSample outputs:")
pprint(processed_large.take(3))
```

## Performance optimization tips

When scaling to larger datasets, consider these optimizations tips:

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
Increase the `concurrency` parameter to add more parallel workers.

**Tune batch size**  
Larger batch sizes may improve throughput but increase memory usage.

**Tune preprocessing and inference stage parallelism**  
Use `repartition()` to control parallelism during your preprocessing stage. On the other hand, the number of inference tasks is determined by `dataset_size / batch_size`, where `batch_size` controls how many rows are grouped for each vLLM engine call. Ensure you have enough tasks to keep all workers busy and enable efficient load balancing.

**Use quantization to reduce memory footprint**  
Quantization reduces model precision to save GPU memory and improve throughput. vLLM supports multiple quantization formats through the `quantization` parameter in `engine_kwargs`. Common options include FP8 (8-bit floating point) and INT4 (4-bit integer), which can reduce memory usage by 2-4x with minimal accuracy loss. For example:

```python
processor_config = vLLMEngineProcessorConfig(
    model_source="meta-llama/Llama-3.1-70B-Instruct",
    engine_kwargs={
        "quantization": "fp8",  # Or "awq", "gptq", etc.
        "max_model_len": 8192,
    },
    batch_size=128,
    accelerator_type="L4",
    concurrency=4,
)
```

**Scale to larger models with model parallelism**  
Model parallelism distributes large models across multiple GPUs when they don't fit on a single GPU. Use tensor parallelism to split model layers horizontally across multiple GPUs within a single node and use pipeline parallelism to split model layers vertically across multiple nodes, with each node processing different layers of the model.

Forward model parallelism parameters to your inference engine using the `engine_kwargs` argument of your `vLLMEngineProcessorConfig` object. If your GPUs span multiple nodes, set `ray` as the distributed executor backend to enable cross-node parallelism:

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

**Monitor GPU utilization**  
Use the Ray Dashboard to identify bottlenecks and adjust parameters.

For performance tuning, see the [Ray Data performance guide](https://docs.ray.io/en/latest/data/performance-tips.html) or the [throughput optimization guide with Anyscale](https://docs.anyscale.com/llm/batch-inference/throughput-optimization). For all available engine parameters, see the [vLLM Engine Arguments documentation](https://docs.vllm.ai/en/stable/serving/engine_args.html).


## Summary

In this notebook, you built an end-to-end batch pipeline: loading a customer dataset from S3 into a Ray Dataset, configuring a vLLM processor for Llama 3.1 8 B, and adding simple pre/post-processing to normalize dates. You validated the flow on 10,000 rows, scaled to 1M+ records, monitored progress in the Ray Dashboard, and saved the results to persistent storage.

See [Anyscale batch inference optimization](https://docs.anyscale.com/llm/batch-inference) for more information on using Ray Data with Anyscale and for more advanced use cases, see [Working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).
