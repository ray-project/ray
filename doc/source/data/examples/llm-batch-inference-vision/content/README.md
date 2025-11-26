<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify llm_batch_inference_vision.ipynb instead, then regenerate this file with:
jupyter nbconvert "$nb_filename" --to markdown --output "README.md"
-->

# Vision-language model batch inference with Ray Data

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/llm-batch-inference-vision"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/data/examples/llm-batch-inference-vision" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

**⏱️ Time to complete**: 20 minutes

This example shows you how to run batch inference for vision-language models (VLMs) using [Ray Data LLM APIs](https://docs.ray.io/en/latest/data/api/llm.html). In this use case, the batch inference job generates captions for a large-scale image dataset.

## When to use LLM batch inference

Offline (batch) inference optimizes for throughput over latency. Unlike online inference, which processes requests one at a time in real-time, batch inference processes thousands or millions of inputs together, maximizing GPU utilization and reducing per-inference costs.

Choose batch inference when:
- You have a fixed dataset to process (such as daily reports or data migrations)
- Throughput matters more than immediate results
- You want to take advantage of fault tolerance and checkpointing for long-running jobs

On contrary, if you are more interested in optimizing for latency, consider [deploying your LLM with Ray Serve LLM for online inference](https://docs.ray.io/en/latest/serve/llm/index.html).


## Prepare a Ray Data dataset with images

Ray Data LLM runs batch inference for VLMs on Ray Data datasets containing images. In this tutorial, you perform batch inference with a vision-language model to generate image captions from the `BLIP3o/BLIP3o-Pretrain-Short-Caption` dataset, which contains approximately 5 million images.

First, load the data from a remote URL then repartition the dataset to ensure the workload can be distributed across multiple GPUs.


```python
%pip install datasets
```


```python
import ray
import datasets
from PIL import Image
from io import BytesIO

# Load the BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face with ~5M images.
print("Loading BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face...")
hf_dataset = datasets.load_dataset("BLIP3o/BLIP3o-Pretrain-Short-Caption", split="train", streaming=True)
hf_dataset = hf_dataset.select_columns(["jpg"])

ds = ray.data.from_huggingface(hf_dataset)
print("Dataset loaded successfully.")

sample = ds.take(2)
print("Sample data:")
for i, item in enumerate(sample):
    print(f"\nSample {i+1}:")
    image = Image.open(BytesIO(item['jpg']['bytes']))
    image.show()
```

For this initial example, limit the dataset to 10,000 rows so you can process and test faster. Later, you can scale up to the full dataset.

If you don't repartition, the system might read a large file into only a few blocks, which limits parallelism in later steps. For example, you might see that only 4 out of 8 GPUs in your cluster are being used. To address this, you can repartition the data into a specific number of blocks so the system can better parallelize work across all available GPUs in the pipeline.


```python
# Limit the dataset to 100,000 images for this example.
print("Limiting dataset to 10,000 images for initial processing.")
ds_small = ds.limit(10_000)

# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, streaming datasets might not be optimally partitioned. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
num_partitions = 64
print(f"Repartitioning dataset into {num_partitions} blocks for parallelism...")
ds_small = ds_small.repartition(num_blocks=num_partitions)
```

## Configure Ray Data LLM

Ray Data LLM provides a unified interface to run batch inference with different VLM engines. Configure the vLLM engine with a vision-language model, define preprocessing and postprocessing functions, and build the processor.

### Configure the processor engine

Configure the model and compute resources needed for inference using `vLLMEngineProcessorConfig` with vision support enabled.

This example uses the `Qwen/Qwen2.5-VL-3B-Instruct` model, a vision-language model. The configuration specifies:
- `model_source`: The Hugging Face model identifier.
- `engine_kwargs`: vLLM engine parameters such as memory settings and batching.
- `batch_size`: Number of requests to batch together (set to 16 for vision models).
- `accelerator_type`: GPU type to use (L4 in this case).
- `concurrency`: Number of parallel workers (4 in this case).
- `has_image`: Enable image input support.

**Note:** Vision models typically require smaller batch sizes than text-only models due to the additional memory needed for image processing. Adjust batch size based on your image resolution and GPU memory.



```python
from ray.data.llm import vLLMEngineProcessorConfig

processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        max_model_len=8192
    ),
    batch_size=16,
    accelerator_type="L4",
    concurrency=4,
    has_image=True,  # Enable image input.
)

```

For more details on the configuration options you can pass to the vLLM engine, see the [vLLM Engine Arguments documentation](https://docs.vllm.ai/en/stable/configuration/engine_args.html).

### Define the preprocess and postprocess functions

The task is to generate descriptive captions for images using a vision-language model.

Define a preprocess function to prepare `messages` with image content and `sampling_params` for the vLLM engine, and a postprocess function to extract the `generated_text`.



```python
from typing import Any
from PIL import Image
from io import BytesIO

# Preprocess function prepares messages with image content for the VLM.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    # Convert bytes image to PIL 
    image = row['jpg']['bytes']
    image = Image.open(BytesIO(image))
    # Resize for consistency + predictable vision-token budget
    image = image.resize((225, 225), Image.Resampling.BICUBIC)
    
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that generates accurate and descriptive captions for images."
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Describe this image in detail. Focus on the main subjects, actions, and setting."
                    },
                    {
                        "type": "image",
                        "image": image  # Ray Data accepts PIL Image or image URL.
                    }
                ]
            },
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=256,
            detokenize=False,
        ),
    )

# Postprocess function extracts the generated caption.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_caption": row["generated_text"],
        # Note: Don't include **row here to avoid returning the large image data.
        # Include only the fields you need in the output.
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
# You can also use writing APIs such as write_parquet() or write_json() to persist the dataset.
processed_small = processed_small.materialize()

# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED CAPTIONS===============\n")
pprint(sampled)
```

## Launch to production with Anyscale Jobs

For production workloads, deploy your batch inference processor as an [Anyscale Job](https://docs.anyscale.com/platform/jobs). Anyscale takes care of the infrastructure layer and runs your jobs on your dedicated clusters with automatic retries, monitoring, and scheduling.

### Configure an Anyscale Job

Save your batch inference code as `batch_vision_inference.py`, then create a job configuration file:

```yaml
# job.yaml
name: my-llm-batch-inference-vision
entrypoint: python batch_inference_vision.py
image_uri: anyscale/ray-llm:2.51.1-py311-cu128
compute_config:
  head_node:
    instance_type: m5.2xlarge
  worker_nodes:
    - instance_type: g6.2xlarge
      min_nodes: 0
      max_nodes: 10
requirements: # Python dependencies - can be list or path to requirements.txt
  - datasets==4.4.1
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
# Check job status.
anyscale job status --name my-llm-batch-inference-vision

# View logs.
anyscale job logs --name my-llm-batch-inference-vision
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

Your Ray Data processing pipeline can easily scale up to process more images. By default, this section processes 1M images.


```python
import os

# The BLIP3o/BLIP3o-Pretrain-Short-Caption dataset has ~5M images
# Configure how many images to process (default: 1M for demonstration).
print(f"Processing 1M images... (or the whole dataset if you picked >5M)")
ds_large = ds.limit(1_000_000)
```

You can scale the number of concurrent replicas based on the compute available in your cluster. In this case, each replica is a copy of your Qwen-VL model and fits in a single L4 GPU.


```python
processor_config_large = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        max_model_len=8192,
    ),
    batch_size=16,
    accelerator_type="L4", # Or upgrade to larger GPU
    concurrency=10, # Increase the number of parallel workers
    has_image=True,  # Enable image input
)
processor_large = build_llm_processor(
    processor_config_large,
    preprocess=preprocess,
    postprocess=postprocess,
)
```

Execute the new pipeline


```python
# Run the compute-scaled processor on the larger dataset.
processed_large = processor_large(ds_large)
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} images successfully.")
print("\nSample outputs:")
pprint(processed_large.take(3))
```

## Performance optimization tips

When scaling to larger datasets, consider these optimizations:

**Analyze your pipeline**
You can use *stats()* to examine the throughput and timing at every step in your pipeline and spot potential bottlenecks.
The *stats()* output reports how long each operator took and its throughput, so you can compare these values to expected throughput for your hardware. If you see a step with significantly lower throughput or much higher task durations than others, that's likely a bottleneck.
The following example shows how to print pipeline stats:
```python
processed = processor(ds).materialize()
print(processed.stats())
```
The outputs include detailed timing, throughput, and resource utilization for each pipeline operator.
For example:
```text
Operator 0 ...

...

Operator 8 MapBatches(vLLMEngineStageUDF): 3908 tasks executed, 3908 blocks produced in 340.21s
    * Remote wall time: 340.21s 
    * Input/output rows: ...
    * Throughput: 2,900 rows/s
    ...

...

Dataset throughput:
    * Ray Data throughput: 2,500 rows/s
    * Estimated single node throughput: 5,000 rows/s
```

Review the per-operator throughput numbers and durations to spot slowest stages or unexpected bottlenecks. You can then adjust batch size, concurrency, or optimize resource usage for affected steps.

**Adjust concurrency**  
Increase the `concurrency` parameter to add more parallel workers and GPUs.

**Tune batch size**  
For vision models, smaller batch sizes (8-32) often work better due to memory constraints from image processing.

**Optimize image loading**  
Pre-resize images to a consistent size to reduce memory usage and improve throughput.

**Tune preprocessing and inference stage parallelism**  
Use `repartition()` to control parallelism during your preprocessing stage. On the other hand, the number of inference tasks is determined by `dataset_size / batch_size`, where `batch_size` controls how many rows are grouped for each vLLM engine call. Ensure you have enough tasks to keep all workers busy and enable efficient load balancing.

**Use quantization to reduce memory footprint**  
Quantization reduces model precision to save GPU memory and improve throughput. vLLM supports multiple quantization formats through the `quantization` parameter in `engine_kwargs`. A common option is FP8 (8-bit floating point), which can reduce memory usage by 2-4x with minimal accuracy loss.

**Scale to larger models with model parallelism**  
Model parallelism distributes large models across multiple GPUs when they don't fit on a single GPU. Use tensor parallelism to split model layers horizontally across multiple GPUs within a single node and use pipeline parallelism to split model layers vertically across multiple nodes, with each node processing different layers of the model.

Forward model parallelism parameters to your inference engine using the `engine_kwargs` argument of your `vLLMEngineProcessorConfig` object. If your GPUs span multiple nodes, set `ray` as the distributed executor backend to enable cross-node parallelism:

```python
processor_config = vLLMEngineProcessorConfig(
    model_source="meta-llama/Llama-3.2-90B-Vision-Instruct",
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

In this notebook, you built an end-to-end vision batch inference pipeline: loading an HuggingFace image dataset into Ray Dataset, configuring a vLLM processor for the Qwen2.5-VL vision-language model, and adding pre/post-processing to generate image captions. You validated the flow on 100,000 images, scaled to 100k images, monitored progress in the Ray Dashboard, and saved the results to persistent storage.

See [Anyscale batch inference optimization](https://docs.anyscale.com/llm/batch-inference) for more information on using Ray Data with Anyscale and for more advanced use cases, see [Working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).

